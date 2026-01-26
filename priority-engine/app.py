import json
import os
import signal
import time
from datetime import datetime
from typing import Dict, Literal, Tuple, Optional

from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, Field

from patient_metadata import get_metadata  # patient context -> scoring tweaks


# input topic model (from alarm-processor)

DataQuality = Literal["clinical", "artifact"]
PriorityLevel = Literal["low", "medium", "high"]

AlarmType = Literal[
    "vitals",
    "ventilator",
    "pump",
    "medication",
    "bed_exit",
    "bed_movement",
    "call_unanswered",
    "pain",
]


class EnrichedAlarmEvent(BaseModel):
    patient_id: str
    timestamp: str          # when processor emitted alarm
    vital_timestamp: str    # original measurement time
    severity: Literal["warning", "critical"]

    alarm_type: AlarmType
    message: str

    # optional -> older processor messages still work
    data_quality: Optional[DataQuality] = None

    # keep ranges aligned with generator/validator/storage
    heart_rate: int = Field(ge=0, le=250)
    systolic_bp: int = Field(ge=0, le=260)  # artifact can be 0..30 etc.
    spo2: int = Field(ge=0, le=100)


# output topic model (to alarm-storage)

class PrioritizedAlarmEvent(BaseModel):
    patient_id: str
    timestamp: str          # computed "now"
    vital_timestamp: str

    severity: Literal["warning", "critical"]
    alarm_type: AlarmType

    message: str
    data_quality: DataQuality = "clinical"  # default -> safe fallback

    heart_rate: int
    systolic_bp: int
    spo2: int

    ward: str
    bed: str
    risk_level: Literal["low", "medium", "high"]

    fall_risk: int = Field(ge=0, le=10)

    priority_score: int
    priority_level: PriorityLevel


# kafka config -> env first, defaults for docker-compose

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
topic_input = os.getenv("KAFKA_TOPIC_INPUT", "enriched_alarms")
topic_output = os.getenv("KAFKA_TOPIC_OUTPUT", "prioritized_alarms")


# alarm-fatigue bits (dedup / throttle)

DEDUP_WINDOWS_SEC = {
    "high": 30,
    "medium": 60,
    "low": 120,
}

# key includes dq -> artifact shouldn't "overwrite silence" clinical
# (patient_id, alarm_type, data_quality) -> last send ts
_last_sent: Dict[Tuple[str, str, str], float] = {}

# same key -> last level (for escalation bypass)
_last_level: Dict[Tuple[str, str, str], str] = {}


def _level_rank(level: str) -> int:
    # tiny helper -> compare levels without if/else jungle
    return {"low": 1, "medium": 2, "high": 3}.get(level, 2)


def should_suppress(patient_id: str, alarm_type: str, data_quality: str, priority_level: str) -> bool:
    """
    throttling per (patient, type, dq)
    - cooldown depends on current level
    - escalation bypass -> if level jumps up, let it through
    """
    now = time.time()
    key = (patient_id, alarm_type, data_quality)

    last_level = _last_level.get(key)
    if last_level is not None and _level_rank(priority_level) > _level_rank(last_level):
        _last_level[key] = priority_level
        _last_sent[key] = now
        return False  # "got worse" -> don't hide it

    cooldown = DEDUP_WINDOWS_SEC.get(priority_level, 60)
    last_ts = _last_sent.get(key)
    if last_ts is not None and (now - last_ts) < cooldown:
        return True  # same-ish alarm too soon

    _last_sent[key] = now
    _last_level[key] = priority_level
    return False


# helpers

def _infer_data_quality(enriched: EnrichedAlarmEvent) -> DataQuality:
    # backwards compat -> if dq missing, guess from message wording
    if enriched.data_quality in ("clinical", "artifact"):
        return enriched.data_quality

    msg = (enriched.message or "").lower()

    # match the phrases used in alarm-processor
    artifact_markers = [
        "spo2 sensor dropout suspected",
        "hr artifact suspected",
        "bp measurement error suspected",
        "measurement error suspected",
        "sensor dropout suspected",
        "artifact suspected",
        "measurement error",
        "dropout suspected",
    ]
    if any(m in msg for m in artifact_markers):
        return "artifact"

    return "clinical"


def _meta_defaults(meta: dict) -> dict:
    # "never crash on missing metadata" -> portfolio-friendly robustness
    m = dict(meta or {})

    # persisted fields
    m.setdefault("risk_level", "medium")
    m.setdefault("ward", "UNKNOWN")
    m.setdefault("bed", "UNKNOWN")

    # scoring knobs
    m.setdefault("fall_risk", 0)
    m.setdefault("cardiac_risk", "low")
    m.setdefault("resp_risk", "low")
    m.setdefault("post_op", False)
    m.setdefault("mobility", "independent")
    m.setdefault("sedation_rass", 0)

    # light cleanup -> ints + clamp
    try:
        m["fall_risk"] = int(m["fall_risk"])
    except Exception:
        m["fall_risk"] = 0

    try:
        m["sedation_rass"] = int(m["sedation_rass"])
    except Exception:
        m["sedation_rass"] = 0

    m["fall_risk"] = max(0, min(10, int(m["fall_risk"])))

    return m


# scoring (context-aware)

def compute_priority(
    enriched: EnrichedAlarmEvent,
    meta: dict,
) -> tuple[int, PriorityLevel]:

    # local vars -> easier to read later
    hr = enriched.heart_rate
    bp = enriched.systolic_bp
    spo2 = enriched.spo2

    risk_level = meta.get("risk_level", "medium")
    fall_risk = int(meta.get("fall_risk", 0))
    cardiac_risk = meta.get("cardiac_risk", "low")
    resp_risk = meta.get("resp_risk", "low")
    post_op = bool(meta.get("post_op", False))
    mobility = meta.get("mobility", "independent")
    sedation = int(meta.get("sedation_rass", 0))

    dq = _infer_data_quality(enriched)

    # base score -> "severity first", but artifacts downgraded on purpose
    if dq == "artifact":
        base = 35 if enriched.severity == "critical" else 20  # don't scream "ICU emergency" for sensor glitches
    else:
        base = 70 if enriched.severity == "critical" else 40

    # patient baseline risk -> adds context, but not too much for artifacts
    risk_bonus = {"high": 30, "medium": 15, "low": 0}.get(risk_level, 15)
    if dq == "artifact":
        risk_bonus = min(risk_bonus, 10)

    score = base + risk_bonus

    # vitals tuning -> only for clinical (artifact already handled upstream)
    if dq == "clinical":
        if spo2 < 85:
            score += 25
        elif spo2 < 90:
            score += 15
        elif spo2 < 93:
            score += 5

        if hr > 150:
            score += 25
        elif hr > 130:
            score += 15
        elif hr < 40:
            score += 25
        elif hr < 50:
            score += 10

        if bp < 90:
            score += 20
        elif bp > 180:
            score += 10

        # extra context -> only if risk flags + vitals match
        if cardiac_risk == "high" and (hr > 130 or hr < 50 or bp < 90 or bp > 180):
            score += 15

        if resp_risk == "high" and spo2 < 90:
            score += 15

        if post_op and (spo2 < 90 or bp < 90):
            score += 10

    # fall risk -> only where it makes sense (bed alarms)
    if enriched.alarm_type == "bed_exit":
        score += 15  # bed exit baseline -> always relevant

        if fall_risk >= 7:
            score += 25
        elif fall_risk >= 4:
            score += 12
        else:
            score += 5

        if sedation <= -2:
            score += 10  # sedated + out of bed -> yikes

        if mobility in ("assisted", "bedbound"):
            score += 8

    elif enriched.alarm_type == "bed_movement":
        if fall_risk >= 7:
            score += 6
        elif fall_risk >= 4:
            score += 3

    # small tweak -> deep sedation + unanswered call = higher concern
    if enriched.alarm_type == "call_unanswered" and sedation <= -3:
        score += 8

    # map score -> simple buckets (readable in UI)
    if score >= 130:
        level: PriorityLevel = "high"
    elif score >= 80:
        level = "medium"
    else:
        level = "low"

    # hard rule -> artifact never becomes "high"
    if dq == "artifact" and level == "high":
        level = "medium"
        score = min(score, 120)  # still ordered, just capped

    return int(score), level


# kafka consumer/producer

def create_consumer() -> KafkaConsumer:
    # no retry loop here -> compose healthchecks handle most of it
    print(f"🔌 [Priority] Connecting consumer to {bootstrap_servers} ...")
    consumer = KafkaConsumer(
        topic_input,
        bootstrap_servers=[bootstrap_servers],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",  # keep it "live", don't replay on restart
        enable_auto_commit=True,
        group_id=os.getenv("KAFKA_GROUP_ID", "priority-engine"),
        consumer_timeout_ms=1000,    # so shutdown signals are noticed
    )
    print("✅ [Priority] Consumer connected.")
    return consumer


def create_producer() -> KafkaProducer:
    print(f"🔌 [Priority] Connecting producer to {bootstrap_servers} ...")
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=int(os.getenv("KAFKA_LINGER_MS", "10")),  # batching a bit -> fewer tiny packets
    )
    print("✅ [Priority] Producer connected.")
    return producer


def main():
    # crude wait -> avoids "first boot race" during demos
    print("⏳ [Priority] Waiting 20 seconds for upstream services to be ready ...")
    time.sleep(20)

    consumer = create_consumer()
    producer = create_producer()

    print(f"🚀 [Priority] Running, reading from '{topic_input}' and writing to '{topic_output}'")

    flush_every = int(os.getenv("KAFKA_FLUSH_EVERY", "20"))  # flush in batches -> speed + still near-real-time
    counter = 0

    running = True

    def _shutdown(*_args):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while running:
            for msg in consumer:
                if not running:
                    break

                data = msg.value

                try:
                    enriched = EnrichedAlarmEvent(**data)
                except Exception as e:
                    # bad input -> skip, keep service running
                    print("❌ [Priority] Invalid enriched alarm, skipping:", data, "| Error:", e)
                    continue

                # metadata lookup -> then "harden" with defaults
                meta = _meta_defaults(get_metadata(enriched.patient_id))
                dq = _infer_data_quality(enriched)

                score, level = compute_priority(enriched=enriched, meta=meta)

                prioritized = PrioritizedAlarmEvent(
                    patient_id=enriched.patient_id,
                    timestamp=datetime.utcnow().isoformat() + "Z",  # keep UTC string same as rest of pipeline
                    vital_timestamp=enriched.vital_timestamp,
                    severity=enriched.severity,
                    alarm_type=enriched.alarm_type,
                    message=enriched.message,
                    data_quality=dq,
                    heart_rate=enriched.heart_rate,
                    systolic_bp=enriched.systolic_bp,
                    spo2=enriched.spo2,
                    ward=meta["ward"],
                    bed=meta["bed"],
                    risk_level=meta["risk_level"],
                    fall_risk=int(meta["fall_risk"]),
                    priority_score=int(score),
                    priority_level=level,
                )

                # throttle/dedup -> keeps UI calmer
                if should_suppress(
                    prioritized.patient_id,
                    prioritized.alarm_type,
                    prioritized.data_quality,
                    prioritized.priority_level,
                ):
                    print(
                        f"⏸️ [Priority] Suppressed duplicate: {prioritized.patient_id} "
                        f"type={prioritized.alarm_type}, dq={prioritized.data_quality}, level={prioritized.priority_level}"
                    )
                    continue

                # pydantic v1/v2 compatibility
                if hasattr(prioritized, "model_dump"):
                    payload = prioritized.model_dump()
                else:
                    payload = prioritized.dict()

                producer.send(topic_output, payload)
                counter += 1

                if counter % flush_every == 0:
                    producer.flush()

                print(
                    f"⭐ PRIORITIZED {prioritized.patient_id}: "
                    f"type={prioritized.alarm_type}, dq={prioritized.data_quality}, "
                    f"fall_risk={prioritized.fall_risk}, severity={prioritized.severity}, "
                    f"risk={prioritized.risk_level}, score={prioritized.priority_score}, "
                    f"level={prioritized.priority_level}, ward={prioritized.ward}, bed={prioritized.bed}"
                )

    finally:
        # graceful close -> avoid losing last buffered messages
        try:
            producer.flush()
            producer.close()
            print("🧹 [Priority] Producer flushed and closed.")
        except Exception:
            pass

        try:
            consumer.close()
            print("🧹 [Priority] Consumer closed.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
