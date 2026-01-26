import json
import os
import signal
import time
from datetime import datetime
from typing import Literal, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel, Field


# env-driven config -> same code works local/docker
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")

# topic names -> keep old env names as fallback (less breaking when renaming)
topic_input = os.getenv("KAFKA_TOPIC_INPUT") or os.getenv("KAFKA_TOPIC_RAW", "raw_alarms")
topic_output = os.getenv("KAFKA_TOPIC_ALERTS") or os.getenv("KAFKA_TOPIC_ENRICHED", "enriched_alarms")


# shared literals -> avoids typos across services
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

# keep it binary for demo -> clinical vs artifact (alarm fatigue angle)
DataQuality = Literal["clinical", "artifact"]


# raw stream schema -> matches generator output
class RawAlarmEvent(BaseModel):
    patient_id: str
    timestamp: str
    source: str
    device_type: Literal[
        "bedside_monitor",
        "ventilator",
        "infusion_pump",
        "medication",
        "bed",
        "call_bell",
        "pain_assessment",
    ]

    # allow "weird" but valid ranges -> artifact cases should still pass schema
    heart_rate: Optional[int] = Field(default=None, ge=0, le=250)
    systolic_bp: Optional[int] = Field(default=None, ge=0, le=260)
    spo2: Optional[int] = Field(default=None, ge=0, le=100)

    # ventilator
    airway_pressure: Optional[float] = None
    tidal_volume: Optional[float] = None

    # pump
    pump_state: Optional[
        Literal["normal", "occlusion", "empty", "battery_low", "failure"]
    ] = None

    # medication
    medication_status: Optional[Literal["none", "due", "overdue"]] = None

    # bed
    bed_event: Optional[Literal["none", "exit", "movement"]] = None

    # call bell
    call_bell_state: Optional[Literal["none", "pressed", "unanswered"]] = None
    call_bell_duration_sec: Optional[int] = None

    # pain
    pain_score: Optional[int] = Field(default=None, ge=0, le=10)
    observation_needed: Optional[bool] = None


# enriched alarm -> downstream scoring + storage + dashboard
class EnrichedAlarmEvent(BaseModel):
    patient_id: str
    timestamp: str          # processor time (when we raised the alarm)
    vital_timestamp: str    # original measurement time (event time)

    severity: Literal["warning", "critical"]
    alarm_type: AlarmType
    message: str

    # key decision -> artifacts travel end-to-end (so they can be treated differently later)
    data_quality: DataQuality = "clinical"

    heart_rate: int
    systolic_bp: int
    spo2: int


def now_iso() -> str:
    # UTC + Z -> consistent across services (no local timezone surprises)
    return datetime.utcnow().isoformat() + "Z"


def create_consumer() -> KafkaConsumer:
    # retry loop -> Kafka might be "up" later than this container
    while True:
        try:
            print(f"🔌 [Processor] Connecting consumer to {bootstrap_servers} ...")
            consumer = KafkaConsumer(
                topic_input,
                bootstrap_servers=[bootstrap_servers],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",     # restart -> continue with new data (demo-friendly)
                enable_auto_commit=True,
                group_id=os.getenv("KAFKA_GROUP_ID", "alarm-processor"),  # stable group -> stable offsets
                consumer_timeout_ms=1000,        # lets us exit cleanly (loop wakes up)
            )
            print("✅ [Processor] Consumer connected.")
            return consumer
        except NoBrokersAvailable:
            print("⚠️ [Processor] Kafka not available yet, retrying in 5s ...")
            time.sleep(5)


def create_producer() -> KafkaProducer:
    # same retry idea -> avoids flaky startup races
    while True:
        try:
            print(f"🔌 [Processor] Connecting producer to {bootstrap_servers} ...")
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # JSON -> bytes
                linger_ms=int(os.getenv("KAFKA_LINGER_MS", "10")),         # small batching -> less overhead
            )
            print("✅ [Processor] Producer connected.")
            return producer
        except NoBrokersAvailable:
            print("⚠️ [Processor] Kafka not available yet, retrying in 5s ...")
            time.sleep(5)


def derive_alarm(raw: RawAlarmEvent) -> Optional[EnrichedAlarmEvent]:
    # fallback vitals -> non-monitor events still become "complete" alarms in DB/UI
    # (so storage schema stays consistent)
    hr = raw.heart_rate if raw.heart_rate is not None else 80
    bp = raw.systolic_bp if raw.systolic_bp is not None else 120
    spo2 = raw.spo2 if raw.spo2 is not None else 98

    severity: Optional[Literal["warning", "critical"]] = None
    message: str = ""
    alarm_type: Optional[AlarmType] = None
    data_quality: DataQuality = "clinical"

    if raw.device_type == "bedside_monitor":
        alarm_type = "vitals"

        # artifacts first -> don't treat sensor glitches like clinical emergencies
        if spo2 <= 30:
            severity = "warning"
            message = f"SpO2 sensor dropout suspected: SpO2={spo2}%"
            data_quality = "artifact"
        elif hr >= 200:
            severity = "warning"
            message = f"HR artifact suspected: HR={hr} bpm"
            data_quality = "artifact"
        elif bp <= 30 or bp >= 240:
            severity = "warning"
            message = f"BP measurement error suspected: BP={bp} mmHg"
            data_quality = "artifact"

        # artifact shortcut -> forward immediately (keeps the "fatigue" story visible)
        if data_quality == "artifact":
            return EnrichedAlarmEvent(
                patient_id=raw.patient_id,
                timestamp=now_iso(),
                vital_timestamp=raw.timestamp,
                severity="warning",
                alarm_type=alarm_type,
                message=message or "Measurement artifact suspected",
                data_quality=data_quality,
                heart_rate=hr,
                systolic_bp=bp,
                spo2=spo2,
            )

        # clinical thresholds -> simple rules, readable, demo-focused
        if spo2 < 85:
            severity = "critical"
            message = f"Severe desaturation: SpO2={spo2}%"
        elif spo2 < 90:
            severity = "warning"
            message = f"Desaturation: SpO2={spo2}%"

        # HR logic -> message only set if nothing else already explained it
        if hr == 0:
            severity = "critical"
            message = message or f"Cardiac arrest suspected: HR={hr} bpm"
        elif hr > 150:
            severity = "critical"
            message = message or f"Severe tachycardia: HR={hr} bpm"
        elif hr > 130:
            severity = severity or "warning"
            message = message or f"Tachycardia: HR={hr} bpm"
        elif hr < 40:
            severity = "critical"
            message = message or f"Severe bradycardia: HR={hr} bpm"
        elif hr < 50:
            severity = severity or "warning"
            message = message or f"Bradycardia: HR={hr} bpm"

        # BP -> can upgrade severity (hypotension is serious)
        if bp < 90:
            severity = "critical"
            message = message or f"Hypotension: BP={bp} mmHg"
        elif bp > 180:
            severity = severity or "warning"
            message = message or f"Hypertension: BP={bp} mmHg"

    elif raw.device_type == "ventilator":
        alarm_type = "ventilator"
        ap = raw.airway_pressure
        vt = raw.tidal_volume

        # order matters -> disconnection first, then pressure, then low volume
        if vt is not None and vt == 0:
            severity = "critical"
            message = "Ventilator disconnection suspected (VT=0)"
        if severity is None and ap is not None and ap > 30:
            severity = "critical"
            message = f"High airway pressure: {ap:.1f} cmH2O"
        if severity is None and vt is not None and vt < 250:
            severity = "warning"
            message = f"Low tidal volume: {vt:.1f} mL"

    elif raw.device_type == "infusion_pump":
        alarm_type = "pump"
        state = raw.pump_state or "normal"
        # treat "failure/occlusion" as critical -> obvious ICU scenario
        if state in ("occlusion", "failure"):
            severity = "critical"
            message = f"Pump alarm: {state}"
        elif state in ("empty", "battery_low"):
            severity = "warning"
            message = f"Pump status: {state}"

    elif raw.device_type == "medication":
        alarm_type = "medication"
        status = raw.medication_status or "none"
        # overdue -> critical (demo assumption)
        if status == "overdue":
            severity = "critical"
            message = "Medication overdue"
        elif status == "due":
            severity = "warning"
            message = "Medication due"

    elif raw.device_type == "bed":
        # mapping bed events -> two alarm types (exit vs movement)
        event = raw.bed_event or "none"
        if event == "exit":
            severity = "critical"
            message = "Bed exit detected"
            alarm_type = "bed_exit"
        elif event == "movement":
            severity = "warning"
            message = "Restlessness / movement detected"
            alarm_type = "bed_movement"

    elif raw.device_type == "call_bell":
        # duration-based escalation -> gives the stream a "time referenced" feel
        state = raw.call_bell_state or "none"
        duration = raw.call_bell_duration_sec or 0
        if state == "unanswered" and duration >= 240:
            severity = "critical"
            message = f"Call bell unanswered > 4 min ({duration}s)"
            alarm_type = "call_unanswered"
        elif state == "unanswered" and duration > 120:
            severity = "warning"
            message = f"Call bell unanswered > 2 min ({duration}s)"
            alarm_type = "call_unanswered"

    elif raw.device_type == "pain_assessment":
        alarm_type = "pain"
        pain = raw.pain_score
        # simple thresholds -> keeps output readable
        if pain is not None:
            if pain >= 9:
                severity = "critical"
                message = f"Severe pain: {pain}/10"
            elif pain >= 7:
                severity = "warning"
                message = f"High pain: {pain}/10"

    # no alarm -> drop it (stream stays busy, but storage only gets alarms)
    if severity is None or alarm_type is None:
        return None

    # last safety net -> message should never be blank
    if not message:
        message = f"{alarm_type} alarm triggered"

    return EnrichedAlarmEvent(
        patient_id=raw.patient_id,
        timestamp=now_iso(),
        vital_timestamp=raw.timestamp,
        severity=severity,
        alarm_type=alarm_type,
        message=message,
        data_quality=data_quality,
        heart_rate=hr,
        systolic_bp=bp,
        spo2=spo2,
    )


def main():
    # ctrl+c friendly stop (docker stop too)
    running = True

    def _shutdown(*_args):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # tiny delay -> avoids noisy start errors when everything spins up together
    print("⏳ [Processor] Waiting 10 seconds for Kafka + generator ...")
    time.sleep(10)

    consumer = create_consumer()
    producer = create_producer()

    print(
        f"[Processor] Running, consuming from '{topic_input}', "
        f"producing enriched alarms to '{topic_output}'"
    )

    # don't flush every message -> batching
    flush_every = int(os.getenv("KAFKA_FLUSH_EVERY", "20"))
    counter = 0

    try:
        while running:
            # consumer_timeout_ms -> this loop wakes up even when topic is quiet
            for msg in consumer:
                if not running:
                    break

                data = msg.value
                try:
                    raw = RawAlarmEvent(**data)
                except Exception as e:
                    # schema mismatch -> skip (keep pipeline alive)
                    print("❌ [Processor] Invalid raw event, skipping:", data, "| Error:", e)
                    continue

                enriched = derive_alarm(raw)
                if enriched is None:
                    # not an alarm -> intentional drop
                    print(
                        f"ℹ️ [Processor] No alarm for {raw.patient_id} "
                        f"({raw.device_type}) – values within normal range."
                    )
                    continue

                # pydantic v2 vs v1 compatibility
                if hasattr(enriched, "model_dump"):
                    payload = enriched.model_dump()
                else:
                    payload = enriched.dict()

                producer.send(topic_output, payload)
                counter += 1

                if counter % flush_every == 0:
                    producer.flush()

                print(
                    f"📦 [Processor] Enriched alarm for {enriched.patient_id}: "
                    f"type={enriched.alarm_type}, severity={enriched.severity}, "
                    f"quality={enriched.data_quality}, message='{enriched.message}', "
                    f"HR={enriched.heart_rate}, BP={enriched.systolic_bp}, SpO2={enriched.spo2}"
                )

    finally:
        # cleanup -> avoid "hanging" sockets on stop/restart
        try:
            producer.flush()
            producer.close()
            print("🧹 [Processor] Producer flushed and closed.")
        except Exception:
            pass

        try:
            consumer.close()
            print("🧹 [Processor] Consumer closed.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
