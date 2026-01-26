import json
import os
import random
import time
from datetime import datetime
from typing import Literal, Optional

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel, Field


# env -> same image, different configs (local vs compose)
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")  # docker dns
topic_raw = os.getenv("KAFKA_TOPIC_RAW", "raw_alarms")                   # ingestion stream

# demo scale -> patient diversity without complexity
PATIENT_IDS = [f"P{str(i).zfill(3)}" for i in range(1, 21)]  # P001..P020

# throughput knobs -> Phase2 volume target
SLEEP_SEC = float(os.getenv("GENERATOR_SLEEP_SEC", "1"))          # 1s watch-mode | 0 = max speed
TOTAL_EVENTS = int(os.getenv("GENERATOR_TOTAL_EVENTS", "0"))      # 0 = endless | e.g. 1000000


# raw schema -> downstream contracts
class VitalEvent(BaseModel):
    # core routing keys
    patient_id: str
    timestamp: str                 # utc iso + Z
    source: Literal["icu_monitor_simulator"]

    # device decides optional payload fields
    device_type: Literal[
        "bedside_monitor",
        "ventilator",
        "infusion_pump",
        "medication",
        "bed",
        "call_bell",
        "pain_assessment",
    ]

    # bedside monitor ranges -> allow artifacts but keep bounds
    heart_rate: Optional[int] = Field(default=None, ge=0, le=250)
    systolic_bp: Optional[int] = Field(default=None, ge=0, le=260)
    spo2: Optional[int] = Field(default=None, ge=0, le=100)

    # ventilator
    airway_pressure: Optional[float] = None
    tidal_volume: Optional[float] = None

    # infusion pump
    pump_state: Optional[
        Literal["normal", "occlusion", "empty", "battery_low", "failure"]
    ] = None

    # medication reminders
    medication_status: Optional[Literal["none", "due", "overdue"]] = None

    # bed sensor
    bed_event: Optional[Literal["none", "exit", "movement"]] = None

    # call bell -> duration only if pressed/unanswered (validated later)
    call_bell_state: Optional[Literal["none", "pressed", "unanswered"]] = None
    call_bell_duration_sec: Optional[int] = None

    # pain / observation
    pain_score: Optional[int] = Field(default=None, ge=0, le=10)
    observation_needed: Optional[bool] = None


def now_iso() -> str:
    # UTC + Z -> consistent timestamps across pipeline
    return datetime.utcnow().isoformat() + "Z"


def create_producer() -> KafkaProducer:
    # kafka can start later -> retry loop
    while True:
        try:
            print(f"🔌 [Generator] producer -> {bootstrap_servers}")
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                # json -> bytes
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("✅ [Generator] Producer connected.")
            return producer
        except NoBrokersAvailable:
            print("⚠️ [Generator] Kafka not ready -> retry 5s")
            time.sleep(5)


# device generators -> readable + extensible


def generate_bedside_monitor_event(patient_id: str) -> VitalEvent:
    # normal baseline -> rare clinical -> rare artifacts (alarm fatigue demo)

    # HR
    hr = random.randint(60, 100)  # baseline
    r = random.random()  # single draw -> clean probs
    if r < 0.03:
        hr = random.choice([random.randint(130, 180), random.randint(30, 45)])  # tachy/brady
    elif r < 0.035:
        hr = random.choice([0, random.randint(10, 25)])  # arrest-ish
    elif r < 0.05:
        hr = random.choice([250, random.randint(200, 240)])  # artifact spikes

    # BP
    bp = random.randint(110, 140)  # baseline
    r = random.random()
    if r < 0.03:
        bp = random.choice([random.randint(70, 90), random.randint(160, 220)])  # hypo/hyper
    elif r < 0.05:
        bp = random.choice([random.randint(0, 30), random.randint(240, 260)])  # cuff off / nonsense

    # SpO2
    spo2 = random.randint(94, 100)  # baseline
    r = random.random()
    if r < 0.03:
        spo2 = random.randint(75, 89)  # clinical desat
    elif r < 0.05:
        spo2 = random.randint(0, 30)   # dropout-ish

    return VitalEvent(
        patient_id=patient_id,
        timestamp=now_iso(),
        source="icu_monitor_simulator",
        device_type="bedside_monitor",
        heart_rate=hr,
        systolic_bp=bp,
        spo2=spo2,
    )


def generate_ventilator_event(patient_id: str) -> VitalEvent:
    # simple params -> enough for rules downstream
    airway_pressure = random.uniform(18, 24)
    tidal_volume = random.uniform(380, 520)

    # rare failures -> not too noisy
    if random.random() < 0.05:
        airway_pressure = random.uniform(30, 45)  # high pressure
    if random.random() < 0.05:
        tidal_volume = random.uniform(150, 260)   # low volume
    if random.random() < 0.01:
        tidal_volume = 0.0                        # disconnect-ish

    return VitalEvent(
        patient_id=patient_id,
        timestamp=now_iso(),
        source="icu_monitor_simulator",
        device_type="ventilator",
        airway_pressure=round(airway_pressure, 1),
        tidal_volume=round(tidal_volume, 1),
    )


def generate_pump_event(patient_id: str) -> VitalEvent:
    # discrete states -> easy demo triggers
    state = "normal"
    r = random.random()
    if r < 0.02:
        state = "occlusion"
    elif r < 0.04:
        state = "empty"
    elif r < 0.06:
        state = "battery_low"
    elif r < 0.07:
        state = "failure"

    return VitalEvent(
        patient_id=patient_id,
        timestamp=now_iso(),
        source="icu_monitor_simulator",
        device_type="infusion_pump",
        pump_state=state,
    )


def generate_medication_event(patient_id: str) -> VitalEvent:
    # non-vitals -> show mixed alarm types
    status = "none"
    r = random.random()
    if r < 0.03:
        status = "due"
    elif r < 0.05:
        status = "overdue"

    return VitalEvent(
        patient_id=patient_id,
        timestamp=now_iso(),
        source="icu_monitor_simulator",
        device_type="medication",
        medication_status=status,
    )


def generate_bed_event(patient_id: str) -> VitalEvent:
    # used for fall-risk logic later
    event = "none"
    r = random.random()
    if r < 0.02:
        event = "exit"
    elif r < 0.05:
        event = "movement"

    return VitalEvent(
        patient_id=patient_id,
        timestamp=now_iso(),
        source="icu_monitor_simulator",
        device_type="bed",
        bed_event=event,
    )


def generate_call_bell_event(patient_id: str) -> VitalEvent:
    # duration -> unanswered-threshold scoring
    state = "none"
    duration = None
    r = random.random()
    if r < 0.03:
        state = "pressed"
        duration = random.randint(5, 60)
    elif r < 0.05:
        state = "unanswered"
        duration = random.randint(60, 300)

    return VitalEvent(
        patient_id=patient_id,
        timestamp=now_iso(),
        source="icu_monitor_simulator",
        device_type="call_bell",
        call_bell_state=state,
        call_bell_duration_sec=duration,
    )


def generate_pain_event(patient_id: str) -> VitalEvent:
    # mostly low -> occasional spikes
    pain = random.randint(0, 3)
    if random.random() < 0.1:
        pain = random.randint(7, 10)

    return VitalEvent(
        patient_id=patient_id,
        timestamp=now_iso(),
        source="icu_monitor_simulator",
        device_type="pain_assessment",
        pain_score=pain,
        observation_needed=pain >= 7,  # tiny derived field demo
    )


# weighting -> "realistic-ish" distribution
DEVICE_WEIGHTS = {
    "bedside_monitor": 0.4,
    "ventilator": 0.15,
    "infusion_pump": 0.15,
    "medication": 0.1,
    "bed": 0.1,
    "call_bell": 0.05,
    "pain_assessment": 0.05,
}


def choose_device_type() -> str:
    # weighted roulette -> no extra deps
    r = random.random()
    cumulative = 0.0
    for device, weight in DEVICE_WEIGHTS.items():
        cumulative += weight
        if r <= cumulative:
            return device
    return "bedside_monitor"  # fallback


def generate_random_event() -> VitalEvent:
    # random patient + device -> correct event schema
    patient_id = random.choice(PATIENT_IDS)
    device = choose_device_type()

    if device == "bedside_monitor":
        return generate_bedside_monitor_event(patient_id)
    if device == "ventilator":
        return generate_ventilator_event(patient_id)
    if device == "infusion_pump":
        return generate_pump_event(patient_id)
    if device == "medication":
        return generate_medication_event(patient_id)
    if device == "bed":
        return generate_bed_event(patient_id)
    if device == "call_bell":
        return generate_call_bell_event(patient_id)
    if device == "pain_assessment":
        return generate_pain_event(patient_id)

    return generate_bedside_monitor_event(patient_id)


def format_human_readable(event: VitalEvent) -> str:
    # terminal view -> quick sanity while debugging
    if event.device_type == "bedside_monitor":
        return f"Monitor HR={event.heart_rate}, BP={event.systolic_bp}, SpO2={event.spo2}"
    if event.device_type == "ventilator":
        return f"Ventilator Paw={event.airway_pressure}, VT={event.tidal_volume}"
    if event.device_type == "infusion_pump":
        return f"Pump state={event.pump_state}"
    if event.device_type == "medication":
        return f"Medication status={event.medication_status}"
    if event.device_type == "bed":
        return f"Bed event={event.bed_event}"
    if event.device_type == "call_bell":
        return f"Call bell state={event.call_bell_state}, duration={event.call_bell_duration_sec}s"
    if event.device_type == "pain_assessment":
        return f"Pain score={event.pain_score}, observation_needed={event.observation_needed}"
    return "Unknown event"


def main():
    producer = create_producer()

    # flush batching -> throughput
    flush_every = int(os.getenv("KAFKA_FLUSH_EVERY", "10"))
    counter = 0

    # log mode -> watch vs speedrun
    mode = "endless" if TOTAL_EVENTS <= 0 else f"total={TOTAL_EVENTS}"
    print(
        f"🚀 [Generator] running ({mode}) -> topic='{topic_raw}' | "
        f"patients {PATIENT_IDS[0]}..{PATIENT_IDS[-1]} | sleep={SLEEP_SEC}s | flush={flush_every}"
    )

    try:
        while True:
            event = generate_random_event()

            # pydantic v2/v1 compat
            payload = event.model_dump() if hasattr(event, "model_dump") else event.dict()

            producer.send(topic_raw, payload)
            counter += 1

            # batch flush -> kafka internal batching still works
            if counter % flush_every == 0:
                producer.flush()

            # don't spam stdout in speed mode
            if SLEEP_SEC > 0:
                print(
                    f"📤 [Generator] {counter} | {event.patient_id} | "
                    f"{event.device_type} | {format_human_readable(event)}"
                )
            else:
                # progress marker -> still shows life for big runs
                if counter % 50000 == 0:
                    print(f"📤 [Generator] sent={counter}")

            # stop condition -> Phase2 requirement
            if TOTAL_EVENTS > 0 and counter >= TOTAL_EVENTS:
                print(f"✅ [Generator] reached TOTAL_EVENTS={TOTAL_EVENTS}")
                break

            # pace control
            if SLEEP_SEC > 0:
                time.sleep(SLEEP_SEC)

    finally:
        # cleanup -> container stop
        try:
            producer.flush()
            producer.close()
            print("🧹 [Generator] producer closed")
        except Exception:
            pass


if __name__ == "__main__":
    main()
