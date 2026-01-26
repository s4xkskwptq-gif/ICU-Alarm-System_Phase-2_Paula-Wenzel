import json
import os
import signal
from typing import Literal, Optional

from kafka import KafkaConsumer
from pydantic import BaseModel, Field, ValidationError, model_validator


class VitalEvent(BaseModel):
    # core fields -> always there (routing / trace)
    patient_id: str
    timestamp: str
    source: Literal["icu_monitor_simulator"]

    # one schema, but device decides which fields must be filled
    device_type: Literal[
        "bedside_monitor",
        "ventilator",
        "infusion_pump",
        "medication",
        "bed",
        "call_bell",
        "pain_assessment",
    ]

    # ranges match generator + downstream assumptions (incl. artifact ranges)
    heart_rate: Optional[int] = Field(default=None, ge=0, le=250)
    systolic_bp: Optional[int] = Field(default=None, ge=0, le=260)
    spo2: Optional[int] = Field(default=None, ge=0, le=100)

    # ventilator fields
    airway_pressure: Optional[float] = None
    tidal_volume: Optional[float] = None

    # discrete device states -> easier than continuous telemetry
    pump_state: Optional[Literal["normal", "occlusion", "empty", "battery_low", "failure"]] = None
    medication_status: Optional[Literal["none", "due", "overdue"]] = None
    bed_event: Optional[Literal["none", "exit", "movement"]] = None

    # call bell -> duration only when it makes sense
    call_bell_state: Optional[Literal["none", "pressed", "unanswered"]] = None
    # duration: no negatives; 1h cap -> "demo guardrail"
    call_bell_duration_sec: Optional[int] = Field(default=None, ge=0, le=3600)

    # pain -> keep it explicit (no hidden inference)
    pain_score: Optional[int] = Field(default=None, ge=0, le=10)
    observation_needed: Optional[bool] = None

    @model_validator(mode="after")
    def check_required_fields_by_device(self):
        # this is the "schema sanity gate" before anything else touches the data

        if self.device_type == "bedside_monitor":
            # monitor must include the actual vitals
            if self.heart_rate is None or self.systolic_bp is None or self.spo2 is None:
                raise ValueError("bedside_monitor requires heart_rate, systolic_bp and spo2")

        elif self.device_type == "ventilator":
            # avoid half-filled ventilator messages
            if self.airway_pressure is None or self.tidal_volume is None:
                raise ValueError("ventilator requires airway_pressure and tidal_volume")

        elif self.device_type == "infusion_pump":
            # state drives alarm rules later
            if self.pump_state is None:
                raise ValueError("infusion_pump requires pump_state")

        elif self.device_type == "medication":
            # same idea -> explicit status
            if self.medication_status is None:
                raise ValueError("medication requires medication_status")

        elif self.device_type == "bed":
            if self.bed_event is None:
                raise ValueError("bed requires bed_event")

        elif self.device_type == "call_bell":
            # state must exist
            if self.call_bell_state is None:
                raise ValueError("call_bell requires call_bell_state")

            # pressed/unanswered -> duration is part of meaning
            if self.call_bell_state in ("pressed", "unanswered"):
                if self.call_bell_duration_sec is None:
                    raise ValueError("call_bell pressed/unanswered requires call_bell_duration_sec")

            # none -> keep payload clean (no confusing stale duration)
            if self.call_bell_state == "none" and self.call_bell_duration_sec is not None:
                raise ValueError("call_bell_state='none' must not include call_bell_duration_sec")

        elif self.device_type == "pain_assessment":
            # here both fields are expected together (generator sets both)
            if self.pain_score is None or self.observation_needed is None:
                raise ValueError("pain_assessment requires pain_score and observation_needed")

        return self


def _pretty(obj) -> str:
    # debugging -> readable output in terminal
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(obj)


def main():
    # offset choice -> "replay everything" vs "only new stuff"
    offset_mode = os.getenv("VALIDATOR_OFFSET_MODE", "earliest").strip().lower()
    if offset_mode not in ("earliest", "latest"):
        offset_mode = "earliest"

    # optional stop after N -> useful when recording screenshots
    max_messages_env = os.getenv("VALIDATOR_MAX_MESSAGES", "").strip()
    max_messages = int(max_messages_env) if max_messages_env.isdigit() else 0  # 0 = unlimited

    # defaults -> run locally against exposed Kafka port
    topic = os.getenv("KAFKA_TOPIC_RAW", "raw_alarms")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "schema-validator-v2")  # separate group -> doesn't steal offsets

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset=offset_mode,
        enable_auto_commit=True,
        group_id=group_id,
    )

    running = True

    def _shutdown(*_args):
        # ctrl+c -> clean exit
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(
        "✅ Raw Schema-Validator läuft\n"
        f"• Topic:   {topic}\n"
        f"• Kafka:   {bootstrap}\n"
        f"• Group:   {group_id}\n"
        f"• Offset:  {offset_mode}\n"
        f"• MaxMsg:  {max_messages if max_messages else '∞'}\n"
    )

    seen = 0
    ok = 0
    bad = 0

    try:
        for msg in consumer:
            if not running:
                break

            data = msg.value
            seen += 1

            try:
                # this is the whole point -> fail fast on schema mismatch
                event = VitalEvent(**data)
                ok += 1
                print(f"✅ Valid raw event: {event.patient_id} | {event.device_type} | ts={event.timestamp}")
            except (ValidationError, ValueError) as e:
                bad += 1
                print("❌ Invalid raw event:")
                print(_pretty(data))
                print(e)

            if max_messages and seen >= max_messages:
                break

    finally:
        try:
            consumer.close()
        except Exception:
            pass

        print(
            "\n🧾 Raw Schema-Validator summary\n"
            f"• Seen: {seen}\n"
            f"• OK:   {ok}\n"
            f"• Bad:  {bad}\n"
        )


if __name__ == "__main__":
    main()
