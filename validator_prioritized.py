import json
import os
import signal
from typing import Literal

from kafka import KafkaConsumer
from pydantic import BaseModel, Field, ValidationError


# schema check -> "what comes out of priority-engine" matches what I expect

class PrioritizedAlarmEvent(BaseModel):
    """
    Prioritized alarm created by the priority-engine.
    keep this in sync with priority-engine -> otherwise silent chaos
    """

    patient_id: str
    timestamp: str          # when priority-engine emitted it
    vital_timestamp: str    # original measurement time

    severity: Literal["warning", "critical"]

    alarm_type: Literal[
        "vitals",
        "ventilator",
        "pump",
        "medication",
        "bed_exit",
        "bed_movement",
        "call_unanswered",
        "pain",
    ]

    message: str

    # dq -> artifact vs clinical (needed for "why did it score like that?")
    data_quality: Literal["clinical", "artifact"] = "clinical"  # default -> backward safe

    # ranges -> same as pipeline decisions (avoid "validator too strict")
    heart_rate: int = Field(ge=0, le=250)
    systolic_bp: int = Field(ge=0, le=260)   # allow weird low (artifact) values
    spo2: int = Field(ge=0, le=100)

    ward: str
    bed: str

    risk_level: Literal["low", "medium", "high"]
    fall_risk: int = Field(ge=0, le=10)

    priority_score: int
    priority_level: Literal["low", "medium", "high"]


def _pretty(obj) -> str:
    # quick debug print -> readable json
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(obj)


def main():
    # offset -> earliest for "pipeline demo", latest for "only new stuff"
    offset_mode = os.getenv("VALIDATOR_OFFSET_MODE", "earliest").strip().lower()
    if offset_mode not in ("earliest", "latest"):
        offset_mode = "earliest"

    # optional cap -> I sometimes just want 20 msgs and done
    max_messages_env = os.getenv("VALIDATOR_MAX_MESSAGES", "").strip()
    max_messages = int(max_messages_env) if max_messages_env.isdigit() else 0  # 0 -> unlimited

    # allow local runs outside docker -> localhost:9092
    topic = os.getenv("KAFKA_TOPIC_PRIORITIZED", "prioritized_alarms")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "priority-validator-v3")  # own group -> doesn't steal offsets

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),  # bytes -> dict
        auto_offset_reset=offset_mode,
        enable_auto_commit=True,
        group_id=group_id,
    )

    running = True

    def _shutdown(*_args):
        # ctrl+c -> exit loop cleanly
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(
        "⭐ Priority-Validator läuft\n"
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
                alarm = PrioritizedAlarmEvent(**data)  # "contract" check
                ok += 1

                # human scan -> does it look plausible?
                print(
                    "\n"
                    "════════════════════════════\n"
                    f"⭐ PRIORITIZED ALARM für {alarm.patient_id}\n"
                    f"• Ward/Bed:   {alarm.ward} / {alarm.bed}\n"
                    f"• Type:       {alarm.alarm_type}\n"
                    f"• Quality:    {alarm.data_quality.upper()}\n"
                    f"• Severity:   {alarm.severity.upper()}\n"
                    f"• Risk:       {alarm.risk_level.upper()} (Fall risk={alarm.fall_risk}/10)\n"
                    f"• Priority:   {alarm.priority_level.upper()} (Score={alarm.priority_score})\n"
                    f"• Reason:     {alarm.message}\n"
                    f"• Values:     HR={alarm.heart_rate}, BP={alarm.systolic_bp}, SpO2={alarm.spo2}\n"
                    "════════════════════════════\n"
                )

            except ValidationError as e:
                bad += 1
                print("❌ Invalid prioritized alarm:")
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
            "\n🧾 Priority-Validator summary\n"
            f"• Seen: {seen}\n"
            f"• OK:   {ok}\n"
            f"• Bad:  {bad}\n"
        )


if __name__ == "__main__":
    main()
