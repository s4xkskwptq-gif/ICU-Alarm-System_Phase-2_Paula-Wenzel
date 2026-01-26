import json
import os
import signal
from typing import Literal

from kafka import KafkaConsumer
from pydantic import BaseModel, Field, ValidationError


# schema mirror -> should match alarm-processor output (quick sanity check)
class EnrichedAlarmEvent(BaseModel):
    # keys we rely on downstream -> keep strict here on purpose

    patient_id: str
    timestamp: str          # processor time
    vital_timestamp: str    # original event time

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

    # dq travels end-to-end -> helps separate "real" vs "sensor glitch"
    # default -> older messages still validate
    data_quality: Literal["clinical", "artifact"] = "clinical"

    # keep same ranges as rest of pipeline -> avoids "validator disagrees with storage"
    heart_rate: int = Field(ge=0, le=250)
    systolic_bp: int = Field(ge=0, le=260)  # includes artifact bp=0..30 etc.
    spo2: int = Field(ge=0, le=100)


def _pretty(obj) -> str:
    # quick readable dump when something fails
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(obj)


def main():
    # offset switch -> sometimes I want "fresh only", sometimes replay for debugging
    offset_mode = os.getenv("VALIDATOR_OFFSET_MODE", "latest").strip().lower()
    if offset_mode not in ("earliest", "latest"):
        offset_mode = "latest"

    # optional stop-after-N -> nice for screenshots / portfolio
    max_messages_env = os.getenv("VALIDATOR_MAX_MESSAGES", "").strip()
    max_messages = int(max_messages_env) if max_messages_env.isdigit() else 0  # 0 = unlimited

    topic = os.getenv("KAFKA_TOPIC_ENRICHED", "enriched_alarms")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")  # localhost for running validators outside docker
    group_id = os.getenv("KAFKA_GROUP_ID", "enriched-alarm-validator-v2")  # dedicated group -> doesn't mess with services

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
        # ctrl+c / docker stop -> clean exit
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(
        "🚨 Enriched-Alarm-Validator läuft\n"
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
                alert = EnrichedAlarmEvent(**data)
                ok += 1

                # print compact -> easy to skim while pipeline runs
                print(
                    "\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🚨 ALARM ({alert.severity.upper()}) für {alert.patient_id}\n"
                    f"• Type:     {alert.alarm_type}\n"
                    f"• Quality:  {alert.data_quality.upper()}\n"
                    f"• Zeit:     {alert.timestamp}\n"
                    f"• Messung:  {alert.vital_timestamp}\n"
                    f"• Grund:    {alert.message}\n"
                    f"• Werte:    HR={alert.heart_rate}, BP={alert.systolic_bp}, SpO2={alert.spo2}\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                )

            except ValidationError as e:
                # schema mismatch -> show payload + pydantic error
                bad += 1
                print("❌ Ungültiger Alarm:")
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
            "\n🧾 Enriched-Alarm-Validator summary\n"
            f"• Seen: {seen}\n"
            f"• OK:   {ok}\n"
            f"• Bad:  {bad}\n"
        )


if __name__ == "__main__":
    main()
