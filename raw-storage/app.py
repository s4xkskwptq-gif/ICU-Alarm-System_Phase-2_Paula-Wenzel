import json
import os
import re
import signal
import time
from datetime import datetime
from typing import Optional

import psycopg2
from kafka import KafkaConsumer
from pydantic import BaseModel, ValidationError


# env -> runtime knobs / reproducible via compose
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC = os.getenv("KAFKA_TOPIC_INPUT", "raw_alarms")

# db connection -> local demo only
DB_HOST = os.getenv("DB_HOST", "alarms-db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "icu_alarms")
DB_USER = os.getenv("DB_USER", "icu_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "icu_password")

# raw table -> proof of ingestion volume
RAW_TABLE_ENV = os.getenv("DB_TABLE_RAW", "icu_raw_events")
COMMIT_EVERY = int(os.getenv("DB_COMMIT_EVERY", "2000"))  # batching -> speed

# identifier safety -> avoid SQL injection via env
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_ident(name: str, fallback: str) -> str:
    # whitelist table names -> safety net
    if name and _IDENT_RE.match(name):
        return name
    return fallback


RAW_TABLE = safe_ident(RAW_TABLE_ENV, "icu_raw_events")


class RawEvent(BaseModel):
    # minimal schema -> only what we need for indexing
    patient_id: str
    timestamp: str
    source: str
    device_type: str


def parse_iso_ts(ts: str) -> Optional[datetime]:
    # ISO + Z -> postgres-friendly timestamp
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def wait_for_db():
    # startup guard -> db might be slower than container
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
            conn.close()
            print("✅ [RawStorage] DB reachable.")
            return
        except Exception as e:
            print(f"⏳ [RawStorage] waiting for DB... {e}")
            time.sleep(5)


def init_db():
    # idempotent schema -> safe restarts
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()

    # raw = append-only -> no business logic here
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            id SERIAL PRIMARY KEY,
            patient_id VARCHAR(50),
            device_type VARCHAR(50),
            event_timestamp TIMESTAMP,
            payload JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
    )

    # indexes -> fast COUNT / filters
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{RAW_TABLE}_ts ON {RAW_TABLE} (event_timestamp);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{RAW_TABLE}_pid ON {RAW_TABLE} (patient_id);")

    cur.close()
    conn.close()
    print(f"✅ [RawStorage] table ready: {RAW_TABLE}")


def create_consumer():
    # independent consumer -> fan-out from Kafka
    print(f"🔌 [RawStorage] consumer -> {KAFKA_BOOTSTRAP} / topic={TOPIC}")
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",   # demo mode -> no replay
        enable_auto_commit=True,
        group_id="raw-storage",
        consumer_timeout_ms=1000,     # graceful shutdown
    )


def main():
    # startup delay -> kafka + db
    print("⏳ [RawStorage] waiting 10s (Kafka/DB startup)")
    time.sleep(10)

    wait_for_db()
    init_db()

    consumer = create_consumer()

    running = True

    def _shutdown(*_args):
        # signal handler -> clean exit
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # single connection -> batch commits
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = False
    cur = conn.cursor()

    seen = 0

    try:
        while running:
            for msg in consumer:
                if not running:
                    break

                data = msg.value

                try:
                    # lightweight validation -> best effort
                    evt = RawEvent(**data)
                except ValidationError:
                    # schema drift allowed -> raw must stay raw
                    evt = None

                ts = parse_iso_ts(data.get("timestamp"))
                patient_id = data.get("patient_id")
                device_type = data.get("device_type")

                # store full payload -> audit / counting
                cur.execute(
                    f"""
                    INSERT INTO {RAW_TABLE}
                        (patient_id, device_type, event_timestamp, payload)
                    VALUES (%s, %s, %s, %s::jsonb);
                    """,
                    (patient_id, device_type, ts, json.dumps(data)),
                )

                seen += 1
                if seen % COMMIT_EVERY == 0:
                    conn.commit()
                    print(f"💾 [RawStorage] stored raw events: {seen}")

    finally:
        # flush + cleanup
        try:
            conn.commit()
        except Exception:
            pass
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass

        print(f"🧾 [RawStorage] done. total stored (session): {seen}")


if __name__ == "__main__":
    main()
