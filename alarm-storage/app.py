import json
import os
import re
import signal
import time
from datetime import datetime
from typing import Literal, Optional

import psycopg2
from kafka import KafkaConsumer
from pydantic import BaseModel, Field, ValidationError


# "what comes in from Kafka" -> keep it strict, fail fast if schema drifts
class PrioritizedAlarmEvent(BaseModel):
    patient_id: str
    timestamp: str
    vital_timestamp: str

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
    data_quality: Literal["clinical", "artifact"] = "clinical"  # dq -> stays end-to-end

    # keep same ranges as upstream -> avoid "DB ok but model rejects"
    heart_rate: int = Field(ge=0, le=250)
    systolic_bp: int = Field(ge=0, le=260)
    spo2: int = Field(ge=0, le=100)

    ward: str
    bed: str
    risk_level: Literal["low", "medium", "high"]

    fall_risk: int = Field(default=0, ge=0, le=10)

    priority_score: int
    priority_level: Literal["low", "medium", "high"]


# env -> same image can run local vs docker
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPIC_INPUT = os.getenv("KAFKA_TOPIC_INPUT", "prioritized_alarms")

DB_HOST = os.getenv("DB_HOST", "alarms-db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "icu_alarms")
DB_USER = os.getenv("DB_USER", "icu_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "icu_password")

ACTIVE_TABLE_ENV = os.getenv("DB_TABLE_ACTIVE", "icu_active_alarms")
HISTORY_TABLE_ENV = os.getenv("DB_TABLE_HISTORY", "icu_alarm_history")

# tiny "SQL injection guard" for table names (since they come from env)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_ident(name: str, fallback: str) -> str:
    # only allow simple identifiers -> no weird SQL surprises
    if name and _IDENT_RE.match(name):
        return name
    return fallback


ACTIVE_TABLE = safe_ident(ACTIVE_TABLE_ENV, "icu_active_alarms")
HISTORY_TABLE = safe_ident(HISTORY_TABLE_ENV, "icu_alarm_history")


def parse_iso_ts(ts: str) -> Optional[datetime]:
    # stream gives ISO strings (+ often Z) -> DB wants datetime
    if not ts:
        return None
    try:
        ts_fixed = ts.replace("Z", "+00:00")  # quick UTC fix
        return datetime.fromisoformat(ts_fixed)
    except Exception:
        return None  # rather store NULL than crash the pipeline


def _safe_index_name(table: str, suffix: str) -> str:
    # avoid invalid index names if table env is odd
    base = re.sub(r"[^A-Za-z0-9_]", "_", table)
    idx = f"idx_{base}_{suffix}"
    return safe_ident(idx, f"idx_{base}_x")


def wait_for_db():
    # DB sometimes starts later than this service -> just wait
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            conn.close()
            print("✅ [Storage] Database reachable.")
            return
        except Exception as e:
            print(f"⏳ [Storage] Waiting for database... Error: {e}")
            time.sleep(5)


def init_db():
    # idempotent init -> safe on restarts
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True  # DDL without manual commit headaches
    cur = conn.cursor()

    # ACTIVE -> "current view" (upsert), HISTORY -> "audit trail" (append)
    # note: UNIQUE includes data_quality -> clinical + artifact can live side-by-side
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ACTIVE_TABLE} (
            id               SERIAL PRIMARY KEY,
            patient_id       VARCHAR(50) NOT NULL,
            ward             VARCHAR(50),
            bed              VARCHAR(20),

            severity         VARCHAR(10),
            risk_level       VARCHAR(10),

            alarm_type       VARCHAR(30) NOT NULL,
            fall_risk        INTEGER,

            data_quality     VARCHAR(10) NOT NULL,

            priority_level   VARCHAR(10),
            priority_score   INTEGER,

            status           VARCHAR(20) DEFAULT 'active',
            acknowledged_at  TIMESTAMP,
            resolved_at      TIMESTAMP,

            message          TEXT,
            heart_rate       INTEGER,
            systolic_bp      INTEGER,
            spo2             INTEGER,

            vital_timestamp  TIMESTAMP,
            alarm_timestamp  TIMESTAMP,

            created_at       TIMESTAMP DEFAULT NOW(),
            updated_at       TIMESTAMP DEFAULT NOW(),

            UNIQUE (patient_id, alarm_type, data_quality)
        );
        """
    )

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
            id               SERIAL PRIMARY KEY,
            patient_id       VARCHAR(50) NOT NULL,
            ward             VARCHAR(50),
            bed              VARCHAR(20),

            severity         VARCHAR(10),
            risk_level       VARCHAR(10),

            alarm_type       VARCHAR(30),
            fall_risk        INTEGER,

            data_quality     VARCHAR(10),

            priority_level   VARCHAR(10),
            priority_score   INTEGER,

            message          TEXT,
            heart_rate       INTEGER,
            systolic_bp      INTEGER,
            spo2             INTEGER,

            vital_timestamp  TIMESTAMP,
            alarm_timestamp  TIMESTAMP,

            created_at       TIMESTAMP DEFAULT NOW()
        );
        """
    )

    # indexes -> faster dashboard queries (prio + time + filters)
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {_safe_index_name(ACTIVE_TABLE, 'priority_score')} ON {ACTIVE_TABLE} (priority_score);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {_safe_index_name(ACTIVE_TABLE, 'alarm_ts')} ON {ACTIVE_TABLE} (alarm_timestamp);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {_safe_index_name(ACTIVE_TABLE, 'priority_level')} ON {ACTIVE_TABLE} (priority_level);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {_safe_index_name(ACTIVE_TABLE, 'alarm_type')} ON {ACTIVE_TABLE} (alarm_type);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {_safe_index_name(ACTIVE_TABLE, 'data_quality')} ON {ACTIVE_TABLE} (data_quality);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {_safe_index_name(ACTIVE_TABLE, 'status')} ON {ACTIVE_TABLE} (status);"
    )

    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {_safe_index_name(HISTORY_TABLE, 'priority_score')} ON {HISTORY_TABLE} (priority_score);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {_safe_index_name(HISTORY_TABLE, 'alarm_ts')} ON {HISTORY_TABLE} (alarm_timestamp);"
    )

    cur.close()
    conn.close()
    print(f"✅ [Storage] Tables are ready ({ACTIVE_TABLE} + {HISTORY_TABLE}).")


def create_consumer() -> KafkaConsumer:
    # consumer group -> exactly this service owns these offsets
    print(f"🔌 [Storage] Connecting consumer to {KAFKA_BOOTSTRAP_SERVERS} ...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC_INPUT,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),  # bytes -> dict
        auto_offset_reset="latest",  # restart -> don't replay old backlog
        enable_auto_commit=True,
        group_id="alarm-storage",
        consumer_timeout_ms=1000,  # lets shutdown work without blocking forever
    )
    print("✅ [Storage] Consumer connected.")
    return consumer


def store_active_alarm(cur, alarm: PrioritizedAlarmEvent):
    # string -> datetime (best effort)
    vital_ts = parse_iso_ts(alarm.vital_timestamp)
    alarm_ts = parse_iso_ts(alarm.timestamp)

    # upsert pattern -> ACTIVE is "latest state"
    cur.execute(
        f"""
        INSERT INTO {ACTIVE_TABLE} (
            patient_id, ward, bed,
            severity, risk_level,
            alarm_type, fall_risk,
            data_quality,
            priority_level, priority_score,
            status, acknowledged_at, resolved_at,
            message, heart_rate, systolic_bp, spo2,
            vital_timestamp, alarm_timestamp
        ) VALUES (
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s,
            %s, %s,
            'active', NULL, NULL,
            %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (patient_id, alarm_type, data_quality)
        DO UPDATE SET
            ward = EXCLUDED.ward,
            bed = EXCLUDED.bed,
            severity = EXCLUDED.severity,
            risk_level = EXCLUDED.risk_level,
            fall_risk = EXCLUDED.fall_risk,
            priority_level = EXCLUDED.priority_level,
            priority_score = EXCLUDED.priority_score,
            message = EXCLUDED.message,
            heart_rate = EXCLUDED.heart_rate,
            systolic_bp = EXCLUDED.systolic_bp,
            spo2 = EXCLUDED.spo2,
            vital_timestamp = EXCLUDED.vital_timestamp,
            alarm_timestamp = EXCLUDED.alarm_timestamp,
            status = 'active',          -- new alarm update -> back to active
            acknowledged_at = NULL,     -- reset ack/resolve on update (choice)
            resolved_at = NULL,
            updated_at = NOW();
        """,
        (
            alarm.patient_id,
            alarm.ward,
            alarm.bed,
            alarm.severity,
            alarm.risk_level,
            alarm.alarm_type,
            int(alarm.fall_risk),
            alarm.data_quality,
            alarm.priority_level,
            int(alarm.priority_score),
            alarm.message,
            int(alarm.heart_rate),
            int(alarm.systolic_bp),
            int(alarm.spo2),
            vital_ts,
            alarm_ts,
        ),
    )


def store_history_alarm(cur, alarm: PrioritizedAlarmEvent):
    # always append -> history is "what happened", not "current state"
    vital_ts = parse_iso_ts(alarm.vital_timestamp)
    alarm_ts = parse_iso_ts(alarm.timestamp)

    cur.execute(
        f"""
        INSERT INTO {HISTORY_TABLE} (
            patient_id, ward, bed,
            severity, risk_level,
            alarm_type, fall_risk,
            data_quality,
            priority_level, priority_score,
            message, heart_rate, systolic_bp, spo2,
            vital_timestamp, alarm_timestamp
        ) VALUES (
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s
        );
        """,
        (
            alarm.patient_id,
            alarm.ward,
            alarm.bed,
            alarm.severity,
            alarm.risk_level,
            alarm.alarm_type,
            int(alarm.fall_risk),
            alarm.data_quality,
            alarm.priority_level,
            int(alarm.priority_score),
            alarm.message,
            int(alarm.heart_rate),
            int(alarm.systolic_bp),
            int(alarm.spo2),
            vital_ts,
            alarm_ts,
        ),
    )


def main():
    # small sleep -> gives kafka/db a moment (plus depends_on healthchecks)
    print("⏳ [Storage] Waiting 10 seconds for upstream services ...")
    time.sleep(10)

    wait_for_db()
    init_db()
    consumer = create_consumer()

    print(f"🚀 [Storage] Running – reading from '{KAFKA_TOPIC_INPUT}' and writing into PostgreSQL")

    running = True

    def _shutdown(*_args):
        # stop nicely -> commit leftovers
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # keep one DB connection open -> faster than connect per message
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = False  # transactions -> rollback possible
    cur = conn.cursor()

    commit_every = int(os.getenv("DB_COMMIT_EVERY", "10"))  # batch -> less overhead
    counter = 0

    try:
        while running:
            for msg in consumer:
                if not running:
                    break

                data = msg.value

                try:
                    alarm = PrioritizedAlarmEvent(**data)  # schema gate
                except ValidationError as e:
                    print("❌ [Storage] Invalid prioritized alarm, skipping:", data)
                    print(e)
                    continue

                try:
                    store_active_alarm(cur, alarm)
                    store_history_alarm(cur, alarm)

                    counter += 1
                    if counter % commit_every == 0:
                        conn.commit()

                    print(
                        f"💾 [Storage] Stored alarm for {alarm.patient_id} "
                        f"(type={alarm.alarm_type}, dq={alarm.data_quality}, "
                        f"priority={alarm.priority_level}, score={alarm.priority_score})"
                    )

                except Exception as e:
                    # any single bad insert -> rollback batch (safer than partial)
                    conn.rollback()
                    print("❌ [Storage] DB insert failed, rolled back.")
                    print("Error:", e)

    finally:
        # last partial batch -> commit (if possible)
        try:
            if counter % commit_every != 0:
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
            print("🧹 [Storage] Consumer closed.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
