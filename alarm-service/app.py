from __future__ import annotations

from datetime import datetime
import os
import re
from typing import Optional, Literal

import psycopg2
from psycopg2 import OperationalError, ProgrammingError
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel


# env -> same code works local + docker
DB_HOST = os.getenv("DB_HOST", "alarms-db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "icu_alarms")
DB_USER = os.getenv("DB_USER", "icu_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "icu_password")

ACTIVE_TABLE_ENV = os.getenv("DB_TABLE_ACTIVE", "icu_active_alarms")
HISTORY_TABLE_ENV = os.getenv("DB_TABLE_HISTORY", "icu_alarm_history")

# tiny "SQL injection guard" (table names come from env)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_ident(name: str, fallback: str) -> str:
    # allow only normal identifiers -> no surprises in f-strings
    if name and _IDENT_RE.match(name):
        return name
    return fallback


ACTIVE_TABLE = safe_ident(ACTIVE_TABLE_ENV, "icu_active_alarms")
HISTORY_TABLE = safe_ident(HISTORY_TABLE_ENV, "icu_alarm_history")


def get_conn():
    # RealDictCursor -> rows come back as dicts (nice for FastAPI JSON)
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


# literals -> keeps filters + response stable (typos show up fast)
Severity = Literal["warning", "critical"]
RiskLevel = Literal["low", "medium", "high"]
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
DataQuality = Literal["clinical", "artifact"]
Status = Literal["active", "acknowledged", "resolved"]


class Alert(BaseModel):
    # API shape == what dashboard expects
    id: int
    patient_id: str

    ward: Optional[str] = None
    bed: Optional[str] = None

    severity: Optional[Severity] = None
    risk_level: Optional[RiskLevel] = None
    alarm_type: Optional[AlarmType] = None
    fall_risk: Optional[int] = None

    priority_level: Optional[PriorityLevel] = None
    priority_score: Optional[int] = None

    data_quality: Optional[DataQuality] = None  # clinical vs artifact

    status: Optional[str] = None  # kept as str here -> DB might still have NULLs
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None

    message: Optional[str] = None
    heart_rate: Optional[int] = None
    systolic_bp: Optional[int] = None
    spo2: Optional[int] = None

    vital_timestamp: Optional[datetime] = None
    alarm_timestamp: Optional[datetime] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


app = FastAPI(title="ICU Alarm Service", version="2.2.0")

# serve the dashboard files via /static
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
def dashboard_root():
    # keep root simple -> open dashboard immediately
    return FileResponse("static/index.html")


@app.get("/health")
def health():
    # quick "container is alive" ping
    return {"status": "ok"}


def _raise_db_503(where: str, err: Exception) -> None:
    # map DB issues to 503 -> dashboard can show "service unavailable"
    print(f"[AlarmService] DB error in {where}: {err}")
    raise HTTPException(status_code=503, detail="Database unavailable")


@app.get("/alerts/active", response_model=list[Alert])
def get_active_alerts(
    limit: int = Query(100, ge=1, le=500),
    priority_level: Optional[PriorityLevel] = Query(None),
    ward: Optional[str] = Query(None),
    alarm_type: Optional[AlarmType] = Query(None),
    min_score: Optional[int] = Query(None, ge=0),
    data_quality: Optional[DataQuality] = Query(None),
    status: Optional[Status] = Query(None),
    include_resolved: bool = Query(False),
):
    try:
        # dynamic filters -> build query step by step (cleaner than many endpoints)
        base_query = f"""
            SELECT
                id, patient_id, ward, bed,
                severity, risk_level, alarm_type, fall_risk,
                data_quality,
                priority_level, priority_score,
                status, acknowledged_at, resolved_at,
                message, heart_rate, systolic_bp, spo2,
                vital_timestamp, alarm_timestamp,
                created_at, updated_at
            FROM {ACTIVE_TABLE}
            WHERE 1=1
        """
        params: list[object] = []

        # default: hide resolved (otherwise "active" view looks empty/noisy)
        if not include_resolved and status is None:
            base_query += " AND (status IS NULL OR status <> 'resolved')"

        if status:
            base_query += " AND status = %s"
            params.append(status)

        if priority_level:
            base_query += " AND priority_level = %s"
            params.append(priority_level)

        if ward:
            base_query += " AND ward = %s"
            params.append(ward)

        if alarm_type:
            base_query += " AND alarm_type = %s"
            params.append(alarm_type)

        if min_score is not None:
            base_query += " AND priority_score >= %s"
            params.append(min_score)

        if data_quality is not None:
            # Phase-2 decision: ACTIVE has UNIQUE(patient_id, alarm_type, data_quality)
            base_query += " AND data_quality = %s"
            params.append(data_quality)

        # ordering -> dashboard wants "most urgent first"
        base_query += """
            ORDER BY priority_score DESC NULLS LAST,
                     alarm_timestamp DESC NULLS LAST,
                     updated_at DESC NULLS LAST,
                     created_at DESC
            LIMIT %s
        """
        params.append(limit)

        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(base_query, params)
            return cur.fetchall()

    except (OperationalError, ProgrammingError) as e:
        _raise_db_503("get_active_alerts", e)
    except HTTPException:
        raise
    except Exception as e:
        print(f"[AlarmService] Unexpected error in get_active_alerts: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error")


@app.get("/alerts/history", response_model=list[Alert])
def get_history(
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    priority_level: Optional[PriorityLevel] = Query(None),
    ward: Optional[str] = Query(None),
    alarm_type: Optional[AlarmType] = Query(None),
    min_score: Optional[int] = Query(None, ge=0),
    data_quality: Optional[DataQuality] = Query(None),
):
    try:
        # HISTORY = append-only -> paging makes sense here
        base_query = f"""
            SELECT
                id, patient_id, ward, bed,
                severity, risk_level, alarm_type, fall_risk,
                data_quality,
                priority_level, priority_score,
                message, heart_rate, systolic_bp, spo2,
                vital_timestamp, alarm_timestamp,
                created_at
            FROM {HISTORY_TABLE}
            WHERE 1=1
        """
        params: list[object] = []

        if priority_level:
            base_query += " AND priority_level = %s"
            params.append(priority_level)

        if ward:
            base_query += " AND ward = %s"
            params.append(ward)

        if alarm_type:
            base_query += " AND alarm_type = %s"
            params.append(alarm_type)

        if min_score is not None:
            base_query += " AND priority_score >= %s"
            params.append(min_score)

        if data_quality is not None:
            base_query += " AND data_quality = %s"
            params.append(data_quality)

        base_query += """
            ORDER BY priority_score DESC NULLS LAST,
                     alarm_timestamp DESC NULLS LAST,
                     created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(base_query, params)
            return cur.fetchall()

    except (OperationalError, ProgrammingError) as e:
        _raise_db_503("get_history", e)
    except HTTPException:
        raise
    except Exception as e:
        print(f"[AlarmService] Unexpected error in get_history: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error")


@app.get("/alerts/{alert_id}", response_model=Alert)
def get_alert(alert_id: int):
    try:
        # details -> only from ACTIVE (current state)
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id, patient_id, ward, bed,
                    severity, risk_level, alarm_type, fall_risk,
                    data_quality,
                    priority_level, priority_score,
                    status, acknowledged_at, resolved_at,
                    message, heart_rate, systolic_bp, spo2,
                    vital_timestamp, alarm_timestamp,
                    created_at, updated_at
                FROM {ACTIVE_TABLE}
                WHERE id = %s
                """,
                (alert_id,),
            )
            row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Alert not found")

        return row

    except (OperationalError, ProgrammingError) as e:
        _raise_db_503("get_alert", e)
    except HTTPException:
        raise
    except Exception as e:
        print(f"[AlarmService] Unexpected error in get_alert: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error")


@app.post("/alerts/{alert_id}/ack")
def ack_alert(alert_id: int):
    try:
        # simple write endpoint -> dashboard buttons can call this
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {ACTIVE_TABLE}
                SET status = 'acknowledged',
                    acknowledged_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (alert_id,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Alert not found")
        return {"status": "ok", "action": "ack", "id": alert_id}

    except (OperationalError, ProgrammingError) as e:
        _raise_db_503("ack_alert", e)
    except HTTPException:
        raise
    except Exception as e:
        print(f"[AlarmService] Unexpected error in ack_alert: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error")


@app.post("/alerts/{alert_id}/resolve")
def resolve_alert(alert_id: int):
    try:
        # resolve -> keeps ACTIVE cleaner (and matches ICU "lifecycle" idea)
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {ACTIVE_TABLE}
                SET status = 'resolved',
                    resolved_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (alert_id,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Alert not found")
        return {"status": "ok", "action": "resolve", "id": alert_id}

    except (OperationalError, ProgrammingError) as e:
        _raise_db_503("resolve_alert", e)
    except HTTPException:
        raise
    except Exception as e:
        print(f"[AlarmService] Unexpected error in resolve_alert: {e}")
        raise HTTPException(status_code=500, detail="Unexpected error")
