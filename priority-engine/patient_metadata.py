# priority-engine/patient_metadata.py

from __future__ import annotations

import re
from typing import Literal, TypedDict


# types -> fewer "oops" moments (keys/values)

Mobility = Literal["independent", "assisted", "bedbound"]
Risk = Literal["low", "medium", "high"]
Cardio = Literal["low", "medium", "high"]
Resp = Literal["low", "medium", "high"]
Infection = Literal["low", "medium", "high"]


class PatientMeta(TypedDict, total=False):
    # scoring knobs (keep it small, but realistic-ish)
    fall_risk: int                 # 0..10
    mobility: Mobility
    cardiac_risk: Cardio
    resp_risk: Resp
    infection_risk: Infection
    post_op: bool
    sedation_rass: int             # ICU-ish range ~ -5..+4

    # UI / DB context fields
    ward: str
    bed: str
    risk_level: Risk


# archetypes -> quick variety without a real database

PATIENT_ARCHETYPES: list[PatientMeta] = [
    {
        "fall_risk": 8,
        "mobility": "assisted",
        "cardiac_risk": "high",
        "resp_risk": "medium",
        "infection_risk": "low",
        "post_op": True,
        "sedation_rass": -2,
        "risk_level": "high",
    },
    {
        "fall_risk": 2,
        "mobility": "independent",
        "cardiac_risk": "low",
        "resp_risk": "low",
        "infection_risk": "medium",
        "post_op": False,
        "sedation_rass": 0,
        "risk_level": "low",
    },
    {
        "fall_risk": 5,
        "mobility": "assisted",
        "cardiac_risk": "medium",
        "resp_risk": "high",
        "infection_risk": "high",
        "post_op": True,
        "sedation_rass": -3,
        "risk_level": "high",
    },
    {
        "fall_risk": 9,
        "mobility": "bedbound",
        "cardiac_risk": "high",
        "resp_risk": "high",
        "infection_risk": "medium",
        "post_op": False,
        "sedation_rass": -4,
        "risk_level": "high",
    },
    {
        "fall_risk": 1,
        "mobility": "independent",
        "cardiac_risk": "medium",
        "resp_risk": "low",
        "infection_risk": "low",
        "post_op": False,
        "sedation_rass": 1,
        "risk_level": "medium",
    },
    {
        "fall_risk": 6,
        "mobility": "assisted",
        "cardiac_risk": "medium",
        "resp_risk": "medium",
        "infection_risk": "high",
        "post_op": True,
        "sedation_rass": -1,
        "risk_level": "medium",
    },
]


# ward/bed -> deterministic mapping (so screenshots look consistent)
# P001..P020 -> ICU-1 / B01..B20

_PAT_ID_RE = re.compile(r"^P(\d{3})$")  # strict on purpose -> avoids weird IDs


def _ward_bed_from_patient_id(patient_id: str) -> tuple[str, str]:
    m = _PAT_ID_RE.match(patient_id.strip())
    if not m:
        return "UNKNOWN", "UNKNOWN"
    n = int(m.group(1))  # 1..999
    return "ICU-1", f"B{n:02d}"  # stable + readable in UI


# concrete IDs -> pick archetypes
# quick "distribution" without random (reproducible demo)

PATIENT_METADATA: dict[str, PatientMeta] = {
    "P001": PATIENT_ARCHETYPES[0],
    "P002": PATIENT_ARCHETYPES[1],
    "P003": PATIENT_ARCHETYPES[2],
    "P004": PATIENT_ARCHETYPES[3],
    "P005": PATIENT_ARCHETYPES[4],
    "P006": PATIENT_ARCHETYPES[5],
    "P007": PATIENT_ARCHETYPES[0],
    "P008": PATIENT_ARCHETYPES[1],
    "P009": PATIENT_ARCHETYPES[2],
    "P010": PATIENT_ARCHETYPES[3],
    "P011": PATIENT_ARCHETYPES[4],
    "P012": PATIENT_ARCHETYPES[5],
    "P013": PATIENT_ARCHETYPES[0],
    "P014": PATIENT_ARCHETYPES[1],
    "P015": PATIENT_ARCHETYPES[2],
    "P016": PATIENT_ARCHETYPES[3],
    "P017": PATIENT_ARCHETYPES[4],
    "P018": PATIENT_ARCHETYPES[5],
    "P019": PATIENT_ARCHETYPES[0],
    "P020": PATIENT_ARCHETYPES[1],
}


# normalization / safety

def _clamp_int(v: int, lo: int, hi: int, default: int) -> int:
    # "bad meta shouldn't break stream"
    try:
        x = int(v)
    except Exception:
        return default
    return max(lo, min(hi, x))


def get_metadata(patient_id: str) -> dict:
    """
    always returns the keys the priority-engine expects
    unknown patient -> neutral defaults + ward/bed derived
    """
    ward, bed = _ward_bed_from_patient_id(patient_id)

    # baseline -> "boring" patient (safe fallback)
    base: PatientMeta = {
        "fall_risk": 0,
        "mobility": "independent",
        "cardiac_risk": "low",
        "resp_risk": "low",
        "infection_risk": "low",
        "post_op": False,
        "sedation_rass": 0,
        "risk_level": "low",
        "ward": ward,
        "bed": bed,
    }

    meta = PATIENT_METADATA.get(patient_id)
    if not meta:
        return dict(base)  # unknown ID -> still usable downstream

    # merge -> base first, archetype overwrites
    full: PatientMeta = dict(base)
    full.update(meta)

    # clamp -> keep scoring sane
    full["fall_risk"] = _clamp_int(full.get("fall_risk", 0), 0, 10, 0)
    full["sedation_rass"] = _clamp_int(full.get("sedation_rass", 0), -5, 4, 0)

    # ward/bed must always exist (even if archetype didn't include them)
    full.setdefault("ward", ward)
    full.setdefault("bed", bed)

    # risk_level must be one of the literals
    rl = full.get("risk_level", "medium")
    if rl not in ("low", "medium", "high"):
        full["risk_level"] = "medium"

    return dict(full)
