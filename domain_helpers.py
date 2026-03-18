from __future__ import annotations

import json
from datetime import datetime


# Constants
NORMAL_OVERRIDE_CHOICES = {"normal", "normal_timing_difference", "expected_timing_difference", "resolved_removal_control"}


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def parse_close_notes(raw_notes: str | None) -> dict:
    if not raw_notes:
        return {"blockers": [], "warnings": []}
    try:
        data = json.loads(raw_notes)
    except json.JSONDecodeError:
        return {"blockers": [], "warnings": [raw_notes]}
    return {
        "blockers": data.get("blockers", []),
        "warnings": data.get("warnings", []),
    }


def round_money(value: float | int | None) -> float:
    if value is None:
        return 0.0
    return round(float(value), 2)


def build_exception_case_key(
    period_month: str | None,
    exception_code: str | None,
    source_table: str | None,
    source_ref: str | None,
    order_id: str | None,
    sku: str | None,
) -> str:
    parts = [
        (period_month or "").strip(),
        (exception_code or "").strip().lower(),
        (source_table or "").strip().lower(),
        (source_ref or "").strip(),
        (order_id or "").strip(),
        (sku or "").strip(),
    ]
    return "|".join(parts)


def is_normal_override(row: dict | None) -> bool:
    if not row:
        return False
    return str(row.get("case_status", "")).strip().lower() == "resolved" and str(row.get("user_choice", "")).strip().lower() in NORMAL_OVERRIDE_CHOICES
