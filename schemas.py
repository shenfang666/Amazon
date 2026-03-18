from __future__ import annotations

from urllib.parse import parse_qs


def query_value(query: str, key: str, default=None, *, strip: bool = False):
    value = parse_qs(query).get(key, [default])[0]
    if strip and isinstance(value, str):
        return value.strip()
    return value


def json_required_string(payload: dict, key: str) -> str:
    value = str(payload.get(key, "")).strip()
    if not value:
        raise ValueError(f"{key} is required.")
    return value


def json_optional_string(payload: dict, key: str) -> str | None:
    value = str(payload.get(key, "")).strip()
    return value or None
