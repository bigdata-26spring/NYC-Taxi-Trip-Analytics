from __future__ import annotations

from typing import Any


def clamp_limit(limit: int, *, default: int = 25, maximum: int = 500) -> int:
    if limit <= 0:
        return default
    return min(limit, maximum)


def compact_meta(**kwargs: Any) -> dict[str, Any]:
    return {key: value for key, value in kwargs.items() if value is not None}


def response(data: Any, **meta: Any) -> dict[str, Any]:
    return {
        "data": data,
        "meta": compact_meta(count=len(data) if isinstance(data, list) else None, **meta),
    }


def year_query(year: int | None) -> dict[str, Any]:
    return {"year": year} if year is not None else {}


def borough_query(field: str, borough: str | None) -> dict[str, Any]:
    return {field: borough} if borough else {}
