from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app import db
from backend.app.utils import response


router = APIRouter(prefix="/business", tags=["business"])


@router.get("/payment-type")
def get_payment_type_summary(year: int | None = Query(default=None)) -> dict:
    if year:
        data = db.find_many(
            "business_payment_type_by_year",
            {"year": year},
            sort=[db.sort_desc("trip_count")],
        )
    else:
        data = db.find_many(
            "business_payment_type_summary",
            sort=[db.sort_desc("trip_count")],
        )

    return response(data, year=year)


@router.get("/trip-behavior-by-hour")
def get_trip_behavior_by_hour() -> dict:
    data = db.find_many(
        "business_trip_behavior_by_hour",
        sort=[db.sort_asc("hour")],
    )
    return response(data)
