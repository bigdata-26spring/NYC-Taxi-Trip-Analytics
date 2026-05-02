from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app import db
from backend.app.utils import clamp_limit, response


router = APIRouter(prefix="/spatial", tags=["spatial"])


@router.get("/top-zones")
def get_top_zones(
    year: int | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=500),
) -> dict:
    limit = clamp_limit(limit, default=25, maximum=500)

    if year:
        data = db.find_many(
            "spatial_top_zones_by_year",
            {"year": year},
            sort=[db.sort_asc("zone_rank_in_year")],
            limit=limit,
        )
    else:
        data = db.find_many(
            "spatial_top_zones_overall",
            sort=[db.sort_desc("total_trips")],
            limit=limit,
        )

    return response(data, year=year, limit=limit)


@router.get("/zone-rank-change")
def get_zone_rank_change(limit: int = Query(default=50, ge=1, le=500)) -> dict:
    limit = clamp_limit(limit, default=50, maximum=500)
    data = db.find_many(
        "spatial_zone_rank_change",
        sort=[db.sort_desc("rank_improvement"), db.sort_desc("trip_count_change")],
        limit=limit,
    )
    return response(data, limit=limit)


@router.get("/top-zones-by-hour")
def get_top_zones_by_hour(
    hour: int | None = Query(default=None, ge=0, le=23),
    limit: int = Query(default=10, ge=1, le=100),
) -> dict:
    limit = clamp_limit(limit, default=10, maximum=100)
    query = {"hour": hour} if hour is not None else {}
    data = db.find_many(
        "spatial_top_zones_by_hour",
        query,
        sort=[db.sort_asc("hour"), db.sort_asc("zone_rank_in_hour")],
        limit=limit if hour is not None else None,
    )
    return response(data, hour=hour, limit=limit)
