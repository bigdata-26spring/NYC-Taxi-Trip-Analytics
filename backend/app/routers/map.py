from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app import db
from backend.app.utils import clamp_limit, response


router = APIRouter(prefix="/map", tags=["map"])


def scoped_query(
    *,
    year: int | None = None,
    month: int | None = None,
    year_month: str | None = None,
    hour: int | None = None,
    borough_field: str | None = None,
    borough: str | None = None,
) -> dict:
    query: dict = {}
    if year is not None:
        query["year"] = year
    if month is not None:
        query["month"] = month
    if year_month:
        query["year_month"] = year_month
    if hour is not None:
        query["hour"] = hour
    if borough_field and borough:
        query[borough_field] = borough
    return query


@router.get("/centroids")
def get_zone_centroids() -> dict:
    data = db.find_many(
        "map_zone_centroids",
        sort=[db.sort_asc("LocationID")],
    )
    return response(data)


@router.get("/dropoff-zone-hour")
def get_dropoff_zone_hour(
    year: int | None = Query(default=None),
    month: int | None = Query(default=None, ge=1, le=12),
    hour: int | None = Query(default=None, ge=0, le=23),
    borough: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict:
    limit = clamp_limit(limit, default=500, maximum=5000)
    data = db.find_many(
        "map_dropoff_zone_hour_features",
        scoped_query(
            year=year,
            month=month,
            hour=hour,
            borough_field="dropoff_borough",
            borough=borough,
        ),
        sort=[db.sort_desc("trip_count")],
        limit=limit,
    )
    return response(data, year=year, month=month, hour=hour, borough=borough, limit=limit)


@router.get("/zone-balance-hour")
def get_zone_balance_hour(
    year: int | None = Query(default=None),
    month: int | None = Query(default=None, ge=1, le=12),
    hour: int | None = Query(default=None, ge=0, le=23),
    borough: str | None = Query(default=None),
    direction: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict:
    limit = clamp_limit(limit, default=500, maximum=5000)
    query = scoped_query(year=year, month=month, hour=hour, borough_field="borough", borough=borough)
    if direction:
        query["balance_direction"] = direction

    data = db.find_many(
        "map_zone_balance_hour",
        query,
        sort=[db.sort_desc("total_zone_activity")],
        limit=limit,
    )
    return response(
        data,
        year=year,
        month=month,
        hour=hour,
        borough=borough,
        direction=direction,
        limit=limit,
    )


@router.get("/od-flow-hour")
def get_od_flow_hour(
    hour: int | None = Query(default=None, ge=0, le=23),
    borough: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict:
    limit = clamp_limit(limit, default=500, maximum=5000)
    query = scoped_query(hour=hour, borough_field="pickup_borough", borough=borough)
    data = db.find_many(
        "map_od_flow_hour",
        query,
        sort=[db.sort_asc("hour"), db.sort_asc("route_rank_in_hour")],
        limit=limit,
    )
    return response(data, hour=hour, borough=borough, limit=limit)


@router.get("/od-flow-year-month")
def get_od_flow_year_month(
    year: int | None = Query(default=None),
    month: int | None = Query(default=None, ge=1, le=12),
    year_month: str | None = Query(default=None),
    borough: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict:
    limit = clamp_limit(limit, default=500, maximum=5000)
    query = scoped_query(
        year=year,
        month=month,
        year_month=year_month,
        borough_field="pickup_borough",
        borough=borough,
    )
    data = db.find_many(
        "map_od_flow_year_month",
        query,
        sort=[db.sort_asc("year"), db.sort_asc("month"), db.sort_asc("route_rank_in_year_month")],
        limit=limit,
    )
    return response(data, year=year, month=month, year_month=year_month, borough=borough, limit=limit)


@router.get("/replay-sample")
def get_replay_sample(
    year: int | None = Query(default=None),
    month: int | None = Query(default=None, ge=1, le=12),
    hour: int | None = Query(default=None, ge=0, le=23),
    limit: int = Query(default=5000, ge=1, le=20000),
) -> dict:
    limit = clamp_limit(limit, default=5000, maximum=20000)
    data = db.find_many(
        "map_replay_sample",
        scoped_query(year=year, month=month, hour=hour),
        sort=[db.sort_asc("pickup_epoch_ms")],
        limit=limit,
    )
    return response(data, year=year, month=month, hour=hour, limit=limit)
