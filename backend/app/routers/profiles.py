from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app import db
from backend.app.utils import clamp_limit, response


router = APIRouter(prefix="/profiles", tags=["profiles"])


@router.get("/zones")
def get_zone_profiles(
    borough: str | None = Query(default=None),
    limit: int = Query(default=300, ge=1, le=1000),
) -> dict:
    limit = clamp_limit(limit, default=300, maximum=1000)
    query = {"pickup_borough": borough} if borough else {}
    data = db.find_many(
        "profile_zone_summary",
        query,
        sort=[db.sort_desc("total_trips")],
        limit=limit,
    )
    return response(data, borough=borough, limit=limit)


@router.get("/zones/{location_id}")
def get_zone_profile(location_id: int) -> dict:
    data = db.find_one("profile_zone_summary", {"PULocationID": location_id}) or {}
    return response(data, location_id=location_id)


@router.get("/routes")
def get_route_profiles(
    pickup_borough: str | None = Query(default=None),
    dropoff_borough: str | None = Query(default=None),
    limit: int = Query(default=300, ge=1, le=1000),
) -> dict:
    limit = clamp_limit(limit, default=300, maximum=1000)
    query = {}
    if pickup_borough:
        query["pickup_borough"] = pickup_borough
    if dropoff_borough:
        query["dropoff_borough"] = dropoff_borough

    data = db.find_many(
        "profile_route_summary",
        query,
        sort=[db.sort_asc("route_rank")],
        limit=limit,
    )
    return response(
        data,
        pickup_borough=pickup_borough,
        dropoff_borough=dropoff_borough,
        limit=limit,
    )


@router.get("/routes/{pickup_location_id}/{dropoff_location_id}")
def get_route_profile(pickup_location_id: int, dropoff_location_id: int) -> dict:
    data = (
        db.find_one(
            "profile_route_summary",
            {
                "PULocationID": pickup_location_id,
                "DOLocationID": dropoff_location_id,
            },
        )
        or {}
    )
    return response(
        data,
        pickup_location_id=pickup_location_id,
        dropoff_location_id=dropoff_location_id,
    )
