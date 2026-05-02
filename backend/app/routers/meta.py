from __future__ import annotations

from fastapi import APIRouter

from backend.app import db
from backend.app.utils import response


router = APIRouter(prefix="/meta", tags=["metadata"])


@router.get("/filters")
def get_filters() -> dict:
    years = db.distinct_values("temporal_yearly_summary", "year")
    boroughs = db.distinct_values("temporal_borough_year_month", "pickup_borough")
    zones = db.find_many(
        "spatial_top_zones_by_year",
        sort=[db.sort_asc("pickup_zone")],
        projection={
            "_id": 0,
            "PULocationID": 1,
            "pickup_zone": 1,
            "pickup_borough": 1,
        },
    )

    seen_zone_ids = set()
    unique_zones = []
    for zone in zones:
        zone_id = zone.get("PULocationID")
        if zone_id in seen_zone_ids:
            continue
        seen_zone_ids.add(zone_id)
        unique_zones.append(zone)

    return response(
        {
            "years": years,
            "boroughs": boroughs,
            "zones": unique_zones,
        }
    )


@router.get("/collections")
def get_collections() -> dict:
    names = sorted(db.get_database().list_collection_names())
    return response(names)
