from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app import db
from backend.app.utils import clamp_limit, response


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/overview")
def get_overview(
    year: int | None = Query(default=None),
    zone_limit: int = Query(default=12, ge=1, le=100),
    route_limit: int = Query(default=8, ge=1, le=100),
) -> dict:
    zone_limit = clamp_limit(zone_limit, default=12, maximum=100)
    route_limit = clamp_limit(route_limit, default=8, maximum=100)

    kpi = db.find_one("kpi_summary") or {}

    if year is None:
        top_zones = db.find_many(
            "spatial_top_zones_overall",
            sort=[db.sort_desc("total_trips")],
            limit=zone_limit,
        )
        heatmap = db.find_many(
            "temporal_weekday_hour_heatmap",
            sort=[db.sort_asc("day_of_week"), db.sort_asc("hour")],
        )
    else:
        top_zones = db.find_many(
            "spatial_top_zones_by_year",
            {"year": year},
            sort=[db.sort_asc("zone_rank_in_year")],
            limit=zone_limit,
        )
        heatmap = db.find_many(
            "temporal_year_weekday_hour_heatmap",
            {"year": year},
            sort=[db.sort_asc("day_of_week"), db.sort_asc("hour")],
        )

    overview = {
        "kpi": kpi,
        "yearly_summary": db.find_many(
            "temporal_yearly_summary",
            sort=[db.sort_asc("year")],
        ),
        "year_month_demand": db.find_many(
            "temporal_year_month_demand",
            {"year": year} if year else {},
            sort=[db.sort_asc("year"), db.sort_asc("month")],
        ),
        "hourly_demand": db.find_many(
            "temporal_year_hourly_pattern" if year else "temporal_hourly_demand",
            {"year": year} if year else {},
            sort=[db.sort_asc("hour")],
        ),
        "weekday_hour_heatmap": heatmap,
        "top_zones": top_zones,
        "top_routes": db.find_many(
            "routes_top_routes",
            sort=[db.sort_asc("route_rank")],
            limit=route_limit,
        ),
    }

    return response(overview, year=year)


@router.get("/temporal-story")
def get_temporal_story(
    year: int | None = Query(default=None),
    borough: str | None = Query(default=None),
) -> dict:
    year_query = {"year": year} if year else {}
    borough_query = {"pickup_borough": borough} if borough else {}
    borough_year_query = {**year_query, **borough_query}

    story = {
        "yearly_summary": db.find_many(
            "temporal_yearly_summary",
            sort=[db.sort_asc("year")],
        ),
        "year_month_demand": db.find_many(
            "temporal_year_month_demand",
            year_query,
            sort=[db.sort_asc("year"), db.sort_asc("month")],
        ),
        "hourly_pattern": db.find_many(
            "temporal_year_hourly_pattern" if year else "temporal_hourly_demand",
            year_query,
            sort=[db.sort_asc("hour")],
        ),
        "weekday_hour_heatmap": db.find_many(
            "temporal_year_weekday_hour_heatmap" if year else "temporal_weekday_hour_heatmap",
            year_query,
            sort=[db.sort_asc("day_of_week"), db.sort_asc("hour")],
        ),
        "weekday_weekend_hourly": db.find_many(
            "temporal_weekday_weekend_hourly",
            sort=[db.sort_asc("is_weekend"), db.sort_asc("hour")],
        ),
        "rush_hour_by_year": db.find_many(
            "temporal_rush_hour_by_year",
            year_query,
            sort=[db.sort_asc("year"), db.sort_asc("time_period_order")],
        ),
        "borough_year_month": db.find_many(
            "temporal_borough_year_month",
            borough_year_query,
            sort=[db.sort_asc("pickup_borough"), db.sort_asc("year"), db.sort_asc("month")],
        ),
        "borough_hourly_pattern": db.find_many(
            "temporal_borough_hourly_pattern",
            borough_query,
            sort=[db.sort_asc("pickup_borough"), db.sort_asc("hour")],
        ),
        "month_of_year_pattern": db.find_many(
            "temporal_month_of_year_pattern",
            sort=[db.sort_asc("month")],
        ),
        "revenue_efficiency": db.find_many(
            "temporal_revenue_efficiency",
            year_query,
            sort=[db.sort_asc("year"), db.sort_asc("month")],
        ),
    }

    return response(story, year=year, borough=borough)


@router.get("/spatial-story")
def get_spatial_story(
    year: int | None = Query(default=None),
    hour: int | None = Query(default=None, ge=0, le=23),
    zone_limit: int = Query(default=25, ge=1, le=500),
) -> dict:
    zone_limit = clamp_limit(zone_limit, default=25, maximum=500)

    if year:
        top_zones = db.find_many(
            "spatial_top_zones_by_year",
            {"year": year},
            sort=[db.sort_asc("zone_rank_in_year")],
            limit=zone_limit,
        )
    else:
        top_zones = db.find_many(
            "spatial_top_zones_overall",
            sort=[db.sort_desc("total_trips")],
            limit=zone_limit,
        )

    hour_query = {"hour": hour} if hour is not None else {}

    story = {
        "top_zones": top_zones,
        "top_zones_by_hour": db.find_many(
            "spatial_top_zones_by_hour",
            hour_query,
            sort=[db.sort_asc("hour"), db.sort_asc("zone_rank_in_hour")],
            limit=zone_limit if hour is not None else None,
        ),
        "zone_rank_change": db.find_many(
            "spatial_zone_rank_change",
            sort=[db.sort_desc("rank_improvement"), db.sort_desc("trip_count_change")],
            limit=zone_limit,
        ),
    }

    return response(story, year=year, hour=hour, zone_limit=zone_limit)


@router.get("/routes-story")
def get_routes_story(route_limit: int = Query(default=25, ge=1, le=500)) -> dict:
    route_limit = clamp_limit(route_limit, default=25, maximum=500)
    story = {
        "top_routes": db.find_many(
            "routes_top_routes",
            sort=[db.sort_asc("route_rank")],
            limit=route_limit,
        ),
        "top_airport_routes": db.find_many(
            "routes_top_airport_routes",
            sort=[db.sort_asc("route_rank"), db.sort_desc("trip_count")],
            limit=route_limit,
        ),
        "top_inter_borough_routes": db.find_many(
            "routes_top_inter_borough_routes",
            sort=[db.sort_asc("route_rank"), db.sort_desc("trip_count")],
            limit=route_limit,
        ),
        "route_borough_matrix": db.find_many(
            "routes_borough_matrix",
            sort=[db.sort_desc("total_trips")],
        ),
        "pickup_dropoff_borough_matrix": db.find_many(
            "routes_pickup_dropoff_borough_matrix",
            sort=[db.sort_desc("trip_count")],
        ),
        "airport_trip_summary": db.find_many(
            "routes_airport_trip_summary",
            sort=[db.sort_asc("year"), db.sort_asc("airport_trip_type")],
        ),
        "route_efficiency_ranking": db.find_many(
            "route_efficiency_ranking",
            sort=[db.sort_desc("revenue_per_mile"), db.sort_desc("trip_count")],
            limit=route_limit,
        ),
        "route_tip_ranking": db.find_many(
            "route_tip_ranking",
            sort=[db.sort_desc("route_tip_share"), db.sort_desc("avg_tip_amount")],
            limit=route_limit,
        ),
    }

    return response(story, route_limit=route_limit)


@router.get("/business-story")
def get_business_story(year: int | None = Query(default=None)) -> dict:
    story = {
        "payment_type": db.find_many(
            "business_payment_type_by_year" if year else "business_payment_type_summary",
            {"year": year} if year else {},
            sort=[db.sort_desc("trip_count")],
        ),
        "trip_behavior_by_hour": db.find_many(
            "business_trip_behavior_by_hour",
            sort=[db.sort_asc("hour")],
        ),
    }

    return response(story, year=year)
