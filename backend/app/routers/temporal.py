from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app import db
from backend.app.utils import response


router = APIRouter(prefix="/temporal", tags=["temporal"])


@router.get("/kpi")
def get_kpi() -> dict:
    return response(db.find_one("kpi_summary") or {})


@router.get("/yearly-summary")
def get_yearly_summary() -> dict:
    data = db.find_many("temporal_yearly_summary", sort=[db.sort_asc("year")])
    return response(data)


@router.get("/daily-demand")
def get_daily_demand(year: int | None = Query(default=None)) -> dict:
    query = {"year": year} if year else {}
    data = db.find_many("temporal_daily_demand", query, sort=[db.sort_asc("pickup_date")])
    return response(data, year=year)


@router.get("/monthly-demand")
def get_monthly_demand(year: int | None = Query(default=None)) -> dict:
    query = {"year": year} if year else {}
    data = db.find_many(
        "temporal_monthly_demand",
        query,
        sort=[db.sort_asc("year"), db.sort_asc("month")],
    )
    return response(data, year=year)


@router.get("/year-month-demand")
def get_year_month_demand(year: int | None = Query(default=None)) -> dict:
    query = {"year": year} if year else {}
    data = db.find_many(
        "temporal_year_month_demand",
        query,
        sort=[db.sort_asc("year"), db.sort_asc("month")],
    )
    return response(data, year=year)


@router.get("/hourly-demand")
def get_hourly_demand(year: int | None = Query(default=None)) -> dict:
    collection_name = "temporal_year_hourly_pattern" if year else "temporal_hourly_demand"
    query = {"year": year} if year else {}
    data = db.find_many(collection_name, query, sort=[db.sort_asc("hour")])
    return response(data, year=year)


@router.get("/weekday-hour-heatmap")
def get_weekday_hour_heatmap(year: int | None = Query(default=None)) -> dict:
    collection_name = (
        "temporal_year_weekday_hour_heatmap"
        if year
        else "temporal_weekday_hour_heatmap"
    )
    query = {"year": year} if year else {}
    data = db.find_many(
        collection_name,
        query,
        sort=[db.sort_asc("day_of_week"), db.sort_asc("hour")],
    )
    return response(data, year=year)


@router.get("/borough-year-month")
def get_borough_year_month(
    year: int | None = Query(default=None),
    borough: str | None = Query(default=None),
) -> dict:
    query = {}
    if year:
        query["year"] = year
    if borough:
        query["pickup_borough"] = borough

    data = db.find_many(
        "temporal_borough_year_month",
        query,
        sort=[db.sort_asc("pickup_borough"), db.sort_asc("year"), db.sort_asc("month")],
    )
    return response(data, year=year, borough=borough)


@router.get("/borough-hourly-pattern")
def get_borough_hourly_pattern(borough: str | None = Query(default=None)) -> dict:
    query = {"pickup_borough": borough} if borough else {}
    data = db.find_many(
        "temporal_borough_hourly_pattern",
        query,
        sort=[db.sort_asc("pickup_borough"), db.sort_asc("hour")],
    )
    return response(data, borough=borough)


@router.get("/rush-hour-by-year")
def get_rush_hour_by_year(year: int | None = Query(default=None)) -> dict:
    query = {"year": year} if year else {}
    data = db.find_many(
        "temporal_rush_hour_by_year",
        query,
        sort=[db.sort_asc("year"), db.sort_asc("time_period_order")],
    )
    return response(data, year=year)


@router.get("/month-of-year-pattern")
def get_month_of_year_pattern() -> dict:
    data = db.find_many(
        "temporal_month_of_year_pattern",
        sort=[db.sort_asc("month")],
    )
    return response(data)


@router.get("/weekday-weekend-hourly")
def get_weekday_weekend_hourly(day_type: str | None = Query(default=None)) -> dict:
    query = {"day_type": day_type} if day_type else {}
    data = db.find_many(
        "temporal_weekday_weekend_hourly",
        query,
        sort=[db.sort_asc("is_weekend"), db.sort_asc("hour")],
    )
    return response(data, day_type=day_type)


@router.get("/revenue-efficiency")
def get_revenue_efficiency(year: int | None = Query(default=None)) -> dict:
    query = {"year": year} if year else {}
    data = db.find_many(
        "temporal_revenue_efficiency",
        query,
        sort=[db.sort_asc("year"), db.sort_asc("month")],
    )
    return response(data, year=year)
