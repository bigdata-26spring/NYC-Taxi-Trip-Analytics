from __future__ import annotations

from fastapi import APIRouter, Query

from backend.app import db
from backend.app.utils import clamp_limit, response


router = APIRouter(prefix="/forecast", tags=["forecast"])


@router.get("/story")
def get_forecast_story(zone_limit: int = Query(default=300, ge=1, le=500)) -> dict:
    zone_limit = clamp_limit(zone_limit, default=300, maximum=500)

    story = {
        "model_comparison": db.find_many(
            "forecast_model_comparison",
            sort=[db.sort_asc("RMSE")],
        ),
        "model_evaluation_metrics": db.find_many(
            "forecast_model_evaluation_metrics",
            sort=[db.sort_asc("RMSE")],
        ),
        "monthly_actual_predicted": db.find_many(
            "forecast_monthly_actual_predicted",
            sort=[db.sort_asc("year_month")],
        ),
        "daily_actual_predicted": db.find_many(
            "forecast_daily_actual_predicted",
            sort=[db.sort_asc("pickup_date")],
        ),
        "error_by_hour": db.find_many(
            "forecast_error_by_hour",
            sort=[db.sort_asc("hour")],
        ),
        "error_by_borough": db.find_many(
            "forecast_error_by_borough",
            sort=[db.sort_desc("aggregate_absolute_error")],
        ),
        "zone_accuracy": db.find_many(
            "forecast_zone_accuracy",
            sort=[db.sort_desc("aggregate_absolute_error")],
            limit=zone_limit,
        ),
        "weekday_hour_error_heatmap": db.find_many(
            "forecast_weekday_hour_error_heatmap",
            sort=[db.sort_asc("day_of_week"), db.sort_asc("hour")],
        ),
    }

    return response(story, evaluation_year=2024, zone_limit=zone_limit)


@router.get("/zone-hour")
def get_forecast_zone_hour(
    year: int | None = Query(default=None),
    month: int | None = Query(default=None, ge=1, le=12),
    hour: int | None = Query(default=None, ge=0, le=23),
    pickup_date: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=10000),
) -> dict:
    limit = clamp_limit(limit, default=1000, maximum=10000)
    query = {}
    if year is not None:
        query["year"] = year
    if month is not None:
        query["month"] = month
    if hour is not None:
        query["hour"] = hour
    if pickup_date:
        query["pickup_date"] = pickup_date

    data = db.find_many(
        "forecast_zone_hour",
        query,
        sort=[db.sort_desc("absolute_error")],
        limit=limit,
    )
    return response(data, year=year, month=month, hour=hour, pickup_date=pickup_date, limit=limit)
