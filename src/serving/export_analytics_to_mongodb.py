"""Export dashboard-ready analytics CSV outputs to MongoDB Atlas.

This script intentionally exports only aggregated analytics tables, not raw
trip records or large processed Parquet tables. It reads Spark CSV output
folders such as outputs/tables/temporal/csv/daily_demand/part-*.csv and writes
each table to a dedicated MongoDB collection.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class TableSpec:
    key: str
    source: Path
    collection: str
    indexes: tuple[tuple[str, ...], ...] = ()


DEFAULT_TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        "kpi_summary",
        PROJECT_ROOT / "outputs" / "tables" / "temporal" / "csv" / "kpi_summary",
        "kpi_summary",
    ),
    TableSpec(
        "hourly_demand",
        PROJECT_ROOT / "outputs" / "tables" / "temporal" / "csv" / "hourly_demand",
        "temporal_hourly_demand",
        (("hour",),),
    ),
    TableSpec(
        "daily_demand",
        PROJECT_ROOT / "outputs" / "tables" / "temporal" / "csv" / "daily_demand",
        "temporal_daily_demand",
        (("pickup_date",), ("year", "month")),
    ),
    TableSpec(
        "monthly_demand",
        PROJECT_ROOT / "outputs" / "tables" / "temporal" / "csv" / "monthly_demand",
        "temporal_monthly_demand",
        (("year", "month"), ("year_month",)),
    ),
    TableSpec(
        "weekday_hour_heatmap",
        PROJECT_ROOT / "outputs" / "tables" / "temporal" / "csv" / "weekday_hour_heatmap",
        "temporal_weekday_hour_heatmap",
        (("day_of_week", "hour"),),
    ),
    TableSpec(
        "borough_hourly_pattern",
        PROJECT_ROOT / "outputs" / "tables" / "temporal" / "csv" / "borough_hourly_pattern",
        "temporal_borough_hourly_pattern",
        (("pickup_borough", "hour"),),
    ),
    TableSpec(
        "top_zones_overall",
        PROJECT_ROOT / "outputs" / "tables" / "temporal" / "csv" / "top_zones_overall",
        "spatial_top_zones_overall",
        (("PULocationID",), ("pickup_borough",), ("total_trips",)),
    ),
    TableSpec(
        "yearly_summary",
        PROJECT_ROOT / "outputs" / "tables" / "temporal_enriched" / "csv" / "yearly_summary",
        "temporal_yearly_summary",
        (("year",),),
    ),
    TableSpec(
        "year_month_demand",
        PROJECT_ROOT / "outputs" / "tables" / "temporal_enriched" / "csv" / "year_month_demand",
        "temporal_year_month_demand",
        (("year", "month"), ("year_month",)),
    ),
    TableSpec(
        "year_hourly_pattern",
        PROJECT_ROOT / "outputs" / "tables" / "temporal_enriched" / "csv" / "year_hourly_pattern",
        "temporal_year_hourly_pattern",
        (("year", "hour"),),
    ),
    TableSpec(
        "year_weekday_hour_heatmap",
        PROJECT_ROOT / "outputs" / "tables" / "temporal_enriched" / "csv" / "year_weekday_hour_heatmap",
        "temporal_year_weekday_hour_heatmap",
        (("year", "day_of_week", "hour"),),
    ),
    TableSpec(
        "borough_year_month",
        PROJECT_ROOT / "outputs" / "tables" / "temporal_enriched" / "csv" / "borough_year_month",
        "temporal_borough_year_month",
        (("pickup_borough", "year", "month"),),
    ),
    TableSpec(
        "rush_hour_by_year",
        PROJECT_ROOT / "outputs" / "tables" / "temporal_enriched" / "csv" / "rush_hour_by_year",
        "temporal_rush_hour_by_year",
        (("year", "time_period_order"),),
    ),
    TableSpec(
        "top_zones_by_year",
        PROJECT_ROOT / "outputs" / "tables" / "temporal_enriched" / "csv" / "top_zones_by_year",
        "spatial_top_zones_by_year",
        (("year", "PULocationID"), ("year", "zone_rank_in_year")),
    ),
    TableSpec(
        "zone_rank_change",
        PROJECT_ROOT / "outputs" / "tables" / "temporal_enriched" / "csv" / "zone_rank_change",
        "spatial_zone_rank_change",
        (("PULocationID",), ("rank_improvement",)),
    ),
    TableSpec(
        "pickup_dropoff_borough_matrix",
        PROJECT_ROOT
        / "outputs"
        / "tables"
        / "trip_route_analytics"
        / "csv"
        / "pickup_dropoff_borough_matrix",
        "routes_pickup_dropoff_borough_matrix",
        (("pickup_borough", "dropoff_borough"),),
    ),
    TableSpec(
        "airport_trip_summary",
        PROJECT_ROOT / "outputs" / "tables" / "trip_route_analytics" / "csv" / "airport_trip_summary",
        "routes_airport_trip_summary",
        (("year", "airport_trip_type"),),
    ),
    TableSpec(
        "route_borough_matrix",
        PROJECT_ROOT / "outputs" / "tables" / "trip_route_analytics" / "csv" / "route_borough_matrix",
        "routes_borough_matrix",
        (("pickup_borough", "dropoff_borough"),),
    ),
    TableSpec(
        "top_airport_routes",
        PROJECT_ROOT / "outputs" / "tables" / "trip_route_analytics" / "csv" / "top_airport_routes",
        "routes_top_airport_routes",
        (("route_rank",), ("PULocationID",), ("DOLocationID",)),
    ),
    TableSpec(
        "top_inter_borough_routes",
        PROJECT_ROOT / "outputs" / "tables" / "trip_route_analytics" / "csv" / "top_inter_borough_routes",
        "routes_top_inter_borough_routes",
        (("route_rank",), ("PULocationID",), ("DOLocationID",)),
    ),
    TableSpec(
        "top_routes_overall",
        PROJECT_ROOT / "outputs" / "tables" / "trip_route_analytics" / "csv" / "top_routes_overall",
        "routes_top_routes",
        (("route_rank",), ("PULocationID",), ("DOLocationID",)),
    ),
    TableSpec(
        "route_concentration",
        PROJECT_ROOT / "outputs" / "tables" / "trip_route_analytics" / "csv" / "route_concentration",
        "route_concentration",
        (("route_order",), ("route_rank",)),
    ),
)


OPTIONAL_TABLES: tuple[TableSpec, ...] = (
    # Atlas-safe optional tables only. Full local tables such as
    # dropoff_zone_hour_features, zone_balance_hour, and forecast_zone_hour are
    # intentionally not exported to MongoDB because they are too large for the
    # 512MB Atlas tier. Keep them as local CSV/Parquet files for offline use.
    TableSpec(
        "top_zones_by_hour",
        PROJECT_ROOT / "outputs" / "tables" / "temporal" / "csv" / "top_zones_by_hour",
        "spatial_top_zones_by_hour",
        (("hour", "PULocationID"), ("hour", "zone_rank_in_hour")),
    ),
    TableSpec(
        "weekday_weekend_hourly",
        PROJECT_ROOT / "outputs" / "tables" / "temporal" / "csv" / "weekday_weekend_hourly",
        "temporal_weekday_weekend_hourly",
        (("day_type", "hour"),),
    ),
    TableSpec(
        "month_of_year_pattern",
        PROJECT_ROOT / "outputs" / "tables" / "temporal_enriched" / "csv" / "month_of_year_pattern",
        "temporal_month_of_year_pattern",
        (("month",),),
    ),
    TableSpec(
        "revenue_efficiency_temporal",
        PROJECT_ROOT
        / "outputs"
        / "tables"
        / "temporal_enriched"
        / "csv"
        / "revenue_efficiency_temporal",
        "temporal_revenue_efficiency",
        (("year", "month"), ("year_month",)),
    ),
    TableSpec(
        "payment_type_summary",
        PROJECT_ROOT / "outputs" / "tables" / "trip_route_analytics" / "csv" / "payment_type_summary",
        "business_payment_type_summary",
        (("payment_type",),),
    ),
    TableSpec(
        "payment_type_by_year",
        PROJECT_ROOT / "outputs" / "tables" / "trip_route_analytics" / "csv" / "payment_type_by_year",
        "business_payment_type_by_year",
        (("year", "payment_type"),),
    ),
    TableSpec(
        "trip_behavior_by_hour",
        PROJECT_ROOT / "outputs" / "tables" / "trip_route_analytics" / "csv" / "trip_behavior_by_hour",
        "business_trip_behavior_by_hour",
        (("hour",),),
    ),
    TableSpec(
        "forecast_model_comparison",
        PROJECT_ROOT / "outputs" / "tables" / "model_comparison.csv",
        "forecast_model_comparison",
        (("model",), ("is_best_model",)),
    ),
    TableSpec(
        "forecast_model_evaluation_metrics",
        PROJECT_ROOT / "outputs" / "tables" / "model_evaluation_metrics.csv",
        "forecast_model_evaluation_metrics",
        (("model",), ("is_best_model",)),
    ),
    TableSpec(
        "forecast_monthly_actual_predicted",
        PROJECT_ROOT / "outputs" / "predictions" / "plot_actual_vs_predicted_monthly.csv",
        "forecast_monthly_actual_predicted",
        (("year_month",),),
    ),
    TableSpec(
        "forecast_daily_actual_predicted",
        PROJECT_ROOT / "outputs" / "predictions" / "forecast_daily_actual_predicted.csv",
        "forecast_daily_actual_predicted",
        (("pickup_date",), ("year", "month")),
    ),
    TableSpec(
        "forecast_error_by_hour",
        PROJECT_ROOT / "outputs" / "predictions" / "forecast_error_by_hour.csv",
        "forecast_error_by_hour",
        (("hour",),),
    ),
    TableSpec(
        "forecast_error_by_borough",
        PROJECT_ROOT / "outputs" / "predictions" / "forecast_error_by_borough.csv",
        "forecast_error_by_borough",
        (("pickup_borough",),),
    ),
    TableSpec(
        "forecast_zone_accuracy",
        PROJECT_ROOT / "outputs" / "predictions" / "forecast_zone_accuracy.csv",
        "forecast_zone_accuracy",
        (("PULocationID",), ("pickup_borough",), ("aggregate_absolute_error",)),
    ),
    TableSpec(
        "forecast_weekday_hour_error_heatmap",
        PROJECT_ROOT / "outputs" / "predictions" / "forecast_weekday_hour_error_heatmap.csv",
        "forecast_weekday_hour_error_heatmap",
        (("day_of_week", "hour"),),
    ),
    TableSpec(
        "zone_centroids",
        PROJECT_ROOT / "outputs" / "tables" / "map" / "csv" / "zone_centroids.csv",
        "map_zone_centroids",
        (("LocationID",), ("borough",)),
    ),
    TableSpec(
        "od_flow_hour",
        PROJECT_ROOT / "outputs" / "tables" / "map" / "csv" / "od_flow_hour",
        "map_od_flow_hour",
        (("hour", "PULocationID", "DOLocationID"), ("hour", "route_rank_in_hour")),
    ),
    TableSpec(
        "od_flow_year_month",
        PROJECT_ROOT / "outputs" / "tables" / "map" / "csv" / "od_flow_year_month",
        "map_od_flow_year_month",
        (("year_month", "PULocationID", "DOLocationID"), ("year_month", "route_rank_in_year_month")),
    ),
    TableSpec(
        "map_replay_sample",
        PROJECT_ROOT / "outputs" / "tables" / "map" / "csv" / "map_replay_sample",
        "map_replay_sample",
        (("pickup_date", "hour"), ("PULocationID",), ("DOLocationID",)),
    ),
    TableSpec(
        "zone_profile_summary",
        PROJECT_ROOT / "outputs" / "tables" / "profiles" / "csv" / "zone_profile_summary",
        "profile_zone_summary",
        (("PULocationID",), ("pickup_borough",), ("total_trips",)),
    ),
    TableSpec(
        "route_profile_summary",
        PROJECT_ROOT / "outputs" / "tables" / "profiles" / "csv" / "route_profile_summary",
        "profile_route_summary",
        (("route_rank",), ("PULocationID", "DOLocationID"), ("route_key",)),
    ),
)


DATE_LIKE_COLUMNS = {
    "activity_date",
    "dropoff_date",
    "pickup_date",
    "start_date",
    "end_date",
    "first_seen_date",
    "last_seen_date",
    "year_month",
}


def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


def find_csv_sources(path: Path) -> list[Path]:
    if path.is_file():
        return [path]

    if path.is_dir():
        return sorted(path.glob("part-*.csv"))

    return []


def coerce_value(column: str, value: str) -> Any:
    cleaned = value.strip()
    if cleaned == "":
        return None

    if column in DATE_LIKE_COLUMNS:
        return cleaned

    try:
        if any(char in cleaned for char in (".", "e", "E")):
            return float(cleaned)
        return int(cleaned)
    except ValueError:
        return cleaned


def read_csv_documents(spec: TableSpec) -> list[dict[str, Any]]:
    sources = find_csv_sources(spec.source)
    if not sources:
        raise FileNotFoundError(f"No CSV part files found for {spec.key}: {spec.source}")

    documents: list[dict[str, Any]] = []

    for source in sources:
        with source.open("r", encoding="utf-8-sig", newline="") as input_file:
            reader = csv.DictReader(input_file)
            for row in reader:
                documents.append(
                    {
                        column: coerce_value(column, value)
                        for column, value in row.items()
                    }
                )

    return documents


def selected_specs(include_optional: bool, only: list[str] | None) -> list[TableSpec]:
    specs = list(DEFAULT_TABLES)
    if include_optional:
        specs.extend(OPTIONAL_TABLES)

    if only:
        wanted = set(only)
        specs = [spec for spec in specs if spec.key in wanted or spec.collection in wanted]
        missing = wanted - {spec.key for spec in specs} - {spec.collection for spec in specs}
        if missing:
            raise ValueError(f"Unknown table key or collection: {', '.join(sorted(missing))}")

    return specs


def get_database(uri: str, db_name: str):
    try:
        from pymongo import MongoClient
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency: pymongo. Install it with "
            "`venv\\Scripts\\pip.exe install pymongo`."
        ) from exc

    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    return client[db_name]


def replace_collection(db, spec: TableSpec, documents: list[dict[str, Any]], batch_size: int) -> None:
    collection = db[spec.collection]
    collection.delete_many({})

    imported_at = datetime.now(timezone.utc)
    source = str(spec.source.relative_to(PROJECT_ROOT))

    enriched_documents = []
    for document in documents:
        document["_source_table"] = spec.key
        document["_source_path"] = source
        document["_imported_at"] = imported_at
        enriched_documents.append(document)

        if len(enriched_documents) >= batch_size:
            collection.insert_many(enriched_documents, ordered=False)
            enriched_documents.clear()

    if enriched_documents:
        collection.insert_many(enriched_documents, ordered=False)

    for index_cols in spec.indexes:
        collection.create_index([(col_name, 1) for col_name in index_cols])


def main() -> None:
    parser = argparse.ArgumentParser(description="Export analytics CSV outputs to MongoDB Atlas")
    parser.add_argument("--uri", default=None, help="MongoDB Atlas connection URI.")
    parser.add_argument("--db", default=None, help="MongoDB database name.")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=PROJECT_ROOT / ".env",
        help="Optional .env file containing MONGODB_URI and MONGODB_DB.",
    )
    parser.add_argument(
        "--include-optional",
        action="store_true",
        help="Also export optional dashboard tables.",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        help="Export only these table keys or collection names.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read CSV files and print the planned export without connecting to MongoDB.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="MongoDB insert batch size.",
    )

    args = parser.parse_args()

    load_dotenv_file(args.env_file)
    uri = args.uri or os.environ.get("MONGODB_URI")
    db_name = args.db or os.environ.get("MONGODB_DB") or "nyc_taxi_analytics"
    specs = selected_specs(args.include_optional, args.only)

    prepared: list[tuple[TableSpec, list[dict[str, Any]]]] = []
    for spec in specs:
        documents = read_csv_documents(spec)
        prepared.append((spec, documents))
        print(
            f"{spec.key:32} -> {spec.collection:38} "
            f"{len(documents):6} docs from {spec.source.relative_to(PROJECT_ROOT)}"
        )

    if args.dry_run:
        print("\nDry run completed. No MongoDB connection was opened.")
        return

    if not uri:
        raise ValueError(
            "MongoDB URI is required. Set MONGODB_URI in .env/environment "
            "or pass --uri."
        )

    db = get_database(uri, db_name)
    print(f"\nConnected to MongoDB database: {db_name}")

    for spec, documents in prepared:
        replace_collection(db, spec, documents, args.batch_size)
        print(f"replaced {spec.collection} ({len(documents)} docs)")

    print("\nMongoDB export completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise
