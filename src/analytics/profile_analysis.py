"""Build zone and route profile summary tables for map detail panels."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from config.config import (  # noqa: E402
    ANALYTICS_APP_NAME,
    DROPOFF_ID_COL,
    FARE_AMOUNT_COL,
    PASSENGER_COUNT_COL,
    PICKUP_ID_COL,
    TABLES_DIR,
    TOTAL_AMOUNT_COL,
    TRIP_DISTANCE_COL,
    TRIP_ENRICHED_PATH,
    ZONE_HOUR_FEATURES_PATH,
)
from src.ingestion.load_raw_data import create_spark_session  # noqa: E402


OUTPUT_DIR = TABLES_DIR / "profiles"
PARQUET_OUTPUT_DIR = OUTPUT_DIR / "parquet"
CSV_OUTPUT_DIR = OUTPUT_DIR / "csv"


ZONE_HOUR_REQUIRED_COLUMNS = [
    PICKUP_ID_COL,
    "pickup_zone",
    "pickup_borough",
    "pickup_service_zone",
    "pickup_date",
    "year",
    "hour",
    "trip_count",
    "total_revenue",
    "total_fare_amount",
    "total_trip_distance",
    "total_trip_duration_min",
    "total_passenger_count",
    "credit_card_trip_count",
    "cash_trip_count",
]


TRIP_REQUIRED_COLUMNS = [
    "pickup_ts",
    "dropoff_ts",
    "pickup_date",
    "year",
    "hour",
    PICKUP_ID_COL,
    "pickup_zone",
    "pickup_borough",
    "pickup_service_zone",
    DROPOFF_ID_COL,
    "dropoff_zone",
    "dropoff_borough",
    "dropoff_service_zone",
    "route_key",
    "route_name",
    "trip_duration_min",
    TRIP_DISTANCE_COL,
    FARE_AMOUNT_COL,
    TOTAL_AMOUNT_COL,
    "tip_amount",
    PASSENGER_COUNT_COL,
    "payment_type",
]


def validate_input_schema(df: DataFrame, required_cols: list[str], table_name: str) -> None:
    missing_cols = [col_name for col_name in required_cols if col_name not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing columns in {table_name}: {', '.join(missing_cols)}. "
            "Please rerun the upstream pipeline first."
        )


def add_zone_weighted_metrics(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "avg_revenue_per_trip",
            F.when(F.col("total_trips") > 0, F.col("total_revenue") / F.col("total_trips")),
        )
        .withColumn(
            "avg_fare_amount",
            F.when(F.col("total_trips") > 0, F.col("total_fare_amount") / F.col("total_trips")),
        )
        .withColumn(
            "avg_trip_distance",
            F.when(F.col("total_trips") > 0, F.col("total_trip_distance") / F.col("total_trips")),
        )
        .withColumn(
            "avg_trip_duration_min",
            F.when(F.col("total_trips") > 0, F.col("total_trip_duration_min") / F.col("total_trips")),
        )
        .withColumn(
            "avg_passenger_count",
            F.when(F.col("total_trips") > 0, F.col("total_passenger_count") / F.col("total_trips")),
        )
        .withColumn(
            "credit_card_share",
            F.when(F.col("total_trips") > 0, F.col("credit_card_trip_count") / F.col("total_trips")),
        )
        .withColumn(
            "cash_share",
            F.when(F.col("total_trips") > 0, F.col("cash_trip_count") / F.col("total_trips")),
        )
        .withColumn(
            "avg_trips_per_active_day",
            F.when(F.col("active_days") > 0, F.col("total_trips") / F.col("active_days")),
        )
    )


def add_trip_metrics(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "trip_speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.col(TRIP_DISTANCE_COL) / (F.col("trip_duration_min") / 60.0),
            ),
        )
        .withColumn(
            "tip_share_of_total",
            F.when(F.col(TOTAL_AMOUNT_COL) > 0, F.col("tip_amount") / F.col(TOTAL_AMOUNT_COL)),
        )
        .withColumn("is_credit_card", F.when(F.col("payment_type") == 1, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_cash", F.when(F.col("payment_type") == 2, F.lit(1)).otherwise(F.lit(0)))
    )


def add_route_weighted_metrics(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "avg_revenue_per_trip",
            F.when(F.col("trip_count") > 0, F.col("total_revenue") / F.col("trip_count")),
        )
        .withColumn(
            "avg_fare_amount",
            F.when(F.col("trip_count") > 0, F.col("total_fare_amount") / F.col("trip_count")),
        )
        .withColumn(
            "avg_trip_distance",
            F.when(F.col("trip_count") > 0, F.col("total_trip_distance") / F.col("trip_count")),
        )
        .withColumn(
            "avg_trip_duration_min",
            F.when(F.col("trip_count") > 0, F.col("total_trip_duration_min") / F.col("trip_count")),
        )
        .withColumn(
            "avg_tip_amount",
            F.when(F.col("trip_count") > 0, F.col("total_tip_amount") / F.col("trip_count")),
        )
        .withColumn(
            "avg_tip_share_of_total",
            F.when(F.col("trip_count") > 0, F.col("total_tip_share_of_total") / F.col("trip_count")),
        )
        .withColumn(
            "avg_speed_mph",
            F.when(F.col("trip_count") > 0, F.col("total_speed_mph") / F.col("trip_count")),
        )
        .withColumn(
            "credit_card_share",
            F.when(F.col("trip_count") > 0, F.col("credit_card_trip_count") / F.col("trip_count")),
        )
        .withColumn(
            "cash_share",
            F.when(F.col("trip_count") > 0, F.col("cash_trip_count") / F.col("trip_count")),
        )
        .withColumn(
            "avg_trips_per_active_day",
            F.when(F.col("active_days") > 0, F.col("trip_count") / F.col("active_days")),
        )
    )


def build_zone_peak_hour(zone_hour_df: DataFrame) -> DataFrame:
    hourly = (
        zone_hour_df.groupBy(PICKUP_ID_COL, "hour")
        .agg(F.sum("trip_count").alias("peak_hour_trip_count"))
    )
    rank_window = Window.partitionBy(PICKUP_ID_COL).orderBy(F.desc("peak_hour_trip_count"), F.asc("hour"))
    return (
        hourly.withColumn("hour_rank", F.row_number().over(rank_window))
        .filter(F.col("hour_rank") == 1)
        .select(PICKUP_ID_COL, F.col("hour").alias("peak_hour"), "peak_hour_trip_count")
    )


def build_zone_top_destination(trip_df: DataFrame) -> DataFrame:
    destinations = (
        trip_df.filter(F.col(PICKUP_ID_COL).isNotNull())
        .filter(F.col(DROPOFF_ID_COL).isNotNull())
        .groupBy(PICKUP_ID_COL, DROPOFF_ID_COL, "dropoff_zone", "dropoff_borough")
        .agg(F.count("*").alias("top_dropoff_trip_count"))
    )
    rank_window = Window.partitionBy(PICKUP_ID_COL).orderBy(F.desc("top_dropoff_trip_count"))
    return (
        destinations.withColumn("destination_rank", F.row_number().over(rank_window))
        .filter(F.col("destination_rank") == 1)
        .select(
            PICKUP_ID_COL,
            F.col(DROPOFF_ID_COL).alias("top_dropoff_location_id"),
            F.col("dropoff_zone").alias("top_dropoff_zone"),
            F.col("dropoff_borough").alias("top_dropoff_borough"),
            "top_dropoff_trip_count",
        )
    )


def build_zone_profile_summary(zone_hour_df: DataFrame, trip_df: DataFrame) -> DataFrame:
    base = (
        zone_hour_df.groupBy(PICKUP_ID_COL, "pickup_zone", "pickup_borough", "pickup_service_zone")
        .agg(
            F.sum("trip_count").alias("total_trips"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("total_fare_amount").alias("total_fare_amount"),
            F.sum("total_trip_distance").alias("total_trip_distance"),
            F.sum("total_trip_duration_min").alias("total_trip_duration_min"),
            F.sum("total_passenger_count").alias("total_passenger_count"),
            F.sum("credit_card_trip_count").alias("credit_card_trip_count"),
            F.sum("cash_trip_count").alias("cash_trip_count"),
            F.min("pickup_date").alias("first_seen_date"),
            F.max("pickup_date").alias("last_seen_date"),
            F.countDistinct("pickup_date").alias("active_days"),
            F.countDistinct("pickup_date", "hour").alias("active_zone_hours"),
        )
    )

    return (
        add_zone_weighted_metrics(base)
        .join(build_zone_peak_hour(zone_hour_df), on=PICKUP_ID_COL, how="left")
        .join(build_zone_top_destination(trip_df), on=PICKUP_ID_COL, how="left")
        .orderBy(F.desc("total_trips"))
    )


def build_route_peak_hour(trip_df: DataFrame) -> DataFrame:
    hourly = (
        trip_df.groupBy("route_key", "hour")
        .agg(F.count("*").alias("peak_hour_trip_count"))
    )
    rank_window = Window.partitionBy("route_key").orderBy(F.desc("peak_hour_trip_count"), F.asc("hour"))
    return (
        hourly.withColumn("hour_rank", F.row_number().over(rank_window))
        .filter(F.col("hour_rank") == 1)
        .select("route_key", F.col("hour").alias("peak_hour"), "peak_hour_trip_count")
    )


def build_route_profile_summary(trip_df: DataFrame, route_top_n: int) -> DataFrame:
    df = add_trip_metrics(trip_df).filter(F.col("route_key").isNotNull())
    base = (
        df.groupBy(
            PICKUP_ID_COL,
            "pickup_zone",
            "pickup_borough",
            "pickup_service_zone",
            DROPOFF_ID_COL,
            "dropoff_zone",
            "dropoff_borough",
            "dropoff_service_zone",
            "route_key",
            "route_name",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.sum(TOTAL_AMOUNT_COL).alias("total_revenue"),
            F.sum(FARE_AMOUNT_COL).alias("total_fare_amount"),
            F.sum(TRIP_DISTANCE_COL).alias("total_trip_distance"),
            F.sum("trip_duration_min").alias("total_trip_duration_min"),
            F.sum("tip_amount").alias("total_tip_amount"),
            F.sum("tip_share_of_total").alias("total_tip_share_of_total"),
            F.sum("trip_speed_mph").alias("total_speed_mph"),
            F.sum(PASSENGER_COUNT_COL).alias("total_passenger_count"),
            F.sum("is_credit_card").alias("credit_card_trip_count"),
            F.sum("is_cash").alias("cash_trip_count"),
            F.min("pickup_date").alias("first_seen_date"),
            F.max("pickup_date").alias("last_seen_date"),
            F.countDistinct("pickup_date").alias("active_days"),
        )
    )
    rank_window = Window.orderBy(F.desc("trip_count"), F.desc("total_revenue"))

    return (
        add_route_weighted_metrics(base)
        .withColumn("route_rank", F.dense_rank().over(rank_window))
        .filter(F.col("route_rank") <= route_top_n)
        .join(build_route_peak_hour(df), on="route_key", how="left")
        .orderBy("route_rank")
    )


def write_output_table(df: DataFrame, table_name: str, write_csv: bool) -> None:
    PARQUET_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = PARQUET_OUTPUT_DIR / table_name
    df.write.mode("overwrite").parquet(str(parquet_path))
    print(f"Parquet saved: {parquet_path}")

    if write_csv:
        CSV_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = CSV_OUTPUT_DIR / table_name
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(csv_path))
        print(f"CSV saved: {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build zone and route profile tables")
    parser.add_argument("--trip-input", default=TRIP_ENRICHED_PATH)
    parser.add_argument("--zone-hour-input", default=ZONE_HOUR_FEATURES_PATH)
    parser.add_argument("--route-top-n", type=int, default=1000)
    parser.add_argument("--write-csv", action="store_true")
    args = parser.parse_args()

    spark = create_spark_session(f"{ANALYTICS_APP_NAME} - Profiles")
    trip_df = spark.read.parquet(args.trip_input)
    zone_hour_df = spark.read.parquet(args.zone_hour_input)

    validate_input_schema(trip_df, TRIP_REQUIRED_COLUMNS, "trip_enriched")
    validate_input_schema(zone_hour_df, ZONE_HOUR_REQUIRED_COLUMNS, "zone_hour_features")

    output_tables = {
        "zone_profile_summary": build_zone_profile_summary(zone_hour_df, trip_df),
        "route_profile_summary": build_route_profile_summary(trip_df, args.route_top_n),
    }

    for table_name, table_df in output_tables.items():
        print(f"\n===== Writing {table_name} =====")
        table_df.show(10, truncate=False)
        write_output_table(table_df, table_name, args.write_csv)

    spark.stop()
    print("\nProfile analytics completed successfully.")


if __name__ == "__main__":
    main()
