"""Build map-ready flow and balance analytics tables.

Outputs are derived from the processed trip-level table and the existing
pickup zone-hour feature table. They support pickup/dropoff maps, OD flow maps,
zone balance maps, and approximate replay layers.
"""

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


OUTPUT_DIR = TABLES_DIR / "map"
PARQUET_OUTPUT_DIR = OUTPUT_DIR / "parquet"
CSV_OUTPUT_DIR = OUTPUT_DIR / "csv"
DEFAULT_CENTROIDS_CSV = CSV_OUTPUT_DIR / "zone_centroids.csv"


TRIP_REQUIRED_COLUMNS = [
    "pickup_ts",
    "dropoff_ts",
    "pickup_date",
    "year",
    "month",
    "year_month",
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


ZONE_HOUR_REQUIRED_COLUMNS = [
    PICKUP_ID_COL,
    "pickup_zone",
    "pickup_borough",
    "pickup_service_zone",
    "pickup_date",
    "year",
    "month",
    "day",
    "year_month",
    "day_of_week",
    "weekday_name",
    "is_weekend",
    "hour",
    "trip_count",
    "total_revenue",
]


def validate_input_schema(df: DataFrame, required_cols: list[str], table_name: str) -> None:
    missing_cols = [col_name for col_name in required_cols if col_name not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing columns in {table_name}: {', '.join(missing_cols)}. "
            "Please rerun the upstream pipeline first."
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


def add_weighted_trip_metrics(df: DataFrame, count_col: str = "trip_count") -> DataFrame:
    return (
        df.withColumn(
            "avg_revenue_per_trip",
            F.when(F.col(count_col) > 0, F.col("total_revenue") / F.col(count_col)),
        )
        .withColumn(
            "avg_fare_amount",
            F.when(F.col(count_col) > 0, F.col("total_fare_amount") / F.col(count_col)),
        )
        .withColumn(
            "avg_trip_distance",
            F.when(F.col(count_col) > 0, F.col("total_trip_distance") / F.col(count_col)),
        )
        .withColumn(
            "avg_trip_duration_min",
            F.when(F.col(count_col) > 0, F.col("total_trip_duration_min") / F.col(count_col)),
        )
        .withColumn(
            "avg_passenger_count",
            F.when(F.col(count_col) > 0, F.col("total_passenger_count") / F.col(count_col)),
        )
        .withColumn(
            "avg_speed_mph",
            F.when(F.col(count_col) > 0, F.col("total_speed_mph") / F.col(count_col)),
        )
        .withColumn(
            "avg_tip_share_of_total",
            F.when(F.col(count_col) > 0, F.col("total_tip_share_of_total") / F.col(count_col)),
        )
        .withColumn(
            "credit_card_share",
            F.when(F.col(count_col) > 0, F.col("credit_card_trip_count") / F.col(count_col)),
        )
        .withColumn(
            "cash_share",
            F.when(F.col(count_col) > 0, F.col("cash_trip_count") / F.col(count_col)),
        )
    )


def aggregate_trip_metrics(df: DataFrame, group_cols: list[str], active_date_col: str) -> DataFrame:
    aggregated = (
        df.groupBy(*group_cols)
        .agg(
            F.count("*").alias("trip_count"),
            F.sum(TOTAL_AMOUNT_COL).alias("total_revenue"),
            F.sum(FARE_AMOUNT_COL).alias("total_fare_amount"),
            F.sum(TRIP_DISTANCE_COL).alias("total_trip_distance"),
            F.sum("trip_duration_min").alias("total_trip_duration_min"),
            F.sum("tip_amount").alias("total_tip_amount"),
            F.sum("tip_share_of_total").alias("total_tip_share_of_total"),
            F.sum(PASSENGER_COUNT_COL).alias("total_passenger_count"),
            F.sum("trip_speed_mph").alias("total_speed_mph"),
            F.sum("is_credit_card").alias("credit_card_trip_count"),
            F.sum("is_cash").alias("cash_trip_count"),
            F.countDistinct(active_date_col).alias("active_days"),
        )
    )
    return add_weighted_trip_metrics(aggregated)


def add_date_fields(df: DataFrame, date_col: str) -> DataFrame:
    return (
        df.withColumn("year", F.year(date_col))
        .withColumn("month", F.month(date_col))
        .withColumn("day", F.dayofmonth(date_col))
        .withColumn("year_month", F.date_format(date_col, "yyyy-MM"))
        .withColumn("day_of_week", F.dayofweek(date_col))
        .withColumn("weekday_name", F.date_format(date_col, "E"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek(date_col).isin([1, 7]), F.lit(1)).otherwise(F.lit(0)),
        )
    )


def build_dropoff_zone_hour_features(trip_df: DataFrame) -> DataFrame:
    df = (
        add_trip_metrics(trip_df)
        .withColumn("dropoff_date", F.to_date("dropoff_ts"))
        .withColumn("hour", F.hour("dropoff_ts"))
        .filter(F.col(DROPOFF_ID_COL).isNotNull())
        .filter(F.col("dropoff_date").isNotNull())
        .filter(F.col("hour").isNotNull())
    )

    aggregated = aggregate_trip_metrics(
        df,
        [
            DROPOFF_ID_COL,
            "dropoff_zone",
            "dropoff_borough",
            "dropoff_service_zone",
            "dropoff_date",
            "hour",
        ],
        "dropoff_date",
    )

    ordered_cols = [
        DROPOFF_ID_COL,
        "dropoff_zone",
        "dropoff_borough",
        "dropoff_service_zone",
        "dropoff_date",
        "year",
        "month",
        "day",
        "year_month",
        "day_of_week",
        "weekday_name",
        "is_weekend",
        "hour",
        "trip_count",
        "total_revenue",
        "avg_revenue_per_trip",
        "avg_fare_amount",
        "avg_trip_distance",
        "avg_trip_duration_min",
        "avg_speed_mph",
        "credit_card_share",
        "cash_share",
    ]

    with_dates = add_date_fields(aggregated, "dropoff_date")
    return with_dates.select(*[col_name for col_name in ordered_cols if col_name in with_dates.columns])


def build_zone_balance_hour(pickup_df: DataFrame, dropoff_df: DataFrame) -> DataFrame:
    pickup = pickup_df.select(
        F.col(PICKUP_ID_COL).alias("LocationID"),
        F.col("pickup_zone").alias("pickup_zone_name"),
        F.col("pickup_borough").alias("pickup_borough_name"),
        F.col("pickup_service_zone").alias("pickup_service_zone_name"),
        F.col("pickup_date").alias("activity_date"),
        F.col("year").alias("pickup_year"),
        F.col("month").alias("pickup_month"),
        F.col("day").alias("pickup_day"),
        F.col("year_month").alias("pickup_year_month"),
        F.col("day_of_week").alias("pickup_day_of_week"),
        F.col("weekday_name").alias("pickup_weekday_name"),
        F.col("is_weekend").alias("pickup_is_weekend"),
        "hour",
        F.col("trip_count").alias("pickup_trip_count"),
        F.col("total_revenue").alias("pickup_revenue"),
    )

    dropoff = dropoff_df.select(
        F.col(DROPOFF_ID_COL).alias("LocationID"),
        F.col("dropoff_zone").alias("dropoff_zone_name"),
        F.col("dropoff_borough").alias("dropoff_borough_name"),
        F.col("dropoff_service_zone").alias("dropoff_service_zone_name"),
        F.col("dropoff_date").alias("activity_date"),
        F.col("year").alias("dropoff_year"),
        F.col("month").alias("dropoff_month"),
        F.col("day").alias("dropoff_day"),
        F.col("year_month").alias("dropoff_year_month"),
        F.col("day_of_week").alias("dropoff_day_of_week"),
        F.col("weekday_name").alias("dropoff_weekday_name"),
        F.col("is_weekend").alias("dropoff_is_weekend"),
        "hour",
        F.col("trip_count").alias("dropoff_trip_count"),
        F.col("total_revenue").alias("dropoff_revenue"),
    )

    balanced = pickup.join(dropoff, on=["LocationID", "activity_date", "hour"], how="full_outer")
    balanced = (
        balanced.withColumn("zone", F.coalesce("pickup_zone_name", "dropoff_zone_name"))
        .withColumn("borough", F.coalesce("pickup_borough_name", "dropoff_borough_name"))
        .withColumn("service_zone", F.coalesce("pickup_service_zone_name", "dropoff_service_zone_name"))
        .withColumn("year", F.coalesce("pickup_year", "dropoff_year"))
        .withColumn("month", F.coalesce("pickup_month", "dropoff_month"))
        .withColumn("day", F.coalesce("pickup_day", "dropoff_day"))
        .withColumn("year_month", F.coalesce("pickup_year_month", "dropoff_year_month"))
        .withColumn("day_of_week", F.coalesce("pickup_day_of_week", "dropoff_day_of_week"))
        .withColumn("weekday_name", F.coalesce("pickup_weekday_name", "dropoff_weekday_name"))
        .withColumn("is_weekend", F.coalesce("pickup_is_weekend", "dropoff_is_weekend"))
        .fillna(
            {
                "pickup_trip_count": 0,
                "dropoff_trip_count": 0,
                "pickup_revenue": 0.0,
                "dropoff_revenue": 0.0,
            }
        )
        .withColumn("total_zone_activity", F.col("pickup_trip_count") + F.col("dropoff_trip_count"))
        .withColumn("net_dropoffs_minus_pickups", F.col("dropoff_trip_count") - F.col("pickup_trip_count"))
        .withColumn("net_pickups_minus_dropoffs", F.col("pickup_trip_count") - F.col("dropoff_trip_count"))
        .withColumn(
            "balance_ratio",
            F.when(
                F.col("total_zone_activity") > 0,
                F.col("net_dropoffs_minus_pickups") / F.col("total_zone_activity"),
            ),
        )
        .withColumn(
            "dropoff_share",
            F.when(F.col("total_zone_activity") > 0, F.col("dropoff_trip_count") / F.col("total_zone_activity")),
        )
        .withColumn(
            "pickup_share",
            F.when(F.col("total_zone_activity") > 0, F.col("pickup_trip_count") / F.col("total_zone_activity")),
        )
        .withColumn(
            "balance_direction",
            F.when(F.col("balance_ratio") >= 0.1, F.lit("dropoff_heavy"))
            .when(F.col("balance_ratio") <= -0.1, F.lit("pickup_heavy"))
            .otherwise(F.lit("balanced")),
        )
    )

    return balanced.select(
        "LocationID",
        "zone",
        "borough",
        "service_zone",
        "activity_date",
        "year",
        "month",
        "day",
        "year_month",
        "day_of_week",
        "weekday_name",
        "is_weekend",
        "hour",
        "pickup_trip_count",
        "dropoff_trip_count",
        "total_zone_activity",
        "net_dropoffs_minus_pickups",
        "net_pickups_minus_dropoffs",
        "balance_ratio",
        "pickup_share",
        "dropoff_share",
        "pickup_revenue",
        "dropoff_revenue",
        "balance_direction",
    )


def build_od_flow_hour(trip_df: DataFrame, top_n: int) -> DataFrame:
    df = add_trip_metrics(trip_df).filter(F.col(PICKUP_ID_COL).isNotNull()).filter(F.col(DROPOFF_ID_COL).isNotNull())
    flows = aggregate_trip_metrics(
        df,
        [
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
        ],
        "pickup_date",
    )

    rank_window = Window.partitionBy("hour").orderBy(F.desc("trip_count"), F.desc("total_revenue"))
    return (
        flows.withColumn("route_rank_in_hour", F.dense_rank().over(rank_window))
        .filter(F.col("route_rank_in_hour") <= top_n)
        .orderBy("hour", "route_rank_in_hour")
    )


def build_od_flow_year_month(trip_df: DataFrame, top_n: int) -> DataFrame:
    df = add_trip_metrics(trip_df).filter(F.col(PICKUP_ID_COL).isNotNull()).filter(F.col(DROPOFF_ID_COL).isNotNull())
    flows = aggregate_trip_metrics(
        df,
        [
            "year",
            "month",
            "year_month",
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
        ],
        "pickup_date",
    )

    rank_window = Window.partitionBy("year_month").orderBy(F.desc("trip_count"), F.desc("total_revenue"))
    return (
        flows.withColumn("route_rank_in_year_month", F.dense_rank().over(rank_window))
        .filter(F.col("route_rank_in_year_month") <= top_n)
        .orderBy("year", "month", "route_rank_in_year_month")
    )


def load_centroids(spark, path: Path) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Centroid CSV not found: {path}. Run src/analytics/zone_centroids.py first."
        )

    return (
        spark.read.option("header", True)
        .csv(str(path))
        .select(
            F.col("LocationID").cast("int").alias("LocationID"),
            F.col("centroid_lon").cast("double").alias("centroid_lon"),
            F.col("centroid_lat").cast("double").alias("centroid_lat"),
        )
    )


def build_map_replay_sample(
    trip_df: DataFrame,
    centroids_df: DataFrame,
    sample_size: int,
    seed: int,
) -> DataFrame:
    pickup_centroids = centroids_df.select(
        F.col("LocationID").alias("pickup_join_id"),
        F.col("centroid_lon").alias("pickup_lon"),
        F.col("centroid_lat").alias("pickup_lat"),
    )
    dropoff_centroids = centroids_df.select(
        F.col("LocationID").alias("dropoff_join_id"),
        F.col("centroid_lon").alias("dropoff_lon"),
        F.col("centroid_lat").alias("dropoff_lat"),
    )

    sampled = (
        trip_df.filter(F.col(PICKUP_ID_COL).isNotNull())
        .filter(F.col(DROPOFF_ID_COL).isNotNull())
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("dropoff_ts").isNotNull())
        .orderBy(F.rand(seed))
        .limit(sample_size)
    )

    joined = (
        sampled.join(F.broadcast(pickup_centroids), sampled[PICKUP_ID_COL] == pickup_centroids["pickup_join_id"])
        .join(F.broadcast(dropoff_centroids), sampled[DROPOFF_ID_COL] == dropoff_centroids["dropoff_join_id"])
        .drop("pickup_join_id", "dropoff_join_id")
        .withColumn("pickup_epoch_ms", F.unix_timestamp("pickup_ts") * F.lit(1000))
        .withColumn("dropoff_epoch_ms", F.unix_timestamp("dropoff_ts") * F.lit(1000))
        .withColumn(
            "replay_trip_id",
            F.row_number().over(Window.orderBy("pickup_ts", PICKUP_ID_COL, DROPOFF_ID_COL)),
        )
    )

    return joined.select(
        "replay_trip_id",
        "pickup_ts",
        "dropoff_ts",
        "pickup_epoch_ms",
        "dropoff_epoch_ms",
        "pickup_date",
        "year",
        "month",
        "year_month",
        "hour",
        PICKUP_ID_COL,
        "pickup_zone",
        "pickup_borough",
        "pickup_lon",
        "pickup_lat",
        DROPOFF_ID_COL,
        "dropoff_zone",
        "dropoff_borough",
        "dropoff_lon",
        "dropoff_lat",
        "route_key",
        "route_name",
        "trip_duration_min",
        TRIP_DISTANCE_COL,
        TOTAL_AMOUNT_COL,
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
    parser = argparse.ArgumentParser(description="Build map flow analytics tables")
    parser.add_argument("--trip-input", default=TRIP_ENRICHED_PATH)
    parser.add_argument("--zone-hour-input", default=ZONE_HOUR_FEATURES_PATH)
    parser.add_argument("--centroids-csv", type=Path, default=DEFAULT_CENTROIDS_CSV)
    parser.add_argument("--write-csv", action="store_true")
    parser.add_argument("--flow-top-n", type=int, default=500)
    parser.add_argument("--replay-sample-size", type=int, default=5000)
    parser.add_argument("--replay-seed", type=int, default=42)
    parser.add_argument("--skip-replay", action="store_true")
    args = parser.parse_args()

    spark = create_spark_session(f"{ANALYTICS_APP_NAME} - Map Flow")
    trip_df = spark.read.parquet(args.trip_input)
    zone_hour_df = spark.read.parquet(args.zone_hour_input)

    validate_input_schema(trip_df, TRIP_REQUIRED_COLUMNS, "trip_enriched")
    validate_input_schema(zone_hour_df, ZONE_HOUR_REQUIRED_COLUMNS, "zone_hour_features")

    dropoff_zone_hour = build_dropoff_zone_hour_features(trip_df).cache()
    output_tables = {
        "dropoff_zone_hour_features": dropoff_zone_hour,
        "zone_balance_hour": build_zone_balance_hour(zone_hour_df, dropoff_zone_hour),
        "od_flow_hour": build_od_flow_hour(trip_df, args.flow_top_n),
        "od_flow_year_month": build_od_flow_year_month(trip_df, args.flow_top_n),
    }

    if not args.skip_replay:
        centroids_df = load_centroids(spark, args.centroids_csv)
        output_tables["map_replay_sample"] = build_map_replay_sample(
            trip_df,
            centroids_df,
            args.replay_sample_size,
            args.replay_seed,
        )

    for table_name, table_df in output_tables.items():
        print(f"\n===== Writing {table_name} =====")
        table_df.show(10, truncate=False)
        write_output_table(table_df, table_name, args.write_csv)

    spark.stop()
    print("\nMap flow analytics completed successfully.")


if __name__ == "__main__":
    main()
