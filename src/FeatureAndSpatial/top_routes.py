from pathlib import Path
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from config.config import (
    TRIP_ENRICHED_PATH,
    TOP_ROUTES_PATH,
    FEATURE_APP_NAME,
    TABLES_DIR,
    PICKUP_ID_COL,
    DROPOFF_ID_COL,
    TRIP_DISTANCE_COL,
    PASSENGER_COUNT_COL,
    FARE_AMOUNT_COL,
    TOTAL_AMOUNT_COL,
)
from src.ingestion.load_raw_data import create_spark_session


def prepare_trip_level_flags(df: DataFrame) -> DataFrame:
    """
    Add lightweight helper flags before route aggregation.
    """
    return (
        df
        .withColumn(
            "is_credit_card",
            F.when(F.col("payment_type") == 1, F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "is_cash",
            F.when(F.col("payment_type") == 2, F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "trip_speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.col(TRIP_DISTANCE_COL) / (F.col("trip_duration_min") / 60.0)
            )
        )
    )


def build_top_routes(trip_enriched_df: DataFrame) -> DataFrame:
    """
    Build route-level aggregated table across the whole dataset.

    One row = one pickup -> dropoff route.
    """
    df = prepare_trip_level_flags(trip_enriched_df)

    # 只保留 route 聚合真正需要的列
    df = df.select(
        PICKUP_ID_COL,
        DROPOFF_ID_COL,
        "pickup_zone",
        "pickup_borough",
        "pickup_service_zone",
        "dropoff_zone",
        "dropoff_borough",
        "dropoff_service_zone",
        "route_key",
        "route_name",
        "pickup_date",
        TRIP_DISTANCE_COL,
        "trip_duration_min",
        FARE_AMOUNT_COL,
        TOTAL_AMOUNT_COL,
        "tip_amount",
        "tolls_amount",
        "airport_fee",
        PASSENGER_COUNT_COL,
        "trip_speed_mph",
        "is_credit_card",
        "is_cash",
    )

    # 过滤关键 route 字段为空的情况
    df = (
        df
        .filter(F.col(PICKUP_ID_COL).isNotNull())
        .filter(F.col(DROPOFF_ID_COL).isNotNull())
        .filter(F.col("route_key").isNotNull())
    )

    routes_df = (
        df.groupBy(
            PICKUP_ID_COL,
            DROPOFF_ID_COL,
            "pickup_zone",
            "pickup_borough",
            "pickup_service_zone",
            "dropoff_zone",
            "dropoff_borough",
            "dropoff_service_zone",
            "route_key",
            "route_name",
        )
        .agg(
            F.count("*").alias("trip_count"),

            F.min("pickup_date").alias("first_seen_date"),
            F.max("pickup_date").alias("last_seen_date"),
            F.countDistinct("pickup_date").alias("active_days"),

            F.avg(TRIP_DISTANCE_COL).alias("avg_trip_distance"),
            F.sum(TRIP_DISTANCE_COL).alias("total_trip_distance"),

            F.avg("trip_duration_min").alias("avg_trip_duration_min"),
            F.sum("trip_duration_min").alias("total_trip_duration_min"),

            F.avg(FARE_AMOUNT_COL).alias("avg_fare_amount"),
            F.sum(FARE_AMOUNT_COL).alias("total_fare_amount"),

            F.avg(TOTAL_AMOUNT_COL).alias("avg_total_amount"),
            F.sum(TOTAL_AMOUNT_COL).alias("total_revenue"),

            F.avg("tip_amount").alias("avg_tip_amount"),
            F.sum("tip_amount").alias("total_tip_amount"),

            F.sum("tolls_amount").alias("total_tolls_amount"),
            F.sum("airport_fee").alias("total_airport_fee"),

            F.avg(PASSENGER_COUNT_COL).alias("avg_passenger_count"),
            F.sum(PASSENGER_COUNT_COL).alias("total_passenger_count"),

            F.avg("trip_speed_mph").alias("avg_speed_mph"),

            F.sum("is_credit_card").alias("credit_card_trip_count"),
            F.sum("is_cash").alias("cash_trip_count"),
        )
    )

    routes_df = (
        routes_df
        .withColumn(
            "credit_card_share",
            F.when(F.col("trip_count") > 0,
                   F.col("credit_card_trip_count") / F.col("trip_count"))
        )
        .withColumn(
            "cash_share",
            F.when(F.col("trip_count") > 0,
                   F.col("cash_trip_count") / F.col("trip_count"))
        )
        .withColumn(
            "avg_trips_per_active_day",
            F.when(F.col("active_days") > 0,
                   F.col("trip_count") / F.col("active_days"))
        )
    )

    # 排名：最常用的就是按 trip_count 排
    rank_window = Window.orderBy(F.desc("trip_count"), F.desc("total_revenue"))
    routes_df = routes_df.withColumn("route_rank", F.dense_rank().over(rank_window))

    return reorder_columns(routes_df)


def reorder_columns(df: DataFrame) -> DataFrame:
    preferred_order = [
        "route_rank",
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

        "trip_count",
        "active_days",
        "avg_trips_per_active_day",
        "first_seen_date",
        "last_seen_date",

        "avg_trip_distance",
        "total_trip_distance",

        "avg_trip_duration_min",
        "total_trip_duration_min",

        "avg_fare_amount",
        "total_fare_amount",

        "avg_total_amount",
        "total_revenue",

        "avg_tip_amount",
        "total_tip_amount",

        "total_tolls_amount",
        "total_airport_fee",

        "avg_passenger_count",
        "total_passenger_count",

        "avg_speed_mph",

        "credit_card_trip_count",
        "cash_trip_count",
        "credit_card_share",
        "cash_share",
    ]

    existing_cols = df.columns
    ordered_cols = [c for c in preferred_order if c in existing_cols]
    remaining_cols = [c for c in existing_cols if c not in ordered_cols]

    return df.select(*ordered_cols, *remaining_cols)


def export_sample_csv(routes_df: DataFrame) -> None:
    """
    Export top-ranked routes as a readable sample CSV.
    """
    try:
        import pandas as pd

        TABLES_DIR.mkdir(parents=True, exist_ok=True)

        sample_pdf = (
            routes_df
            .orderBy("route_rank")
            .limit(100)
            .toPandas()
        )

        float_cols = [
            "avg_trips_per_active_day",
            "avg_trip_distance",
            "total_trip_distance",
            "avg_trip_duration_min",
            "total_trip_duration_min",
            "avg_fare_amount",
            "total_fare_amount",
            "avg_total_amount",
            "total_revenue",
            "avg_tip_amount",
            "total_tip_amount",
            "total_tolls_amount",
            "total_airport_fee",
            "avg_passenger_count",
            "total_passenger_count",
            "avg_speed_mph",
            "credit_card_share",
            "cash_share",
        ]

        for col in float_cols:
            if col in sample_pdf.columns:
                sample_pdf[col] = sample_pdf[col].round(4)

        sample_path = TABLES_DIR / "top_routes_sample.csv"
        sample_pdf.to_csv(sample_path, index=False)

        print(f"\nSample CSV saved to: {sample_path}")

    except Exception as e:
        print(f"\nSkipping sample CSV export: {e}")


def main() -> None:
    spark = create_spark_session(FEATURE_APP_NAME)

    trip_enriched_df = spark.read.parquet(TRIP_ENRICHED_PATH)

    top_routes_df = build_top_routes(trip_enriched_df)

    print("\n===== top_routes schema =====")
    top_routes_df.printSchema()

    print("\n===== top_routes preview =====")
    top_routes_df.orderBy("route_rank").show(30, truncate=False)

    top_routes_df.write.mode("overwrite").parquet(TOP_ROUTES_PATH)
    print(f"\ntop_routes saved to: {TOP_ROUTES_PATH}")

    export_sample_csv(top_routes_df)

    spark.stop()


if __name__ == "__main__":
    main()