from pathlib import Path
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from config.config import (
    TRIP_ENRICHED_PATH,
    ZONE_HOUR_FEATURES_PATH,
    FEATURE_APP_NAME,
    TABLES_DIR,
    PICKUP_ID_COL,
    TRIP_DISTANCE_COL,
    PASSENGER_COUNT_COL,
    FARE_AMOUNT_COL,
    TOTAL_AMOUNT_COL,
)
from src.ingestion.load_raw_data import create_spark_session


def prepare_trip_level_features(df: DataFrame) -> DataFrame:
    """
    Add a few lightweight helper fields before aggregation.
    These are still computed at trip level.
    """
    return (
        df
        .withColumn(
            "trip_speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.col(TRIP_DISTANCE_COL) / (F.col("trip_duration_min") / 60.0)
            )
        )
        .withColumn(
            "is_credit_card",
            F.when(F.col("payment_type") == 1, F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "is_cash",
            F.when(F.col("payment_type") == 2, F.lit(1)).otherwise(F.lit(0))
        )
    )


def build_zone_hour_features(trip_enriched_df: DataFrame) -> DataFrame:
    """
    Core aggregation table:
    one row = one pickup zone in one date-hour bucket.
    """
    df = prepare_trip_level_features(trip_enriched_df)

    # 只保留构建核心表真正需要的列，减少后续聚合压力
    df = df.select(
        PICKUP_ID_COL,
        "pickup_zone",
        "pickup_borough",
        "pickup_service_zone",
        "pickup_date",
        "hour",
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

    # 过滤掉关键粒度字段为空的记录
    df = df.filter(F.col(PICKUP_ID_COL).isNotNull())
    df = df.filter(F.col("pickup_date").isNotNull())
    df = df.filter(F.col("hour").isNotNull())

    zone_hour_df = (
        df.groupBy(
            PICKUP_ID_COL,
            "pickup_zone",
            "pickup_borough",
            "pickup_service_zone",
            "pickup_date",
            "hour",
        )
        .agg(
            F.count("*").alias("trip_count"),

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

    # 再从 pickup_date 派生时间字段，避免把太多字段放进 groupBy
    zone_hour_df = (
        zone_hour_df
        .withColumn("year", F.year("pickup_date"))
        .withColumn("month", F.month("pickup_date"))
        .withColumn("day", F.dayofmonth("pickup_date"))
        .withColumn("year_month", F.date_format("pickup_date", "yyyy-MM"))
        .withColumn("day_of_week", F.dayofweek("pickup_date"))
        .withColumn("weekday_name", F.date_format("pickup_date", "E"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("pickup_date").isin([1, 7]), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "credit_card_share",
            F.when(F.col("trip_count") > 0, F.col("credit_card_trip_count") / F.col("trip_count"))
        )
        .withColumn(
            "cash_share",
            F.when(F.col("trip_count") > 0, F.col("cash_trip_count") / F.col("trip_count"))
        )
    )

    return reorder_columns(zone_hour_df)


def reorder_columns(df: DataFrame) -> DataFrame:
    """
    Put dimension columns first, then metrics.
    """
    preferred_order = [
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


def export_sample_csv(zone_hour_df: DataFrame) -> None:
    """
    Export a small human-readable sample CSV for teammates / quick inspection.
    """
    try:
        import pandas as pd

        TABLES_DIR.mkdir(parents=True, exist_ok=True)

        sample_pdf = (
            zone_hour_df
            .orderBy("pickup_date", "hour", "pickup_borough", "pickup_zone")
            .limit(100)
            .toPandas()
        )

        # 让 sample 更好读
        float_cols = [
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

        sample_path = TABLES_DIR / "zone_hour_features_sample.csv"
        sample_pdf.to_csv(sample_path, index=False)

        print(f"\nSample CSV saved to: {sample_path}")

    except Exception as e:
        print(f"\nSkipping sample CSV export: {e}")


def export_full_csv(zone_hour_df: DataFrame) -> None:
    """
    Export the full zone_hour_features table as CSV for D3 / frontend use.

    Note:
    Spark writes CSV as a directory containing part-*.csv files,
    not as a single CSV file by default.
    """
    csv_output_path = TABLES_DIR / "zone_hour_features_csv"

    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    (
        zone_hour_df
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(csv_output_path))
    )

    print(f"\nFull CSV saved to: {csv_output_path}")

def main() -> None:
    spark = create_spark_session(FEATURE_APP_NAME)

    trip_enriched_df = spark.read.parquet(TRIP_ENRICHED_PATH)

    zone_hour_df = build_zone_hour_features(trip_enriched_df)

    print("\n===== zone_hour_features schema =====")
    zone_hour_df.printSchema()

    # print("\n===== zone_hour_features preview =====")
    # zone_hour_df.show(30, truncate=False)

    zone_hour_df.write.mode("overwrite").parquet(ZONE_HOUR_FEATURES_PATH)
    print(f"\nzone_hour_features saved to: {ZONE_HOUR_FEATURES_PATH}")

    zone_hour_export_df = spark.read.parquet(ZONE_HOUR_FEATURES_PATH)
    export_full_csv(zone_hour_export_df)

    spark.stop()


if __name__ == "__main__":
    main()
