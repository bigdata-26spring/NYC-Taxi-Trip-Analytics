from pathlib import Path
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from config.config import (
    ZONE_HOUR_FEATURES_PATH,
    BOROUGH_HOUR_FEATURES_PATH,
    FEATURE_APP_NAME,
    TABLES_DIR,
)
from src.ingestion.load_raw_data import create_spark_session


def build_borough_hour_features(zone_hour_df: DataFrame) -> DataFrame:
    """
    Build borough_hour_features from zone_hour_features.

    One row = one pickup borough in one date-hour bucket.
    """
    borough_hour_df = (
        zone_hour_df.groupBy(
            "pickup_borough",
            "pickup_date",
            "year",
            "month",
            "day",
            "year_month",
            "day_of_week",
            "weekday_name",
            "is_weekend",
            "hour",
        )
        .agg(
            F.sum("trip_count").alias("trip_count"),

            F.sum("total_trip_distance").alias("total_trip_distance"),
            F.sum("total_trip_duration_min").alias("total_trip_duration_min"),

            F.sum("total_fare_amount").alias("total_fare_amount"),
            F.sum("total_revenue").alias("total_revenue"),

            F.sum("total_tip_amount").alias("total_tip_amount"),
            F.sum("total_tolls_amount").alias("total_tolls_amount"),
            F.sum("total_airport_fee").alias("total_airport_fee"),

            F.sum("total_passenger_count").alias("total_passenger_count"),

            F.sum("credit_card_trip_count").alias("credit_card_trip_count"),
            F.sum("cash_trip_count").alias("cash_trip_count"),
        )
    )

    borough_hour_df = (
        borough_hour_df
        .withColumn(
            "avg_trip_distance",
            F.when(F.col("trip_count") > 0,
                   F.col("total_trip_distance") / F.col("trip_count"))
        )
        .withColumn(
            "avg_trip_duration_min",
            F.when(F.col("trip_count") > 0,
                   F.col("total_trip_duration_min") / F.col("trip_count"))
        )
        .withColumn(
            "avg_fare_amount",
            F.when(F.col("trip_count") > 0,
                   F.col("total_fare_amount") / F.col("trip_count"))
        )
        .withColumn(
            "avg_total_amount",
            F.when(F.col("trip_count") > 0,
                   F.col("total_revenue") / F.col("trip_count"))
        )
        .withColumn(
            "avg_tip_amount",
            F.when(F.col("trip_count") > 0,
                   F.col("total_tip_amount") / F.col("trip_count"))
        )
        .withColumn(
            "avg_passenger_count",
            F.when(F.col("trip_count") > 0,
                   F.col("total_passenger_count") / F.col("trip_count"))
        )
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("total_trip_duration_min") > 0,
                F.col("total_trip_distance") / (F.col("total_trip_duration_min") / 60.0)
            )
        )
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
    )

    return reorder_columns(borough_hour_df)


def reorder_columns(df: DataFrame) -> DataFrame:
    preferred_order = [
        "pickup_borough",
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


def export_sample_csv(borough_hour_df: DataFrame) -> None:
    try:
        import pandas as pd

        TABLES_DIR.mkdir(parents=True, exist_ok=True)

        sample_pdf = (
            borough_hour_df
            .orderBy("pickup_date", "hour", "pickup_borough")
            .limit(100)
            .toPandas()
        )

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

        sample_path = TABLES_DIR / "borough_hour_features_sample.csv"
        sample_pdf.to_csv(sample_path, index=False)

        print(f"\nSample CSV saved to: {sample_path}")

    except Exception as e:
        print(f"\nSkipping sample CSV export: {e}")


def main() -> None:
    spark = create_spark_session(FEATURE_APP_NAME)

    zone_hour_df = spark.read.parquet(ZONE_HOUR_FEATURES_PATH)

    borough_hour_df = build_borough_hour_features(zone_hour_df)

    print("\n===== borough_hour_features schema =====")
    borough_hour_df.printSchema()

    print("\n===== borough_hour_features preview =====")
    borough_hour_df.show(30, truncate=False)

    borough_hour_df.write.mode("overwrite").parquet(BOROUGH_HOUR_FEATURES_PATH)
    print(f"\nborough_hour_features saved to: {BOROUGH_HOUR_FEATURES_PATH}")

    export_sample_csv(borough_hour_df)

    spark.stop()


if __name__ == "__main__":
    main()