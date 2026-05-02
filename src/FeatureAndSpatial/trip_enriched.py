import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from config.config import (
    CLEANED_TRIPS_PATH,
    TRIP_ENRICHED_PATH,
    FEATURE_APP_NAME,
    PICKUP_ID_COL,
    DROPOFF_ID_COL,
    TABLES_DIR,
    ZONE_LOOKUP_ID_COL,
    BOROUGH_COL,
    ZONE_COL,
    SERVICE_ZONE_COL,
)

from src.ingestion.load_raw_data import create_spark_session, load_zone_lookup


def add_time_features(df: DataFrame) -> DataFrame:
    """
    Add reusable time features derived from pickup_ts.
    """
    return (
        df
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("year", F.year("pickup_ts"))
        .withColumn("month", F.month("pickup_ts"))
        .withColumn("day", F.dayofmonth("pickup_ts"))
        .withColumn("hour", F.hour("pickup_ts"))
        .withColumn("day_of_week", F.dayofweek("pickup_ts"))  # 1=Sunday, 7=Saturday
        .withColumn("weekday_name", F.date_format("pickup_ts", "E"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("pickup_ts").isin([1, 7]), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn("year_month", F.date_format("pickup_ts", "yyyy-MM"))
    )


def add_business_labels(df: DataFrame) -> DataFrame:
    """
    Decode several coded business fields based on the field dictionary.
    """
    vendor_map = {
        1: "Creative Mobile Technologies, LLC",
        2: "Curb Mobility, LLC",
        6: "Myle Technologies Inc",
        7: "Helix",
    }

    ratecode_map = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride",
        99: "Null/unknown",
    }

    payment_map = {
        0: "Flex Fare trip",
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
    }

    vendor_expr = F.create_map([F.lit(x) for kv in vendor_map.items() for x in kv])
    ratecode_expr = F.create_map([F.lit(x) for kv in ratecode_map.items() for x in kv])
    payment_expr = F.create_map([F.lit(x) for kv in payment_map.items() for x in kv])

    df = (
        df
        .withColumn(
            "vendor_name",
            F.coalesce(
                vendor_expr.getItem(F.col("VendorID").cast("int")),
                F.lit("Unknown")
            )
        )
        .withColumn(
            "ratecode_desc",
            F.coalesce(
                ratecode_expr.getItem(F.col("RatecodeID").cast("int")),
                F.lit("Unknown")
            )
        )
        .withColumn(
            "payment_type_desc",
            F.coalesce(
                payment_expr.getItem(F.col("payment_type").cast("int")),
                F.lit("Unknown")
            )
        )
        .withColumn(
            "store_and_fwd_desc",
            F.when(F.col("store_and_fwd_flag") == "Y", F.lit("Store and forward trip"))
             .when(F.col("store_and_fwd_flag") == "N", F.lit("Not a store and forward trip"))
             .otherwise(F.lit("Unknown"))
        )
    )

    return df


def join_zone_lookup(df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """
    Join taxi zone lookup twice:
    - pickup side
    - dropoff side
    """
    pickup_lookup = zones_df.select(
        F.col(ZONE_LOOKUP_ID_COL).cast("int").alias("pu_join_id"),
        F.col(ZONE_COL).alias("pickup_zone"),
        F.col(BOROUGH_COL).alias("pickup_borough"),
        F.col(SERVICE_ZONE_COL).alias("pickup_service_zone"),
    )

    dropoff_lookup = zones_df.select(
        F.col(ZONE_LOOKUP_ID_COL).cast("int").alias("do_join_id"),
        F.col(ZONE_COL).alias("dropoff_zone"),
        F.col(BOROUGH_COL).alias("dropoff_borough"),
        F.col(SERVICE_ZONE_COL).alias("dropoff_service_zone"),
    )

    enriched_df = (
        df
        .join(
            F.broadcast(pickup_lookup),
            df[PICKUP_ID_COL] == pickup_lookup["pu_join_id"],
            how="left",
        )
        .join(
            F.broadcast(dropoff_lookup),
            df[DROPOFF_ID_COL] == dropoff_lookup["do_join_id"],
            how="left",
        )
        .drop("pu_join_id", "do_join_id")
    )

    return enriched_df


def add_route_fields(df: DataFrame) -> DataFrame:
    """
    Add helper route fields for downstream top-routes analysis / visualization.
    """
    return (
        df
        .withColumn(
            "route_key",
            F.concat_ws(
                "->",
                F.col(PICKUP_ID_COL).cast("string"),
                F.col(DROPOFF_ID_COL).cast("string"),
            )
        )
        .withColumn(
            "route_name",
            F.concat_ws(" -> ", F.col("pickup_zone"), F.col("dropoff_zone"))
        )
    )


def reorder_columns(df: DataFrame) -> DataFrame:
    """
    Put the most useful enriched fields in front for readability.
    """
    preferred_order = [
        "pickup_ts",
        "dropoff_ts",
        "pickup_date",
        "year",
        "month",
        "day",
        "hour",
        "day_of_week",
        "weekday_name",
        "is_weekend",
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
        "trip_duration_min",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "tip_amount",
        "tolls_amount",
        "airport_fee",
        "passenger_count",
        "payment_type",
        "payment_type_desc",
        "RatecodeID",
        "ratecode_desc",
        "VendorID",
        "vendor_name",
        "store_and_fwd_flag",
        "store_and_fwd_desc",
    ]

    existing_cols = df.columns
    ordered_cols = [c for c in preferred_order if c in existing_cols]
    remaining_cols = [c for c in existing_cols if c not in ordered_cols]

    return df.select(*ordered_cols, *remaining_cols)


def build_trip_enriched(cleaned_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """
    Main feature-building pipeline for trip_enriched.
    """
    df = cleaned_df
    df = join_zone_lookup(df, zones_df)
    df = add_business_labels(df)
    df = add_time_features(df)
    df = add_route_fields(df)
    df = reorder_columns(df)
    return df

def export_sample_csv(trip_enriched_df: DataFrame) -> None:
    """
    Export a small readable CSV sample for quick inspection / teammates.
    """
    try:
        import pandas as pd

        TABLES_DIR.mkdir(parents=True, exist_ok=True)

        sample_pdf = (
            trip_enriched_df
            .orderBy("pickup_ts")
            .limit(100)
            .toPandas()
        )

        float_cols = [
            "trip_distance",
            "trip_duration_min",
            "fare_amount",
            "total_amount",
            "tip_amount",
            "tolls_amount",
            "airport_fee",
        ]

        for col in float_cols:
            if col in sample_pdf.columns:
                sample_pdf[col] = sample_pdf[col].round(4)

        sample_path = TABLES_DIR / "trip_enriched_sample.csv"
        sample_pdf.to_csv(sample_path, index=False)

        print(f"\nSample CSV saved to: {sample_path}")

    except Exception as e:
        print(f"\nSkipping sample CSV export: {e}")


def export_full_csv(trip_enriched_df: DataFrame) -> None:
    """
    Export the full trip_enriched table as CSV for D3 / frontend use.

    Note:
    Spark writes CSV as a directory containing part-*.csv files,
    not as a single CSV file by default.
    """
    csv_output_path = TABLES_DIR / "trip_enriched_csv"

    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    (
        trip_enriched_df
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(csv_output_path))
    )

    print(f"\nFull CSV saved to: {csv_output_path}")


def main() -> None:
    spark = create_spark_session(FEATURE_APP_NAME)

    cleaned_df = spark.read.parquet(CLEANED_TRIPS_PATH)
    zones_df = load_zone_lookup(spark)

    trip_enriched_df = build_trip_enriched(cleaned_df, zones_df)

    print("\n===== trip_enriched schema =====")
    trip_enriched_df.printSchema()

    # print("\n===== trip_enriched preview =====")
    # trip_enriched_df.show(30, truncate=False)

    trip_enriched_df.write.mode("overwrite").parquet(TRIP_ENRICHED_PATH)

    print(f"\ntrip_enriched saved to: {TRIP_ENRICHED_PATH}")

    trip_enriched_export_df = spark.read.parquet(TRIP_ENRICHED_PATH)
    export_sample_csv(trip_enriched_export_df)

    spark.stop()


if __name__ == "__main__":
    main()
