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
    TOP_ROUTES_PATH,
    TOTAL_AMOUNT_COL,
    TRIP_DISTANCE_COL,
    TRIP_ENRICHED_PATH,
)

from src.ingestion.load_raw_data import create_spark_session  # noqa: E402


OUTPUT_DIR = TABLES_DIR / "trip_route_analytics"
PARQUET_OUTPUT_DIR = OUTPUT_DIR / "parquet"
CSV_OUTPUT_DIR = OUTPUT_DIR / "csv"


TRIP_ENRICHED_REQUIRED_COLUMNS = [
    "pickup_ts",
    "dropoff_ts",
    "pickup_date",
    "year",
    "month",
    "hour",
    "day_of_week",
    "weekday_name",
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
    "payment_type_desc",
    "RatecodeID",
    "ratecode_desc",
    "VendorID",
    "vendor_name",
]


TOP_ROUTES_REQUIRED_COLUMNS = [
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
    "avg_passenger_count",
    "total_passenger_count",
    "avg_speed_mph",
    "credit_card_trip_count",
    "cash_trip_count",
    "credit_card_share",
    "cash_share",
]


def validate_input_schema(df: DataFrame, required_cols: list[str], table_name: str) -> None:
    missing_cols = [col_name for col_name in required_cols if col_name not in df.columns]

    if missing_cols:
        raise ValueError(
            f"Missing columns in {table_name}: "
            + ", ".join(missing_cols)
            + "\nPlease rerun Stage 3 first."
        )


def add_trip_flags(df: DataFrame) -> DataFrame:
    pickup_airport = (F.col("pickup_service_zone") == "Airports") | (F.col("pickup_borough") == "EWR")
    dropoff_airport = (F.col("dropoff_service_zone") == "Airports") | (F.col("dropoff_borough") == "EWR")

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
        .withColumn(
            "fare_per_mile",
            F.when(F.col(TRIP_DISTANCE_COL) > 0, F.col(TOTAL_AMOUNT_COL) / F.col(TRIP_DISTANCE_COL)),
        )
        .withColumn(
            "is_inter_borough",
            F.when(F.col("pickup_borough") != F.col("dropoff_borough"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn("is_pickup_airport", F.when(pickup_airport, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_dropoff_airport", F.when(dropoff_airport, F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "airport_trip_type",
            F.when(pickup_airport & dropoff_airport, F.lit("airport_to_airport"))
            .when(pickup_airport, F.lit("airport_pickup"))
            .when(dropoff_airport, F.lit("airport_dropoff"))
            .otherwise(F.lit("non_airport")),
        )
        .withColumn(
            "distance_bucket",
            F.when(F.col(TRIP_DISTANCE_COL) < 1, F.lit("00_<1_mile"))
            .when(F.col(TRIP_DISTANCE_COL) < 3, F.lit("01_1_3_miles"))
            .when(F.col(TRIP_DISTANCE_COL) < 6, F.lit("02_3_6_miles"))
            .when(F.col(TRIP_DISTANCE_COL) < 12, F.lit("03_6_12_miles"))
            .when(F.col(TRIP_DISTANCE_COL) < 25, F.lit("04_12_25_miles"))
            .otherwise(F.lit("05_25_plus_miles")),
        )
        .withColumn(
            "duration_bucket",
            F.when(F.col("trip_duration_min") < 5, F.lit("00_<5_min"))
            .when(F.col("trip_duration_min") < 10, F.lit("01_5_10_min"))
            .when(F.col("trip_duration_min") < 20, F.lit("02_10_20_min"))
            .when(F.col("trip_duration_min") < 40, F.lit("03_20_40_min"))
            .when(F.col("trip_duration_min") < 60, F.lit("04_40_60_min"))
            .otherwise(F.lit("05_60_plus_min")),
        )
        .withColumn(
            "fare_bucket",
            F.when(F.col(TOTAL_AMOUNT_COL) < 10, F.lit("00_<10"))
            .when(F.col(TOTAL_AMOUNT_COL) < 20, F.lit("01_10_20"))
            .when(F.col(TOTAL_AMOUNT_COL) < 40, F.lit("02_20_40"))
            .when(F.col(TOTAL_AMOUNT_COL) < 80, F.lit("03_40_80"))
            .when(F.col(TOTAL_AMOUNT_COL) < 120, F.lit("04_80_120"))
            .otherwise(F.lit("05_120_plus")),
        )
    )


def add_trip_summary_metrics(df: DataFrame) -> DataFrame:
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
            "avg_fare_per_mile",
            F.when(F.col("trip_count") > 0, F.col("total_fare_per_mile") / F.col("trip_count")),
        )
        .withColumn(
            "avg_speed_mph",
            F.when(F.col("trip_count") > 0, F.col("total_speed_mph") / F.col("trip_count")),
        )
    )


def aggregate_trips(df: DataFrame, group_cols: list[str]) -> DataFrame:
    agg_df = (
        df.groupBy(*group_cols)
        .agg(
            F.count("*").alias("trip_count"),
            F.sum(TOTAL_AMOUNT_COL).alias("total_revenue"),
            F.sum(FARE_AMOUNT_COL).alias("total_fare_amount"),
            F.sum(TRIP_DISTANCE_COL).alias("total_trip_distance"),
            F.sum("trip_duration_min").alias("total_trip_duration_min"),
            F.sum("tip_amount").alias("total_tip_amount"),
            F.sum("tip_share_of_total").alias("total_tip_share_of_total"),
            F.sum("fare_per_mile").alias("total_fare_per_mile"),
            F.sum("trip_speed_mph").alias("total_speed_mph"),
            F.sum(PASSENGER_COUNT_COL).alias("total_passenger_count"),
            F.countDistinct("pickup_date").alias("active_days"),
            F.sum("is_inter_borough").alias("inter_borough_trip_count"),
            F.sum("is_pickup_airport").alias("airport_pickup_trip_count"),
            F.sum("is_dropoff_airport").alias("airport_dropoff_trip_count"),
        )
    )

    return add_trip_summary_metrics(agg_df)


def add_route_weighted_metrics(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "avg_revenue_per_trip",
            F.when(F.col("total_trips") > 0, F.col("total_revenue") / F.col("total_trips")),
        )
        .withColumn(
            "avg_route_trip_distance",
            F.when(F.col("total_trips") > 0, F.col("total_trip_distance") / F.col("total_trips")),
        )
        .withColumn(
            "avg_route_trip_duration_min",
            F.when(F.col("total_trips") > 0, F.col("total_trip_duration_min") / F.col("total_trips")),
        )
        .withColumn(
            "avg_tip_amount",
            F.when(F.col("total_trips") > 0, F.col("total_tip_amount") / F.col("total_trips")),
        )
        .withColumn(
            "route_tip_share",
            F.when(F.col("total_revenue") > 0, F.col("total_tip_amount") / F.col("total_revenue")),
        )
        .withColumn(
            "revenue_per_mile",
            F.when(F.col("total_trip_distance") > 0, F.col("total_revenue") / F.col("total_trip_distance")),
        )
        .withColumn(
            "credit_card_share",
            F.when(F.col("total_trips") > 0, F.col("credit_card_trip_count") / F.col("total_trips")),
        )
        .withColumn(
            "cash_share",
            F.when(F.col("total_trips") > 0, F.col("cash_trip_count") / F.col("total_trips")),
        )
    )


def build_payment_type_summary(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, ["payment_type", "payment_type_desc"]).orderBy(F.desc("trip_count"))


def build_payment_type_by_year(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, ["year", "payment_type", "payment_type_desc"]).orderBy(
        "year",
        F.desc("trip_count"),
    )


def build_vendor_summary(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, ["VendorID", "vendor_name"]).orderBy(F.desc("trip_count"))


def build_ratecode_summary(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, ["RatecodeID", "ratecode_desc"]).orderBy(F.desc("trip_count"))


def build_passenger_count_summary(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, [PASSENGER_COUNT_COL]).orderBy(PASSENGER_COUNT_COL)


def build_distance_bucket_summary(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, ["distance_bucket"]).orderBy("distance_bucket")


def build_duration_bucket_summary(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, ["duration_bucket"]).orderBy("duration_bucket")


def build_fare_bucket_summary(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, ["fare_bucket"]).orderBy("fare_bucket")


def build_pickup_dropoff_borough_matrix(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, ["pickup_borough", "dropoff_borough"]).orderBy(
        F.desc("trip_count"),
        "pickup_borough",
        "dropoff_borough",
    )


def build_airport_trip_summary(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, ["year", "airport_trip_type"]).orderBy("year", "airport_trip_type")


def build_trip_behavior_by_hour(df: DataFrame) -> DataFrame:
    return aggregate_trips(df, ["hour"]).orderBy("hour")


def build_route_borough_matrix(routes_df: DataFrame) -> DataFrame:
    agg_df = (
        routes_df.groupBy("pickup_borough", "dropoff_borough")
        .agg(
            F.count("*").alias("route_count"),
            F.sum("trip_count").alias("total_trips"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("total_trip_distance").alias("total_trip_distance"),
            F.sum("total_trip_duration_min").alias("total_trip_duration_min"),
            F.sum("total_tip_amount").alias("total_tip_amount"),
            F.sum("credit_card_trip_count").alias("credit_card_trip_count"),
            F.sum("cash_trip_count").alias("cash_trip_count"),
            F.min("route_rank").alias("best_route_rank"),
        )
    )

    return add_route_weighted_metrics(agg_df).orderBy(F.desc("total_trips"))


def build_top_routes_overall(routes_df: DataFrame, top_n: int) -> DataFrame:
    selected_cols = [
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
        "avg_trip_duration_min",
        "avg_fare_amount",
        "avg_total_amount",
        "total_revenue",
        "avg_tip_amount",
        "avg_passenger_count",
        "avg_speed_mph",
        "credit_card_share",
        "cash_share",
    ]

    return (
        routes_df.select(*[col_name for col_name in selected_cols if col_name in routes_df.columns])
        .orderBy("route_rank", F.desc("trip_count"), F.desc("total_revenue"))
        .limit(top_n)
    )


def build_top_inter_borough_routes(routes_df: DataFrame, top_n: int) -> DataFrame:
    return (
        routes_df.filter(F.col("pickup_borough") != F.col("dropoff_borough"))
        .orderBy(F.desc("trip_count"), F.desc("total_revenue"))
        .limit(top_n)
    )


def build_top_airport_routes(routes_df: DataFrame, top_n: int) -> DataFrame:
    pickup_airport = (F.col("pickup_service_zone") == "Airports") | (F.col("pickup_borough") == "EWR")
    dropoff_airport = (F.col("dropoff_service_zone") == "Airports") | (F.col("dropoff_borough") == "EWR")

    return (
        routes_df.filter(pickup_airport | dropoff_airport)
        .orderBy(F.desc("trip_count"), F.desc("total_revenue"))
        .limit(top_n)
    )


def build_route_efficiency_ranking(routes_df: DataFrame, top_n: int, min_route_trips: int) -> DataFrame:
    return (
        routes_df.withColumn(
            "revenue_per_mile",
            F.when(F.col("total_trip_distance") > 0, F.col("total_revenue") / F.col("total_trip_distance")),
        )
        .withColumn(
            "revenue_per_minute",
            F.when(
                F.col("total_trip_duration_min") > 0,
                F.col("total_revenue") / F.col("total_trip_duration_min"),
            ),
        )
        .filter(F.col("trip_count") >= min_route_trips)
        .orderBy(F.desc("revenue_per_mile"), F.desc("trip_count"))
        .limit(top_n)
    )


def build_route_tip_ranking(routes_df: DataFrame, top_n: int, min_route_trips: int) -> DataFrame:
    return (
        routes_df.withColumn(
            "route_tip_share",
            F.when(F.col("total_revenue") > 0, F.col("total_tip_amount") / F.col("total_revenue")),
        )
        .filter(F.col("trip_count") >= min_route_trips)
        .orderBy(F.desc("route_tip_share"), F.desc("avg_tip_amount"), F.desc("trip_count"))
        .limit(top_n)
    )


def build_route_concentration(routes_df: DataFrame) -> DataFrame:
    ordered_window = Window.orderBy(F.desc("trip_count"), F.desc("total_revenue")).rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
    rank_window = Window.orderBy(F.desc("trip_count"), F.desc("total_revenue"))
    all_routes_window = Window.partitionBy()

    return (
        routes_df.withColumn("route_order", F.row_number().over(rank_window))
        .withColumn("overall_trips", F.sum("trip_count").over(all_routes_window))
        .withColumn("cumulative_trips", F.sum("trip_count").over(ordered_window))
        .withColumn(
            "cumulative_trip_share",
            F.when(F.col("overall_trips") > 0, F.col("cumulative_trips") / F.col("overall_trips")),
        )
        .select(
            "route_order",
            "route_rank",
            "route_key",
            "route_name",
            "pickup_borough",
            "dropoff_borough",
            "trip_count",
            "total_revenue",
            "overall_trips",
            "cumulative_trips",
            "cumulative_trip_share",
        )
        .orderBy("route_order")
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
    parser = argparse.ArgumentParser(description="Stage 4C - Trip and Route Analytics")
    parser.add_argument(
        "--trip-enriched-input",
        default=TRIP_ENRICHED_PATH,
        help="Input path for Stage 3 trip_enriched parquet.",
    )
    parser.add_argument(
        "--top-routes-input",
        default=TOP_ROUTES_PATH,
        help="Input path for Stage 3 top_routes parquet.",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Also write CSV outputs.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=25,
        help="Number of rows to keep for top route ranking tables.",
    )
    parser.add_argument(
        "--min-route-trips",
        type=int,
        default=1000,
        help="Minimum route trip count for route efficiency and tip rankings.",
    )

    args = parser.parse_args()

    spark = create_spark_session(f"{ANALYTICS_APP_NAME} - Trip Route Analytics")
    trip_enriched_df = spark.read.parquet(args.trip_enriched_input)
    top_routes_df = spark.read.parquet(args.top_routes_input)

    validate_input_schema(
        trip_enriched_df,
        TRIP_ENRICHED_REQUIRED_COLUMNS,
        "trip_enriched",
    )
    validate_input_schema(top_routes_df, TOP_ROUTES_REQUIRED_COLUMNS, "top_routes")

    trip_df = add_trip_flags(trip_enriched_df)

    print("\n===== trip_enriched input schema =====")
    trip_enriched_df.printSchema()
    print("\n===== top_routes input schema =====")
    top_routes_df.printSchema()

    output_tables = {
        "payment_type_summary": build_payment_type_summary(trip_df),
        "payment_type_by_year": build_payment_type_by_year(trip_df),
        "vendor_summary": build_vendor_summary(trip_df),
        "ratecode_summary": build_ratecode_summary(trip_df),
        "passenger_count_summary": build_passenger_count_summary(trip_df),
        "distance_bucket_summary": build_distance_bucket_summary(trip_df),
        "duration_bucket_summary": build_duration_bucket_summary(trip_df),
        "fare_bucket_summary": build_fare_bucket_summary(trip_df),
        "pickup_dropoff_borough_matrix": build_pickup_dropoff_borough_matrix(trip_df),
        "airport_trip_summary": build_airport_trip_summary(trip_df),
        "trip_behavior_by_hour": build_trip_behavior_by_hour(trip_df),
        "route_borough_matrix": build_route_borough_matrix(top_routes_df),
        "top_routes_overall": build_top_routes_overall(top_routes_df, args.top_n),
        "top_inter_borough_routes": build_top_inter_borough_routes(top_routes_df, args.top_n),
        "top_airport_routes": build_top_airport_routes(top_routes_df, args.top_n),
        "route_efficiency_ranking": build_route_efficiency_ranking(
            top_routes_df,
            args.top_n,
            args.min_route_trips,
        ),
        "route_tip_ranking": build_route_tip_ranking(
            top_routes_df,
            args.top_n,
            args.min_route_trips,
        ),
        "route_concentration": build_route_concentration(top_routes_df),
    }

    for table_name, table_df in output_tables.items():
        print(f"\n===== Writing {table_name} =====")
        table_df.show(20, truncate=False)
        write_output_table(table_df, table_name, args.write_csv)

    spark.stop()
    print("\nTrip and route analytics completed successfully.")


if __name__ == "__main__":
    main()
