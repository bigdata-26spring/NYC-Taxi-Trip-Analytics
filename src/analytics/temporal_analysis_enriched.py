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
    PICKUP_ID_COL,
    TABLES_DIR,
    ZONE_HOUR_FEATURES_PATH,
)

from src.ingestion.load_raw_data import create_spark_session  # noqa: E402


OUTPUT_DIR = TABLES_DIR / "temporal_enriched"
PARQUET_OUTPUT_DIR = OUTPUT_DIR / "parquet"
CSV_OUTPUT_DIR = OUTPUT_DIR / "csv"


REQUIRED_COLUMNS = [
    PICKUP_ID_COL,
    "pickup_zone",
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
    "total_revenue",
    "total_fare_amount",
    "total_trip_distance",
    "total_trip_duration_min",
    "total_passenger_count",
    "credit_card_trip_count",
    "cash_trip_count",
]


def validate_input_schema(df: DataFrame) -> None:
    missing_cols = [col_name for col_name in REQUIRED_COLUMNS if col_name not in df.columns]

    if missing_cols:
        raise ValueError(
            "Missing columns in zone_hour_features: "
            + ", ".join(missing_cols)
            + "\nPlease rerun Stage 3 first."
        )


def add_weighted_metrics(df: DataFrame) -> DataFrame:
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
            F.when(
                F.col("total_trips") > 0,
                F.col("total_trip_duration_min") / F.col("total_trips"),
            ),
        )
        .withColumn(
            "avg_passenger_count",
            F.when(
                F.col("total_trips") > 0,
                F.col("total_passenger_count") / F.col("total_trips"),
            ),
        )
        .withColumn(
            "credit_card_share",
            F.when(
                F.col("total_trips") > 0,
                F.col("credit_card_trip_count") / F.col("total_trips"),
            ),
        )
        .withColumn(
            "cash_share",
            F.when(F.col("total_trips") > 0, F.col("cash_trip_count") / F.col("total_trips")),
        )
    )


def aggregate_demand(df: DataFrame, group_cols: list[str]) -> DataFrame:
    agg_df = (
        df.groupBy(*group_cols)
        .agg(
            F.sum("trip_count").alias("total_trips"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("total_fare_amount").alias("total_fare_amount"),
            F.sum("total_trip_distance").alias("total_trip_distance"),
            F.sum("total_trip_duration_min").alias("total_trip_duration_min"),
            F.sum("total_passenger_count").alias("total_passenger_count"),
            F.sum("credit_card_trip_count").alias("credit_card_trip_count"),
            F.sum("cash_trip_count").alias("cash_trip_count"),
            F.countDistinct("pickup_date").alias("active_days"),
            F.countDistinct(PICKUP_ID_COL).alias("active_pickup_zones"),
        )
    )

    return add_weighted_metrics(agg_df)


def add_partition_share(
    df: DataFrame,
    partition_cols: list[str],
    value_col: str,
    share_col: str,
) -> DataFrame:
    window = Window.partitionBy(*partition_cols)
    partition_total = F.sum(value_col).over(window)

    return df.withColumn(
        share_col,
        F.when(partition_total > 0, F.col(value_col) / partition_total),
    )


def add_revenue_efficiency_metrics(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "revenue_per_mile",
            F.when(
                F.col("total_trip_distance") > 0,
                F.col("total_revenue") / F.col("total_trip_distance"),
            ),
        )
        .withColumn(
            "revenue_per_trip_minute",
            F.when(
                F.col("total_trip_duration_min") > 0,
                F.col("total_revenue") / F.col("total_trip_duration_min"),
            ),
        )
        .withColumn(
            "trips_per_active_zone_day",
            F.when(
                (F.col("active_days") > 0) & (F.col("active_pickup_zones") > 0),
                F.col("total_trips") / (F.col("active_days") * F.col("active_pickup_zones")),
            ),
        )
    )


def build_yearly_summary(df: DataFrame) -> DataFrame:
    yearly_df = aggregate_demand(df, ["year"]).orderBy("year")
    year_window = Window.orderBy("year")

    return (
        yearly_df.withColumn("prev_year_trips", F.lag("total_trips").over(year_window))
        .withColumn("prev_year_revenue", F.lag("total_revenue").over(year_window))
        .withColumn(
            "yoy_trip_change",
            F.when(
                F.col("prev_year_trips") > 0,
                (F.col("total_trips") - F.col("prev_year_trips")) / F.col("prev_year_trips"),
            ),
        )
        .withColumn(
            "yoy_revenue_change",
            F.when(
                F.col("prev_year_revenue") > 0,
                (F.col("total_revenue") - F.col("prev_year_revenue"))
                / F.col("prev_year_revenue"),
            ),
        )
    )


def build_year_month_demand(df: DataFrame) -> DataFrame:
    monthly_df = aggregate_demand(df, ["year", "month", "year_month"])

    return (
        add_partition_share(monthly_df, ["year"], "total_trips", "monthly_trip_share_in_year")
        .withColumn("month_index", F.col("year") * F.lit(12) + F.col("month"))
        .orderBy("year", "month")
    )


def build_month_of_year_pattern(df: DataFrame) -> DataFrame:
    active_years_df = df.groupBy("month").agg(F.countDistinct("year").alias("active_years"))
    month_name = F.date_format(
        F.to_date(
            F.concat(
                F.lit("2024-"),
                F.format_string("%02d", F.col("month")),
                F.lit("-01"),
            )
        ),
        "MMM",
    )

    return (
        aggregate_demand(df, ["month"])
        .join(active_years_df, on="month", how="left")
        .withColumn("month_name", month_name)
        .withColumn(
            "avg_trips_per_active_year",
            F.when(F.col("active_years") > 0, F.col("total_trips") / F.col("active_years")),
        )
        .select(
            "month",
            "month_name",
            "active_years",
            "total_trips",
            "avg_trips_per_active_year",
            "total_revenue",
            "avg_revenue_per_trip",
            "avg_fare_amount",
            "avg_trip_distance",
            "avg_trip_duration_min",
            "credit_card_share",
            "cash_share",
            "active_days",
            "active_pickup_zones",
        )
        .orderBy("month")
    )


def build_year_hourly_pattern(df: DataFrame) -> DataFrame:
    hourly_df = aggregate_demand(df, ["year", "hour"])

    return (
        add_partition_share(hourly_df, ["year"], "total_trips", "hourly_trip_share_in_year")
        .orderBy("year", "hour")
    )


def build_year_weekday_hour_heatmap(df: DataFrame) -> DataFrame:
    heatmap_df = aggregate_demand(df, ["year", "day_of_week", "weekday_name", "hour"])

    return (
        add_partition_share(heatmap_df, ["year"], "total_trips", "cell_trip_share_in_year")
        .orderBy("year", "day_of_week", "hour")
    )


def build_borough_year_month(df: DataFrame) -> DataFrame:
    monthly_df = aggregate_demand(df, ["pickup_borough", "year", "month", "year_month"])

    return (
        add_partition_share(
            monthly_df,
            ["pickup_borough", "year"],
            "total_trips",
            "monthly_trip_share_in_borough_year",
        )
        .orderBy("pickup_borough", "year", "month")
    )


def build_borough_year_hour(df: DataFrame) -> DataFrame:
    hourly_df = aggregate_demand(df, ["pickup_borough", "year", "hour"])

    return (
        add_partition_share(
            hourly_df,
            ["pickup_borough", "year"],
            "total_trips",
            "hourly_trip_share_in_borough_year",
        )
        .orderBy("pickup_borough", "year", "hour")
    )


def add_rush_hour_labels(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "time_period_order",
            F.when((F.col("hour") >= 0) & (F.col("hour") <= 5), F.lit(1))
            .when((F.col("hour") >= 6) & (F.col("hour") <= 10), F.lit(2))
            .when((F.col("hour") >= 11) & (F.col("hour") <= 15), F.lit(3))
            .when((F.col("hour") >= 16) & (F.col("hour") <= 19), F.lit(4))
            .otherwise(F.lit(5)),
        )
        .withColumn(
            "time_period",
            F.when((F.col("hour") >= 0) & (F.col("hour") <= 5), F.lit("late_night_00_05"))
            .when((F.col("hour") >= 6) & (F.col("hour") <= 10), F.lit("morning_peak_06_10"))
            .when((F.col("hour") >= 11) & (F.col("hour") <= 15), F.lit("midday_11_15"))
            .when((F.col("hour") >= 16) & (F.col("hour") <= 19), F.lit("evening_peak_16_19"))
            .otherwise(F.lit("night_20_23")),
        )
    )


def build_rush_hour_by_year(df: DataFrame) -> DataFrame:
    rush_df = aggregate_demand(add_rush_hour_labels(df), ["year", "time_period_order", "time_period"])

    return (
        add_partition_share(rush_df, ["year"], "total_trips", "time_period_trip_share_in_year")
        .orderBy("year", "time_period_order")
    )


def build_peak_hour_summary(df: DataFrame, top_n: int) -> DataFrame:
    hourly_df = aggregate_demand(df, ["year", "hour"])
    rank_window = Window.partitionBy("year").orderBy(F.desc("total_trips"), F.desc("total_revenue"))

    return (
        hourly_df.withColumn("hour_rank_in_year", F.dense_rank().over(rank_window))
        .filter(F.col("hour_rank_in_year") <= top_n)
        .orderBy("year", "hour_rank_in_year")
    )


def build_top_zones_by_year(df: DataFrame, top_n: int) -> DataFrame:
    zone_year_df = aggregate_demand(
        df,
        ["year", PICKUP_ID_COL, "pickup_zone", "pickup_borough"],
    )
    rank_window = Window.partitionBy("year").orderBy(F.desc("total_trips"), F.desc("total_revenue"))

    return (
        zone_year_df.withColumn("zone_rank_in_year", F.dense_rank().over(rank_window))
        .filter(F.col("zone_rank_in_year") <= top_n)
        .orderBy("year", "zone_rank_in_year")
    )


def build_zone_rank_change(df: DataFrame, rank_change_n: int) -> DataFrame:
    zone_year_df = aggregate_demand(
        df,
        ["year", PICKUP_ID_COL, "pickup_zone", "pickup_borough"],
    )
    rank_window = Window.partitionBy("year").orderBy(F.desc("total_trips"), F.desc("total_revenue"))
    ranked_df = zone_year_df.withColumn("zone_rank", F.dense_rank().over(rank_window))
    year_bounds = df.agg(F.min("year").alias("first_year"), F.max("year").alias("last_year"))

    first_df = (
        ranked_df.crossJoin(year_bounds)
        .filter(F.col("year") == F.col("first_year"))
        .select(
            PICKUP_ID_COL,
            F.col("pickup_zone").alias("first_pickup_zone"),
            F.col("pickup_borough").alias("first_pickup_borough"),
            F.col("year").alias("first_year"),
            F.col("zone_rank").alias("first_rank"),
            F.col("total_trips").alias("first_year_trips"),
            F.col("total_revenue").alias("first_year_revenue"),
        )
    )

    last_df = (
        ranked_df.crossJoin(year_bounds)
        .filter(F.col("year") == F.col("last_year"))
        .select(
            PICKUP_ID_COL,
            F.col("pickup_zone").alias("last_pickup_zone"),
            F.col("pickup_borough").alias("last_pickup_borough"),
            F.col("year").alias("last_year"),
            F.col("zone_rank").alias("last_rank"),
            F.col("total_trips").alias("last_year_trips"),
            F.col("total_revenue").alias("last_year_revenue"),
        )
    )

    return (
        first_df.join(last_df, on=PICKUP_ID_COL, how="full_outer")
        .withColumn("pickup_zone", F.coalesce("last_pickup_zone", "first_pickup_zone"))
        .withColumn("pickup_borough", F.coalesce("last_pickup_borough", "first_pickup_borough"))
        .withColumn(
            "rank_improvement",
            F.when(
                F.col("first_rank").isNotNull() & F.col("last_rank").isNotNull(),
                F.col("first_rank") - F.col("last_rank"),
            ),
        )
        .withColumn(
            "trip_count_change",
            F.coalesce(F.col("last_year_trips"), F.lit(0))
            - F.coalesce(F.col("first_year_trips"), F.lit(0)),
        )
        .withColumn(
            "trip_count_pct_change",
            F.when(
                F.col("first_year_trips") > 0,
                (F.col("last_year_trips") - F.col("first_year_trips"))
                / F.col("first_year_trips"),
            ),
        )
        .filter(
            (F.col("first_rank") <= rank_change_n)
            | (F.col("last_rank") <= rank_change_n)
            | F.col("first_rank").isNull()
            | F.col("last_rank").isNull()
        )
        .select(
            PICKUP_ID_COL,
            "pickup_zone",
            "pickup_borough",
            "first_year",
            "last_year",
            "first_rank",
            "last_rank",
            "rank_improvement",
            "first_year_trips",
            "last_year_trips",
            "trip_count_change",
            "trip_count_pct_change",
            "first_year_revenue",
            "last_year_revenue",
        )
        .orderBy(F.desc("rank_improvement"), F.desc("trip_count_change"))
    )


def build_weekday_weekend_by_year(df: DataFrame) -> DataFrame:
    day_type_df = (
        aggregate_demand(df, ["year", "is_weekend", "hour"])
        .withColumn(
            "day_type",
            F.when(F.col("is_weekend") == 1, F.lit("weekend")).otherwise(F.lit("weekday")),
        )
    )

    return (
        add_partition_share(
            day_type_df,
            ["year", "day_type"],
            "total_trips",
            "hourly_trip_share_in_day_type_year",
        )
        .select(
            "year",
            "day_type",
            "is_weekend",
            "hour",
            "total_trips",
            "total_revenue",
            "avg_revenue_per_trip",
            "avg_fare_amount",
            "avg_trip_distance",
            "avg_trip_duration_min",
            "avg_passenger_count",
            "credit_card_share",
            "cash_share",
            "active_days",
            "active_pickup_zones",
            "hourly_trip_share_in_day_type_year",
        )
        .orderBy("year", "is_weekend", "hour")
    )


def build_revenue_efficiency_temporal(df: DataFrame) -> DataFrame:
    return (
        add_revenue_efficiency_metrics(aggregate_demand(df, ["year", "month", "year_month"]))
        .select(
            "year",
            "month",
            "year_month",
            "total_trips",
            "total_revenue",
            "avg_revenue_per_trip",
            "avg_fare_amount",
            "avg_trip_distance",
            "avg_trip_duration_min",
            "revenue_per_mile",
            "revenue_per_trip_minute",
            "trips_per_active_zone_day",
            "credit_card_share",
            "cash_share",
            "active_days",
            "active_pickup_zones",
        )
        .orderBy("year", "month")
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
    parser = argparse.ArgumentParser(description="Stage 4B - Enriched Temporal Analytics")
    parser.add_argument(
        "--input",
        default=ZONE_HOUR_FEATURES_PATH,
        help="Input path for Stage 3 zone_hour_features parquet.",
    )
    parser.add_argument(
        "--write-csv",
        action="store_true",
        help="Also write CSV outputs.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top zones or peak hours to keep in ranking tables.",
    )
    parser.add_argument(
        "--rank-change-n",
        type=int,
        default=50,
        help="Keep zones ranked in the top N in the first or last year.",
    )

    args = parser.parse_args()

    spark = create_spark_session(f"{ANALYTICS_APP_NAME} - Temporal Enriched")
    zone_hour_df = spark.read.parquet(args.input)
    validate_input_schema(zone_hour_df)

    print("\n===== temporal enriched input schema =====")
    zone_hour_df.printSchema()

    output_tables = {
        "yearly_summary": build_yearly_summary(zone_hour_df),
        "year_month_demand": build_year_month_demand(zone_hour_df),
        "month_of_year_pattern": build_month_of_year_pattern(zone_hour_df),
        "year_hourly_pattern": build_year_hourly_pattern(zone_hour_df),
        "year_weekday_hour_heatmap": build_year_weekday_hour_heatmap(zone_hour_df),
        "borough_year_month": build_borough_year_month(zone_hour_df),
        "borough_year_hour": build_borough_year_hour(zone_hour_df),
        "rush_hour_by_year": build_rush_hour_by_year(zone_hour_df),
        "peak_hour_summary": build_peak_hour_summary(zone_hour_df, args.top_n),
        "top_zones_by_year": build_top_zones_by_year(zone_hour_df, args.top_n),
        "zone_rank_change": build_zone_rank_change(zone_hour_df, args.rank_change_n),
        "weekday_weekend_by_year": build_weekday_weekend_by_year(zone_hour_df),
        "revenue_efficiency_temporal": build_revenue_efficiency_temporal(zone_hour_df),
    }

    for table_name, table_df in output_tables.items():
        print(f"\n===== Writing {table_name} =====")
        table_df.show(20, truncate=False)
        write_output_table(table_df, table_name, args.write_csv)

    spark.stop()
    print("\nTemporal enriched analytics completed successfully.")


if __name__ == "__main__":
    main()
