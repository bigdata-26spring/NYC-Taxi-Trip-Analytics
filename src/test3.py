from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    if len(sys.argv) >= 2:
        input_path = Path(sys.argv[1])
    else:
        input_path = Path("data/processed/cleaned_trips")

    if not input_path.exists():
        print(f"Path not found: {input_path}")
        sys.exit(1)

    print(f"Reading cleaned_trips from: {input_path}")

    spark = (
        SparkSession.builder
        .appName("CheckCleanedTripsYears")
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    df = spark.read.parquet(str(input_path))

    print("\n===== Schema =====")
    df.printSchema()

    print("\n===== Basic Info =====")
    print(f"Rows: {df.count():,}")
    print(f"Columns: {df.columns}")

    # cleaned_trips 里通常应该有 pickup_ts
    if "pickup_ts" in df.columns:
        time_col = "pickup_ts"
    elif "tpep_pickup_datetime" in df.columns:
        time_col = "tpep_pickup_datetime"
    elif "pickup_date" in df.columns:
        time_col = "pickup_date"
    else:
        print("\nNo pickup time column found.")
        print("Expected one of: pickup_ts, tpep_pickup_datetime, pickup_date")
        spark.stop()
        sys.exit(1)

    print(f"\nUsing time column: {time_col}")

    df_time = (
        df
        .withColumn("pickup_year", F.year(F.col(time_col)))
        .withColumn("pickup_month", F.month(F.col(time_col)))
        .withColumn("pickup_year_month", F.date_format(F.col(time_col), "yyyy-MM"))
    )

    print("\n===== Pickup Time Range =====")
    df_time.select(
        F.min(time_col).alias("min_pickup_time"),
        F.max(time_col).alias("max_pickup_time"),
    ).show(truncate=False)

    print("\n===== Row Count by Year =====")
    (
        df_time
        .groupBy("pickup_year")
        .agg(F.count("*").alias("row_count"))
        .orderBy("pickup_year")
        .show(100, truncate=False)
    )

    print("\n===== Row Count by Year-Month =====")
    (
        df_time
        .groupBy("pickup_year_month")
        .agg(F.count("*").alias("row_count"))
        .orderBy("pickup_year_month")
        .show(200, truncate=False)
    )

    print("\n===== Rows Outside 2021-2024 =====")
    outside_df = df_time.filter(
        (F.col("pickup_year") < 2021) | (F.col("pickup_year") > 2024)
    )

    outside_count = outside_df.count()
    print(f"Rows outside 2021-2024: {outside_count:,}")

    if outside_count > 0:
        outside_df.select(
            time_col,
            "pickup_year",
            "pickup_year_month",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "total_amount",
        ).orderBy(time_col).show(50, truncate=False)

    print("\n===== Expected Year Check =====")
    expected_years = {2021, 2022, 2023, 2024}

    actual_years = {
        row["pickup_year"]
        for row in df_time.select("pickup_year").distinct().collect()
        if row["pickup_year"] is not None
    }

    print(f"Expected years: {sorted(expected_years)}")
    print(f"Actual years:   {sorted(actual_years)}")
    print(f"Missing years:  {sorted(expected_years - actual_years)}")
    print(f"Extra years:    {sorted(actual_years - expected_years)}")

    print("\n===== Expected Month Check =====")
    expected_months = {
        f"{y}-{m:02d}"
        for y in range(2021, 2025)
        for m in range(1, 13)
    }

    actual_months = {
        row["pickup_year_month"]
        for row in df_time.select("pickup_year_month").distinct().collect()
        if row["pickup_year_month"] is not None
    }

    print(f"Expected month count: {len(expected_months)}")
    print(f"Actual month count:   {len(actual_months)}")
    print(f"Missing months:       {sorted(expected_months - actual_months)}")
    print(f"Extra months:         {sorted(actual_months - expected_months)}")

    spark.stop()


if __name__ == "__main__":
    main()