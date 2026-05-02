from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    if len(sys.argv) >= 2:
        input_path = Path(sys.argv[1])
    else:
        input_path = Path("data/processed/zone_hour_features")

    if not input_path.exists():
        print(f"Path not found: {input_path}")
        sys.exit(1)

    print(f"Reading zone_hour_features from: {input_path}")

    spark = (
        SparkSession.builder
        .appName("CheckZoneHourFeaturesTime")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    df = spark.read.parquet(str(input_path))

    print("\n===== Schema =====")
    df.printSchema()

    print("\n===== Basic Info =====")
    row_count = df.count()
    print(f"Rows: {row_count:,}")
    print(f"Columns: {df.columns}")

    required_cols = ["pickup_date", "year", "month", "year_month", "hour"]

    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        print("\nMissing required time columns:")
        for c in missing_cols:
            print(f"- {c}")
        spark.stop()
        sys.exit(1)

    print("\n===== Pickup Date Range =====")
    df.select(
        F.min("pickup_date").alias("min_pickup_date"),
        F.max("pickup_date").alias("max_pickup_date"),
    ).show(truncate=False)

    print("\n===== Distinct Years =====")
    (
        df
        .select("year")
        .distinct()
        .orderBy("year")
        .show(50, truncate=False)
    )

    print("\n===== Row Count by Year =====")
    (
        df
        .groupBy("year")
        .agg(F.count("*").alias("row_count"))
        .orderBy("year")
        .show(50, truncate=False)
    )

    print("\n===== Row Count by Year-Month =====")
    (
        df
        .groupBy("year_month")
        .agg(F.count("*").alias("row_count"))
        .orderBy("year_month")
        .show(100, truncate=False)
    )

    print("\n===== Hour Range =====")
    df.select(
        F.min("hour").alias("min_hour"),
        F.max("hour").alias("max_hour"),
    ).show(truncate=False)

    print("\n===== Hour Distribution =====")
    (
        df
        .groupBy("hour")
        .agg(F.count("*").alias("row_count"))
        .orderBy("hour")
        .show(30, truncate=False)
    )

    print("\n===== Rows Outside 2021-2024 =====")
    outside_df = df.filter(
        (F.col("year") < 2021) | (F.col("year") > 2024)
    )

    outside_count = outside_df.count()
    print(f"Rows outside 2021-2024: {outside_count:,}")

    if outside_count > 0:
        outside_df.select(
            "pickup_date",
            "year",
            "month",
            "year_month",
            "hour",
            "pickup_zone",
            "pickup_borough",
            "trip_count",
        ).orderBy("pickup_date", "hour").show(50, truncate=False)

    print("\n===== Expected Year Check =====")
    expected_years = [2021, 2022, 2023, 2024]

    actual_years = [
        row["year"]
        for row in df.select("year").distinct().orderBy("year").collect()
    ]

    print(f"Expected years: {expected_years}")
    print(f"Actual years:   {actual_years}")

    missing_years = sorted(set(expected_years) - set(actual_years))
    extra_years = sorted(set(actual_years) - set(expected_years))

    print(f"Missing years:  {missing_years}")
    print(f"Extra years:    {extra_years}")

    print("\n===== Date Column Consistency Check =====")

    consistency_df = (
        df
        .withColumn("year_from_date", F.year("pickup_date"))
        .withColumn("month_from_date", F.month("pickup_date"))
        .withColumn("year_month_from_date", F.date_format("pickup_date", "yyyy-MM"))
    )

    bad_year_count = consistency_df.filter(
        F.col("year") != F.col("year_from_date")
    ).count()

    bad_month_count = consistency_df.filter(
        F.col("month") != F.col("month_from_date")
    ).count()

    bad_year_month_count = consistency_df.filter(
        F.col("year_month") != F.col("year_month_from_date")
    ).count()

    print(f"Rows with inconsistent year:       {bad_year_count:,}")
    print(f"Rows with inconsistent month:      {bad_month_count:,}")
    print(f"Rows with inconsistent year_month: {bad_year_month_count:,}")

    if bad_year_count > 0 or bad_month_count > 0 or bad_year_month_count > 0:
        print("\nSample inconsistent rows:")
        consistency_df.filter(
            (F.col("year") != F.col("year_from_date")) |
            (F.col("month") != F.col("month_from_date")) |
            (F.col("year_month") != F.col("year_month_from_date"))
        ).select(
            "pickup_date",
            "year",
            "year_from_date",
            "month",
            "month_from_date",
            "year_month",
            "year_month_from_date",
            "hour",
            "pickup_zone",
            "trip_count",
        ).show(50, truncate=False)

    print("\n===== Month Coverage Check =====")

    expected_months = spark.createDataFrame(
        [(y, m, f"{y}-{m:02d}") for y in expected_years for m in range(1, 13)],
        ["expected_year", "expected_month", "expected_year_month"]
    )

    actual_months = (
        df
        .select(F.col("year_month").alias("actual_year_month"))
        .distinct()
    )

    missing_months = (
        expected_months
        .join(
            actual_months,
            expected_months["expected_year_month"] == actual_months["actual_year_month"],
            "left_anti"
        )
        .orderBy("expected_year_month")
    )

    print("Missing year-months from 2021-01 to 2024-12:")
    missing_months.show(100, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()