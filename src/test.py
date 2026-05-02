from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    if len(sys.argv) < 2:
        print("Usage: python src/test.py <parquet_file_path>")
        sys.exit(1)

    parquet_path = Path(sys.argv[1])

    if not parquet_path.exists():
        print(f"File not found: {parquet_path}")
        sys.exit(1)

    print(f"Reading parquet file: {parquet_path}")

    spark = (
        SparkSession.builder
        .appName("CheckYellowTaxiParquetTime")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    df = spark.read.parquet(str(parquet_path))

    print("\n===== Schema =====")
    df.printSchema()

    print("\n===== Basic Info =====")
    print(f"Rows: {df.count():,}")
    print(f"Columns: {df.columns}")

    pickup_col = "tpep_pickup_datetime"
    dropoff_col = "tpep_dropoff_datetime"

    if pickup_col not in df.columns or dropoff_col not in df.columns:
        print("\nTime columns not found.")
        print("Expected columns:")
        print(f"- {pickup_col}")
        print(f"- {dropoff_col}")
        spark.stop()
        sys.exit(1)

    print("\n===== Pickup / Dropoff Time Range =====")
    df.select(
        F.min(pickup_col).alias("min_pickup_time"),
        F.max(pickup_col).alias("max_pickup_time"),
        F.min(dropoff_col).alias("min_dropoff_time"),
        F.max(dropoff_col).alias("max_dropoff_time"),
    ).show(truncate=False)

    print("\n===== Null Time Count =====")
    df.select(
        F.sum(F.col(pickup_col).isNull().cast("int")).alias("null_pickup_time"),
        F.sum(F.col(dropoff_col).isNull().cast("int")).alias("null_dropoff_time"),
    ).show(truncate=False)

    jan_start = "2021-01-01 00:00:00"
    feb_start = "2021-02-01 00:00:00"

    df_checked = df.withColumn(
        "pickup_in_2021_01",
        (F.col(pickup_col) >= F.lit(jan_start)) &
        (F.col(pickup_col) < F.lit(feb_start))
    )

    print("\n===== Pickup Date Validation =====")
    df_checked.groupBy("pickup_in_2021_01").count().show()

    print("\n===== Pickup Year-Month Distribution =====")
    (
        df
        .withColumn("pickup_year_month", F.date_format(F.col(pickup_col), "yyyy-MM"))
        .groupBy("pickup_year_month")
        .count()
        .orderBy("pickup_year_month")
        .show(100, truncate=False)
    )

    print("\n===== Dropoff Year-Month Distribution =====")
    (
        df
        .withColumn("dropoff_year_month", F.date_format(F.col(dropoff_col), "yyyy-MM"))
        .groupBy("dropoff_year_month")
        .count()
        .orderBy("dropoff_year_month")
        .show(100, truncate=False)
    )

    print("\n===== Sample Rows Outside 2021-01 Pickup Time =====")
    (
        df_checked
        .filter(~F.col("pickup_in_2021_01"))
        .select(
            pickup_col,
            dropoff_col,
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "total_amount",
        )
        .show(20, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()