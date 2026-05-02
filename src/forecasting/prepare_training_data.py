import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from tqdm.auto import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from src.ingestion.load_raw_data import create_spark_session


APP_NAME = "Prepare Forecasting Training Data"

ZONE_HOUR_FEATURES_PATH = PROJECT_ROOT / "data" / "processed" / "zone_hour_features"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "forecasting"

FULL_OUTPUT_PATH = OUTPUT_DIR / "training_data_full.csv"
TRAIN_OUTPUT_PATH = OUTPUT_DIR / "training_data_train.csv"
EVAL_OUTPUT_PATH = OUTPUT_DIR / "training_data_eval.csv"
SPLIT_INFO_PATH = OUTPUT_DIR / "train_eval_split_info.csv"

FULL_START_DATE = "2021-01-01"
FULL_END_DATE = "2024-12-31"
TRAIN_START_DATE = "2021-01-01"
TRAIN_END_DATE = "2023-12-31"
EVAL_START_DATE = "2024-01-01"
EVAL_END_DATE = "2024-12-31"


def main():
    spark = create_spark_session(APP_NAME)
    progress = tqdm(total=8, desc="Preparing forecasting data", unit="step")

    print("Reading zone_hour_features...")
    df = spark.read.parquet(str(ZONE_HOUR_FEATURES_PATH))
    progress.update(1)

    print("Creating lag and rolling features...")

    window_zone_time = Window.partitionBy("PULocationID").orderBy("pickup_date", "hour")

    rolling_24h_window = (
        Window.partitionBy("PULocationID")
        .orderBy("pickup_date", "hour")
        .rowsBetween(-24, -1)
    )

    df = (
        df
        .withColumn("lag_1_hour_trip_count", F.lag("trip_count", 1).over(window_zone_time))
        .withColumn("lag_24_hour_trip_count", F.lag("trip_count", 24).over(window_zone_time))
        .withColumn("rolling_24h_avg_trip_count", F.avg("trip_count").over(rolling_24h_window))
    )
    progress.update(1)

    selected_cols = [
        "PULocationID",
        "pickup_zone",
        "pickup_borough",
        "pickup_date",
        "year",
        "month",
        "day",
        "day_of_week",
        "is_weekend",
        "hour",
        "avg_trip_distance",
        "avg_trip_duration_min",
        "avg_fare_amount",
        "avg_total_amount",
        "avg_passenger_count",
        "avg_speed_mph",
        "credit_card_share",
        "cash_share",
        "lag_1_hour_trip_count",
        "lag_24_hour_trip_count",
        "rolling_24h_avg_trip_count",
        "trip_count",
    ]

    df = df.select(*selected_cols)

    df = df.fillna({
        "avg_trip_distance": 0.0,
        "avg_trip_duration_min": 0.0,
        "avg_fare_amount": 0.0,
        "avg_total_amount": 0.0,
        "avg_passenger_count": 0.0,
        "avg_speed_mph": 0.0,
        "credit_card_share": 0.0,
        "cash_share": 0.0,
        "lag_1_hour_trip_count": 0.0,
        "lag_24_hour_trip_count": 0.0,
        "rolling_24h_avg_trip_count": 0.0,
    })
    progress.update(1)

    print("Converting Spark DataFrame to Pandas...")
    pdf = df.toPandas()
    progress.update(1)

    print("Cleaning and sorting full training data...")
    pdf["pickup_date"] = pd.to_datetime(pdf["pickup_date"])

    pdf = pdf[
        (pdf["pickup_date"] >= FULL_START_DATE)
        & (pdf["pickup_date"] <= FULL_END_DATE)
    ].copy()

    pdf = pdf.sort_values(["pickup_date", "hour", "PULocationID"]).reset_index(drop=True)
    progress.update(1)

    print("Splitting data by time: 2021-2023 train, 2024 evaluation/test...")

    train_df = pdf[
        (pdf["pickup_date"] >= TRAIN_START_DATE)
        & (pdf["pickup_date"] <= TRAIN_END_DATE)
    ].copy()

    eval_df = pdf[
        (pdf["pickup_date"] >= EVAL_START_DATE)
        & (pdf["pickup_date"] <= EVAL_END_DATE)
    ].copy()

    train_df = train_df.sort_values(["pickup_date", "hour", "PULocationID"]).reset_index(drop=True)
    eval_df = eval_df.sort_values(["pickup_date", "hour", "PULocationID"]).reset_index(drop=True)
    progress.update(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Saving full data to: {FULL_OUTPUT_PATH}")
    pdf.to_csv(FULL_OUTPUT_PATH, index=False)

    print(f"Saving train data to: {TRAIN_OUTPUT_PATH}")
    train_df.to_csv(TRAIN_OUTPUT_PATH, index=False)

    print(f"Saving evaluation data to: {EVAL_OUTPUT_PATH}")
    eval_df.to_csv(EVAL_OUTPUT_PATH, index=False)
    progress.update(1)

    split_info = pd.DataFrame([
        {
            "dataset": "full",
            "rows": len(pdf),
            "start_date": pdf["pickup_date"].min(),
            "end_date": pdf["pickup_date"].max(),
        },
        {
            "dataset": "train",
            "rows": len(train_df),
            "start_date": train_df["pickup_date"].min(),
            "end_date": train_df["pickup_date"].max(),
        },
        {
            "dataset": "evaluation",
            "rows": len(eval_df),
            "start_date": eval_df["pickup_date"].min(),
            "end_date": eval_df["pickup_date"].max(),
        },
    ])

    split_info.to_csv(SPLIT_INFO_PATH, index=False)
    progress.update(1)
    progress.close()

    print("\nDone.")
    print(f"Full rows: {len(pdf)}")
    print(f"Train rows: {len(train_df)}")
    print(f"Evaluation rows: {len(eval_df)}")
    print(f"Split info saved to: {SPLIT_INFO_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
