"""Normalize zone-hour forecast predictions into a dashboard table."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_PATH = PROJECT_ROOT / "outputs" / "predictions" / "forecast_predictions.csv"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "outputs" / "tables" / "forecast" / "csv" / "forecast_zone_hour.csv"


OUTPUT_FIELDS = [
    "pickup_date",
    "year",
    "month",
    "year_month",
    "day",
    "hour",
    "PULocationID",
    "pickup_zone",
    "pickup_borough",
    "actual_trip_count",
    "predicted_trip_count",
    "prediction_error",
    "absolute_error",
    "absolute_percentage_error",
]


def to_float(value: str | None, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    return float(value)


def normalized_row(row: dict[str, str]) -> dict[str, Any]:
    actual = to_float(row.get("trip_count") or row.get("actual_trip_count"))
    predicted = to_float(row.get("predicted_trip_count"))
    error = to_float(row.get("prediction_error"), actual - predicted)
    absolute_error = to_float(row.get("absolute_error"), abs(error))
    pickup_date = row.get("pickup_date", "")
    year_month = row.get("year_month") or pickup_date[:7]

    return {
        "pickup_date": pickup_date,
        "year": row.get("year", ""),
        "month": row.get("month", ""),
        "year_month": year_month,
        "day": row.get("day", ""),
        "hour": row.get("hour", ""),
        "PULocationID": row.get("PULocationID", ""),
        "pickup_zone": row.get("pickup_zone", ""),
        "pickup_borough": row.get("pickup_borough", ""),
        "actual_trip_count": actual,
        "predicted_trip_count": predicted,
        "prediction_error": error,
        "absolute_error": absolute_error,
        "absolute_percentage_error": (absolute_error / actual * 100.0) if actual else None,
    }


def export_forecast_zone_hour(input_path: Path, output_path: Path) -> int:
    if not input_path.exists():
        raise FileNotFoundError(f"Forecast predictions not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0

    with input_path.open("r", encoding="utf-8-sig", newline="") as input_file:
        reader = csv.DictReader(input_file)

        with output_path.open("w", encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=OUTPUT_FIELDS)
            writer.writeheader()

            for row in reader:
                writer.writerow(normalized_row(row))
                rows_written += 1

    return rows_written


def main() -> None:
    parser = argparse.ArgumentParser(description="Export forecast zone-hour dashboard table")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    rows = export_forecast_zone_hour(args.input, args.output)
    print(f"forecast_zone_hour saved to: {args.output}")
    print(f"rows: {rows}")


if __name__ == "__main__":
    main()
