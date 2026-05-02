"""Prepare stable CSV assets for the D3 dashboard.

Spark writes CSV output as directories that contain part files. A browser-based
D3 page cannot list those folders, so this script merges each Spark CSV folder
into a predictable file under src/visualization/dashboard/data/.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPORAL_CSV_DIR = PROJECT_ROOT / "outputs" / "tables" / "temporal" / "csv"
TABLES_DIR = PROJECT_ROOT / "outputs" / "tables"
PREDICTIONS_DIR = PROJECT_ROOT / "outputs" / "predictions"
DEFAULT_DASHBOARD_DATA_DIR = Path(__file__).resolve().parent / "dashboard" / "data"


TEMPORAL_TABLES = [
    "kpi_summary",
    "hourly_demand",
    "daily_demand",
    "monthly_demand",
    "weekday_weekend_hourly",
    "weekday_hour_heatmap",
    "borough_hourly_pattern",
    "rush_hour_summary",
    "top_zones_by_hour",
    "top_zones_overall",
]

FORECAST_HEADER = [
    "pickup_date",
    "hour",
    "pickup_zone",
    "actual_trip_count",
    "predicted_trip_count",
]

TOP_ROUTES_HEADER = [
    "route_rank",
    "PULocationID",
    "pickup_zone",
    "pickup_borough",
    "DOLocationID",
    "dropoff_zone",
    "dropoff_borough",
    "route_name",
    "trip_count",
    "avg_trip_distance",
    "avg_trip_duration_min",
    "total_revenue",
]

FORECAST_CANDIDATES = [
    PREDICTIONS_DIR / "forecast_demand.csv",
    PREDICTIONS_DIR / "demand_forecast.csv",
    PREDICTIONS_DIR / "predictions.csv",
    TABLES_DIR / "forecasting" / "forecast_demand.csv",
    TABLES_DIR / "forecasting" / "csv" / "forecast_demand",
]


def find_csv_sources(path: Path) -> list[Path]:
    """Return a single CSV file or Spark part files for a CSV output path."""
    if path.is_file():
        return [path]

    if path.is_dir():
        return sorted(path.glob("part-*.csv"))

    return []


def write_header_only(destination: Path, header: list[str]) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(header)


def merge_csv_sources(sources: list[Path], destination: Path) -> int:
    """Merge CSV files with the same header into destination.

    Returns the number of data rows written.
    """
    if not sources:
        return 0

    destination.parent.mkdir(parents=True, exist_ok=True)

    header: list[str] | None = None
    rows_written = 0

    with destination.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file)

        for source in sources:
            with source.open("r", encoding="utf-8-sig", newline="") as input_file:
                reader = csv.reader(input_file)
                source_header = next(reader, None)

                if not source_header:
                    continue

                if header is None:
                    header = source_header
                    writer.writerow(header)
                elif source_header != header:
                    raise ValueError(
                        f"Header mismatch while merging {source} into {destination}"
                    )

                for row in reader:
                    if row:
                        writer.writerow(row)
                        rows_written += 1

    return rows_written


def copy_temporal_outputs(data_dir: Path) -> None:
    for table_name in TEMPORAL_TABLES:
        source_dir = TEMPORAL_CSV_DIR / table_name
        destination = data_dir / f"{table_name}.csv"
        rows = merge_csv_sources(find_csv_sources(source_dir), destination)

        if rows == 0 and not destination.exists():
            print(f"warning: no CSV source found for {table_name}: {source_dir}")
        else:
            print(f"wrote {destination.relative_to(PROJECT_ROOT)} ({rows} rows)")


def copy_top_routes(data_dir: Path) -> None:
    source = TABLES_DIR / "top_routes_sample.csv"
    destination = data_dir / "top_routes.csv"
    rows = merge_csv_sources(find_csv_sources(source), destination)

    if rows == 0:
        write_header_only(destination, TOP_ROUTES_HEADER)
        print(f"warning: top routes sample not found, wrote empty {destination}")
    else:
        print(f"wrote {destination.relative_to(PROJECT_ROOT)} ({rows} rows)")


def copy_forecast_output(data_dir: Path) -> None:
    destination = data_dir / "forecast_demand.csv"

    for candidate in FORECAST_CANDIDATES:
        rows = merge_csv_sources(find_csv_sources(candidate), destination)
        if rows > 0:
            print(
                "wrote "
                f"{destination.relative_to(PROJECT_ROOT)} from "
                f"{candidate.relative_to(PROJECT_ROOT)} ({rows} rows)"
            )
            return

    write_header_only(destination, FORECAST_HEADER)
    print(
        "stage5 forecast output not found; wrote empty "
        f"{destination.relative_to(PROJECT_ROOT)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare data for the D3 dashboard")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DASHBOARD_DATA_DIR,
        help="Output directory for dashboard CSV assets.",
    )
    args = parser.parse_args()

    data_dir = args.data_dir.resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    if not TEMPORAL_CSV_DIR.exists():
        raise FileNotFoundError(
            f"Stage 4 CSV outputs were not found: {TEMPORAL_CSV_DIR}. "
            "Run src/analytics/temporal_analysis.py --write-csv first."
        )

    copy_temporal_outputs(data_dir)
    copy_top_routes(data_dir)
    copy_forecast_output(data_dir)

    print(f"\nDashboard data is ready in {data_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise
