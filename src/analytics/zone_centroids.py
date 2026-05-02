"""Build taxi zone centroid lookup for map layers.

The TLC trip data only stores taxi zone IDs. This script derives stable
longitude/latitude anchor points from the taxi zone GeoJSON so OD flow and
replay layers can join zone IDs to coordinates without recalculating them in
the browser.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_GEOJSON_PATH = PROJECT_ROOT / "frontend" / "public" / "data" / "taxi_zones.geojson"
DEFAULT_LOOKUP_PATH = PROJECT_ROOT / "data" / "lookup" / "taxi_zone_lookup.csv"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "outputs" / "tables" / "map" / "csv" / "zone_centroids.csv"


def load_zone_lookup(path: Path) -> dict[int, dict[str, str]]:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8-sig", newline="") as input_file:
        reader = csv.DictReader(input_file)
        return {
            int(row["LocationID"]): {
                "zone": row.get("Zone", ""),
                "borough": row.get("Borough", ""),
                "service_zone": row.get("service_zone", ""),
            }
            for row in reader
            if row.get("LocationID")
        }


def iter_exterior_rings(geometry: dict[str, Any]) -> list[list[list[float]]]:
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates") or []

    if geometry_type == "Polygon":
        return [coordinates[0]] if coordinates else []

    if geometry_type == "MultiPolygon":
        return [polygon[0] for polygon in coordinates if polygon]

    return []


def ring_centroid(ring: list[list[float]]) -> tuple[float, float, float]:
    """Return signed area and centroid for a lon/lat ring."""
    if len(ring) < 3:
        return 0.0, 0.0, 0.0

    area_twice = 0.0
    centroid_x = 0.0
    centroid_y = 0.0

    for index, point in enumerate(ring):
        next_point = ring[(index + 1) % len(ring)]
        x0, y0 = float(point[0]), float(point[1])
        x1, y1 = float(next_point[0]), float(next_point[1])
        cross = x0 * y1 - x1 * y0
        area_twice += cross
        centroid_x += (x0 + x1) * cross
        centroid_y += (y0 + y1) * cross

    area = area_twice / 2.0
    if abs(area) < 1e-12:
        return 0.0, 0.0, 0.0

    return area, centroid_x / (6.0 * area), centroid_y / (6.0 * area)


def feature_centroid(feature: dict[str, Any]) -> tuple[float, float, float, float, float, float]:
    rings = iter_exterior_rings(feature.get("geometry") or {})
    all_points = [point for ring in rings for point in ring]
    if not all_points:
        raise ValueError("Feature has no polygon coordinates")

    weighted_area = 0.0
    weighted_x = 0.0
    weighted_y = 0.0

    for ring in rings:
        area, centroid_x, centroid_y = ring_centroid(ring)
        weight = abs(area)
        weighted_area += weight
        weighted_x += centroid_x * weight
        weighted_y += centroid_y * weight

    if weighted_area > 0:
        centroid_lon = weighted_x / weighted_area
        centroid_lat = weighted_y / weighted_area
    else:
        centroid_lon = sum(float(point[0]) for point in all_points) / len(all_points)
        centroid_lat = sum(float(point[1]) for point in all_points) / len(all_points)

    lon_values = [float(point[0]) for point in all_points]
    lat_values = [float(point[1]) for point in all_points]
    return (
        centroid_lon,
        centroid_lat,
        min(lon_values),
        min(lat_values),
        max(lon_values),
        max(lat_values),
    )


def build_rows(geojson_path: Path, lookup_path: Path) -> list[dict[str, Any]]:
    lookup = load_zone_lookup(lookup_path)
    geojson = json.loads(geojson_path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []

    for feature in geojson.get("features", []):
        properties = feature.get("properties") or {}
        location_id = int(properties["LocationID"])
        lookup_row = lookup.get(location_id, {})
        centroid_lon, centroid_lat, min_lon, min_lat, max_lon, max_lat = feature_centroid(feature)

        rows.append(
            {
                "LocationID": location_id,
                "zone": lookup_row.get("zone") or properties.get("zone", ""),
                "borough": lookup_row.get("borough") or properties.get("borough", ""),
                "service_zone": lookup_row.get("service_zone", ""),
                "centroid_lon": round(centroid_lon, 8),
                "centroid_lat": round(centroid_lat, 8),
                "min_lon": round(min_lon, 8),
                "min_lat": round(min_lat, 8),
                "max_lon": round(max_lon, 8),
                "max_lat": round(max_lat, 8),
            }
        )

    return sorted(rows, key=lambda row: int(row["LocationID"]))


def write_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "LocationID",
        "zone",
        "borough",
        "service_zone",
        "centroid_lon",
        "centroid_lat",
        "min_lon",
        "min_lat",
        "max_lon",
        "max_lat",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build taxi zone centroid lookup table")
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON_PATH)
    parser.add_argument("--lookup", type=Path, default=DEFAULT_LOOKUP_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    if not args.geojson.exists():
        raise FileNotFoundError(f"GeoJSON not found: {args.geojson}")

    rows = build_rows(args.geojson, args.lookup)
    write_csv(rows, args.output)
    print(f"zone_centroids saved to: {args.output}")
    print(f"rows: {len(rows)}")


if __name__ == "__main__":
    main()
