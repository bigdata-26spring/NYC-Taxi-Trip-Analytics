from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def load_env_file(path: Path = PROJECT_ROOT / ".env") -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


def parse_csv_env(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return default

    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    mongodb_uri: str
    mongodb_db: str
    cors_origins: list[str]
    app_name: str = "NYC Taxi Trip Analytics API"
    api_prefix: str = "/api"


def get_settings() -> Settings:
    load_env_file()

    mongodb_uri = os.environ.get("MONGODB_URI", "")
    mongodb_db = os.environ.get("MONGODB_DB", "nyc_taxi_analytics")
    cors_origins = parse_csv_env(
        os.environ.get("CORS_ORIGINS"),
        [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:8000",
            "http://127.0.0.1:8000",
        ],
    )

    return Settings(
        mongodb_uri=mongodb_uri,
        mongodb_db=mongodb_db,
        cors_origins=cors_origins,
    )
