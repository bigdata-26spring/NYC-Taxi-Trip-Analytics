from __future__ import annotations

from functools import lru_cache
from typing import Any

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from backend.app.core.config import get_settings


@lru_cache(maxsize=1)
def get_client() -> MongoClient:
    settings = get_settings()
    if not settings.mongodb_uri:
        raise RuntimeError("MONGODB_URI is not configured.")

    return MongoClient(settings.mongodb_uri, serverSelectionTimeoutMS=10000)


def get_database() -> Database:
    settings = get_settings()
    return get_client()[settings.mongodb_db]


def ping_database() -> bool:
    get_client().admin.command("ping")
    return True


def collection(name: str) -> Collection:
    return get_database()[name]


def clean_document(document: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}

    for key, value in document.items():
        if key == "_id":
            continue
        if key == "_imported_at" and value is not None:
            cleaned[key] = value.isoformat()
        else:
            cleaned[key] = value

    return cleaned


def clean_documents(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [clean_document(document) for document in documents]


def find_many(
    collection_name: str,
    query: dict[str, Any] | None = None,
    *,
    sort: list[tuple[str, int]] | None = None,
    limit: int | None = None,
    projection: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    cursor = collection(collection_name).find(query or {}, projection)

    if sort:
        cursor = cursor.sort(sort)
    if limit:
        cursor = cursor.limit(limit)

    return clean_documents(list(cursor))


def find_one(collection_name: str, query: dict[str, Any] | None = None) -> dict[str, Any] | None:
    document = collection(collection_name).find_one(query or {})
    return clean_document(document) if document else None


def distinct_values(collection_name: str, field: str, query: dict[str, Any] | None = None) -> list[Any]:
    values = collection(collection_name).distinct(field, query or {})
    return sorted(value for value in values if value is not None)


def sort_asc(field: str) -> tuple[str, int]:
    return (field, ASCENDING)


def sort_desc(field: str) -> tuple[str, int]:
    return (field, DESCENDING)
