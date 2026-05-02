from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app import db
from backend.app.core.config import get_settings
from backend.app.routers import (
    business,
    dashboard,
    forecast,
    map as map_router,
    meta,
    profiles,
    routes,
    spatial,
    temporal,
)


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="Backend API for NYC Taxi Trip Analytics dashboard.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root() -> dict:
    return {
        "name": settings.app_name,
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
def health() -> dict:
    db.ping_database()
    return {
        "status": "ok",
        "database": settings.mongodb_db,
    }


app.include_router(meta.router, prefix=settings.api_prefix)
app.include_router(dashboard.router, prefix=settings.api_prefix)
app.include_router(temporal.router, prefix=settings.api_prefix)
app.include_router(spatial.router, prefix=settings.api_prefix)
app.include_router(routes.router, prefix=settings.api_prefix)
app.include_router(map_router.router, prefix=settings.api_prefix)
app.include_router(profiles.router, prefix=settings.api_prefix)
app.include_router(business.router, prefix=settings.api_prefix)
app.include_router(forecast.router, prefix=settings.api_prefix)
