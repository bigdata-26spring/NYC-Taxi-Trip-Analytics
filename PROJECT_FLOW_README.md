# NYC Taxi Trip Analytics - Project Flow

This document is a project-flow README for the current implementation. It does not replace the existing `README.md`; it is meant to explain how the pipeline, backend, MongoDB serving layer, and frontend dashboard fit together.

## Project Goal

The project builds an end-to-end analytics system for NYC Yellow Taxi trips:

1. Ingest official TLC trip data and taxi zone reference data.
2. Clean and standardize trip-level records.
3. Build reusable spatial-temporal feature tables.
4. Generate dashboard-ready analytics, map layers, profiles, and route summaries.
5. Train and export demand forecasting outputs.
6. Serve curated results through FastAPI + MongoDB and visualize them in a Vue dashboard.

The current product direction is a production-style operations dashboard: demand patterns, zone intelligence, route networks, map exploration, forecasting, and data tables.

## Repository Structure

```text
NYC-Taxi-Trip-Analytics/
  config/
    config.py                         # Shared paths and Spark app names

  data/
    raw/                              # Official Yellow Taxi parquet files
    lookup/                           # Taxi zone lookup CSV
    processed/                        # Intermediate parquet outputs
      cleaned_trips/
      trip_enriched/
      zone_hour_features/
      zone_daily_features/
      borough_hour_features/
      top_routes/

  outputs/
    tables/                           # Dashboard and analytics CSV outputs
    predictions/                      # Forecast outputs
    figures/                          # Optional chart/figure artifacts

  src/
    ingestion/                        # Raw data loading and schema checks
    cleaning/                         # Trip cleaning
    FeatureAndSpatial/                # Core feature tables
    analytics/                        # Analytical tables, map layers, profiles
    forecasting/                      # Training and forecast export
    serving/                          # MongoDB export script
    visualization/                    # Earlier static dashboard artifacts

  backend/
    app/                              # FastAPI app and routers

  frontend/
    src/                              # Vue + D3 dashboard
```

## Stage 1 - Ingestion

**Purpose:** Load the raw official TLC files into Spark and make sure the schema is usable across months/years.

**Main script:**

```text
src/ingestion/load_raw_data.py
```

**Input:**

```text
data/raw/*.parquet
data/lookup/taxi_zone_lookup.csv
```

**Output:**

```text
data/processed/ingestion/ingestion_summary.txt
```

**What this stage does:**

- Reads NYC Yellow Taxi parquet files.
- Normalizes schema differences across files.
- Loads the taxi zone lookup table.
- Produces an ingestion summary for row counts, schema, and validation notes.

This stage is the foundation for the rest of the pipeline. It does not create dashboard tables directly.

## Stage 2 - Cleaning

**Purpose:** Remove invalid records and standardize trip-level data before any analytics or ML work.

**Main script:**

```text
src/cleaning/clean_trips.py
```

**Input:**

```text
data/raw/*.parquet
```

**Output:**

```text
data/processed/cleaned_trips/
data/processed/cleaning/cleaning_report.txt
```

**What this stage does:**

- Filters invalid pickup/dropoff timestamps.
- Removes impossible or suspicious distances, fares, passenger counts, and durations.
- Standardizes the trip fields used by downstream feature engineering.
- Writes the cleaned parquet dataset into `data/processed/cleaned_trips/`.

The cleaned trip table is still trip-level data, so it should stay local or in a large-data storage layer. It is not intended for MongoDB Atlas.

## Stage 3 - Feature Engineering And Spatial Enrichment

**Purpose:** Build reusable processed tables that later analytics, maps, forecasts, and profiles can share.

**Main scripts:**

```text
src/FeatureAndSpatial/trip_enriched.py
src/FeatureAndSpatial/zone_hour_features.py
src/FeatureAndSpatial/zone_daily_features.py
src/FeatureAndSpatial/borough_hour_features.py
src/FeatureAndSpatial/top_routes.py
```

**Core outputs:**

```text
data/processed/trip_enriched/
data/processed/zone_hour_features/
data/processed/zone_daily_features/
data/processed/borough_hour_features/
data/processed/top_routes/
```

**What this stage does:**

- Joins trips with taxi zone metadata.
- Adds pickup/dropoff borough and zone names.
- Extracts temporal fields such as year, month, day, hour, weekday, and date.
- Aggregates demand by zone-hour and borough-hour.
- Builds route-level aggregates from pickup/dropoff zone pairs.

The two most important reusable processed datasets are:

```text
data/processed/trip_enriched/
data/processed/zone_hour_features/
```

Most newer dashboard tables should be derived from these two datasets instead of re-reading raw parquet files.

## Stage 4 - Analytics, Map Tables, And Profiles

**Purpose:** Turn processed feature data into dashboard-ready tables.

**Main scripts:**

```text
src/analytics/temporal_analysis.py
src/analytics/temporal_analysis_enriched.py
src/analytics/trip_route_analysis.py
src/analytics/zone_centroids.py
src/analytics/map_flow_analysis.py
src/analytics/profile_analysis.py
```

**Major output areas:**

```text
outputs/tables/temporal/csv/
outputs/tables/temporal_enriched/csv/
outputs/tables/trip_route_analytics/csv/
outputs/tables/map/csv/
outputs/tables/profiles/csv/
```

**What this stage does:**

- Builds KPI, hourly, daily, monthly, weekday/hour, borough/hour, and seasonal demand tables.
- Builds route summaries such as top routes, airport routes, inter-borough routes, and borough OD matrices.
- Builds map support tables:
  - `zone_centroids`
  - `od_flow_hour`
  - `od_flow_year_month`
  - `map_replay_sample`
  - `dropoff_zone_hour_features`
  - `zone_balance_hour`
- Builds profile tables:
  - `zone_profile_summary`
  - `route_profile_summary`

**MongoDB size decision:**

Some map tables are intentionally large. The current serving decision is:

```text
Keep local only:
  dropoff_zone_hour_features
  zone_balance_hour
  forecast_zone_hour

Export to MongoDB:
  zone_centroids
  od_flow_hour
  od_flow_year_month
  map_replay_sample
  zone_profile_summary
  route_profile_summary
```

This keeps the Atlas 512MB tier focused on small, dashboard-ready collections instead of large offline feature tables.

## Stage 5 - Forecasting

**Purpose:** Train demand forecasting models and export forecast evaluation data for the dashboard.

**Main scripts:**

```text
src/forecasting/prepare_training_data.py
src/forecasting/train_model.py
src/forecasting/evaluate_model.py
src/forecasting/export_zone_hour_forecast.py
```

**Input:**

```text
data/processed/zone_hour_features/
```

**Output examples:**

```text
outputs/tables/model_comparison.csv
outputs/tables/model_evaluation_metrics.csv
outputs/predictions/plot_actual_vs_predicted_monthly.csv
outputs/predictions/forecast_daily_actual_predicted.csv
outputs/predictions/forecast_error_by_hour.csv
outputs/predictions/forecast_error_by_borough.csv
outputs/predictions/forecast_zone_accuracy.csv
outputs/predictions/forecast_weekday_hour_error_heatmap.csv
outputs/tables/forecast/csv/forecast_zone_hour.csv
```

**What this stage does:**

- Prepares zone-hour training data.
- Trains and evaluates forecasting models.
- Exports model comparison and error-analysis tables.
- Produces zone-level forecast accuracy summaries for the frontend map.

`forecast_zone_hour.csv` is useful for local analysis, but it is too large for the current Atlas plan, so it should stay local unless the database tier is upgraded or the table is sampled/aggregated further.

## Stage 6 - Serving Layer And Frontend Dashboard

**Purpose:** Move curated outputs into MongoDB, serve them through FastAPI, and render the production-style dashboard in Vue.

### MongoDB Export

**Main script:**

```text
src/serving/export_analytics_to_mongodb.py
```

**Default behavior:**

- Exports core dashboard collections.
- Does not export raw trips.
- Does not export large processed parquet tables.
- Does not export the known oversized local-only tables.

**Useful commands:**

```powershell
# Preview what would be exported without opening MongoDB connection
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb --dry-run

# Export core tables
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb

# Export core + Atlas-safe optional tables
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb --include-optional
```

MongoDB settings are read from `.env`:

```text
MONGODB_URI=...
MONGODB_DB=nyc_taxi_analytics
CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
```

### Backend

**Framework:** FastAPI

**Main app:**

```text
backend/app/main.py
```

**Routers:**

```text
backend/app/routers/meta.py
backend/app/routers/dashboard.py
backend/app/routers/temporal.py
backend/app/routers/spatial.py
backend/app/routers/routes.py
backend/app/routers/map.py
backend/app/routers/profiles.py
backend/app/routers/business.py
backend/app/routers/forecast.py
```

**Local start command:**

```powershell
venv\Scripts\python.exe -m uvicorn backend.app.main:app --reload --port 8001
```

**Local API docs:**

```text
http://127.0.0.1:8001/docs
```

### Frontend

**Framework:** Vue 3 + Vite + D3

**Main files:**

```text
frontend/src/App.vue
frontend/src/api/client.js
frontend/src/styles.css
frontend/src/components/
```

**Current dashboard sections:**

- Command Center
- Demand Patterns
- Zone Intelligence
- Route Network
- Forecast Lab
- Data Tables

**Local start command:**

```powershell
cd frontend
npm run dev
```

**Local frontend URL:**

```text
http://127.0.0.1:5173
```

## End-To-End Pipeline Order

When rebuilding everything from scratch, the conceptual order is:

```text
Stage 1  ingestion
Stage 2  cleaning
Stage 3  feature engineering and spatial enrichment
Stage 4  analytics, map tables, profiles
Stage 5  forecasting
Stage 6  MongoDB export, backend API, frontend dashboard
```

The project should avoid pushing trip-level or very large hour-zone tables into MongoDB Atlas. MongoDB should be treated as the dashboard serving layer, not the full data lake.

## Deployment Notes For Next Step

The next deployment task should decide:

1. Where to host the FastAPI backend.
2. Where to host the Vue static frontend.
3. Whether MongoDB Atlas remains on the 512MB tier.
4. How environment variables will be injected on the server.
5. Whether the large local-only tables need object storage, a larger database tier, or pre-aggregated API-specific extracts.

Recommended deployment split:

```text
Large data / Spark outputs:
  local disk, cloud object storage, or future HDFS/S3 layer

Dashboard serving data:
  MongoDB Atlas curated collections

Backend:
  FastAPI service with MONGODB_URI, MONGODB_DB, CORS_ORIGINS

Frontend:
  Vite production build served as static files
```

Before deployment, run a frontend production build and a backend health check locally, then configure server CORS to the final frontend domain.
