# NYC Taxi Trip Analytics

End-to-end big data analytics project for NYC Yellow Taxi trips. The project
builds an offline Spark pipeline, demand forecasting outputs, MongoDB serving
tables, a FastAPI backend, and a Vue + D3 dashboard.

The online app does not run Spark jobs or train models. Heavy data processing is
done offline first, then curated dashboard-ready tables are exported to MongoDB
Atlas and served through the API.

## Architecture

```text
TLC Yellow Taxi parquet files
  -> ingestion
  -> cleaning
  -> feature engineering and spatial enrichment
  -> analytics, map, route, and profile tables
  -> forecasting
  -> MongoDB export
  -> FastAPI backend
  -> Vue + Vite + D3 frontend
```

Main layers:

| Layer | Location | Purpose |
|---|---|---|
| Data pipeline | `src/` | Spark and Python jobs that process raw taxi data |
| Processed data | `data/processed/` | Local intermediate parquet and CSV artifacts |
| Analytics outputs | `outputs/` | Final tables, figures, predictions, and models |
| Serving export | `src/serving/` | Loads selected CSV outputs into MongoDB Atlas |
| Backend | `backend/` | FastAPI service that reads MongoDB collections |
| Frontend | `frontend/` | Vue dashboard that calls the backend API |

## Repository Structure

```text
NYC-Taxi-Trip-Analytics/
  config/
    config.py                         Shared paths and Spark runtime settings

  data/
    raw/                              TLC Yellow Taxi parquet files
    lookup/                           Taxi zone lookup table
    taxi_zones/                       Taxi zone shapefile
    processed/                        Generated local pipeline outputs

  src/
    ingestion/                        Raw data loading and schema validation
    cleaning/                         Trip-level filtering and standardization
    FeatureAndSpatial/                Feature tables and spatial enrichment
    analytics/                        Temporal, route, map, and profile tables
    forecasting/                      Training data, models, and evaluation
    serving/                          MongoDB export script

  backend/
    app/                              FastAPI application and routers
    requirements.txt                  Lightweight backend dependencies

  frontend/
    src/                              Vue application source
    public/data/taxi_zones.geojson    Map geometry used by the frontend
    package.json                      Frontend scripts and dependencies

  scripts/
    build_spark_package.ps1           Builds zip package for spark-submit

  outputs/                            Generated analytics and model artifacts
  render.yaml                         Render backend deployment config
  requirements.txt                    Full local data pipeline dependencies
```

## Prerequisites

- Python 3.11 recommended
- Java runtime for PySpark
- Node.js and npm for the frontend
- MongoDB Atlas database for the served dashboard
- Enough local disk and memory for the selected parquet data

The root `requirements.txt` is for local data processing. The backend has a
separate smaller dependency file at `backend/requirements.txt` for deployment.

## Environment Setup

From the project root:

```powershell
python -m venv venv
venv\Scripts\python.exe -m pip install --upgrade pip
venv\Scripts\pip.exe install -r requirements.txt
```

Install frontend dependencies:

```powershell
cd frontend
npm install
cd ..
```

Create a project-root `.env` file for MongoDB and backend CORS settings:

```text
MONGODB_URI=mongodb+srv://<user>:<password>@<cluster-url>/?retryWrites=true&w=majority
MONGODB_DB=nyc_taxi_analytics
CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
```

For local frontend development, `frontend/.env.example` shows the default API
base:

```text
VITE_API_BASE=http://127.0.0.1:8001/api
```

## Data Inputs

The project expects these source files:

```text
data/raw/yellow_tripdata_YYYY-MM.parquet
data/lookup/taxi_zone_lookup.csv
frontend/public/data/taxi_zones.geojson
```

The included `.gitignore` keeps generated folders out of git:

```text
data/processed/
outputs/
dist/
frontend/node_modules/
frontend/dist/
venv/
```

Raw TLC data can be large. This repository is configured to keep selected
2021-2024 Yellow Taxi parquet files while ignoring other raw files by default. Full raw data can be downloaded from the TLC website:

```text
https://www.nyc.gov/html/tlc/html/about/about_the_tlc_trip_data_page.shtml
```

## End-To-End Local Pipeline

Run commands from the project root unless a command says otherwise.

### 1. Ingestion

```powershell
venv\Scripts\python.exe src\ingestion\load_raw_data.py
```

Purpose:

- Starts a Spark session.
- Reads all raw Yellow Taxi parquet files from `data/raw/`.
- Loads `data/lookup/taxi_zone_lookup.csv`.
- Normalizes schema differences across monthly files.
- Writes an ingestion summary.

Main output:

```text
data/processed/ingestion/ingestion_summary.txt
```

### 2. Cleaning

```powershell
venv\Scripts\python.exe src\cleaning\clean_trips.py
```

Purpose:

- Reuses the ingestion loader.
- Filters invalid timestamps, unrealistic durations, fares, distances, passenger
  counts, and invalid location IDs.
- Produces a clean trip-level parquet dataset partitioned by year and month.

Main outputs:

```text
data/processed/cleaned_trips/
data/processed/cleaning/cleaning_report.txt
```

### 3. Feature Engineering And Spatial Enrichment

```powershell
venv\Scripts\python.exe src\FeatureAndSpatial\trip_enriched.py
venv\Scripts\python.exe src\FeatureAndSpatial\zone_hour_features.py
venv\Scripts\python.exe src\FeatureAndSpatial\zone_daily_features.py
venv\Scripts\python.exe src\FeatureAndSpatial\borough_hour_features.py
venv\Scripts\python.exe src\FeatureAndSpatial\top_routes.py
```

Purpose:

- Joins cleaned trips with taxi zone lookup data.
- Adds pickup/dropoff borough and zone names.
- Adds time features such as year, month, date, weekday, weekend flag, and hour.
- Builds zone-hour, zone-day, borough-hour, and route-level feature tables.

Main outputs:

```text
data/processed/trip_enriched/
data/processed/zone_hour_features/
data/processed/zone_daily_features/
data/processed/borough_hour_features/
data/processed/top_routes/
outputs/tables/*_csv/
```

Important tables:

| Table | Grain | Main use |
|---|---|---|
| `trip_enriched` | One row per taxi trip | Reusable enriched trip-level table |
| `zone_hour_features` | Pickup zone + date + hour | Core demand table for analytics and forecasting |
| `zone_daily_features` | Pickup zone + date | Daily zone trend analysis |
| `borough_hour_features` | Borough + hour | Borough-level demand pattern analysis |
| `top_routes` | Pickup zone + dropoff zone | Route ranking and OD flow analysis |

### 4. Analytics Tables

Temporal analytics:

```powershell
venv\Scripts\python.exe src\analytics\temporal_analysis.py --write-csv
```

Enriched temporal analytics:

```powershell
venv\Scripts\python.exe src\analytics\temporal_analysis_enriched.py --write-csv
```

Trip and route analytics:

```powershell
venv\Scripts\python.exe src\analytics\trip_route_analysis.py --write-csv --top-n 500
```

Map flow analytics:

```powershell
venv\Scripts\python.exe src\analytics\zone_centroids.py
venv\Scripts\python.exe src\analytics\map_flow_analysis.py --write-csv --flow-top-n 500
```

Zone and route profiles:

```powershell
venv\Scripts\python.exe src\analytics\profile_analysis.py --write-csv
```

Purpose:

- Converts processed feature tables into dashboard-ready analytics tables.
- Produces demand trends, heatmaps, route rankings, airport summaries, OD flows,
  zone profiles, route profiles, and map support tables.
- Writes parquet outputs and, with `--write-csv`, CSV outputs for MongoDB export.

Main output areas:

```text
outputs/tables/temporal/
outputs/tables/temporal_enriched/
outputs/tables/trip_route_analytics/
outputs/tables/map/
outputs/tables/profiles/
```

### 5. Forecasting

```powershell
venv\Scripts\python.exe src\forecasting\prepare_training_data.py
venv\Scripts\python.exe src\forecasting\train_model.py
venv\Scripts\python.exe src\forecasting\evaluate_model.py
venv\Scripts\python.exe src\forecasting\export_zone_hour_forecast.py
```

Purpose:

- Reads `data/processed/zone_hour_features/`.
- Builds lag and rolling demand features.
- Splits training and evaluation data by time.
- Trains Linear Regression, Random Forest, and Gradient Boosting models.
- Selects the best model by validation RMSE.
- Writes model metrics, prediction tables, plot data, figures, and a dashboard
  forecast table.

Main outputs:

```text
data/processed/forecasting/
outputs/models/
outputs/tables/model_comparison.csv
outputs/tables/model_evaluation_metrics.csv
outputs/predictions/
outputs/figures/
outputs/tables/forecast/csv/forecast_zone_hour.csv
```

### 6. Export Dashboard Tables To MongoDB

Preview the export first:

```powershell
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb --dry-run
```

Export the default dashboard tables:

```powershell
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb
```

Export default plus optional Atlas-safe tables:

```powershell
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb --include-optional
```

Purpose:

- Reads selected CSV outputs from `outputs/`.
- Converts CSV rows into MongoDB documents.
- Replaces target dashboard collections.
- Creates indexes used by API filters and route queries.

This script intentionally avoids raw trip records and very large local-only
tables. MongoDB should store curated dashboard aggregates, not the full taxi
trip dataset.

## Run The Application Locally

Start the FastAPI backend:

```powershell
venv\Scripts\python.exe -m uvicorn backend.app.main:app --reload --port 8001
```

Open:

```text
http://127.0.0.1:8001/docs
http://127.0.0.1:8001/health
```

Start the frontend:

```powershell
cd frontend
npm install
npm run dev
```

Open:

```text
http://127.0.0.1:5173
```

The frontend calls:

```text
http://127.0.0.1:8001/api
```

unless `VITE_API_BASE` is changed.

## Backend API Overview

The backend entry point is:

```text
backend/app/main.py
```

Routers:

| Router | Prefix | Purpose |
|---|---|---|
| `meta.py` | `/api/meta` | Filter values and collection metadata |
| `dashboard.py` | `/api/dashboard` | Overview and story endpoints |
| `temporal.py` | `/api/temporal` | Time-based demand endpoints |
| `spatial.py` | `/api/spatial` | Zone rankings and hotspot tables |
| `routes.py` | `/api/routes` | Route, airport, and borough movement tables |
| `map.py` | `/api/map` | OD flow data for map layers |
| `profiles.py` | `/api/profiles` | Zone and route profile panels |
| `business.py` | `/api/business` | Payment and trip behavior summaries |
| `forecast.py` | `/api/forecast` | Forecast metrics and error views |

Useful checks:

```text
GET /health
GET /api/meta/filters
GET /api/dashboard/overview
GET /api/temporal/year-month-demand
GET /api/spatial/top-zones
GET /api/routes/top
```

## Frontend Overview

The frontend is a Vue 3 app built with Vite and D3. Main files:

```text
frontend/src/App.vue
frontend/src/api/client.js
frontend/src/styles.css
frontend/src/components/
```

Dashboard sections:

| Section | Purpose |
|---|---|
| Command Center | KPI overview, insight banner, and map explorer |
| Demand Patterns | Temporal charts, heatmaps, and trend views |
| Zone Intelligence | Zone rankings and hotspot comparisons |
| Route Network | OD flows, route rankings, airport routes, borough movement |
| Forecast Lab | Model metrics, actual vs predicted charts, error analysis |
| Data Tables | Tabular drill-down into analytical outputs |

## Deployment

The recommended deployment split is:

```text
Frontend: Vercel
Backend: Render Web Service
Database: MongoDB Atlas
Data pipeline: local machine or external Spark cluster
```

Do not deploy generated data folders such as `data/processed/`, `outputs/`,
`venv/`, or `frontend/node_modules/`.

### Render Backend

`render.yaml` defines a Python web service:

```text
Build Command: pip install -r backend/requirements.txt
Start Command: python -m uvicorn backend.app.main:app --host 0.0.0.0 --port $PORT
Health Check Path: /health
```

Required environment variables:

```text
MONGODB_URI=<MongoDB Atlas connection string>
MONGODB_DB=nyc_taxi_analytics
CORS_ORIGINS=https://your-vercel-app.vercel.app,http://localhost:5173,http://127.0.0.1:5173
PYTHON_VERSION=3.11.9
```

Verify after deployment:

```text
https://your-render-service.onrender.com/health
https://your-render-service.onrender.com/docs
https://your-render-service.onrender.com/api/meta/filters
```

### Vercel Frontend

Deploy only the `frontend/` directory.

Suggested settings:

```text
Framework Preset: Vite
Root Directory: frontend
Install Command: npm install
Build Command: npm run build
Output Directory: dist
```

Required frontend environment variable:

```text
VITE_API_BASE=https://your-render-service.onrender.com/api
```

After Vercel is deployed, add the Vercel URL to Render's `CORS_ORIGINS` and
redeploy the backend.

## Scalable To Big Data

This project is designed so the same logical pipeline can run on a laptop for
development or on a Spark cluster for larger historical datasets. Scaling has
two dimensions:

- More storage and compute: move `DATA_ROOT` from local disk to HDFS or object
  storage, then run the PySpark jobs with `spark-submit`.
- More time coverage: add more monthly Yellow Taxi parquet files, then rerun the
  generated pipeline stages.

### Runtime And Storage Strategy

Local mode is the default:

```powershell
$env:SPARK_MASTER="local[*]"
$env:DATA_ROOT="$PWD/data"
venv\Scripts\python.exe src\cleaning\clean_trips.py
```

For cluster mode, use the same scripts with external paths and Spark settings:

```bash
export SPARK_MASTER=yarn
export DATA_ROOT=hdfs:///user/nyc_taxi/data
export SPARK_DRIVER_MEMORY=8g
export SPARK_EXECUTOR_MEMORY=8g
export SPARK_SQL_SHUFFLE_PARTITIONS=200
```

`DATA_ROOT` can point to local storage, HDFS, or object storage:

```text
<project-root>/data
hdfs:///user/nyc_taxi/data
s3a://nyc-taxi-data/data
```

The expected layout under any `DATA_ROOT` is:

```text
<DATA_ROOT>/
  raw/
    yellow_tripdata_YYYY-MM.parquet
  lookup/
    taxi_zone_lookup.csv
  processed/
```

The ingestion code lists parquet files through local filesystem APIs in laptop
mode and Hadoop FileSystem APIs in remote mode, so `hdfs://` and `s3a://` data
roots can be used without changing the pipeline commands.

`DATA_ROOT` controls only the project data tree: `raw/`, `lookup/`, and
`processed/`. The final dashboard artifacts under `outputs/` are currently
configured through `OUTPUTS_DIR` in `config/config.py`, which points to the
project-root `outputs/` folder. MongoDB export therefore expects the generated
CSV files to exist in the repo's local `outputs/` directory.

### Running On A Spark Cluster

Build the Python package zip from Windows PowerShell:

```powershell
.\scripts\build_spark_package.ps1
```

Example `spark-submit`:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 8 \
  --executor-cores 4 \
  --executor-memory 8g \
  --py-files dist/nyc_taxi_project.zip \
  src/cleaning/clean_trips.py
```

Run each Spark pipeline stage in the same order as the local workflow. On a
cluster, large processed parquet tables should stay under
`<DATA_ROOT>/processed/`. If CSV-producing analytics jobs are also run on the
cluster, copy the generated `outputs/` artifacts back to the repo before
MongoDB export, or run the CSV-producing analytics/export steps on the same
machine where the repo and `.env` file live.

### Adding More Years Of Taxi Data

Download additional official TLC Yellow Taxi parquet files from:

```text
https://www.nyc.gov/html/tlc/html/about/about_the_tlc_trip_data_page.shtml
```

Keep the original filename pattern:

```text
yellow_tripdata_YYYY-MM.parquet
```

For example:

```text
yellow_tripdata_2020-01.parquet
yellow_tripdata_2020-02.parquet
yellow_tripdata_2025-01.parquet
```

Place local files under:

```text
data/raw/
```

For cluster mode, upload them under:

```text
<DATA_ROOT>/raw/
```

Then validate the combined raw dataset before rebuilding:

```powershell
venv\Scripts\python.exe src\ingestion\load_raw_data.py
```

Check the summary:

```text
data/processed/ingestion/ingestion_summary.txt
```

The ingestion layer reads monthly parquet files one by one, casts important
columns to stable types, and unions schemas with missing columns allowed. This
handles common TLC schema differences, including extra columns in newer files,
as long as the core fields still exist:

```text
tpep_pickup_datetime
tpep_dropoff_datetime
PULocationID
DOLocationID
trip_distance
fare_amount
total_amount
passenger_count
payment_type
```

### Code Notes For Extending Years

Most analytics and API code is already year-dynamic. The temporal, route, map,
profile, MongoDB export, and backend filter endpoints group or filter by the
`year` values produced by the data, so they do not need code changes just
because a new year is present.

These places should be adjusted before expanding outside 2021-2024:

| Area | File | Current behavior | Suggested change |
|---|---|---|---|
| Cleaning date window | `src/cleaning/clean_trips.py` | `remove_out_of_target_date_range()` keeps pickups from `2021-01-01` to before `2025-01-01`, so 2020 or 2025+ data would be filtered out. | Update the start and end bounds to match the intended analysis period. If the project will keep expanding, move the bounds into constants or environment variables so the date window is easy to change. |
| Forecast split | `src/forecasting/prepare_training_data.py` | `FULL_*`, `TRAIN_*`, and `EVAL_*` dates are hardcoded for 2021-2024, with 2024 as evaluation. | Choose the new modeling design explicitly. For example, after adding 2025 data, train on 2021-2024 and evaluate on 2025, or use a wider training window and reserve only the newest period as the test set. Also update the printed split message. |
| Forecast metadata | `backend/app/routers/forecast.py` | The response metadata currently reports `evaluation_year=2024`. | Update this value when the evaluation period changes, or derive it from the exported forecast rows so API metadata stays consistent with model outputs. |

Recommended update flow:

- First update the cleaning date window, then rerun ingestion and cleaning to
  confirm the new years are present in `data/processed/cleaned_trips/`.
- Next rerun feature engineering and analytics; these stages should pick up the
  new `year` values automatically.
- If forecasting is included, update the train/evaluation dates before running
  `prepare_training_data.py`, then check `train_eval_split_info.csv`.
- Finally update backend forecast metadata to match the new evaluation period.

### Rebuilding After Scaling

After adding years in local mode, rerun generated stages in order:

```powershell
venv\Scripts\python.exe src\cleaning\clean_trips.py
venv\Scripts\python.exe src\FeatureAndSpatial\trip_enriched.py
venv\Scripts\python.exe src\FeatureAndSpatial\zone_hour_features.py
venv\Scripts\python.exe src\FeatureAndSpatial\zone_daily_features.py
venv\Scripts\python.exe src\FeatureAndSpatial\borough_hour_features.py
venv\Scripts\python.exe src\FeatureAndSpatial\top_routes.py
venv\Scripts\python.exe src\analytics\temporal_analysis.py --write-csv
venv\Scripts\python.exe src\analytics\temporal_analysis_enriched.py --write-csv
venv\Scripts\python.exe src\analytics\trip_route_analysis.py --write-csv --top-n 500
venv\Scripts\python.exe src\analytics\zone_centroids.py
venv\Scripts\python.exe src\analytics\map_flow_analysis.py --write-csv --flow-top-n 500
venv\Scripts\python.exe src\analytics\profile_analysis.py --write-csv
```

For cluster mode, submit the same Spark stages in the same order with
`spark-submit`, using the cluster environment variables shown above. The script
order stays the same; the execution command changes from local
`venv\Scripts\python.exe ...` to cluster `spark-submit ... <script.py>`.

If the dashboard should include updated forecast outputs, rerun forecasting too:

```powershell
venv\Scripts\python.exe src\forecasting\prepare_training_data.py
venv\Scripts\python.exe src\forecasting\train_model.py
venv\Scripts\python.exe src\forecasting\evaluate_model.py
venv\Scripts\python.exe src\forecasting\export_zone_hour_forecast.py
```

After dashboard CSV files are available in the local repo's `outputs/` folder,
preview the MongoDB export:

```powershell
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb --dry-run
```

Then export:

```powershell
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb --include-optional
```
