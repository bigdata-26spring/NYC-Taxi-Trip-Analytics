# NYC Taxi Analytics Backend

FastAPI backend for serving MongoDB Atlas analytics collections to the Vue + D3
frontend.

## Setup

From the project root:

```bash
venv\Scripts\pip.exe install -r backend\requirements.txt
```

The backend reads MongoDB settings from the project-root `.env` file or from
environment variables:

```text
MONGODB_URI=mongodb+srv://user_1:<password>@nyctaxitrip.fhkn7x7.mongodb.net/?appName=NYCTaxiTrip
MONGODB_DB=nyc_taxi_analytics
CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
```

## Run Locally

```bash
venv\Scripts\python.exe -m uvicorn backend.app.main:app --reload --port 8001
```

Open:

```text
http://127.0.0.1:8001/docs
```

## Core Endpoints

```text
GET /health
GET /api/meta/filters
GET /api/dashboard/overview
GET /api/temporal/daily-demand
GET /api/temporal/year-month-demand
GET /api/temporal/weekday-hour-heatmap
GET /api/spatial/top-zones
GET /api/routes/top
```
