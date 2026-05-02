# Stage 6 - Visualization

This stage builds a D3 dashboard from the existing Stage 3 and Stage 4 output
tables. It does not rerun Spark; it only prepares browser-friendly CSV assets.

## Data Inputs

- `outputs/tables/temporal/csv/*/part-*.csv`
- `outputs/tables/top_routes_sample.csv`
- Optional Stage 5 forecast file:
  - `outputs/predictions/forecast_demand.csv`
  - or `outputs/predictions/demand_forecast.csv`
  - or `outputs/predictions/predictions.csv`

Expected forecast columns:

```text
pickup_date,hour,pickup_zone,actual_trip_count,predicted_trip_count
```

## Run

From the project root:

```bash
python src/visualization/prepare_dashboard_data.py
python -m http.server 8000 --directory src/visualization/dashboard
```

Open:

```text
http://localhost:8000/
```

The dashboard uses D3 from a CDN. For an offline presentation, download
`d3.v7.min.js` into the dashboard folder and update `index.html` to load that
local file.
