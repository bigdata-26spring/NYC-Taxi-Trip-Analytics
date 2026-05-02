# Deployment: Vercel Frontend + Render Backend

This guide deploys the project without Oracle Cloud.

```text
Frontend: Vercel
Backend: Render Web Service
Database: MongoDB Atlas
Big data pipeline: local only
```

Do not deploy `data/`, `outputs/`, `venv/`, or `frontend/node_modules/`. The online app only needs curated MongoDB collections, backend code, and frontend source.

## 1. Prepare GitHub

Push the repository to GitHub first. Make sure the repo includes these files:

```text
backend/requirements.txt
render.yaml
frontend/package.json
frontend/.env.production.example
```

Do not commit secrets:

```text
.env
frontend/.env.production
```

## 2. MongoDB Atlas

Keep using the existing MongoDB Atlas database.

In Atlas, check:

```text
Database: nyc_taxi_analytics
Collections: dashboard/temporal/spatial/routes/map/profile/forecast collections
```

For deployment, Render must be able to connect to Atlas. The simplest course-demo setting is:

```text
Network Access: 0.0.0.0/0
```

For a more controlled setup, use Render outbound IPs if your Render plan exposes stable IP behavior.

## 3. Deploy Backend On Render

Use Render's Web Service because the backend is a FastAPI app.

### Option A: Use `render.yaml`

1. Open Render Dashboard.
2. Click `New`.
3. Choose `Blueprint`.
4. Connect the GitHub repository.
5. Render should detect `render.yaml`.
6. Create the service.

The backend build/start commands are already defined:

```text
Build Command:
pip install -r backend/requirements.txt

Start Command:
python -m uvicorn backend.app.main:app --host 0.0.0.0 --port $PORT
```

### Option B: Manual Web Service

If you do not use Blueprint:

```text
Service type: Web Service
Runtime: Python
Build command: pip install -r backend/requirements.txt
Start command: python -m uvicorn backend.app.main:app --host 0.0.0.0 --port $PORT
```

### Render Environment Variables

Set these in Render:

```text
MONGODB_URI=<your MongoDB Atlas connection string>
MONGODB_DB=nyc_taxi_analytics
CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
```

After the Vercel frontend is deployed, update `CORS_ORIGINS` to include the Vercel URL:

```text
CORS_ORIGINS=https://your-vercel-app.vercel.app,http://localhost:5173,http://127.0.0.1:5173
```

### Backend Verification

After Render deploys, open:

```text
https://your-render-service.onrender.com/health
https://your-render-service.onrender.com/docs
```

Expected health response:

```json
{
  "status": "ok",
  "database": "nyc_taxi_analytics"
}
```

If `/health` fails, check:

- `MONGODB_URI`
- Atlas Network Access
- Atlas database user password
- Render logs

## 4. Deploy Frontend On Vercel

Use Vercel for the Vue frontend.

1. Open Vercel Dashboard.
2. Click `Add New Project`.
3. Import the same GitHub repository.
4. Set the project root directory to:

```text
frontend
```

5. Vercel should detect Vite. If needed, set:

```text
Framework Preset: Vite
Build Command: npm run build
Output Directory: dist
Install Command: npm install
```

6. Add this Vercel environment variable:

```text
VITE_API_BASE=https://your-render-service.onrender.com/api
```

7. Deploy.

## 5. Final CORS Update

After Vercel gives you a URL, for example:

```text
https://nyc-taxi-dashboard.vercel.app
```

Go back to Render and update:

```text
CORS_ORIGINS=https://nyc-taxi-dashboard.vercel.app,http://localhost:5173,http://127.0.0.1:5173
```

Then redeploy the Render backend.

## 6. Final Test

Open the Vercel URL and test:

- Dashboard loads.
- Filters load.
- KPI cards load.
- Map panels load.
- Forecast panels load.
- Browser console has no CORS errors.

Useful API checks:

```text
https://your-render-service.onrender.com/api/meta/filters
https://your-render-service.onrender.com/api/dashboard/overview
https://your-render-service.onrender.com/api/map/centroids
```

## 7. Important Limits

Render free web services can be slow on first request after inactivity. For a course demo, open the backend `/health` URL once before presenting.

Do not run these on Render or Vercel:

```text
Spark pipeline
model training
MongoDB export script
large CSV generation
```

Run those locally, upload curated outputs to MongoDB Atlas, then let the deployed app read from Atlas.
