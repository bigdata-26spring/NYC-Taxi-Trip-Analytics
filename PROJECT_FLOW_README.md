# NYC Taxi Trip Analytics - Project Flow / 项目流程说明

This document explains the current end-to-end project workflow. It does not replace the original `README.md`; it is a more detailed handoff document for data processing, analytics, forecasting, backend serving, frontend visualization, and deployment.

本文档用于说明当前项目的完整流程，不替代原来的 `README.md`。它更像一份项目交接文档，重点解释数据处理、分析建表、预测、后端服务、前端可视化和部署之间的关系。

## 1. Project Goal / 项目目标

**English**

This project builds an end-to-end analytics system for NYC Yellow Taxi trip data. The final product is a production-style dashboard that helps users understand taxi demand, spatial hotspots, route flows, zone profiles, business patterns, and forecasting performance.

**中文**

本项目基于 NYC Yellow Taxi 官方行程数据，构建一个端到端的数据分析系统。最终产品是一个接近生产级别的可视化 dashboard，用来展示出租车需求变化、空间热点、路线流向、区域画像、业务指标和预测模型效果。

The high-level workflow is:

整体流程如下：

```text
Raw TLC trip data
  -> Ingestion
  -> Cleaning
  -> Feature engineering and spatial enrichment
  -> Analytics, map tables, and profiles
  -> Forecasting
  -> MongoDB serving tables
  -> FastAPI backend
  -> Vue + D3 frontend dashboard
```

The online deployment does not run Spark or model training. It only serves already-computed dashboard tables from MongoDB Atlas.

线上部署环境不运行 Spark，也不训练模型。线上只负责从 MongoDB Atlas 读取已经计算好的 dashboard 表并展示。

## 2. Architecture / 系统架构

**English**

The project is split into three layers:

1. **Data pipeline layer**: Spark jobs and Python scripts that generate cleaned data, feature tables, analytics outputs, and forecast outputs.
2. **Serving layer**: MongoDB Atlas stores curated dashboard tables. FastAPI exposes API endpoints.
3. **Presentation layer**: Vue + Vite + D3 frontend renders the dashboard.

**中文**

项目分成三层：

1. **数据 pipeline 层**：用 Spark 和 Python 脚本生成清洗数据、特征表、分析表和预测结果。
2. **服务层**：MongoDB Atlas 存储经过筛选的 dashboard 表，FastAPI 提供 API。
3. **展示层**：Vue + Vite + D3 前端负责可视化页面。

```text
Local machine / 本地:
  Spark pipeline
  model training
  CSV generation
  MongoDB export script

MongoDB Atlas:
  curated dashboard collections

Render:
  FastAPI backend

Vercel:
  Vue production frontend
```

## 3. Repository Structure / 项目结构

```text
NYC-Taxi-Trip-Analytics/
  config/
    config.py

  data/
    raw/
    lookup/
    processed/
      cleaned_trips/
      trip_enriched/
      zone_hour_features/
      zone_daily_features/
      borough_hour_features/
      top_routes/

  outputs/
    tables/
    predictions/
    figures/

  src/
    ingestion/
    cleaning/
    FeatureAndSpatial/
    analytics/
    forecasting/
    serving/
    visualization/

  backend/
    app/
    requirements.txt

  frontend/
    src/
    package.json

  render.yaml
  requirements.txt
  PROJECT_FLOW_README.md
```

**Directory roles / 目录作用**

`config/`

- EN: Stores shared path definitions and Spark application names.
- 中文：统一管理路径和 Spark 应用名，避免不同脚本里重复写硬编码路径。

`data/raw/`

- EN: Stores original TLC parquet files.
- 中文：存放官方原始 Yellow Taxi parquet 文件。

`data/lookup/`

- EN: Stores reference data such as taxi zone lookup tables.
- 中文：存放辅助映射表，例如 taxi zone lookup，用于把 LocationID 映射为 borough 和 zone。

`data/processed/`

- EN: Stores intermediate parquet outputs from cleaning and feature engineering.
- 中文：存放 pipeline 中间结果，例如清洗后的 trips、zone-hour 特征表、borough-hour 特征表。

`outputs/`

- EN: Stores final analytics CSVs, forecast outputs, and optional figures.
- 中文：存放最终分析结果、预测结果和图表输出。

`src/`

- EN: Contains all offline data pipeline scripts.
- 中文：存放离线数据处理、分析、预测和导出脚本。

`backend/`

- EN: FastAPI backend that queries MongoDB Atlas and returns JSON.
- 中文：FastAPI 后端，从 MongoDB Atlas 查询数据并返回 JSON。

`frontend/`

- EN: Vue + Vite + D3 production dashboard.
- 中文：Vue + Vite + D3 前端 dashboard。

## 4. Stage 1 - Ingestion / 数据读取阶段

**Purpose / 目的**

EN: Load official TLC trip files into Spark and normalize schema differences across files.

中文：读取官方 TLC 行程数据，并处理不同月份/年份 parquet 文件之间可能存在的 schema 差异。

**Main script / 主脚本**

```text
src/ingestion/load_raw_data.py
```

**Input / 输入**

```text
data/raw/*.parquet
data/lookup/taxi_zone_lookup.csv
```

**Output / 输出**

```text
data/processed/ingestion/ingestion_summary.txt
```

**What it does / 具体做了什么**

- EN: Creates a Spark session for the project.
- 中文：创建项目统一使用的 Spark session。

- EN: Reads all raw Yellow Taxi parquet files from `data/raw/`.
- 中文：读取 `data/raw/` 下所有 Yellow Taxi 原始 parquet 文件。

- EN: Normalizes schema differences so downstream stages can use a consistent DataFrame.
- 中文：统一 schema，保证后续清洗和特征工程可以使用一致的数据结构。

- EN: Loads taxi zone lookup data.
- 中文：读取 taxi zone lookup，用于后续空间字段映射。

- EN: Writes an ingestion summary for validation.
- 中文：输出 ingestion summary，用来检查 schema、字段和数据量。

**Role in the system / 在系统中的作用**

EN: This stage is the entry point of the data pipeline. It does not produce dashboard tables directly; it guarantees that all later stages read raw data in a consistent way.

中文：这是整个数据 pipeline 的入口。它不直接生成 dashboard 表，而是保证后面的 cleaning、feature engineering、analytics 都能稳定读取原始数据。

## 5. Stage 2 - Cleaning / 数据清洗阶段

**Purpose / 目的**

EN: Remove invalid or unrealistic trip records and produce a clean trip-level dataset.

中文：过滤无效或明显异常的行程记录，生成标准化的 trip-level 清洗数据。

**Main script / 主脚本**

```text
src/cleaning/clean_trips.py
```

**Input / 输入**

```text
data/raw/*.parquet
```

**Output / 输出**

```text
data/processed/cleaned_trips/
data/processed/cleaning/cleaning_report.txt
```

**What it does / 具体做了什么**

- EN: Reuses the ingestion logic to load raw trips.
- 中文：复用 ingestion 阶段的读取逻辑，而不是每个脚本重新写一套 raw data loader。

- EN: Removes records with invalid pickup/dropoff timestamps.
- 中文：过滤上车/下车时间无效的记录。

- EN: Removes impossible or suspicious trip distances, fares, passenger counts, and durations.
- 中文：过滤距离、金额、乘客数、行程时长等明显异常的数据。

- EN: Keeps a standardized set of columns for downstream processing.
- 中文：保留后续分析需要的标准字段。

- EN: Writes a cleaning report.
- 中文：输出清洗报告，方便说明过滤规则和结果。

**Role in the system / 在系统中的作用**

EN: Cleaning protects all downstream analysis from raw-data noise. Without this stage, route rankings, demand aggregations, and model training could be distorted by invalid trips.

中文：清洗阶段保证后续分析不会被原始数据里的脏数据污染。如果没有这一步，路线排行、需求聚合和预测模型都会受到异常 trip 的影响。

**Storage note / 存储说明**

EN: `cleaned_trips` is still a large trip-level dataset. It should stay local or in a big-data storage layer, not MongoDB Atlas.

中文：`cleaned_trips` 仍然是大规模明细数据，应留在本地或大数据存储层，不应该导入 MongoDB Atlas。

## 6. Stage 3 - Feature Engineering And Spatial Enrichment / 特征工程与空间增强

**Purpose / 目的**

EN: Convert cleaned trips into reusable analytical feature tables.

中文：把清洗后的 trip 明细转换成后续分析、地图和预测都能复用的特征表。

**Main scripts / 主脚本**

```text
src/FeatureAndSpatial/trip_enriched.py
src/FeatureAndSpatial/zone_hour_features.py
src/FeatureAndSpatial/zone_daily_features.py
src/FeatureAndSpatial/borough_hour_features.py
src/FeatureAndSpatial/top_routes.py
```

**Core outputs / 核心输出**

```text
data/processed/trip_enriched/
data/processed/zone_hour_features/
data/processed/zone_daily_features/
data/processed/borough_hour_features/
data/processed/top_routes/
```

### 6.1 `trip_enriched`

**EN**

This table joins cleaned trip records with taxi zone metadata. It adds pickup/dropoff borough names, zone names, and time-derived fields. It is the most important reusable trip-level dataset.

**中文**

这张表把清洗后的 trip 明细和 taxi zone lookup 进行 join，补充 pickup/dropoff borough、zone name，以及 year、month、hour、weekday 等时间字段。它是最重要的可复用 trip-level 数据集。

**Role / 作用**

- EN: Base table for route analysis, map flow analysis, business analysis, and profile summaries.
- 中文：路线分析、地图流向分析、业务分析、区域画像都可以基于它继续聚合。

### 6.2 `zone_hour_features`

**EN**

This table aggregates taxi activity by pickup zone and hour. It is the core table for temporal-spatial demand modeling.

**中文**

这张表按 pickup zone 和 hour 聚合出租车需求，是时空需求分析和预测建模的核心表。

**Role / 作用**

- EN: Base table for forecasting, hotspot ranking, demand maps, and hourly zone comparisons.
- 中文：用于预测模型、热点排行、需求地图、区域小时级对比。

### 6.3 `zone_daily_features`

**EN**

This table aggregates demand at the zone-day level.

**中文**

这张表按 zone-day 聚合需求，用于观察每日层面的空间变化。

### 6.4 `borough_hour_features`

**EN**

This table aggregates demand by borough and hour.

**中文**

这张表按 borough-hour 聚合需求，用于比较 Manhattan、Queens、Brooklyn 等区域在一天内的需求节奏。

### 6.5 `top_routes`

**EN**

This table summarizes frequent pickup-dropoff zone pairs.

**中文**

这张表统计高频 pickup-dropoff 区域对，用于路线排行和 OD flow 可视化。

## 7. Stage 4 - Analytics, Map Tables, And Profiles / 分析表、地图表与画像表

**Purpose / 目的**

EN: Convert processed feature tables into dashboard-ready CSV outputs.

中文：把 processed 特征表进一步聚合成 dashboard 可以直接读取或导入 MongoDB 的结果表。

**Main scripts / 主脚本**

```text
src/analytics/temporal_analysis.py
src/analytics/temporal_analysis_enriched.py
src/analytics/trip_route_analysis.py
src/analytics/zone_centroids.py
src/analytics/map_flow_analysis.py
src/analytics/profile_analysis.py
```

**Output areas / 输出目录**

```text
outputs/tables/temporal/csv/
outputs/tables/temporal_enriched/csv/
outputs/tables/trip_route_analytics/csv/
outputs/tables/map/csv/
outputs/tables/profiles/csv/
```

### 7.1 Temporal analytics / 时间维度分析

**Scripts / 脚本**

```text
src/analytics/temporal_analysis.py
src/analytics/temporal_analysis_enriched.py
```

**What it generates / 生成内容**

- EN: KPI summary, hourly demand, daily demand, monthly demand.
- 中文：核心 KPI、小时需求、每日需求、月度需求。

- EN: Weekday-hour heatmaps and borough-hour patterns.
- 中文：weekday-hour 热力图和 borough-hour 需求模式。

- EN: Year/month trend tables and rank-change tables.
- 中文：年份/月度趋势表和区域排名变化表。

**Dashboard role / 前端作用**

- EN: Feeds the Demand Patterns section.
- 中文：支撑前端 Demand Patterns 页面。

### 7.2 Route and business analytics / 路线与业务分析

**Script / 脚本**

```text
src/analytics/trip_route_analysis.py
```

**What it generates / 生成内容**

- EN: Top routes overall.
- 中文：总体热门路线。

- EN: Airport trip summaries.
- 中文：机场路线统计。

- EN: Inter-borough route summaries.
- 中文：跨 borough 路线统计。

- EN: Pickup-dropoff borough matrices.
- 中文：pickup-dropoff borough OD 矩阵。

- EN: Payment type and trip behavior summaries.
- 中文：支付方式和行程行为统计。

**Dashboard role / 前端作用**

- EN: Feeds Route Network and Data Tables.
- 中文：支撑 Route Network 和 Data Tables 页面。

### 7.3 Map support tables / 地图支持表

**Scripts / 脚本**

```text
src/analytics/zone_centroids.py
src/analytics/map_flow_analysis.py
```

**Tables / 表**

`zone_centroids`

- EN: Provides latitude/longitude centroids for taxi zones.
- 中文：提供 taxi zone 的中心点坐标，用于地图上的点、线和流向绘制。

`od_flow_hour`

- EN: Top OD flows by hour.
- 中文：按小时统计的热门起终点流向。

`od_flow_year_month`

- EN: Top OD flows by year-month.
- 中文：按年月统计的热门起终点流向。

`map_replay_sample`

- EN: Sampled trip records for map replay or animated trip display.
- 中文：用于地图回放或动画效果的抽样 trip 数据。

`dropoff_zone_hour_features`

- EN: Dropoff-side zone-hour demand features.
- 中文：按下车区域和小时聚合的需求特征。

`zone_balance_hour`

- EN: Compares pickup and dropoff activity by zone-hour.
- 中文：比较每个区域在每个小时的上车/下车活动平衡。

**MongoDB storage decision / MongoDB 存储取舍**

```text
Keep local only / 只保留本地:
  dropoff_zone_hour_features
  zone_balance_hour
  forecast_zone_hour

Export to MongoDB / 导入 MongoDB:
  zone_centroids
  od_flow_hour
  od_flow_year_month
  map_replay_sample
  zone_profile_summary
  route_profile_summary
```

**Reason / 原因**

EN: Some zone-hour tables are too large for the current MongoDB Atlas 512MB tier. They are useful for offline analysis but not suitable for online serving.

中文：部分 zone-hour 大表体积过大，不适合放进当前 512MB 的 MongoDB Atlas。它们适合本地分析，不适合线上 dashboard 直接服务。

### 7.4 Profile tables / 画像表

**Script / 脚本**

```text
src/analytics/profile_analysis.py
```

**Tables / 表**

`zone_profile_summary`

- EN: Summarizes each pickup zone: total trips, revenue, trip distance, activity patterns, and ranking.
- 中文：总结每个上车区域的总行程数、收入、距离、活跃模式和排名。

`route_profile_summary`

- EN: Summarizes each pickup-dropoff route pair.
- 中文：总结每条 pickup-dropoff 路线组合的整体表现。

**Dashboard role / 前端作用**

- EN: Supports click-to-inspect behavior in map and route panels.
- 中文：支持前端地图和路线面板里的点击查看详情功能。

## 8. Stage 5 - Forecasting / 预测建模阶段

**Purpose / 目的**

EN: Train demand forecasting models and export prediction/evaluation tables for the dashboard.

中文：训练需求预测模型，并导出预测结果和模型评估表供 dashboard 展示。

**Main scripts / 主脚本**

```text
src/forecasting/prepare_training_data.py
src/forecasting/train_model.py
src/forecasting/evaluate_model.py
src/forecasting/export_zone_hour_forecast.py
```

**Input / 输入**

```text
data/processed/zone_hour_features/
```

**Output examples / 输出示例**

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

**What it does / 具体做了什么**

- EN: Converts zone-hour features into model-ready training data.
- 中文：把 zone-hour 特征表转换为模型训练数据。

- EN: Trains multiple regression models.
- 中文：训练多个回归预测模型。

- EN: Compares models using evaluation metrics such as MAE, RMSE, and R2.
- 中文：用 MAE、RMSE、R2 等指标比较模型表现。

- EN: Exports actual-vs-predicted and error breakdown tables.
- 中文：导出真实值 vs 预测值，以及按小时、borough、zone 拆分的误差表。

**Dashboard role / 前端作用**

- EN: Feeds Forecast Lab, including model cards, actual/predicted charts, forecast error maps, and error breakdowns.
- 中文：支撑 Forecast Lab 页面，包括模型指标卡、真实/预测趋势图、预测误差地图和误差拆解图。

**Storage note / 存储说明**

EN: `forecast_zone_hour.csv` can be large. It should stay local unless it is sampled, aggregated, or the database tier is upgraded.

中文：`forecast_zone_hour.csv` 可能很大，除非进行抽样/聚合或升级数据库容量，否则应保留在本地。

## 9. Stage 6 - Serving Layer And Frontend Dashboard / 服务层与前端展示

**Purpose / 目的**

EN: Serve curated analytics tables through FastAPI and visualize them in the Vue dashboard.

中文：通过 FastAPI 提供分析结果 API，并在 Vue 前端中进行可视化展示。

### 9.1 MongoDB export / MongoDB 导入

**Main script / 主脚本**

```text
src/serving/export_analytics_to_mongodb.py
```

**What it does / 具体做了什么**

- EN: Reads dashboard-ready CSV outputs from `outputs/`.
- 中文：读取 `outputs/` 中已经生成好的 dashboard CSV 表。

- EN: Converts CSV rows into MongoDB documents.
- 中文：把 CSV 行转换为 MongoDB documents。

- EN: Replaces target MongoDB collections.
- 中文：替换目标 MongoDB collection，保证线上读到的是最新聚合结果。

- EN: Creates useful indexes for API query performance.
- 中文：为常用查询字段创建索引，提高 API 查询速度。

**Commands / 命令**

```powershell
# Preview planned exports without connecting to MongoDB
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb --dry-run

# Export core tables
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb

# Export core + Atlas-safe optional tables
venv\Scripts\python.exe -m src.serving.export_analytics_to_mongodb --include-optional
```

**Important / 重要说明**

EN: The export script intentionally avoids raw trips and very large local-only tables.

中文：导入脚本刻意避开原始 trip 明细和超大的本地备用表。

### 9.2 Backend / 后端

**Framework / 框架**

```text
FastAPI
```

**Main app / 主入口**

```text
backend/app/main.py
```

**Backend role / 后端作用**

- EN: Reads MongoDB settings from environment variables.
- 中文：从环境变量读取 MongoDB 配置。

- EN: Connects to MongoDB Atlas.
- 中文：连接 MongoDB Atlas。

- EN: Provides REST API endpoints for dashboard sections.
- 中文：为前端各个 dashboard 页面提供 REST API。

- EN: Handles query filters such as year, month, hour, borough, zone, and route limits.
- 中文：处理 year、month、hour、borough、zone、limit 等查询参数。

**Routers / 路由模块**

```text
backend/app/routers/meta.py        # filter metadata / 筛选器元数据
backend/app/routers/dashboard.py   # dashboard story endpoints / 页面综合接口
backend/app/routers/temporal.py    # temporal demand / 时间需求
backend/app/routers/spatial.py     # zone rankings / 空间区域排行
backend/app/routers/routes.py      # route analytics / 路线分析
backend/app/routers/map.py         # map layers / 地图图层数据
backend/app/routers/profiles.py    # zone and route profiles / 区域和路线画像
backend/app/routers/business.py    # business summaries / 业务统计
backend/app/routers/forecast.py    # forecast outputs / 预测结果
```

**Local start command / 本地启动命令**

```powershell
venv\Scripts\python.exe -m uvicorn backend.app.main:app --reload --port 8001
```

**Local docs / 本地接口文档**

```text
http://127.0.0.1:8001/docs
```

### 9.3 Frontend / 前端

**Framework / 框架**

```text
Vue 3 + Vite + D3
```

**Main files / 主要文件**

```text
frontend/src/App.vue
frontend/src/api/client.js
frontend/src/styles.css
frontend/src/components/
```

**Frontend role / 前端作用**

- EN: Provides the production-style dashboard interface.
- 中文：提供接近生产级别的 dashboard 交互界面。

- EN: Calls FastAPI endpoints through `VITE_API_BASE`.
- 中文：通过 `VITE_API_BASE` 调用 FastAPI 接口。

- EN: Renders charts, maps, tables, filters, and profile panels.
- 中文：渲染图表、地图、表格、筛选器和详情面板。

**Current dashboard sections / 当前页面模块**

`Command Center`

- EN: Main overview page with KPI strip, insight banner, and map explorer.
- 中文：总览页面，展示 KPI、核心洞察和地图探索器。

`Demand Patterns`

- EN: Time-based demand charts, heatmaps, and trend analysis.
- 中文：展示时间维度需求变化、热力图和趋势分析。

`Zone Intelligence`

- EN: Zone rankings, hotspot maps, and zone-level comparisons.
- 中文：展示区域排行、热点地图和区域对比。

`Route Network`

- EN: OD flow maps, top routes, airport routes, and inter-borough movement.
- 中文：展示 OD 流向、热门路线、机场路线和跨 borough 出行。

`Forecast Lab`

- EN: Forecast accuracy, model comparison, and prediction error views.
- 中文：展示预测准确性、模型对比和误差分析。

`Data Tables`

- EN: Tabular drill-down for analytical outputs.
- 中文：以表格形式查看详细分析结果。

**Local start command / 本地启动命令**

```powershell
cd frontend
npm run dev
```

**Local frontend URL / 本地前端地址**

```text
http://127.0.0.1:5173
```

## 10. Requirements Strategy / 依赖文件策略

The project has two Python requirements files with different purposes.

项目里有两个 Python 依赖文件，它们用途不同。

### Root `requirements.txt` / 根目录依赖

**Purpose / 用途**

EN: Full local data pipeline environment.

中文：本地完整数据 pipeline 环境。

It includes:

包括：

```text
pyspark
pandas
numpy
pyarrow
scikit-learn
joblib
matplotlib
tqdm
fastapi
uvicorn
pymongo
```

Use it when running local Spark jobs, forecasting scripts, or full offline analysis.

当你要在本地运行 Spark、预测模型或完整离线分析时使用它。

### `backend/requirements.txt` / 后端轻量依赖

**Purpose / 用途**

EN: Lightweight backend deployment environment for Render.

中文：Render 后端部署使用的轻量依赖。

It includes only:

只包含：

```text
fastapi
uvicorn[standard]
pymongo[srv]
```

Render should use:

Render 应该使用：

```bash
pip install -r backend/requirements.txt
```

Render should not use the root `requirements.txt`, because installing Spark and ML packages is unnecessary and heavy for the backend service.

Render 不应该使用根目录 `requirements.txt`，因为后端服务不需要安装 Spark 和机器学习依赖。

## 11. End-To-End Pipeline Order / 端到端流程顺序

When rebuilding everything from scratch, the conceptual order is:

如果从零重新生成所有数据，概念顺序是：

```text
Stage 1  ingestion
Stage 2  cleaning
Stage 3  feature engineering and spatial enrichment
Stage 4  analytics, map tables, profiles
Stage 5  forecasting
Stage 6  MongoDB export, backend API, frontend dashboard
```

Recommended execution principle:

推荐执行原则：

- EN: Generate heavy data locally.
- 中文：大数据处理放本地跑。

- EN: Export only curated, dashboard-ready tables to MongoDB.
- 中文：只把 dashboard 真正需要的小型聚合表导入 MongoDB。

- EN: Serve MongoDB data through FastAPI.
- 中文：通过 FastAPI 服务 MongoDB 数据。

- EN: Deploy only backend and frontend online.
- 中文：线上只部署后端和前端。

## 12. Deployment - Render Backend And Vercel Frontend / 部署流程

The current deployment plan avoids Oracle Cloud and uses managed web hosting.

当前部署方案不使用 Oracle Cloud，而是使用托管平台：

```text
Frontend / 前端:
  Vercel

Backend / 后端:
  Render Web Service

Database / 数据库:
  MongoDB Atlas curated collections

Big data pipeline / 大数据处理:
  local only
```

### 12.1 What not to deploy / 不要部署的内容

Do not deploy these directories or generated artifacts:

不要部署这些目录或生成文件：

```text
data/
outputs/
venv/
frontend/node_modules/
frontend/dist/
```

**Reason / 原因**

EN: These files are large, generated, local-only, or rebuilt automatically by deployment platforms.

中文：这些文件要么很大，要么是本地生成物，要么部署平台会自动重新生成，不适合提交或部署。

### 12.2 Backend on Render / Render 后端部署

Render runs only the FastAPI backend.

Render 只运行 FastAPI 后端。

**Render settings / Render 配置**

```text
Service type: Web Service
Runtime: Python
Build Command: pip install -r backend/requirements.txt
Start Command: python -m uvicorn backend.app.main:app --host 0.0.0.0 --port $PORT
Health Check Path: /health
```

**Why this start command is used / 为什么这么启动**

```bash
python -m uvicorn backend.app.main:app --host 0.0.0.0 --port $PORT
```

- EN: `backend.app.main:app` points to the FastAPI app in `backend/app/main.py`.
- 中文：`backend.app.main:app` 指向 `backend/app/main.py` 里的 FastAPI app。

- EN: `--host 0.0.0.0` allows Render to route traffic into the service.
- 中文：`--host 0.0.0.0` 允许 Render 把外部流量转进服务。

- EN: `--port $PORT` uses the dynamic port assigned by Render.
- 中文：`--port $PORT` 使用 Render 自动分配的端口，不能固定写成本地的 8001。

**Render environment variables / Render 环境变量**

```text
MONGODB_URI=<MongoDB Atlas connection string>
MONGODB_DB=nyc_taxi_analytics
CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
PYTHON_VERSION=3.11.9
```

After Vercel deployment, update:

Vercel 部署完成后，更新：

```text
CORS_ORIGINS=https://your-vercel-app.vercel.app,http://localhost:5173,http://127.0.0.1:5173
```

Then redeploy Render.

然后重新部署 Render。

**Backend verification / 后端验证**

```text
https://your-render-service.onrender.com/health
https://your-render-service.onrender.com/docs
https://your-render-service.onrender.com/api/meta/filters
```

Expected `/health` response:

`/health` 预期返回：

```json
{
  "status": "ok",
  "database": "nyc_taxi_analytics"
}
```

### 12.3 MongoDB Atlas Network Access / Atlas 网络访问

Render must be allowed to connect to MongoDB Atlas.

Render 必须被允许访问 MongoDB Atlas。

Course-demo simple option:

课程 demo 的简单方案：

```text
Atlas -> Network Access -> Add IP Address -> 0.0.0.0/0
```

More restrictive option:

更严格的方案：

```text
Render service -> Connect -> Outbound
Copy outbound IP ranges
Atlas -> Network Access -> Add those IP ranges
```

If Atlas only allows your personal laptop IP, Render will fail to connect even though your local backend works.

如果 Atlas 只允许你自己电脑的 IP，那么本地可以连，但 Render 后端会连接失败。

### 12.4 Frontend on Vercel / Vercel 前端部署

Vercel should deploy only the `frontend/` directory. Do not deploy the backend on Vercel.

Vercel 只负责部署 `frontend/` 目录，不负责部署后端。

**Vercel settings / Vercel 配置**

```text
Framework Preset: Vite
Root Directory: frontend
Install Command: npm install
Build Command: npm run build
Output Directory: dist
```

**Vercel environment variable / Vercel 环境变量**

```text
VITE_API_BASE=https://your-render-service.onrender.com/api
```

Set it for:

设置到：

```text
Production and Preview
```

**Why this variable is needed / 为什么需要这个变量**

EN: The frontend is static after build, so it needs to know the public backend API URL at build time.

中文：前端 build 后是静态文件，所以需要在 build 时知道后端 API 的公网地址。

### 12.5 Public access / 公网访问

The user-facing URL is the Vercel URL:

最终给别人访问的是 Vercel 前端地址：

```text
https://your-vercel-app.vercel.app
```

The Render URL is mainly for backend health checks and API debugging:

Render 地址主要用于后端健康检查和 API 调试：

```text
https://your-render-service.onrender.com/health
https://your-render-service.onrender.com/docs
```

### 12.6 Final deployment test / 最终部署检查

Test in this order:

按这个顺序检查：

1. Open Render health:

```text
https://your-render-service.onrender.com/health
```

2. Open one API endpoint:

```text
https://your-render-service.onrender.com/api/meta/filters
```

3. Open Vercel frontend:

```text
https://your-vercel-app.vercel.app
```

4. Check browser console.

检查浏览器 console。

If the console shows CORS errors:

如果 console 出现 CORS 错误：

```text
1. Add the Vercel URL to Render CORS_ORIGINS
2. Redeploy Render
3. Refresh the Vercel page
```

### 12.7 Free-tier notes / 免费层注意事项

EN: Render free web services can spin down after inactivity. Before a live demo, open `/health` once to wake the backend.

中文：Render 免费服务在一段时间无访问后可能休眠。演示前先打开一次 `/health`，让后端唤醒。

EN: Vercel serves the frontend as static files, so it is lightweight.

中文：Vercel 只服务前端静态文件，所以资源占用很低。

EN: MongoDB Atlas stores persistent dashboard data. Render should not be used as persistent file storage.

中文：MongoDB Atlas 存储持久化 dashboard 数据。Render 不应该被当作长期文件存储使用。
