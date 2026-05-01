from pathlib import Path

# ============================================================
# Project Root
# ============================================================
# Path(__file__).resolve() 表示当前 config.py 的绝对路径
# parents[1] 表示向上退一级到项目根目录 project/
# 不依赖当前终端在哪个目录运行，所有其他路径都可以基于 PROJECT_ROOT 统一构造
# 后面如果从不同脚本入口运行，也更稳定
PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ============================================================
# Data Directories
# ============================================================
# data/ 是整个项目的数据根目录
# 按项目文档，下面分成：
# - raw/       原始输入数据（官方下载的 parquet/csv）
# - lookup/    辅助映射表，例如 taxi zone lookup
# - processed/ 各层处理后的中间结果
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
LOOKUP_DIR = DATA_DIR / "lookup"
PROCESSED_DIR = DATA_DIR / "processed"


# ============================================================
# Output Directories
# ============================================================
# outputs/ 用来存最终分析、预测、可视化的结果文件
# - figures/      图表
# - tables/       汇总表
# - predictions/  预测结果
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
FIGURES_DIR = OUTPUTS_DIR / "figures"
TABLES_DIR = OUTPUTS_DIR / "tables"
PREDICTIONS_DIR = OUTPUTS_DIR / "predictions"


# ============================================================
# Raw Input Paths
# ============================================================
# RAW_DATA_DIR 表示原始 Yellow Taxi parquet 文件所在目录
# ingestion 脚本里会用 Python 自己去列出所有 parquet 文件，再逐个读取并统一 schema
RAW_DATA_DIR = RAW_DIR

# Taxi Zone Lookup Table 的固定路径
LOOKUP_PATH = str(LOOKUP_DIR / "taxi_zone_lookup.csv")


# ============================================================
# Processed Output Paths
# ============================================================
# 下面这些路径用于存储 pipeline 各层的中间结果
#
# 设计原则：
# 每一层的输出写到 processed/，下一层直接从 processed/ 读取，而不是反复从 raw/ 开始
# 避免重复清洗原始大数据，组内成员按层协作，让整个项目符合真正的数据工程 pipeline

# 清洗后的标准化 trip 数据
CLEANED_TRIPS_PATH = str(PROCESSED_DIR / "cleaned_trips")
CLEANING_REPORT_PATH = str(PROCESSED_DIR / "cleaning" / "cleaning_report.txt")
# join 了 zone lookup 之后的 enriched trip 数据
TRIP_ENRICHED_PATH = str(PROCESSED_DIR / "trip_enriched")

TOP_ROUTES_PATH = str(PROCESSED_DIR / "top_routes")

# 最关键的中间核心表：zone-hour aggregation/features table
ZONE_HOUR_FEATURES_PATH = str(PROCESSED_DIR / "zone_hour_features")

ZONE_DAILY_FEATURES_PATH = str(PROCESSED_DIR / "zone_daily_features")

BOROUGH_HOUR_FEATURES_PATH = str(PROCESSED_DIR / "borough_hour_features")

# ingestion 阶段生成的 summary 文本文件
# 用来记录 schema、row count、sample rows 等信息
INGESTION_SUMMARY_PATH = str(PROCESSED_DIR / "ingestion" / "ingestion_summary.txt")


# ============================================================
# Common Column Names
# ============================================================
# 这里把项目里最常用的核心列名统一定义成常量
#
# 避免每个脚本都手写字符串，减少拼写错误
# 如果以后需要修改列名映射，只改这里一处，让 cleaning、features、analytics、forecasting 层更统一

# 原始 trip 数据中的时间列
PICKUP_TIME_COL = "tpep_pickup_datetime"
DROPOFF_TIME_COL = "tpep_dropoff_datetime"

# 原始 trip 数据中的位置 ID 列
PICKUP_ID_COL = "PULocationID"
DROPOFF_ID_COL = "DOLocationID"

# 常用数值列
TRIP_DISTANCE_COL = "trip_distance"
PASSENGER_COUNT_COL = "passenger_count"
FARE_AMOUNT_COL = "fare_amount"
TOTAL_AMOUNT_COL = "total_amount"

# Taxi Zone Lookup Table 中的列
ZONE_LOOKUP_ID_COL = "LocationID"
BOROUGH_COL = "Borough"
ZONE_COL = "Zone"
SERVICE_ZONE_COL = "service_zone"


# ============================================================
# Spark Application Names
# ============================================================
# 给不同阶段的 Spark 脚本预留统一的 application name
# 在 Spark UI、日志、报错信息里，能快速看出当前是哪一层在运行
INGESTION_APP_NAME = "NYC Taxi Trip Analytics - Ingestion"
CLEANING_APP_NAME = "NYC Taxi Trip Analytics - Cleaning"
FEATURE_APP_NAME = "NYC Taxi Trip Analytics - Feature Engineering"
ANALYTICS_APP_NAME = "NYC Taxi Trip Analytics - Analytics"
FORECAST_APP_NAME = "NYC Taxi Trip Analytics - Forecasting"
