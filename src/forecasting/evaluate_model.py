from pathlib import Path

import joblib
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from tqdm.auto import tqdm


PROJECT_ROOT = Path(__file__).resolve().parents[2]

EVAL_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "forecasting" / "training_data_eval.csv"

MODEL_DIR = PROJECT_ROOT / "outputs" / "models"
LINEAR_MODEL_PATH = MODEL_DIR / "linear_regression_model.pkl"
RF_MODEL_PATH = MODEL_DIR / "random_forest_model.pkl"
GB_MODEL_PATH = MODEL_DIR / "gradient_boosting_model.pkl"

PREDICTION_DIR = PROJECT_ROOT / "outputs" / "predictions"
TABLE_DIR = PROJECT_ROOT / "outputs" / "tables"
FIGURE_DIR = PROJECT_ROOT / "outputs" / "figures"

PREDICTION_OUTPUT_PATH = PREDICTION_DIR / "forecast_predictions.csv"
DAILY_FORECAST_OUTPUT_PATH = PREDICTION_DIR / "forecast_daily_actual_predicted.csv"
ERROR_BY_HOUR_OUTPUT_PATH = PREDICTION_DIR / "forecast_error_by_hour.csv"
ERROR_BY_BOROUGH_OUTPUT_PATH = PREDICTION_DIR / "forecast_error_by_borough.csv"
ZONE_ACCURACY_OUTPUT_PATH = PREDICTION_DIR / "forecast_zone_accuracy.csv"
WEEKDAY_HOUR_ERROR_OUTPUT_PATH = PREDICTION_DIR / "forecast_weekday_hour_error_heatmap.csv"
METRICS_OUTPUT_PATH = TABLE_DIR / "model_evaluation_metrics.csv"

TARGET_COL = "trip_count"


def rmse(y_true, y_pred):
    return mean_squared_error(y_true, y_pred) ** 0.5


def get_feature_columns():
    categorical_features = ["pickup_borough"]

    numeric_features = [
        "PULocationID",
        "year",
        "month",
        "day",
        "day_of_week",
        "is_weekend",
        "hour",
        "avg_trip_distance",
        "avg_trip_duration_min",
        "avg_fare_amount",
        "avg_total_amount",
        "avg_passenger_count",
        "avg_speed_mph",
        "credit_card_share",
        "cash_share",
        "lag_1_hour_trip_count",
        "lag_24_hour_trip_count",
        "rolling_24h_avg_trip_count",
    ]

    return categorical_features + numeric_features


def evaluate_single_model(model_name, model_path, X_eval, y_eval):
    print(f"Evaluating model: {model_name}")

    model = joblib.load(model_path)
    y_pred = model.predict(X_eval)

    return {
        "model": model_name,
        "MAE": mean_absolute_error(y_eval, y_pred),
        "RMSE": rmse(y_eval, y_pred),
        "R2": r2_score(y_eval, y_pred),
        "predictions": y_pred,
    }


def add_aggregate_error_columns(df):
    df["prediction_error"] = df["trip_count"] - df["predicted_trip_count"]
    df["absolute_error"] = df["prediction_error"].abs()
    df["absolute_percentage_error"] = (
        df["absolute_error"] / df["trip_count"].where(df["trip_count"] != 0)
    ) * 100
    return df


def build_error_summary(df, group_cols):
    work_df = df.copy()
    work_df["squared_error"] = work_df["prediction_error"] ** 2
    work_df["absolute_percentage_error"] = (
        work_df["absolute_error"] / work_df["trip_count"].where(work_df["trip_count"] != 0)
    ) * 100

    summary_df = (
        work_df.groupby(group_cols, dropna=False)
        .agg(
            records=("trip_count", "size"),
            actual_trip_count=("trip_count", "sum"),
            predicted_trip_count=("predicted_trip_count", "sum"),
            mean_error=("prediction_error", "mean"),
            mae=("absolute_error", "mean"),
            rmse=("squared_error", lambda values: values.mean() ** 0.5),
            mape=("absolute_percentage_error", "mean"),
            total_absolute_error=("absolute_error", "sum"),
        )
        .reset_index()
    )

    summary_df["aggregate_error"] = (
        summary_df["actual_trip_count"] - summary_df["predicted_trip_count"]
    )
    summary_df["aggregate_absolute_error"] = summary_df["aggregate_error"].abs()
    summary_df["aggregate_absolute_percentage_error"] = (
        summary_df["aggregate_absolute_error"]
        / summary_df["actual_trip_count"].where(summary_df["actual_trip_count"] != 0)
    ) * 100

    return summary_df


def main():
    PREDICTION_DIR.mkdir(parents=True, exist_ok=True)
    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    FIGURE_DIR.mkdir(parents=True, exist_ok=True)

    print("Reading evaluation data...")
    eval_df = pd.read_csv(EVAL_DATA_PATH)
    eval_df["pickup_date"] = pd.to_datetime(eval_df["pickup_date"])

    eval_df = eval_df.sort_values(
        ["pickup_date", "hour", "PULocationID"]
    ).reset_index(drop=True)

    feature_cols = get_feature_columns()

    X_eval = eval_df[feature_cols]
    y_eval = eval_df[TARGET_COL]

    model_paths = {
        "Linear Regression": LINEAR_MODEL_PATH,
        "Random Forest": RF_MODEL_PATH,
        "Gradient Boosting": GB_MODEL_PATH,
    }

    results = []
    model_progress = tqdm(model_paths.items(), total=len(model_paths), desc="Evaluating models", unit="model")
    for model_name, model_path in model_progress:
        model_progress.set_postfix(model=model_name)
        results.append(evaluate_single_model(model_name, model_path, X_eval, y_eval))

    metrics_rows = []
    for result in results:
        metrics_rows.append(
            {
                "model": result["model"],
                "MAE": result["MAE"],
                "RMSE": result["RMSE"],
                "R2": result["R2"],
            }
        )

    metrics_df = pd.DataFrame(metrics_rows).sort_values("RMSE").reset_index(drop=True)
    metrics_df["is_best_model"] = False
    metrics_df.loc[0, "is_best_model"] = True

    metrics_df.to_csv(METRICS_OUTPUT_PATH, index=False)

    print("\n===== Final Evaluation Metrics =====")
    print(metrics_df)
    print(f"Metrics saved to: {METRICS_OUTPUT_PATH}")

    best_model_name = metrics_df.loc[0, "model"]
    best_result = next(result for result in results if result["model"] == best_model_name)
    best_pred = best_result["predictions"]

    eval_df["predicted_trip_count"] = best_pred
    eval_df["prediction_error"] = eval_df["trip_count"] - eval_df["predicted_trip_count"]
    eval_df["absolute_error"] = eval_df["prediction_error"].abs()
    eval_df["weekday_name"] = eval_df["day_of_week"].map(
        {
            1: "Sun",
            2: "Mon",
            3: "Tue",
            4: "Wed",
            5: "Thu",
            6: "Fri",
            7: "Sat",
        }
    )

    prediction_cols = [
        "pickup_date",
        "year",
        "month",
        "day",
        "hour",
        "PULocationID",
        "pickup_zone",
        "pickup_borough",
        "trip_count",
        "predicted_trip_count",
        "prediction_error",
        "absolute_error",
    ]

    output_steps = tqdm(total=13, desc="Writing evaluation outputs", unit="step")

    eval_df[prediction_cols].to_csv(PREDICTION_OUTPUT_PATH, index=False)
    print(f"Prediction table saved to: {PREDICTION_OUTPUT_PATH}")
    output_steps.update(1)

    eval_df[prediction_cols].to_csv(
        PREDICTION_DIR / "plot_actual_vs_predicted_hourly.csv",
        index=False,
    )
    output_steps.update(1)

    monthly_plot_df = eval_df.copy()
    monthly_plot_df["year_month"] = monthly_plot_df["pickup_date"].dt.to_period("M").astype(str)

    monthly_plot_df = (
        monthly_plot_df.groupby("year_month")[["trip_count", "predicted_trip_count"]]
        .sum()
        .reset_index()
        .sort_values("year_month")
    )

    monthly_plot_df.to_csv(
        PREDICTION_DIR / "plot_actual_vs_predicted_monthly.csv",
        index=False,
    )
    output_steps.update(1)

    daily_forecast_df = (
        eval_df.groupby(["pickup_date", "year", "month", "day"])[
            ["trip_count", "predicted_trip_count"]
        ]
        .sum()
        .reset_index()
        .sort_values("pickup_date")
    )
    daily_forecast_df = add_aggregate_error_columns(daily_forecast_df)
    daily_forecast_df.to_csv(DAILY_FORECAST_OUTPUT_PATH, index=False)
    output_steps.update(1)

    error_by_hour_df = build_error_summary(eval_df, ["hour"]).sort_values("hour")
    error_by_hour_df.to_csv(ERROR_BY_HOUR_OUTPUT_PATH, index=False)
    output_steps.update(1)

    error_by_borough_df = build_error_summary(eval_df, ["pickup_borough"]).sort_values(
        "aggregate_absolute_error",
        ascending=False,
    )
    error_by_borough_df.to_csv(ERROR_BY_BOROUGH_OUTPUT_PATH, index=False)
    output_steps.update(1)

    zone_accuracy_df = build_error_summary(
        eval_df,
        ["PULocationID", "pickup_zone", "pickup_borough"],
    ).sort_values("aggregate_absolute_error", ascending=False)
    zone_accuracy_df.to_csv(ZONE_ACCURACY_OUTPUT_PATH, index=False)
    output_steps.update(1)

    weekday_hour_error_df = build_error_summary(
        eval_df,
        ["day_of_week", "weekday_name", "hour"],
    ).sort_values(["day_of_week", "hour"])
    weekday_hour_error_df.to_csv(WEEKDAY_HOUR_ERROR_OUTPUT_PATH, index=False)
    output_steps.update(1)

    error_series = eval_df["prediction_error"].dropna()
    low = error_series.quantile(0.01)
    high = error_series.quantile(0.99)
    clipped_errors = error_series[(error_series >= low) & (error_series <= high)]

    pd.DataFrame({"prediction_error": clipped_errors}).to_csv(
        PREDICTION_DIR / "plot_error_distribution.csv",
        index=False,
    )

    metrics_df.to_csv(
        PREDICTION_DIR / "plot_model_comparison.csv",
        index=False,
    )
    output_steps.update(1)

    plt.figure(figsize=(8, 6))
    plt.scatter(y_eval, best_pred, alpha=0.25)

    min_value = min(y_eval.min(), best_pred.min())
    max_value = max(y_eval.max(), best_pred.max())
    plt.plot([min_value, max_value], [min_value, max_value], linestyle="--", linewidth=2)

    plt.xlabel("Actual Trip Count")
    plt.ylabel("Predicted Trip Count")
    plt.title(f"Actual vs Predicted Taxi Demand ({best_model_name})")
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "actual_vs_predicted_scatter.png", dpi=300)
    plt.close()
    output_steps.update(1)

    plt.figure(figsize=(14, 6))
    plt.plot(
        monthly_plot_df["year_month"],
        monthly_plot_df["trip_count"],
        marker="o",
        label="Actual",
    )
    plt.plot(
        monthly_plot_df["year_month"],
        monthly_plot_df["predicted_trip_count"],
        marker="o",
        label="Predicted",
    )

    plt.xlabel("Month")
    plt.ylabel("Total Trip Count")
    plt.title(f"Actual vs Predicted Monthly Taxi Demand ({best_model_name})")
    plt.legend()
    plt.xticks(rotation=60)
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "actual_vs_predicted_monthly_curve.png", dpi=300)
    plt.close()
    output_steps.update(1)

    plt.figure(figsize=(8, 6))
    plt.hist(clipped_errors, bins=60)
    plt.xlabel("Prediction Error")
    plt.ylabel("Frequency")
    plt.title("Prediction Error Distribution")
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "prediction_error_distribution.png", dpi=300)
    plt.close()
    output_steps.update(1)

    plt.figure(figsize=(8, 6))
    plt.bar(metrics_df["model"], metrics_df["RMSE"])
    plt.xlabel("Model")
    plt.ylabel("RMSE")
    plt.title("Model Comparison by RMSE on Evaluation Set")
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "model_comparison.png", dpi=300)
    plt.close()
    output_steps.update(1)
    output_steps.close()

    print("\nEvaluation completed.")
    print(f"Best model: {best_model_name}")
    print(f"Figures saved to: {FIGURE_DIR}")
    print(f"Plot data CSV files saved to: {PREDICTION_DIR}")


if __name__ == "__main__":
    main()
