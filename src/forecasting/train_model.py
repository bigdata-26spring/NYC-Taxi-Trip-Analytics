from pathlib import Path

import joblib
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from tqdm.auto import tqdm


PROJECT_ROOT = Path(__file__).resolve().parents[2]

TRAIN_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "forecasting" / "training_data_train.csv"

MODEL_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "models"
TABLE_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "tables"

MODEL_COMPARISON_PATH = TABLE_OUTPUT_DIR / "model_comparison.csv"
BEST_MODEL_PATH = MODEL_OUTPUT_DIR / "best_model.pkl"

TARGET_COL = "trip_count"
VALIDATION_SIZE = 0.20
RANDOM_STATE = 42


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

    return categorical_features, numeric_features


def main():
    MODEL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TABLE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Reading training data...")
    train_df = pd.read_csv(TRAIN_DATA_PATH)
    train_df["pickup_date"] = pd.to_datetime(train_df["pickup_date"])

    print(f"Training data rows: {len(train_df)}")

    categorical_features, numeric_features = get_feature_columns()
    feature_cols = categorical_features + numeric_features

    X = train_df[feature_cols]
    y = train_df[TARGET_COL]

    X_train, X_val, y_train, y_val = train_test_split(
        X,
        y,
        test_size=VALIDATION_SIZE,
        random_state=RANDOM_STATE,
        shuffle=True,
    )

    print(f"Model training rows: {len(X_train)}")
    print(f"Validation rows: {len(X_val)}")

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
            ("num", StandardScaler(), numeric_features),
        ]
    )

    models = {
        "Linear Regression": LinearRegression(),
        "Random Forest": RandomForestRegressor(
            n_estimators=60,
            max_depth=18,
            min_samples_leaf=3,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        ),
        "Gradient Boosting": GradientBoostingRegressor(
            n_estimators=80,
            learning_rate=0.08,
            max_depth=4,
            random_state=RANDOM_STATE,
        ),
    }

    results = []
    best_model_name = None
    best_rmse = float("inf")
    best_pipeline = None

    model_progress = tqdm(models.items(), total=len(models), desc="Training models", unit="model")
    for model_name, model in model_progress:
        model_progress.set_postfix(model=model_name)
        print(f"\nTraining model: {model_name}")

        pipeline = Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                ("model", model),
            ]
        )

        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_val)

        model_mae = mean_absolute_error(y_val, y_pred)
        model_rmse = rmse(y_val, y_pred)
        model_r2 = r2_score(y_val, y_pred)

        print(f"Validation MAE:  {model_mae:.4f}")
        print(f"Validation RMSE: {model_rmse:.4f}")
        print(f"Validation R2:   {model_r2:.4f}")

        results.append(
            {
                "model": model_name,
                "MAE": model_mae,
                "RMSE": model_rmse,
                "R2": model_r2,
            }
        )

        model_file_name = model_name.lower().replace(" ", "_") + "_model.pkl"
        model_path = MODEL_OUTPUT_DIR / model_file_name
        joblib.dump(pipeline, model_path)
        print(f"Saved model to: {model_path}")

        if model_rmse < best_rmse:
            best_rmse = model_rmse
            best_model_name = model_name
            best_pipeline = pipeline

    results_df = pd.DataFrame(results).sort_values("RMSE").reset_index(drop=True)
    results_df["is_best_model"] = False
    results_df.loc[0, "is_best_model"] = True

    results_df.to_csv(MODEL_COMPARISON_PATH, index=False)
    joblib.dump(best_pipeline, BEST_MODEL_PATH)

    print("\n===== Model Comparison on Validation Set =====")
    print(results_df)

    print(f"\nBest model: {best_model_name}")
    print(f"Best model saved to: {BEST_MODEL_PATH}")
    print(f"Model comparison saved to: {MODEL_COMPARISON_PATH}")


if __name__ == "__main__":
    main()
