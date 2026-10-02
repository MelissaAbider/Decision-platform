"""Train and evaluate a first supervised regression model."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBRegressor

from ai_decision_platform.training.baseline_forecast import (
    DEFAULT_DATABASE_URL,
    build_forecasting_dataset,
    chronological_split,
    load_hourly_mart,
    regression_metrics,
)

NUMERIC_FEATURES = ["consumption_mw", "temperature_c", "hour", "month", "weekday_iso"]
CATEGORICAL_FEATURES = [
    "is_weekend",
    "is_public_holiday",
    "is_daytime",
    "demand_period",
]
TARGET = "target_next_hour_mw"


def build_linear_regression_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", StandardScaler(), NUMERIC_FEATURES),
            ("categorical", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
        ]
    )
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", LinearRegression()),
        ]
    )


def build_hist_gradient_boosting_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", "passthrough", NUMERIC_FEATURES),
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                CATEGORICAL_FEATURES,
            ),
        ]
    )
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "model",
                HistGradientBoostingRegressor(
                    learning_rate=0.06,
                    max_iter=300,
                    max_leaf_nodes=31,
                    random_state=42,
                ),
            ),
        ]
    )


def build_random_forest_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", "passthrough", NUMERIC_FEATURES),
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                CATEGORICAL_FEATURES,
            ),
        ]
    )
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "model",
                RandomForestRegressor(
                    n_estimators=200,
                    min_samples_leaf=5,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )


def build_xgboost_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", "passthrough", NUMERIC_FEATURES),
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                CATEGORICAL_FEATURES,
            ),
        ]
    )
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "model",
                XGBRegressor(
                    objective="reg:squarederror",
                    n_estimators=300,
                    learning_rate=0.05,
                    max_depth=5,
                    subsample=0.9,
                    colsample_bytree=0.9,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )


def model_candidates() -> dict[str, Pipeline]:
    return {
        "linear_regression": build_linear_regression_pipeline(),
        "hist_gradient_boosting": build_hist_gradient_boosting_pipeline(),
        "random_forest": build_random_forest_pipeline(),
        "xgboost": build_xgboost_pipeline(),
    }


def evaluate_models(splits: dict[str, pd.DataFrame]) -> pd.DataFrame:
    train = splits["train"]

    rows = []
    for model_name, model in model_candidates().items():
        model.fit(train[NUMERIC_FEATURES + CATEGORICAL_FEATURES], train[TARGET])
        for split_name, split in splits.items():
            predictions = model.predict(split[NUMERIC_FEATURES + CATEGORICAL_FEATURES])
            rows.append(
                {
                    "split": split_name,
                    "model": model_name,
                    "rows": len(split),
                    "start_timestamp": split["timestamp_utc"].min(),
                    "end_timestamp": split["timestamp_utc"].max(),
                    "mae_mw": mean_absolute_error(split[TARGET], predictions),
                    "rmse_mw": mean_squared_error(split[TARGET], predictions) ** 0.5,
                    "mape_percent": (
                        (pd.Series(predictions, index=split.index) - split[TARGET]).abs()
                        / split[TARGET].abs()
                    ).mean()
                    * 100,
                }
            )
    return pd.DataFrame(rows)


def compare_with_baseline(
    splits: dict[str, pd.DataFrame], model_metrics: pd.DataFrame
) -> pd.DataFrame:
    baseline_rows = []
    for split_name, split in splits.items():
        metrics = regression_metrics(split[TARGET], split["baseline_last_hour_mw"])
        baseline_rows.append(
            {
                "split": split_name,
                "model": "baseline_last_hour",
                "rows": len(split),
                **metrics,
            }
        )
    return pd.concat([pd.DataFrame(baseline_rows), model_metrics], ignore_index=True)


def write_outputs(metrics: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(output_dir / "model_metrics.csv", index=False)

    validation_metrics = metrics.loc[metrics["split"] == "validation"].set_index("model")
    test_metrics = metrics.loc[metrics["split"] == "test"].set_index("model")
    model_rows = validation_metrics.drop(index="baseline_last_hour")
    best_model_name = model_rows["mae_mw"].idxmin()
    baseline_mae = test_metrics.loc["baseline_last_hour", "mae_mw"]
    best_model_mae = test_metrics.loc[best_model_name, "mae_mw"]
    improvement = (baseline_mae - best_model_mae) / baseline_mae * 100

    (output_dir / "model_summary.md").write_text(
        "\n".join(
            [
                "# First Regression Model Summary",
                "",
                f"- Best validation model: {best_model_name}",
                f"- Test baseline MAE: {baseline_mae:.1f} MW",
                f"- Test best model MAE: {best_model_mae:.1f} MW",
                f"- MAE improvement vs baseline: {improvement:.2f}%",
                f"- Test best model RMSE: {test_metrics.loc[best_model_name, 'rmse_mw']:.1f} MW",
                "",
                (
                    "Next step: inspect errors by hour, month and demand period before "
                    "deciding the next feature work."
                ),
            ]
        ),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the first one-hour-ahead regression model")
    parser.add_argument(
        "--database-url",
        default=os.getenv("ADP_POSTGRES_URL", DEFAULT_DATABASE_URL),
        help="SQLAlchemy PostgreSQL URL",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / "first_model",
        help="Directory where model metrics are written",
    )
    args = parser.parse_args()

    frame = load_hourly_mart(args.database_url)
    dataset = build_forecasting_dataset(frame)
    splits = chronological_split(dataset)
    model_metrics = evaluate_models(splits)
    metrics = compare_with_baseline(splits, model_metrics)
    write_outputs(metrics, args.output_dir)
    print(f"Model report written to {args.output_dir} ({len(dataset)} rows)")


if __name__ == "__main__":
    main()
