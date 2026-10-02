"""Train and save the final tuned energy forecasting model."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import joblib
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

from ai_decision_platform.training.baseline_forecast import (
    DEFAULT_DATABASE_URL,
    build_forecasting_dataset,
    chronological_split,
    load_hourly_mart,
)
from ai_decision_platform.training.train_regression import (
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    TARGET,
)
from ai_decision_platform.training.tune_hist_gradient_boosting import build_tuning_pipeline

FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
FINAL_MODEL_PARAMS = {
    "model__l2_regularization": 0.0,
    "model__learning_rate": 0.1,
    "model__max_iter": 300,
    "model__max_leaf_nodes": 31,
}


def build_final_model():
    model = build_tuning_pipeline()
    model.set_params(**FINAL_MODEL_PARAMS)
    return model


def evaluate_model(model, test: pd.DataFrame) -> dict[str, float | int]:
    predictions = model.predict(test[FEATURES])
    error = pd.Series(predictions, index=test.index) - test[TARGET]
    return {
        "rows": len(test),
        "mae_mw": mean_absolute_error(test[TARGET], predictions),
        "rmse_mw": mean_squared_error(test[TARGET], predictions) ** 0.5,
        "mape_percent": (error.abs() / test[TARGET].abs()).mean() * 100,
    }


def write_model_card(
    output_dir: Path,
    metrics: dict[str, float | int],
    train_validation: pd.DataFrame,
    test: pd.DataFrame,
) -> None:
    lines = [
        "# Final Energy Forecasting Model Card",
        "",
        "## Purpose",
        "",
        "Predict the next-hour electricity consumption in MW from the hourly energy mart.",
        "",
        "## Model",
        "",
        "- Algorithm: HistGradientBoostingRegressor",
        "- Selection process: model comparison, error analysis, GridSearchCV tuning",
        "- Training data used for final fit: train + validation split",
        "- Final evaluation data: untouched chronological test split",
        "",
        "## Features",
        "",
    ]
    for feature in FEATURES:
        lines.append(f"- `{feature}`")

    lines.extend(
        [
            "",
            "## Target",
            "",
            f"- `{TARGET}`",
            "",
            "## Final parameters",
            "",
        ]
    )
    for name, value in FINAL_MODEL_PARAMS.items():
        lines.append(f"- `{name}`: {value}")

    lines.extend(
        [
            "",
            "## Final test metrics",
            "",
            f"- Test rows: {int(metrics['rows'])}",
            f"- Test MAE: {metrics['mae_mw']:.1f} MW",
            f"- Test RMSE: {metrics['rmse_mw']:.1f} MW",
            f"- Test MAPE: {metrics['mape_percent']:.2f}%",
            "",
            "## Data periods",
            "",
            f"- Train + validation start: {train_validation['timestamp_utc'].min()}",
            f"- Train + validation end: {train_validation['timestamp_utc'].max()}",
            f"- Test start: {test['timestamp_utc'].min()}",
            f"- Test end: {test['timestamp_utc'].max()}",
            "",
            "## Limits",
            "",
            "- The model is trained on historical French electricity and weather features.",
            "- It predicts one hour ahead only.",
            "- It should be monitored on new periods before operational use.",
        ]
    )
    (output_dir / "model_card.md").write_text("\n".join(lines), encoding="utf-8")


def write_metadata(
    output_dir: Path,
    metrics: dict[str, float | int],
    train_validation: pd.DataFrame,
    test: pd.DataFrame,
) -> None:
    metadata = {
        "model_name": "energy_next_hour_hist_gradient_boosting",
        "trained_at_utc": datetime.now(UTC).isoformat(),
        "target": TARGET,
        "numeric_features": NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "final_model_params": FINAL_MODEL_PARAMS,
        "metrics": metrics,
        "train_validation_period": {
            "start": str(train_validation["timestamp_utc"].min()),
            "end": str(train_validation["timestamp_utc"].max()),
            "rows": len(train_validation),
        },
        "test_period": {
            "start": str(test["timestamp_utc"].min()),
            "end": str(test["timestamp_utc"].max()),
            "rows": len(test),
        },
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8"
    )


def train_final_model(frame: pd.DataFrame, output_dir: Path) -> dict[str, float | int]:
    dataset = build_forecasting_dataset(frame)
    splits = chronological_split(dataset)
    train_validation = pd.concat([splits["train"], splits["validation"]], ignore_index=True)
    test = splits["test"]

    model = build_final_model()
    model.fit(train_validation[FEATURES], train_validation[TARGET])
    metrics = evaluate_model(model, test)

    output_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, output_dir / "energy_forecast_model.joblib")
    write_metadata(output_dir, metrics, train_validation, test)
    write_model_card(output_dir, metrics, train_validation, test)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Train and save the final forecasting model")
    parser.add_argument(
        "--database-url",
        default=os.getenv("ADP_POSTGRES_URL", DEFAULT_DATABASE_URL),
        help="SQLAlchemy PostgreSQL URL",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("models") / "energy_forecast",
        help="Directory where the final model artifact is written",
    )
    args = parser.parse_args()

    frame = load_hourly_mart(args.database_url)
    metrics = train_final_model(frame, args.output_dir)
    print(
        "Final model written to "
        f"{args.output_dir} - MAE={metrics['mae_mw']:.1f} MW, "
        f"RMSE={metrics['rmse_mw']:.1f} MW"
    )


if __name__ == "__main__":
    main()
