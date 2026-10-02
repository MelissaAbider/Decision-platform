"""Tune the selected HistGradientBoosting forecasting model."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

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

FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def build_tuning_pipeline() -> Pipeline:
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
            ("model", HistGradientBoostingRegressor(random_state=42)),
        ]
    )


def parameter_grid() -> dict[str, list[float | int]]:
    return {
        "model__learning_rate": [0.03, 0.06, 0.1],
        "model__max_iter": [200, 300, 500],
        "model__max_leaf_nodes": [15, 31, 63],
        "model__l2_regularization": [0.0, 0.1],
    }


def tune_model(train: pd.DataFrame) -> GridSearchCV:
    search = GridSearchCV(
        estimator=build_tuning_pipeline(),
        param_grid=parameter_grid(),
        scoring="neg_mean_absolute_error",
        cv=TimeSeriesSplit(n_splits=3),
        n_jobs=-1,
        refit=True,
        return_train_score=True,
    )
    search.fit(train[FEATURES], train[TARGET])
    return search


def evaluate_best_model(search: GridSearchCV, splits: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    best_model = search.best_estimator_
    for split_name, split in splits.items():
        predictions = best_model.predict(split[FEATURES])
        error = pd.Series(predictions, index=split.index) - split[TARGET]
        rows.append(
            {
                "split": split_name,
                "model": "hist_gradient_boosting_tuned",
                "rows": len(split),
                "mae_mw": mean_absolute_error(split[TARGET], predictions),
                "rmse_mw": mean_squared_error(split[TARGET], predictions) ** 0.5,
                "mape_percent": (error.abs() / split[TARGET].abs()).mean() * 100,
            }
        )
    return pd.DataFrame(rows)


def write_outputs(search: GridSearchCV, metrics: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    results = pd.DataFrame(search.cv_results_).sort_values("rank_test_score")
    results.to_csv(output_dir / "grid_search_results.csv", index=False)
    metrics.to_csv(output_dir / "tuned_model_metrics.csv", index=False)

    best_params = search.best_params_
    validation_mae = -search.best_score_
    test_metrics = metrics.loc[metrics["split"] == "test"].iloc[0]

    lines = [
        "# HistGradientBoosting Tuning Summary",
        "",
        f"- Tested parameter combinations: {len(results)}",
        "- Cross-validation strategy: TimeSeriesSplit with 3 splits",
        f"- Best validation CV MAE: {validation_mae:.1f} MW",
        f"- Test MAE: {test_metrics['mae_mw']:.1f} MW",
        f"- Test RMSE: {test_metrics['rmse_mw']:.1f} MW",
        f"- Test MAPE: {test_metrics['mape_percent']:.2f}%",
        "",
        "## Best parameters",
        "",
    ]
    for name, value in best_params.items():
        lines.append(f"- `{name}`: {value}")
    lines.extend(
        [
            "",
            "## How to interpret",
            "",
            "GridSearchCV trains one model for each parameter combination.",
            "TimeSeriesSplit keeps validation after training periods to respect time order.",
            "The selected model is the one with the lowest MAE during cross-validation.",
        ]
    )
    (output_dir / "tuning_summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Tune HistGradientBoosting for energy forecasting")
    parser.add_argument(
        "--database-url",
        default=os.getenv("ADP_POSTGRES_URL", DEFAULT_DATABASE_URL),
        help="SQLAlchemy PostgreSQL URL",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / "model_tuning",
        help="Directory where tuning outputs are written",
    )
    args = parser.parse_args()

    frame = load_hourly_mart(args.database_url)
    dataset = build_forecasting_dataset(frame)
    splits = chronological_split(dataset)
    search = tune_model(splits["train"])
    metrics = evaluate_best_model(search, splits)
    write_outputs(search, metrics, args.output_dir)
    print(f"Tuning report written to {args.output_dir}")


if __name__ == "__main__":
    main()
