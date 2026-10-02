"""Analyze where the selected forecasting model makes errors."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

from ai_decision_platform.training.baseline_forecast import (
    DEFAULT_DATABASE_URL,
    build_forecasting_dataset,
    chronological_split,
    load_hourly_mart,
    regression_metrics,
)
from ai_decision_platform.training.train_regression import (
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    TARGET,
    build_hist_gradient_boosting_pipeline,
)

BEST_MODEL_NAME = "hist_gradient_boosting"
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def build_predictions(splits: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Train the selected model and return validation/test predictions."""
    model = build_hist_gradient_boosting_pipeline()
    train = splits["train"]
    model.fit(train[FEATURES], train[TARGET])

    prediction_frames = []
    for split_name in ["validation", "test"]:
        split = splits[split_name].copy()
        split["split"] = split_name
        split["model"] = BEST_MODEL_NAME
        split["prediction_next_hour_mw"] = model.predict(split[FEATURES])
        split["error_mw"] = split["prediction_next_hour_mw"] - split[TARGET]
        split["absolute_error_mw"] = split["error_mw"].abs()
        split["absolute_percentage_error"] = split["absolute_error_mw"] / split[TARGET].abs() * 100
        prediction_frames.append(split)

    return pd.concat(prediction_frames, ignore_index=True)


def summarize_by_group(predictions: pd.DataFrame, group_column: str) -> pd.DataFrame:
    """Aggregate errors for one business dimension."""
    return (
        predictions.groupby(["split", group_column], dropna=False)
        .agg(
            rows=("timestamp_utc", "count"),
            mae_mw=("absolute_error_mw", "mean"),
            rmse_mw=("error_mw", lambda values: (values.pow(2).mean()) ** 0.5),
            mape_percent=("absolute_percentage_error", "mean"),
            mean_error_mw=("error_mw", "mean"),
            max_absolute_error_mw=("absolute_error_mw", "max"),
        )
        .reset_index()
        .sort_values(["split", "mae_mw"], ascending=[True, False])
    )


def summarize_global_metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split_name, split in predictions.groupby("split"):
        rows.append(
            {
                "split": split_name,
                "model": BEST_MODEL_NAME,
                "rows": len(split),
                **regression_metrics(split[TARGET], split["prediction_next_hour_mw"]),
            }
        )
    return pd.DataFrame(rows)


def write_outputs(predictions: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    global_metrics = summarize_global_metrics(predictions)
    by_hour = summarize_by_group(predictions, "hour")
    by_month = summarize_by_group(predictions, "month")
    by_weekday = summarize_by_group(predictions, "weekday_iso")
    by_demand_period = summarize_by_group(predictions, "demand_period")
    by_weekend = summarize_by_group(predictions, "is_weekend")
    by_public_holiday = summarize_by_group(predictions, "is_public_holiday")

    predictions.to_csv(output_dir / "predictions.csv", index=False)
    global_metrics.to_csv(output_dir / "global_metrics.csv", index=False)
    by_hour.to_csv(output_dir / "errors_by_hour.csv", index=False)
    by_month.to_csv(output_dir / "errors_by_month.csv", index=False)
    by_weekday.to_csv(output_dir / "errors_by_weekday.csv", index=False)
    by_demand_period.to_csv(output_dir / "errors_by_demand_period.csv", index=False)
    by_weekend.to_csv(output_dir / "errors_by_weekend.csv", index=False)
    by_public_holiday.to_csv(output_dir / "errors_by_public_holiday.csv", index=False)

    worst_hours = by_hour.loc[by_hour["split"] == "test"].head(5)
    worst_periods = by_demand_period.loc[by_demand_period["split"] == "test"].head(5)
    test_metrics = global_metrics.loc[global_metrics["split"] == "test"].iloc[0]

    summary_lines = [
        "# Forecast Error Analysis",
        "",
        f"- Model analyzed: {BEST_MODEL_NAME}",
        f"- Test rows: {int(test_metrics['rows'])}",
        f"- Test MAE: {test_metrics['mae_mw']:.1f} MW",
        f"- Test RMSE: {test_metrics['rmse_mw']:.1f} MW",
        f"- Test MAPE: {test_metrics['mape_percent']:.2f}%",
        "",
        "## Worst test hours by MAE",
        "",
    ]
    for row in worst_hours.itertuples(index=False):
        summary_lines.append(f"- Hour {row.hour}: MAE {row.mae_mw:.1f} MW on {row.rows} rows")

    summary_lines.extend(["", "## Worst test demand periods by MAE", ""])
    for row in worst_periods.itertuples(index=False):
        summary_lines.append(f"- {row.demand_period}: MAE {row.mae_mw:.1f} MW on {row.rows} rows")

    summary_lines.extend(
        [
            "",
            "## How to read this report",
            "",
            "High MAE groups show where the model is less reliable.",
            "A positive mean_error_mw means the model overpredicts on average.",
            "A negative mean_error_mw means the model underpredicts on average.",
            "The next feature work should target the groups with the highest MAE.",
        ]
    )
    (output_dir / "error_analysis_summary.md").write_text(
        "\n".join(summary_lines), encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze forecast errors by business dimensions")
    parser.add_argument(
        "--database-url",
        default=os.getenv("ADP_POSTGRES_URL", DEFAULT_DATABASE_URL),
        help="SQLAlchemy PostgreSQL URL",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / "error_analysis",
        help="Directory where error analysis files are written",
    )
    args = parser.parse_args()

    frame = load_hourly_mart(args.database_url)
    dataset = build_forecasting_dataset(frame)
    splits = chronological_split(dataset)
    predictions = build_predictions(splits)
    write_outputs(predictions, args.output_dir)
    print(f"Error analysis written to {args.output_dir} ({len(predictions)} predictions)")


if __name__ == "__main__":
    main()
