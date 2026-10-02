"""Build a one-hour-ahead forecasting dataset and evaluate simple baselines."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

DEFAULT_DATABASE_URL = "postgresql+psycopg://adp:adp@localhost:5432/adp"
QUERY = """
SELECT
    timestamp_utc,
    consumption_mw,
    temperature_c,
    hour,
    month,
    weekday_iso,
    is_weekend,
    is_public_holiday,
    is_daytime,
    demand_period
FROM analytics.mart_energy_hourly
ORDER BY timestamp_utc
"""


def load_hourly_mart(database_url: str) -> pd.DataFrame:
    engine = create_engine(database_url)
    frame = pd.read_sql_query(QUERY, engine)
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"])
    return frame


def build_forecasting_dataset(frame: pd.DataFrame) -> pd.DataFrame:
    dataset = frame.sort_values("timestamp_utc").copy()
    dataset["target_timestamp_utc"] = dataset["timestamp_utc"].shift(-1)
    dataset["target_next_hour_mw"] = dataset["consumption_mw"].shift(-1)
    dataset["baseline_last_hour_mw"] = dataset["consumption_mw"]
    dataset["baseline_previous_day_same_hour_mw"] = dataset["consumption_mw"].shift(24)
    dataset = dataset.dropna(
        subset=[
            "target_next_hour_mw",
            "baseline_last_hour_mw",
            "baseline_previous_day_same_hour_mw",
        ]
    ).copy()
    return dataset


def chronological_split(
    dataset: pd.DataFrame,
    train_ratio: float = 0.70,
    validation_ratio: float = 0.15,
) -> dict[str, pd.DataFrame]:
    if not 0 < train_ratio < 1 or not 0 < validation_ratio < 1:
        raise ValueError("Les ratios doivent etre entre 0 et 1")
    if train_ratio + validation_ratio >= 1:
        raise ValueError("train_ratio + validation_ratio doit etre < 1")

    train_end = int(len(dataset) * train_ratio)
    validation_end = int(len(dataset) * (train_ratio + validation_ratio))
    return {
        "train": dataset.iloc[:train_end].copy(),
        "validation": dataset.iloc[train_end:validation_end].copy(),
        "test": dataset.iloc[validation_end:].copy(),
    }


def regression_metrics(actual: pd.Series, predicted: pd.Series) -> dict[str, float]:
    error = predicted - actual
    mae = error.abs().mean()
    rmse = (error.pow(2).mean()) ** 0.5
    mape = (error.abs() / actual.abs()).mean() * 100
    return {"mae_mw": mae, "rmse_mw": rmse, "mape_percent": mape}


def evaluate_baselines(splits: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    baselines = {
        "last_hour": "baseline_last_hour_mw",
        "previous_day_same_hour": "baseline_previous_day_same_hour_mw",
    }
    for split_name, split in splits.items():
        for baseline_name, prediction_column in baselines.items():
            metrics = regression_metrics(
                split["target_next_hour_mw"],
                split[prediction_column],
            )
            rows.append(
                {
                    "split": split_name,
                    "baseline": baseline_name,
                    "rows": len(split),
                    "start_timestamp": split["timestamp_utc"].min(),
                    "end_timestamp": split["timestamp_utc"].max(),
                    **metrics,
                }
            )
    return pd.DataFrame(rows)


def write_outputs(dataset: pd.DataFrame, metrics: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(output_dir / "forecasting_dataset.csv", index=False)
    metrics.to_csv(output_dir / "baseline_metrics.csv", index=False)

    best_validation = metrics.loc[metrics["split"] == "validation"].sort_values("mae_mw").iloc[0]
    best_test = metrics.loc[
        (metrics["split"] == "test") & (metrics["baseline"] == best_validation["baseline"])
    ].iloc[0]
    (output_dir / "baseline_summary.md").write_text(
        "\n".join(
            [
                "# Baseline Forecast Summary",
                "",
                f"- Dataset rows: {len(dataset)}",
                f"- First feature timestamp: {dataset['timestamp_utc'].min()}",
                f"- Last feature timestamp: {dataset['timestamp_utc'].max()}",
                f"- Best validation baseline: {best_validation['baseline']}",
                f"- Validation MAE: {best_validation['mae_mw']:.1f} MW",
                f"- Validation RMSE: {best_validation['rmse_mw']:.1f} MW",
                f"- Test MAE with selected baseline: {best_test['mae_mw']:.1f} MW",
                f"- Test RMSE with selected baseline: {best_test['rmse_mw']:.1f} MW",
                "",
                (
                    "Next step: train a first supervised regression model and compare it "
                    "to this baseline."
                ),
            ]
        ),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate one-hour-ahead forecast baselines")
    parser.add_argument(
        "--database-url",
        default=os.getenv("ADP_POSTGRES_URL", DEFAULT_DATABASE_URL),
        help="SQLAlchemy PostgreSQL URL",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / "baseline_forecast",
        help="Directory where dataset and metrics are written",
    )
    args = parser.parse_args()

    frame = load_hourly_mart(args.database_url)
    dataset = build_forecasting_dataset(frame)
    splits = chronological_split(dataset)
    metrics = evaluate_baselines(splits)
    write_outputs(dataset, metrics, args.output_dir)
    print(f"Baseline report written to {args.output_dir} ({len(dataset)} rows)")


if __name__ == "__main__":
    main()
