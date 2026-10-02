"""Generate an exploratory data analysis report from the hourly energy mart."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine

DEFAULT_DATABASE_URL = "postgresql+psycopg://adp:adp@localhost:5432/adp"
QUERY = """
SELECT
    timestamp_utc,
    date_utc,
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
    frame["date_utc"] = pd.to_datetime(frame["date_utc"]).dt.date
    return frame


def build_quality_summary(frame: pd.DataFrame) -> pd.DataFrame:
    expected = pd.date_range(
        frame["timestamp_utc"].min(),
        frame["timestamp_utc"].max(),
        freq="h",
    )
    missing_hours = expected.difference(frame["timestamp_utc"])
    duplicate_timestamps = frame["timestamp_utc"].duplicated().sum()
    null_rows = (
        frame[["timestamp_utc", "consumption_mw", "temperature_c", "hour", "month"]]
        .isna()
        .any(axis=1)
        .sum()
    )

    return pd.DataFrame(
        [
            {"metric": "rows", "value": len(frame)},
            {"metric": "first_timestamp", "value": frame["timestamp_utc"].min()},
            {"metric": "last_timestamp", "value": frame["timestamp_utc"].max()},
            {"metric": "distinct_timestamps", "value": frame["timestamp_utc"].nunique()},
            {"metric": "missing_hours", "value": len(missing_hours)},
            {"metric": "duplicate_timestamps", "value": int(duplicate_timestamps)},
            {"metric": "rows_with_null_core_fields", "value": int(null_rows)},
        ]
    )


def build_business_summaries(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "monthly_profile": frame.groupby("month", as_index=False).agg(
            avg_consumption_mw=("consumption_mw", "mean"),
            peak_consumption_mw=("consumption_mw", "max"),
            avg_temperature_c=("temperature_c", "mean"),
        ),
        "hourly_profile": frame.groupby("hour", as_index=False).agg(
            avg_consumption_mw=("consumption_mw", "mean"),
            p10_consumption_mw=("consumption_mw", lambda x: x.quantile(0.10)),
            p90_consumption_mw=("consumption_mw", lambda x: x.quantile(0.90)),
        ),
        "demand_period_profile": frame.groupby("demand_period", as_index=False).agg(
            hours=("timestamp_utc", "count"),
            avg_consumption_mw=("consumption_mw", "mean"),
            peak_consumption_mw=("consumption_mw", "max"),
        ),
        "calendar_profile": frame.groupby(["is_weekend", "is_public_holiday"], as_index=False).agg(
            hours=("timestamp_utc", "count"),
            avg_consumption_mw=("consumption_mw", "mean"),
            peak_consumption_mw=("consumption_mw", "max"),
        ),
        "correlations": frame[["consumption_mw", "temperature_c", "hour", "month"]]
        .corr(numeric_only=True)
        .reset_index()
        .rename(columns={"index": "feature"}),
    }


def save_plots(frame: pd.DataFrame, output_dir: Path) -> None:
    sns.set_theme(style="whitegrid")

    monthly = frame.groupby("month", as_index=False)["consumption_mw"].mean()
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=monthly, x="month", y="consumption_mw", marker="o")
    plt.title("Average hourly consumption by month")
    plt.xlabel("Month")
    plt.ylabel("Consumption (MW)")
    plt.tight_layout()
    plt.savefig(output_dir / "01_monthly_consumption.png", dpi=160)
    plt.close()

    hourly = frame.groupby("hour", as_index=False)["consumption_mw"].mean()
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=hourly, x="hour", y="consumption_mw", marker="o")
    plt.title("Average consumption by hour")
    plt.xlabel("Hour UTC")
    plt.ylabel("Consumption (MW)")
    plt.tight_layout()
    plt.savefig(output_dir / "02_hourly_consumption.png", dpi=160)
    plt.close()

    plt.figure(figsize=(9, 6))
    sns.scatterplot(
        data=frame.sample(min(len(frame), 5000), random_state=42),
        x="temperature_c",
        y="consumption_mw",
        hue="is_weekend",
        alpha=0.35,
    )
    plt.title("Consumption vs temperature")
    plt.xlabel("Temperature (C)")
    plt.ylabel("Consumption (MW)")
    plt.tight_layout()
    plt.savefig(output_dir / "03_temperature_vs_consumption.png", dpi=160)
    plt.close()

    pivot = frame.pivot_table(
        index="weekday_iso",
        columns="hour",
        values="consumption_mw",
        aggfunc="mean",
    )
    plt.figure(figsize=(12, 5))
    sns.heatmap(pivot, cmap="viridis")
    plt.title("Average consumption by weekday and hour")
    plt.xlabel("Hour UTC")
    plt.ylabel("Weekday ISO")
    plt.tight_layout()
    plt.savefig(output_dir / "04_weekday_hour_heatmap.png", dpi=160)
    plt.close()


def write_report(frame: pd.DataFrame, output_dir: Path) -> None:
    summaries = build_business_summaries(frame)
    quality = build_quality_summary(frame)
    quality.to_csv(output_dir / "quality_summary.csv", index=False)
    for name, summary in summaries.items():
        summary.to_csv(output_dir / f"{name}.csv", index=False)
    save_plots(frame, output_dir)

    best_peak = (
        summaries["hourly_profile"].sort_values("avg_consumption_mw", ascending=False).head(3)
    )
    cold_corr = frame["consumption_mw"].corr(frame["temperature_c"])
    quality_values = quality.set_index("metric")["value"]
    best_peak_lines = [
        f"- hour {row.hour}: {row.avg_consumption_mw:.1f} MW"
        for row in best_peak.itertuples(index=False)
    ]
    (output_dir / "eda_summary.md").write_text(
        "\n".join(
            [
                "# Energy EDA Summary",
                "",
                f"- Rows: {len(frame)}",
                f"- Period: {frame['timestamp_utc'].min()} to {frame['timestamp_utc'].max()}",
                f"- Missing hours: {quality_values['missing_hours']}",
                f"- Duplicate timestamps: {quality_values['duplicate_timestamps']}",
                f"- Core-null rows: {quality_values['rows_with_null_core_fields']}",
                f"- Consumption/temperature correlation: {cold_corr:.3f}",
                "",
                "Top average consumption hours:",
                *best_peak_lines,
                "",
                (
                    "Next data science step: create chronological splits and compare "
                    "forecasting baselines."
                ),
            ]
        ),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run EDA on analytics.mart_energy_hourly")
    parser.add_argument(
        "--database-url",
        default=os.getenv("ADP_POSTGRES_URL", DEFAULT_DATABASE_URL),
        help="SQLAlchemy PostgreSQL URL",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / "eda_energy",
        help="Directory where CSV summaries and plots are written",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    frame = load_hourly_mart(args.database_url)
    write_report(frame, args.output_dir)
    print(f"EDA report written to {args.output_dir} ({len(frame)} rows)")


if __name__ == "__main__":
    main()
