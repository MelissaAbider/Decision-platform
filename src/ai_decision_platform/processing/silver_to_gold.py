"""Transformation Silver vers Gold avec Spark et Delta Lake."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class GoldTables:
    energy_features_hourly: Path


def read_silver_tables(
    spark: SparkSession,
    silver_dir: Path,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    rte = spark.read.format("delta").load(str(silver_dir / "rte_consumption"))
    weather = spark.read.format("delta").load(str(silver_dir / "weather_hourly"))
    calendar = spark.read.format("delta").load(str(silver_dir / "calendar_daily"))
    return rte, weather, calendar


def hourly_consumption_features(rte_consumption: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    observed = rte_consumption.filter(F.col("measurement_status") == "observed")

    return observed.groupBy(F.date_trunc("hour", "timestamp_utc").alias("timestamp_utc")).agg(
        F.avg("consumption_mw").alias("consumption_mw")
    )


def hourly_weather_features(weather_hourly: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return weather_hourly.groupBy("timestamp_utc").agg(
        F.avg("temperature_2m_c").alias("temperature_c")
    )


def build_energy_features_hourly(
    rte_consumption: DataFrame,
    weather_hourly: DataFrame,
    calendar_daily: DataFrame,
) -> DataFrame:
    from pyspark.sql import functions as F

    consumption_hourly = hourly_consumption_features(rte_consumption)
    weather_features = hourly_weather_features(weather_hourly)

    joined = (
        consumption_hourly.join(weather_features, on="timestamp_utc", how="inner")
        .withColumn("date_utc", F.to_date("timestamp_utc"))
        .join(calendar_daily, F.col("date_utc") == F.col("date"), how="inner")
    )

    return (
        joined.select(
            F.col("timestamp_utc"),
            F.col("date_utc"),
            F.round("consumption_mw", 3).alias("consumption_mw"),
            F.round("temperature_c", 3).alias("temperature_c"),
            F.hour("timestamp_utc").alias("hour"),
            F.month("timestamp_utc").alias("month"),
            F.col("weekday_iso"),
            F.col("is_weekend"),
            F.col("is_public_holiday"),
        )
        .dropDuplicates(["timestamp_utc"])
        .orderBy("timestamp_utc")
    )


def transform_silver_to_gold(silver_dir: Path, gold_dir: Path) -> GoldTables:
    from ai_decision_platform.processing.bronze_to_silver import build_spark, write_delta

    tables = GoldTables(energy_features_hourly=gold_dir / "energy_features_hourly")
    spark = build_spark("ADP-Silver-To-Gold")
    try:
        spark.sparkContext.setLogLevel("WARN")
        rte_consumption, weather_hourly, calendar_daily = read_silver_tables(spark, silver_dir)
        features = build_energy_features_hourly(rte_consumption, weather_hourly, calendar_daily)
        write_delta(features, tables.energy_features_hourly, "date_utc")
    finally:
        spark.stop()
    return tables


def main() -> None:
    parser = argparse.ArgumentParser(description="Transforme les tables Silver en table Gold")
    parser.add_argument(
        "--silver-dir",
        type=Path,
        default=Path.home() / "ia-decision-platform-data" / "silver",
        help="Dossier contenant les tables Silver Delta",
    )
    parser.add_argument(
        "--gold-dir",
        type=Path,
        default=Path.home() / "ia-decision-platform-data" / "gold",
        help="Dossier de sortie Gold Delta",
    )
    args = parser.parse_args()

    tables = transform_silver_to_gold(args.silver_dir, args.gold_dir)
    print("Gold features energie:", tables.energy_features_hourly)


if __name__ == "__main__":
    main()
