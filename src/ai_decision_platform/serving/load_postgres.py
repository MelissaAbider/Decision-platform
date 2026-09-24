"""Charge la table Gold dans PostgreSQL."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text

from ai_decision_platform.processing.bronze_to_silver import build_spark

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession


DEFAULT_POSTGRES_URL = "postgresql+psycopg://adp:adp@localhost:5432/adp"
GOLD_TABLE = "energy_features_hourly"
POSTGRES_TABLE = "energy_features_hourly_postgres"
POSTGRES_SCHEMA = "public"
POSTGRES_COLUMNS = [
    "timestamp_utc",
    "date_utc",
    "consumption_mw",
    "temperature_c",
    "hour",
    "month",
    "weekday_iso",
    "is_weekend",
    "is_public_holiday",
]


def read_gold_features(spark: SparkSession, gold_dir: Path) -> pd.DataFrame:
    frame = spark.read.format("delta").load(str(gold_dir / GOLD_TABLE))
    pandas_frame = frame.select(*POSTGRES_COLUMNS).orderBy("timestamp_utc").toPandas()
    pandas_frame["timestamp_utc"] = pandas_frame["timestamp_utc"].dt.tz_localize(None)
    pandas_frame["date_utc"] = pandas_frame["date_utc"].astype(str)
    return pandas_frame


def write_postgres(frame: pd.DataFrame, database_url: str) -> None:
    engine = create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {POSTGRES_SCHEMA}.{POSTGRES_TABLE}"))
    frame.to_sql(
        POSTGRES_TABLE,
        engine,
        schema=POSTGRES_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                ALTER TABLE {POSTGRES_SCHEMA}.{POSTGRES_TABLE}
                ADD PRIMARY KEY (timestamp_utc)
                """
            )
        )
        connection.execute(
            text(
                f"""
                CREATE INDEX IF NOT EXISTS ix_energy_features_hourly_date_utc
                ON {POSTGRES_SCHEMA}.{POSTGRES_TABLE} (date_utc)
                """
            )
        )


def load_gold_to_postgres(gold_dir: Path, database_url: str) -> int:
    spark = build_spark("ADP-Load-Gold-Postgres")
    try:
        spark.sparkContext.setLogLevel("ERROR")
        features = read_gold_features(spark, gold_dir)
    finally:
        spark.stop()

    write_postgres(features, database_url)
    return len(features)


def main() -> None:
    parser = argparse.ArgumentParser(description="Charge Gold Delta dans PostgreSQL")
    parser.add_argument(
        "--gold-dir",
        type=Path,
        default=Path.home() / "ia-decision-platform-data" / "gold",
        help="Dossier contenant les tables Gold Delta",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("ADP_POSTGRES_URL", DEFAULT_POSTGRES_URL),
        help="URL SQLAlchemy PostgreSQL",
    )
    args = parser.parse_args()

    rows = load_gold_to_postgres(args.gold_dir, args.database_url)
    print(f"PostgreSQL table loaded: {POSTGRES_SCHEMA}.{POSTGRES_TABLE} ({rows} rows)")


if __name__ == "__main__":
    main()
