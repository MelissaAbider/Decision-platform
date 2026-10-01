"""Validation Great Expectations de la table Gold."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import great_expectations as gx
import pandas as pd
from great_expectations.core import ExpectationSuite
from great_expectations.data_context import EphemeralDataContext
from great_expectations.expectations import (
    ExpectColumnMaxToBeBetween,
    ExpectColumnMinToBeBetween,
    ExpectColumnPairValuesToBeEqual,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToNotBeNull,
    ExpectTableColumnsToMatchSet,
    ExpectTableRowCountToEqual,
)

from ai_decision_platform.processing.bronze_to_silver import build_spark

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


GOLD_TABLE = "energy_features_hourly"
REQUIRED_COLUMNS = [
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
DERIVED_DATE_COLUMN = "timestamp_date_utc"
DERIVED_HOUR_COLUMN = "timestamp_hour_utc"


def read_gold_as_pandas(spark: SparkSession, gold_dir: Path) -> pd.DataFrame:
    frame = spark.read.format("delta").load(str(gold_dir / GOLD_TABLE))
    pandas_frame = frame.toPandas()
    pandas_frame["date_utc"] = pd.to_datetime(pandas_frame["date_utc"]).dt.strftime("%Y-%m-%d")
    pandas_frame[DERIVED_DATE_COLUMN] = pd.to_datetime(pandas_frame["timestamp_utc"]).dt.strftime(
        "%Y-%m-%d"
    )
    pandas_frame[DERIVED_HOUR_COLUMN] = pd.to_datetime(pandas_frame["timestamp_utc"]).dt.floor("h")
    return pandas_frame


def build_gold_suite(
    context: EphemeralDataContext, expected_start: str, expected_end: str
) -> ExpectationSuite:
    days = (date.fromisoformat(expected_end) - date.fromisoformat(expected_start)).days + 1
    if days < 1:
        raise ValueError("Periode attendue invalide")
    suite = context.suites.add(gx.ExpectationSuite(name="gold_energy_features_hourly"))

    suite.add_expectation(
        ExpectTableColumnsToMatchSet(column_set=REQUIRED_COLUMNS, exact_match=False)
    )
    suite.add_expectation(ExpectTableRowCountToEqual(value=days * 24))
    suite.add_expectation(ExpectColumnValuesToBeUnique(column="timestamp_utc"))
    suite.add_expectation(
        ExpectColumnMinToBeBetween(
            column="date_utc",
            min_value=expected_start,
            max_value=expected_start,
        )
    )
    suite.add_expectation(
        ExpectColumnMaxToBeBetween(
            column="date_utc",
            min_value=expected_end,
            max_value=expected_end,
        )
    )
    suite.add_expectation(
        ExpectColumnPairValuesToBeEqual(
            column_A="date_utc",
            column_B=DERIVED_DATE_COLUMN,
            ignore_row_if="either_value_is_missing",
        )
    )
    suite.add_expectation(
        ExpectColumnPairValuesToBeEqual(
            column_A="timestamp_utc",
            column_B=DERIVED_HOUR_COLUMN,
            ignore_row_if="either_value_is_missing",
        )
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="consumption_mw",
            min_value=0,
            strict_min=True,
        )
    )
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="temperature_c", min_value=-30.0, max_value=50.0)
    )
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="hour", min_value=0, max_value=23))
    suite.add_expectation(ExpectColumnValuesToBeBetween(column="month", min_value=1, max_value=12))
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(column="weekday_iso", min_value=1, max_value=7)
    )

    for column in REQUIRED_COLUMNS:
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=column))

    return suite


def validate_gold_with_gx(
    gold_dir: Path, expected_start: str | None = None, expected_end: str | None = None
) -> bool:
    if (expected_start is None) != (expected_end is None):
        raise ValueError("Fournir ensemble --expected-start et --expected-end")
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas(name="pandas_runtime")
    asset = datasource.add_dataframe_asset(name=GOLD_TABLE)
    batch_definition = asset.add_batch_definition_whole_dataframe("gold_batch")

    spark = build_spark("ADP-GX-Validate-Gold")
    try:
        spark.sparkContext.setLogLevel("ERROR")
        pandas_frame = read_gold_as_pandas(spark, gold_dir)
    finally:
        spark.stop()

    if pandas_frame.empty:
        raise ValueError("Table Gold vide")
    if expected_start is None:
        expected_start = pandas_frame["date_utc"].min()
        expected_end = pandas_frame["date_utc"].max()
    suite = build_gold_suite(context, expected_start, expected_end)
    pandas_frame = pandas_frame.loc[
        pandas_frame["date_utc"].between(expected_start, expected_end)
    ].copy()

    batch = batch_definition.get_batch(batch_parameters={"dataframe": pandas_frame})
    result = batch.validate(suite)
    print_gx_results(result.results)
    return bool(result.success)


def print_gx_results(results: list[object]) -> None:
    for item in results:
        expectation = item.expectation_config.type
        kwargs = item.expectation_config.kwargs
        column = kwargs.get("column") or kwargs.get("column_A") or "table"
        status = "OK" if item.success else "FAIL"
        details = item.result
        print(f"[{status}] {expectation} - {column} - {details}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Valide Gold avec Great Expectations")
    parser.add_argument(
        "--gold-dir",
        type=Path,
        default=Path.home() / "ia-decision-platform-data" / "gold",
        help="Dossier contenant les tables Gold Delta",
    )
    parser.add_argument("--expected-start", type=date.fromisoformat)
    parser.add_argument("--expected-end", type=date.fromisoformat)
    args = parser.parse_args()

    success = validate_gold_with_gx(
        args.gold_dir,
        str(args.expected_start) if args.expected_start else None,
        str(args.expected_end) if args.expected_end else None,
    )
    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
