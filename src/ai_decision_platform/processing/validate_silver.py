"""Validation technique des tables Silver Delta.

Cette étape contrôle que les tables Silver existent, que leurs clés métier sont
uniques et que les colonnes nécessaires aux étapes suivantes sont bien remplies.
Elle sert de garde-fou entre le nettoyage Silver et la création des features Gold.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    details: str


def read_silver_tables(
    spark: SparkSession,
    silver_dir: Path,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    rte = spark.read.format("delta").load(str(silver_dir / "rte_consumption"))
    weather = spark.read.format("delta").load(str(silver_dir / "weather_hourly"))
    calendar = spark.read.format("delta").load(str(silver_dir / "calendar_daily"))
    return rte, weather, calendar


def check_min_rows(frame: DataFrame, table_name: str, minimum: int) -> CheckResult:
    row_count = frame.count()
    return CheckResult(
        name=f"{table_name}: nombre de lignes",
        passed=row_count >= minimum,
        details=f"{row_count} lignes trouvees, minimum attendu {minimum}",
    )


def check_no_duplicate_key(frame: DataFrame, table_name: str, keys: list[str]) -> CheckResult:
    duplicate_count = frame.groupBy(*keys).count().filter("count > 1").count()
    return CheckResult(
        name=f"{table_name}: unicite {', '.join(keys)}",
        passed=duplicate_count == 0,
        details=f"{duplicate_count} cle(s) en doublon",
    )


def check_no_null(frame: DataFrame, table_name: str, column: str) -> CheckResult:
    from pyspark.sql import functions as F

    null_count = frame.filter(F.col(column).isNull()).count()
    return CheckResult(
        name=f"{table_name}: {column} non null",
        passed=null_count == 0,
        details=f"{null_count} valeur(s) nulles",
    )


def check_allowed_values(
    frame: DataFrame,
    table_name: str,
    column: str,
    allowed_values: set[str],
) -> CheckResult:
    from pyspark.sql import functions as F

    invalid_count = frame.filter(~F.col(column).isin(sorted(allowed_values))).count()
    return CheckResult(
        name=f"{table_name}: valeurs autorisees pour {column}",
        passed=invalid_count == 0,
        details=f"{invalid_count} valeur(s) hors {sorted(allowed_values)}",
    )


def check_date_range(
    frame: DataFrame,
    table_name: str,
    date_column: str,
    expected_start: str,
    expected_end: str,
) -> CheckResult:
    from pyspark.sql import functions as F

    bounds = frame.select(
        F.min(date_column).cast("string").alias("min_date"),
        F.max(date_column).cast("string").alias("max_date"),
    ).collect()[0]

    min_date = bounds["min_date"]
    max_date = bounds["max_date"]

    return CheckResult(
        name=f"{table_name}: periode couverte",
        passed=min_date == expected_start and max_date == expected_end,
        details=(
            f"periode trouvee {min_date} -> {max_date}, attendue {expected_start} -> {expected_end}"
        ),
    )


def validate_silver(silver_dir: Path) -> list[CheckResult]:
    from ai_decision_platform.processing.bronze_to_silver import build_spark

    spark = build_spark("ADP-Validate-Silver")
    try:
        spark.sparkContext.setLogLevel("WARN")
        rte, weather, calendar = read_silver_tables(spark, silver_dir)

        return [
            check_min_rows(rte, "silver.rte_consumption", 1),
            check_no_duplicate_key(rte, "silver.rte_consumption", ["timestamp_utc"]),
            check_no_null(rte, "silver.rte_consumption", "timestamp_utc"),
            check_no_null(rte, "silver.rte_consumption", "date_utc"),
            check_allowed_values(
                rte,
                "silver.rte_consumption",
                "measurement_status",
                {"observed", "missing_measurement"},
            ),
            check_date_range(
                rte,
                "silver.rte_consumption",
                "date_utc",
                "2024-01-01",
                "2024-01-07",
            ),
            check_min_rows(weather, "silver.weather_hourly", 1),
            check_no_duplicate_key(
                weather,
                "silver.weather_hourly",
                ["timestamp_utc", "requested_latitude", "requested_longitude"],
            ),
            check_no_null(weather, "silver.weather_hourly", "timestamp_utc"),
            check_no_null(weather, "silver.weather_hourly", "temperature_2m_c"),
            check_date_range(
                weather,
                "silver.weather_hourly",
                "date_utc",
                "2024-01-01",
                "2024-01-07",
            ),
            check_min_rows(calendar, "silver.calendar_daily", 1),
            check_no_duplicate_key(calendar, "silver.calendar_daily", ["date"]),
            check_no_null(calendar, "silver.calendar_daily", "date"),
            check_date_range(
                calendar,
                "silver.calendar_daily",
                "date",
                "2024-01-01",
                "2024-01-07",
            ),
        ]
    finally:
        spark.stop()


def print_results(results: list[CheckResult]) -> None:
    for result in results:
        status = "OK" if result.passed else "FAILED"
        print(f"[{status}] {result.name} - {result.details}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Valide les tables Silver Delta")
    parser.add_argument(
        "--silver-dir",
        type=Path,
        default=Path.home() / "ia-decision-platform-data" / "silver",
        help="Dossier contenant les tables Silver Delta",
    )
    args = parser.parse_args()

    results = validate_silver(args.silver_dir)
    print_results(results)

    if not all(result.passed for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
