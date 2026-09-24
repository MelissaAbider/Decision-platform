"""Transformation Bronze vers Silver avec Spark et Delta Lake."""

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from tempfile import gettempdir

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ai_decision_platform.config import load_settings


@dataclass(frozen=True)
class SilverTables:
    rte_consumption: Path
    weather_hourly: Path
    calendar_daily: Path


def latest_successful_bronze_run(data_dir: Path) -> Path:
    """Retourne le dernier run Bronze dont le manifeste est en succes."""
    runs_dir = data_dir / "bronze" / "runs"
    candidates = sorted(runs_dir.glob("*"), reverse=True)
    for run_dir in candidates:
        manifest_path = run_dir / "manifest.json"
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("status") == "success":
            return run_dir
    raise FileNotFoundError(f"Aucun run Bronze success sous {runs_dir}")


def build_spark(app_name: str = "ADP-Bronze-To-Silver") -> SparkSession:
    """Cree une session Spark locale avec Delta Lake active."""
    runtime_dir = Path(gettempdir()) / "ai-decision-platform-spark"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.dir", str(runtime_dir / "local"))
        .config("spark.jars.ivy", str(runtime_dir / "ivy"))
        .config("spark.sql.warehouse.dir", str(runtime_dir / "warehouse"))
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_bronze_source(spark: SparkSession, run_dir: Path, source: str) -> DataFrame:
    return spark.read.parquet(str(run_dir / source / "date=*" / "data.parquet"))


def clean_rte_consumption(frame: DataFrame, run_id: str) -> DataFrame:
    typed_columns = frame.select(
        F.to_timestamp("date_heure").alias("timestamp_utc"),
        F.col("consommation").cast("long").alias("consumption_mw"),
    )

    with_date = typed_columns.withColumn("date_utc", F.to_date("timestamp_utc"))

    with_status = with_date.withColumn(
        "measurement_status",
        F.when(F.col("consumption_mw").isNull(), F.lit("missing_measurement")).otherwise(
            F.lit("observed")
        ),
    )

    with_lineage = with_status.withColumn("bronze_run_id", F.lit(run_id))

    return with_lineage.dropDuplicates(["timestamp_utc"])


def clean_weather_hourly(frame: DataFrame, run_id: str) -> DataFrame:
    typed_columns = frame.select(
        F.to_timestamp("timestamp_utc", "yyyy-MM-dd'T'HH:mmX").alias("timestamp_utc"),
        F.col("temperature_2m_c").cast("double").alias("temperature_2m_c"),
        F.col("requested_latitude").cast("double").alias("requested_latitude"),
        F.col("requested_longitude").cast("double").alias("requested_longitude"),
    )

    with_date = typed_columns.withColumn("date_utc", F.to_date("timestamp_utc"))
    with_lineage = with_date.withColumn("bronze_run_id", F.lit(run_id))

    deduplication_keys = ["timestamp_utc", "requested_latitude", "requested_longitude"]

    return with_lineage.dropDuplicates(deduplication_keys)


def clean_calendar_daily(frame: DataFrame, run_id: str) -> DataFrame:
    typed_columns = frame.select(
        F.col("date").cast("date").alias("date"),
        F.col("weekday_iso").cast("int").alias("weekday_iso"),
        F.col("is_weekend").cast("boolean").alias("is_weekend"),
        F.col("is_public_holiday").cast("boolean").alias("is_public_holiday"),
        F.col("holiday_name").cast("string").alias("holiday_name"),
    )

    with_lineage = typed_columns.withColumn("bronze_run_id", F.lit(run_id))

    return with_lineage.dropDuplicates(["date"])


def write_delta(frame: DataFrame, path: Path, partition_by: str) -> None:
    print(f"Ecriture Delta: {path}")
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(partition_by)
        .save(str(path))
    )


def transform_bronze_to_silver(
    data_dir: Path,
    run_dir: Path | None = None,
    silver_dir: Path | None = None,
) -> SilverTables:
    selected_run = run_dir or latest_successful_bronze_run(data_dir)
    run_id = selected_run.name
    silver_dir = silver_dir or data_dir / "silver"
    tables = SilverTables(
        rte_consumption=silver_dir / "rte_consumption",
        weather_hourly=silver_dir / "weather_hourly",
        calendar_daily=silver_dir / "calendar_daily",
    )
    spark = build_spark()
    try:
        spark.sparkContext.setLogLevel("WARN")
        print(f"Run Bronze utilise: {selected_run}")
        write_delta(
            clean_rte_consumption(read_bronze_source(spark, selected_run, "rte"), run_id),
            tables.rte_consumption,
            "date_utc",
        )
        write_delta(
            clean_weather_hourly(read_bronze_source(spark, selected_run, "weather"), run_id),
            tables.weather_hourly,
            "date_utc",
        )
        write_delta(
            clean_calendar_daily(read_bronze_source(spark, selected_run, "calendar"), run_id),
            tables.calendar_daily,
            "date",
        )
    finally:
        spark.stop()
    return tables


def main() -> None:
    parser = argparse.ArgumentParser(description="Transforme Bronze Parquet en Silver Delta")
    parser.add_argument("--run-dir", type=Path, help="Run Bronze precis a transformer")
    parser.add_argument(
        "--silver-dir",
        type=Path,
        help="Dossier de sortie Silver. Utile sous WSL pour ecrire Delta hors /mnt/c.",
    )
    args = parser.parse_args()
    tables = transform_bronze_to_silver(load_settings().data_dir, args.run_dir, args.silver_dir)
    print("Silver RTE:", tables.rte_consumption)
    print("Silver meteo:", tables.weather_hourly)
    print("Silver calendrier:", tables.calendar_daily)


if __name__ == "__main__":
    main()
