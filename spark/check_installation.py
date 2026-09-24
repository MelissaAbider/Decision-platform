"""Vérification de l'installation uniquement : aucune transformation Silver."""

import os
from pathlib import Path

from pyspark.sql import SparkSession

root = Path(__file__).resolve().parents[1]
builder = (
    SparkSession.builder.master("local[2]")
    .appName("ADP-Installation-Check")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars.ivy", str(root / ".runtime/ivy"))
    .config("spark.sql.warehouse.dir", (root / ".runtime/warehouse").as_uri())
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.session.timeZone", "UTC")
)
# En mode local, les JAR déjà téléchargés sont chargés directement.
# Cela évite la redistribution Hadoop des dépendances sur Windows.
jars = sorted((root / ".runtime/ivy/jars").glob("*.jar"))
if len(jars) < 3:
    raise RuntimeError("Dépendances JVM Delta absentes dans .runtime/ivy/jars")
builder = builder.config("spark.driver.extraClassPath", os.pathsep.join(map(str, jars)))
spark = builder.getOrCreate()
try:
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark:", spark.version)
    spark._jvm.java.lang.Class.forName(
        "io.delta.tables.DeltaTable",
        True,
        spark._jvm.java.lang.Thread.currentThread().getContextClassLoader(),
    )
    print("Delta JVM: loaded")
    paths = sorted((root / "data/bronze/runs").glob("*/rte/date=*/data.parquet"))
    if paths:
        df = spark.read.parquet(str(paths[0]))
        df.select("date_heure", "consommation").printSchema()
        print("Bronze rows:", df.count())
    else:
        assert spark.range(5).count() == 5
finally:
    spark.stop()
