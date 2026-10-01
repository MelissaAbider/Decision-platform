"""DAG Airflow du pipeline batch energie."""

from __future__ import annotations

import pendulum
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from airflow import DAG

PROJECT_DIR = "/opt/airflow/project"
DATA_ROOT = "/opt/airflow/data"
PYTHON_BIN = "/opt/airflow/.venvs/ia-decision-platform/bin/python"
DBT_BIN = "/opt/airflow/.venvs/ia-decision-platform/bin/dbt"
COMMON_ENV = {
    "ADP_DATA_DIR": "data",
    "ADP_POSTGRES_URL": "postgresql+psycopg://adp:adp@postgres:5432/adp",
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "PYTHONPATH": "/opt/airflow/project/src",
    "UV_PROJECT_ENVIRONMENT": "/opt/airflow/.venvs/ia-decision-platform",
    "UV_LINK_MODE": "copy",
    "SPARK_LOCAL_DIRS": "/tmp/ai-decision-platform-spark",
    "DBT_POSTGRES_HOST": "postgres",
    "DBT_POSTGRES_PORT": "5432",
    "DBT_POSTGRES_USER": "adp",
    "DBT_POSTGRES_PASSWORD": "adp",
    "DBT_POSTGRES_DB": "adp",
    "DBT_POSTGRES_SCHEMA": "analytics",
    "BACKFILL_START": "{{ params.start_date }}",
    "BACKFILL_END": "{{ params.end_date }}",
}


def project_command(command: str) -> str:
    return f"cd {PROJECT_DIR} && {command}"


with DAG(
    dag_id="energy_batch_pipeline",
    description="Ingestion historique -> Bronze -> Silver -> Gold -> PostgreSQL -> dbt",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": Param("2024-01-01", type="string", format="date"),
        "end_date": Param("2024-01-07", type="string", format="date"),
    },
    default_args={"retries": 1},
    tags=["ai-decision-platform", "data-engineering"],
) as dag:
    sync_project = BashOperator(
        task_id="sync_project_dependencies",
        bash_command=project_command(
            f"{PYTHON_BIN} -c "
            "'import pyspark, delta, great_expectations, pandas, sqlalchemy, psycopg, dbt; "
            'print("Dependencies ready")\''
        ),
        env=COMMON_ENV,
    )

    ingest_history = BashOperator(
        task_id="ingest_history",
        bash_command=project_command(
            f"{PYTHON_BIN} -m ai_decision_platform.ingestion.backfill "
            '--start "$BACKFILL_START" --end "$BACKFILL_END"'
        ),
        env=COMMON_ENV,
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=project_command(
            f"{PYTHON_BIN} -m ai_decision_platform.processing.bronze_to_silver "
            f"--silver-dir {DATA_ROOT}/silver --all-runs"
        ),
        env=COMMON_ENV,
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=project_command(
            f"{PYTHON_BIN} -m ai_decision_platform.processing.validate_silver "
            f"--silver-dir {DATA_ROOT}/silver "
            '--expected-start "$BACKFILL_START" --expected-end "$BACKFILL_END"'
        ),
        env=COMMON_ENV,
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=project_command(
            f"{PYTHON_BIN} -m ai_decision_platform.processing.silver_to_gold "
            f"--silver-dir {DATA_ROOT}/silver "
            f"--gold-dir {DATA_ROOT}/gold"
        ),
        env=COMMON_ENV,
    )

    validate_gold = BashOperator(
        task_id="validate_gold_gx",
        bash_command=project_command(
            f"{PYTHON_BIN} -m ai_decision_platform.quality.validate_gold_gx "
            f"--gold-dir {DATA_ROOT}/gold "
            '--expected-start "$BACKFILL_START" --expected-end "$BACKFILL_END"'
        ),
        env=COMMON_ENV,
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=project_command(
            f"{PYTHON_BIN} -m ai_decision_platform.serving.load_postgres "
            f"--gold-dir {DATA_ROOT}/gold "
            '--database-url "$ADP_POSTGRES_URL"'
        ),
        env=COMMON_ENV,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command=project_command(
            f"{DBT_BIN} run --project-dir dbt --profiles-dir dbt && "
            f"{DBT_BIN} test --project-dir dbt --profiles-dir dbt"
        ),
        env=COMMON_ENV,
    )

    (
        sync_project
        >> ingest_history
        >> bronze_to_silver
        >> validate_silver
        >> silver_to_gold
        >> validate_gold
        >> load_postgres
        >> run_dbt
    )
