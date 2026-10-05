# AI Decision Platform

AI Decision Platform is an end-to-end data engineering project built around electricity consumption, weather and calendar data. The goal is to reproduce a realistic company-style data pipeline: ingest raw data, transform it into analytical layers, validate data quality, orchestrate the workflow and publish a clean business table for downstream analytics or machine learning.

The data engineering foundation is implemented, and the first data science cycle is complete: exploratory analysis, forecasting baselines, model comparison, error analysis, hyperparameter tuning and final model export.

## Project goals

The project is designed to practice the core tools and patterns used in modern data teams:

- API data ingestion in batch mode
- Bronze, Silver and Gold data layers
- Spark transformations with PySpark
- Delta Lake tables and partitioned storage
- Data quality checks with Great Expectations
- PostgreSQL as a serving database
- Kafka near real-time weather ingestion
- Apache Airflow orchestration
- Docker-based local services
- Automated Python quality checks
- One-hour-ahead electricity consumption forecasting

## Current pipeline

```mermaid
flowchart LR
    A["Batch API ingestion"] --> B["Bronze raw files"]
    B --> C["PySpark Bronze to Silver"]
    C --> D["Silver Delta tables"]
    D --> E["Silver validation"]
    E --> F["PySpark Silver to Gold"]
    F --> G["Gold feature table"]
    G --> H["Great Expectations validation"]
    H --> I["PostgreSQL serving table"]
    J["Apache Airflow"] --> C
    J --> E
    J --> F
    J --> H
    J --> I
```

The Airflow DAG runs the pipeline in this order:

```text
sync_project_dependencies
-> bronze_to_silver
-> validate_silver
-> silver_to_gold
-> validate_gold_gx
-> load_postgres
run_dbt_models
```

## Data layers

### Bronze

Bronze stores raw batch ingestion runs from external APIs. Each run has a manifest and keeps the data close to the source format.

### Silver

Silver stores cleaned and typed Delta tables:

- `rte_consumption`
- `weather_hourly`
- `calendar_daily`

This layer normalizes column names, casts types, removes duplicates and adds technical metadata such as run lineage.

### Gold

Gold contains the business-ready hourly feature table:

- `energy_features_hourly`

It combines electricity consumption, weather and calendar features into one table that can be used by analytics, dashboards, APIs or future ML models.

## Tech stack

| Area | Tools |
|---|---|
| Language | Python 3.12 |
| Dependency management | uv |
| Batch ingestion | Python, Polars |
| Distributed processing | PySpark |
| Lakehouse storage | Delta Lake |
| Data quality | Great Expectations |
| Serving database | PostgreSQL |
| SQL modeling | dbt |
| Exploratory analysis | JupyterLab, pandas, Matplotlib, Seaborn |
| Machine learning | scikit-learn, XGBoost, joblib |
| Streaming | Kafka, Open-Meteo live weather API |
| Orchestration | Apache Airflow |
| Local infrastructure | Docker Compose |
| Quality checks | Ruff, Pytest, pre-commit |
| CI | GitHub Actions |

## Repository structure

```text
src/ai_decision_platform/
  ingestion/          Batch data ingestion
  processing/         Bronze -> Silver and Silver -> Gold transformations
  quality/            Great Expectations validation
  serving/            PostgreSQL loading
  analysis/           Automated exploratory data analysis
  training/           Baselines, model training, tuning and final model export

airflow/dags/         Airflow DAG definition
docker/airflow/       Custom Airflow Docker image
sql/                  SQL validation queries
scripts/              Local setup scripts
tests/                Unit tests
notebooks/            Data science framing and exploratory analysis
```

Generated data, local environments, logs and secrets are intentionally excluded from Git.

## Local setup

### Python setup

```bash
uv sync --frozen --extra dev
uv run --frozen ruff check .
uv run --frozen pytest
```

### Run batch ingestion

```bash
uv run --frozen python -m ai_decision_platform.ingestion.batch \
  --start 2024-01-01 \
  --end 2024-01-07
```

### Run Spark transformations manually from WSL

```bash
export UV_PROJECT_ENVIRONMENT="$HOME/.venvs/ia-decision-platform"
export UV_LINK_MODE=copy

uv run --frozen --extra spark python -m ai_decision_platform.processing.bronze_to_silver \
  --silver-dir "$HOME/ia-decision-platform-data/silver"

uv run --frozen --extra spark python -m ai_decision_platform.processing.validate_silver \
  --silver-dir "$HOME/ia-decision-platform-data/silver"

uv run --frozen --extra spark python -m ai_decision_platform.processing.silver_to_gold \
  --silver-dir "$HOME/ia-decision-platform-data/silver" \
  --gold-dir "$HOME/ia-decision-platform-data/gold"
```

### Validate Gold with Great Expectations

```bash
uv run --frozen --extra spark --extra quality python -m ai_decision_platform.quality.validate_gold_gx \
  --gold-dir "$HOME/ia-decision-platform-data/gold"
```

### Start PostgreSQL and load the serving table

```bash
docker compose up -d postgres

uv run --frozen --extra spark --extra serving python -m ai_decision_platform.serving.load_postgres \
  --gold-dir "$HOME/ia-decision-platform-data/gold"
```

Check the PostgreSQL table:

```bash
docker compose exec -T postgres psql -U adp -d adp < sql/check_energy_features_hourly.sql
```

## Run dbt manually

After PostgreSQL has been loaded, run dbt models and tests:

```bash
uv run --frozen --extra analytics dbt run --project-dir dbt --profiles-dir dbt
uv run --frozen --extra analytics dbt test --project-dir dbt --profiles-dir dbt
```

dbt reads the PostgreSQL source table:

```text
public.energy_features_hourly_postgres
```

and creates the analytics marts:

```text
analytics.mart_energy_hourly
analytics.mart_energy_daily
analytics.mart_energy_peak_analysis
```

## Run Kafka weather streaming manually

Start Kafka and PostgreSQL:

```bash
docker compose up -d postgres kafka
```

Publish live weather events from Open-Meteo to Kafka:

```bash
uv run --frozen --extra streaming python -m ai_decision_platform.streaming.weather_producer \
  --topic weather.current \
  --bootstrap-servers localhost:29092 \
  --interval-seconds 10 \
  --max-events 5
```

Consume Kafka events into PostgreSQL:

```bash
uv run --frozen --extra streaming python -m ai_decision_platform.streaming.weather_consumer \
  --topic weather.current \
  --bootstrap-servers localhost:29092 \
  --database-url postgresql://adp:adp@localhost:5432/adp \
  --max-messages 5
```

Check the ingested weather stream:

```bash
docker compose exec -T postgres psql -U adp -d adp < sql/check_weather_stream.sql
```

## Run the full pipeline with Airflow

Build and start the local Airflow stack:

```bash
docker compose build airflow-init
docker compose up airflow-init
docker compose up -d postgres airflow-webserver airflow-scheduler
```

Open Airflow:

```text
http://localhost:8080
username: airflow
password: airflow
```

Trigger the DAG:

```bash
docker compose exec airflow-webserver airflow dags trigger energy_batch_pipeline
```

A successful run produces the Gold Delta table, loads it into PostgreSQL, then builds and tests the dbt mart:

```text
public.energy_features_hourly_postgres
analytics.mart_energy_hourly
analytics.mart_energy_daily
analytics.mart_energy_peak_analysis
```

## Validation status

The current pipeline has been executed successfully through Airflow with all tasks green:

```text
sync_project_dependencies
bronze_to_silver
validate_silver
silver_to_gold
validate_gold_gx
load_postgres
run_dbt_models
```

Local checks run before publication:

```text
ruff check: passed
pytest: 17 passed
```

## Data science: forecasting workflow

The data science layer uses `analytics.mart_energy_hourly` from PostgreSQL to build a one-hour-ahead electricity consumption forecasting model. The workflow is intentionally split into clear scripts so each step can be reviewed independently:

```text
src/ai_decision_platform/analysis/eda_energy.py
src/ai_decision_platform/training/baseline_forecast.py
src/ai_decision_platform/training/train_regression.py
src/ai_decision_platform/training/error_analysis.py
src/ai_decision_platform/training/tune_hist_gradient_boosting.py
src/ai_decision_platform/training/train_final_model.py
```

The final selected model is a tuned `HistGradientBoostingRegressor` trained on the chronological train + validation period and evaluated on the final untouched test period. Current final test metrics are:

```text
MAE: 500.9 MW
RMSE: 701.9 MW
MAPE: 0.95%
```

Run the full data science sequence from WSL after the PostgreSQL mart has been built:

```bash
export PATH="$HOME/.local/bin:$PATH"
export UV_PROJECT_ENVIRONMENT="$HOME/.venvs/ia-decision-datascience"
export UV_LINK_MODE=copy
export ADP_POSTGRES_URL='postgresql+psycopg://adp:adp@localhost:5432/adp'
docker compose up -d postgres

uv run --frozen --extra datascience python -m ai_decision_platform.analysis.eda_energy
uv run --frozen --extra datascience python -m ai_decision_platform.training.baseline_forecast
uv run --frozen --extra datascience python -m ai_decision_platform.training.train_regression
uv run --frozen --extra datascience python -m ai_decision_platform.training.error_analysis
uv run --frozen --extra datascience python -m ai_decision_platform.training.tune_hist_gradient_boosting
uv run --frozen --extra datascience python -m ai_decision_platform.training.train_final_model
```

The final model artifact is generated locally under `models/energy_forecast/` and is intentionally not committed to Git. The next project step is experiment tracking and model management with MLflow.

A notebook is also available for interactive exploration:

```bash
uv run --frozen --extra datascience jupyter lab --notebook-dir notebooks
```

Open `notebooks/01_energy_eda.ipynb`. Notebook outputs are intentionally kept lightweight for repository readability.

## MLOps: MLflow tracking and registry

The first MLOps step logs the final forecasting model to MLflow using a dedicated PostgreSQL database named `mlflow` on the local PostgreSQL server. It records model parameters, final test metrics, documentation artifacts and the scikit-learn model itself. The model is registered as `energy_forecast_model` in the MLflow Model Registry.

```bash
export PATH="$HOME/.local/bin:$PATH"
export UV_PROJECT_ENVIRONMENT="$HOME/.venvs/ia-decision-mlops"
export UV_LINK_MODE=copy

docker compose up -d postgres
docker compose exec -T postgres createdb -U adp mlflow || true

uv run --frozen --extra mlops --extra datascience python -m ai_decision_platform.mlops.log_final_model \
  --model-dir models/energy_forecast \
  --experiment-name energy_forecasting \
  --run-name final_hist_gradient_boosting \
  --registered-model-name energy_forecast_model
```

Open the local MLflow UI:

```bash
uv run --frozen --extra mlops mlflow ui \
  --backend-store-uri postgresql+psycopg://adp:adp@localhost:5432/mlflow \
  --default-artifact-root .runtime/mlflow/artifacts \
  --host 127.0.0.1 \
  --port 5000
```

Then visit `http://127.0.0.1:5000`. Local MLflow runtime files are intentionally excluded from Git.

## Next steps

Planned extensions:

- add experiment tracking and model management with MLflow
- expose predictions through an API
- build a dashboard for business users
- add monitoring and alerting

