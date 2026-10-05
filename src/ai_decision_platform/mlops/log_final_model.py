"""Log the final forecasting model to MLflow Tracking."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import joblib
import mlflow.sklearn
from mlflow.tracking import MlflowClient

import mlflow

DEFAULT_EXPERIMENT_NAME = "energy_forecasting"
DEFAULT_RUN_NAME = "final_hist_gradient_boosting"
DEFAULT_MODEL_NAME = "energy_forecast_model"
DEFAULT_TRACKING_URI = "postgresql+psycopg://adp:adp@localhost:5432/mlflow"
DEFAULT_ARTIFACT_LOCATION = ".runtime/mlflow/artifacts"


def ensure_experiment(experiment_name: str, artifact_location: str) -> None:
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        Path(artifact_location).mkdir(parents=True, exist_ok=True)
        client.create_experiment(
            name=experiment_name,
            artifact_location=Path(artifact_location).resolve().as_uri(),
        )
    mlflow.set_experiment(experiment_name)


def load_metadata(model_dir: Path) -> dict[str, Any]:
    metadata_path = model_dir / "metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing metadata file: {metadata_path}")
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def require_model_files(model_dir: Path) -> tuple[Path, Path, Path]:
    model_path = model_dir / "energy_forecast_model.joblib"
    metadata_path = model_dir / "metadata.json"
    model_card_path = model_dir / "model_card.md"
    missing = [path for path in [model_path, metadata_path, model_card_path] if not path.exists()]
    if missing:
        missing_list = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing final model artifact(s): {missing_list}")
    return model_path, metadata_path, model_card_path


def log_params(metadata: dict[str, Any]) -> None:
    mlflow.log_param("model_name", metadata["model_name"])
    mlflow.log_param("target", metadata["target"])
    mlflow.log_param("numeric_features", ",".join(metadata["numeric_features"]))
    mlflow.log_param("categorical_features", ",".join(metadata["categorical_features"]))
    for name, value in metadata["final_model_params"].items():
        clean_name = name.replace("model__", "")
        mlflow.log_param(clean_name, value)


def log_metrics(metadata: dict[str, Any]) -> None:
    metrics = metadata["metrics"]
    mlflow.log_metric("test_rows", metrics["rows"])
    mlflow.log_metric("test_mae_mw", metrics["mae_mw"])
    mlflow.log_metric("test_rmse_mw", metrics["rmse_mw"])
    mlflow.log_metric("test_mape_percent", metrics["mape_percent"])


def log_final_model(
    model_dir: Path,
    tracking_uri: str,
    experiment_name: str,
    artifact_location: str,
    run_name: str,
    registered_model_name: str | None,
) -> str:
    model_path, metadata_path, model_card_path = require_model_files(model_dir)
    metadata = load_metadata(model_dir)
    model = joblib.load(model_path)

    Path(DEFAULT_ARTIFACT_LOCATION).mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(tracking_uri)
    ensure_experiment(experiment_name, artifact_location)

    with mlflow.start_run(run_name=run_name) as run:
        log_params(metadata)
        log_metrics(metadata)
        mlflow.set_tags(
            {
                "project": "ai-decision-platform",
                "stage": "final_model",
                "model_type": "forecasting",
                "framework": "scikit-learn",
            }
        )
        mlflow.log_artifact(str(metadata_path), artifact_path="metadata")
        mlflow.log_artifact(str(model_card_path), artifact_path="documentation")
        mlflow.log_artifact(str(model_path), artifact_path="joblib")
        mlflow.sklearn.log_model(
            sk_model=model,
            name="model",
            registered_model_name=registered_model_name,
            skops_trusted_types=[
                "sklearn.ensemble._hist_gradient_boosting.predictor.TreePredictor"
            ],
        )
        return run.info.run_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Log the final model to MLflow")
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=Path("models") / "energy_forecast",
        help="Directory containing energy_forecast_model.joblib, metadata.json and model_card.md",
    )
    parser.add_argument(
        "--tracking-uri",
        default=DEFAULT_TRACKING_URI,
        help="MLflow tracking URI. The default uses the local PostgreSQL mlflow database.",
    )
    parser.add_argument("--experiment-name", default=DEFAULT_EXPERIMENT_NAME)
    parser.add_argument(
        "--artifact-location",
        default=DEFAULT_ARTIFACT_LOCATION,
        help="Local or remote artifact location for MLflow artifacts",
    )
    parser.add_argument("--run-name", default=DEFAULT_RUN_NAME)
    parser.add_argument(
        "--registered-model-name",
        default=DEFAULT_MODEL_NAME,
        help="Registered model name. Use an empty string to skip registry registration.",
    )
    args = parser.parse_args()

    registered_model_name = args.registered_model_name or None
    run_id = log_final_model(
        model_dir=args.model_dir,
        tracking_uri=args.tracking_uri,
        experiment_name=args.experiment_name,
        artifact_location=args.artifact_location,
        run_name=args.run_name,
        registered_model_name=registered_model_name,
    )
    print(f"Logged final model to MLflow run_id={run_id}")


if __name__ == "__main__":
    main()
