"""Register the best MLflow run to the Model Registry."""

import argparse

import mlflow
from mlflow.tracking import MlflowClient


MODEL_NAME = "paysense-fraud-detector"


def register_best_model(experiment_name: str, metric: str) -> str:
    """Register the best run by metric and promote it to Production."""
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise ValueError(f"Experiment '{experiment_name}' not found")

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=[f"metrics.{metric} DESC"],
        max_results=1,
    )
    if not runs:
        raise ValueError(f"No runs found in experiment '{experiment_name}'")

    best_run = runs[0]
    if metric not in best_run.data.metrics:
        raise ValueError(
            f"Metric '{metric}' not found on best run candidate {best_run.info.run_id}"
        )

    run_id = best_run.info.run_id
    artifact_uri = f"runs:/{run_id}/model"

    print(f"Best run: {run_id} | {metric}={best_run.data.metrics[metric]:.4f}")
    print(f"Registering as '{MODEL_NAME}'...")

    model_version = mlflow.register_model(artifact_uri, MODEL_NAME)
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=model_version.version,
        stage="Production",
    )

    model_uri = f"models:/{MODEL_NAME}/Production"
    print(f"Registered version {model_version.version} -> Production")
    print(f"Load URI: {model_uri}")
    return model_uri


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", default="paysense-fraud-detector")
    parser.add_argument("--metric", default="f1")
    args = parser.parse_args()

    register_best_model(args.experiment, args.metric)


if __name__ == "__main__":
    main()
