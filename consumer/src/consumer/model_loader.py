"""Load a fraud model from MLflow and run predictions."""

import os

import mlflow.sklearn
import numpy as np


class ModelLoader:
    """Load the configured model and expose a small prediction API."""

    def __init__(self) -> None:
        model_uri = os.environ.get("MODEL_URI")
        if not model_uri:
            raise ValueError("MODEL_URI environment variable is required")

        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)

        self._model = mlflow.sklearn.load_model(model_uri)

    def predict(self, features: list[float]) -> tuple[bool, float]:
        x = np.array([features])
        prob_fraud = float(self._model.predict_proba(x)[0, 1])
        label = prob_fraud >= 0.5
        confidence = prob_fraud if label else 1.0 - prob_fraud
        return label, round(confidence, 4)
