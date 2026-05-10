import numpy as np
import pytest


def test_model_loader_requires_model_uri_env(monkeypatch):
    monkeypatch.delenv("MODEL_URI", raising=False)
    from consumer.model_loader import ModelLoader

    with pytest.raises(ValueError, match="MODEL_URI"):
        ModelLoader()


def test_model_loader_loads_and_predicts(tmp_path, monkeypatch):
    """Train a tiny local model and verify loader can predict on it."""
    import mlflow
    import mlflow.sklearn
    from sklearn.ensemble import RandomForestClassifier

    tracking_uri = f"sqlite:///{tmp_path}/mlflow.db"
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("test-loader")

    X = np.array(
        [
            [100.0, 25, 0, 14, 0, 0.5, 0, 1, 0, 2],
            [5000.0, 45, 1, 2, 1, 3.1, 1, 3, 2, 5],
        ]
    )
    y = np.array([0, 1])

    with mlflow.start_run() as run:
        model = RandomForestClassifier(n_estimators=2, random_state=0)
        model.fit(X, y)
        mlflow.sklearn.log_model(model, "model")
        run_id = run.info.run_id

    monkeypatch.setenv("MODEL_URI", f"runs:/{run_id}/model")
    monkeypatch.setenv("MLFLOW_TRACKING_URI", tracking_uri)

    from importlib import reload

    import consumer.model_loader as ml_mod

    reload(ml_mod)

    loader = ml_mod.ModelLoader()
    transaction_features = [100.0, 25, 0, 14, 0, 0.5, 0, 1, 0, 2]
    label, confidence = loader.predict(transaction_features)

    assert label in (True, False)
    assert 0.0 <= confidence <= 1.0
