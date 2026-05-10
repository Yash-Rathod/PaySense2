"""Train XGBoost fraud classifier and log to MLflow."""
import json
import argparse
from pathlib import Path

import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score
from xgboost import XGBClassifier
from imblearn.over_sampling import SMOTE


CATEGORICAL_COLS = ["currency", "merchant_category", "card_type", "merchant_country"]
FEATURE_COLS = [
    "amount", "customer_age", "is_international",
    "hour_of_day", "is_weekend", "amount_zscore",
    "currency_enc", "merchant_category_enc", "card_type_enc", "merchant_country_enc",
]


def load_data(path: str) -> pd.DataFrame:
    records = []
    with open(path) as f:
        for line in f:
            records.append(json.loads(line))
    return pd.DataFrame(records)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour_of_day"] = df["timestamp"].dt.hour
    df["is_weekend"] = df["timestamp"].dt.dayofweek.isin([5, 6]).astype(int)
    df["amount_zscore"] = (df["amount"] - df["amount"].mean()) / df["amount"].std()
    df["is_international"] = df["is_international"].astype(int)

    for col in CATEGORICAL_COLS:
        le = LabelEncoder()
        df[f"{col}_enc"] = le.fit_transform(df[col].astype(str))

    # Log encoding maps so consumer can reproduce identical encodings at inference time
    _amount_mean = float(df["amount"].mean())
    _amount_std = float(df["amount"].std())

    return df, _amount_mean, _amount_std


def train(data_path: str, params: dict) -> None:
    mlflow.set_experiment("paysense-fraud-detector")

    df = load_data(data_path)
    df, amount_mean, amount_std = engineer_features(df)

    X = df[FEATURE_COLS].values
    y = df["is_fraud"].astype(int).values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    smote = SMOTE(random_state=42)
    X_train_res, y_train_res = smote.fit_resample(X_train, y_train)

    with mlflow.start_run():
        mlflow.log_params(params)
        mlflow.log_param("n_train", len(X_train_res))
        mlflow.log_param("n_test", len(X_test))
        mlflow.log_param("features", FEATURE_COLS)

        model = XGBClassifier(
            n_estimators=params.get("n_estimators", 100),
            max_depth=params.get("max_depth", 5),
            learning_rate=params.get("learning_rate", 0.1),
            scale_pos_weight=1,
            eval_metric="logloss",
            random_state=42,
        )
        model.fit(X_train_res, y_train_res)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        metrics = {
            "precision": precision_score(y_test, y_pred),
            "recall": recall_score(y_test, y_pred),
            "f1": f1_score(y_test, y_pred),
            "auc_roc": roc_auc_score(y_test, y_prob),
        }
        mlflow.log_metrics(metrics)
        # Log normalization stats so consumer can reproduce identical amount_zscore at inference
        mlflow.log_params({"amount_mean": amount_mean, "amount_std": amount_std})

        feature_importance = dict(zip(FEATURE_COLS, model.feature_importances_.tolist()))
        mlflow.log_dict(feature_importance, "feature_importance.json")

        # Log LabelEncoder category order so consumer uses identical integer mapping
        # LabelEncoder sorts alphabetically — document canonical orders here
        # currency: CAD=0, EUR=1, GBP=2, USD=3
        # merchant_category: crypto=0, e-commerce=1, electronics=2, gambling=3, grocery=4, healthcare=5, luxury=6, restaurant=7, retail=8, transport=9, wire-transfer=10
        # card_type: Amex=0, Discover=1, Mastercard=2, Visa=3
        # merchant_country: AU=0, BR=1, CA=2, DE=3, GB=4, NG=5, PK=6, RO=7, UA=8, US=9
        mlflow.log_dict({
            "currency": {"CAD": 0, "EUR": 1, "GBP": 2, "USD": 3},
            "merchant_category": {"crypto": 0, "e-commerce": 1, "electronics": 2, "gambling": 3, "grocery": 4, "healthcare": 5, "luxury": 6, "restaurant": 7, "retail": 8, "transport": 9, "wire-transfer": 10},
            "card_type": {"Amex": 0, "Discover": 1, "Mastercard": 2, "Visa": 3},
            "merchant_country": {"AU": 0, "BR": 1, "CA": 2, "DE": 3, "GB": 4, "NG": 5, "PK": 6, "RO": 7, "UA": 8, "US": 9},
        }, "encoding_maps.json")

        mlflow.sklearn.log_model(model, "model")

        print(f"Metrics: {metrics}")
        print(f"Run ID: {mlflow.active_run().info.run_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/transactions.jsonl")
    parser.add_argument("--n-estimators", type=int, default=100)
    parser.add_argument("--max-depth", type=int, default=5)
    parser.add_argument("--learning-rate", type=float, default=0.1)
    args = parser.parse_args()

    params = {
        "n_estimators": args.n_estimators,
        "max_depth": args.max_depth,
        "learning_rate": args.learning_rate,
    }
    train(args.data, params)
