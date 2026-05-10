import time
from datetime import datetime
from consumer.model_loader import ModelLoader

# These maps mirror the alphabetical order sklearn LabelEncoder assigns during training.
# If the generator adds new categories, update both train.py and here in sync.
# Canonical order is logged as encoding_maps.json artifact in the MLflow run.
_CURRENCY_MAP = {"CAD": 0, "EUR": 1, "GBP": 2, "USD": 3}
_CATEGORY_MAP = {
    "crypto": 0, "e-commerce": 1, "electronics": 2, "gambling": 3,
    "grocery": 4, "healthcare": 5, "luxury": 6, "restaurant": 7,
    "retail": 8, "transport": 9, "wire-transfer": 10,
}
_CARD_MAP = {"Amex": 0, "Discover": 1, "Mastercard": 2, "Visa": 3}
_COUNTRY_MAP = {
    "AU": 0, "BR": 1, "CA": 2, "DE": 3, "GB": 4,
    "NG": 5, "PK": 6, "RO": 7, "UA": 8, "US": 9,
}
# These must match the training dataset statistics logged as MLflow params (amount_mean, amount_std).
# Update if retraining with a different dataset size or fraud rate.
_GLOBAL_AMOUNT_MEAN = 300.0
_GLOBAL_AMOUNT_STD = 500.0


class FraudClassifier:
    def __init__(self) -> None:
        self._loader = ModelLoader()

    def classify(self, transaction: dict) -> tuple[bool, float]:
        features = self._extract_features(transaction)
        return self._loader.predict(features)

    def _extract_features(self, t: dict) -> list[float]:
        ts = datetime.fromisoformat(t["timestamp"])
        hour = ts.hour
        is_weekend = int(ts.weekday() >= 5)
        amount = float(t["amount"])
        amount_zscore = (amount - _GLOBAL_AMOUNT_MEAN) / _GLOBAL_AMOUNT_STD
        return [
            amount,
            float(t["customer_age"]),
            float(t["is_international"]),
            float(hour),
            float(is_weekend),
            amount_zscore,
            float(_CURRENCY_MAP.get(t.get("currency", "USD"), 0)),
            float(_CATEGORY_MAP.get(t.get("merchant_category", "retail"), 1)),
            float(_CARD_MAP.get(t.get("card_type", "Visa"), 0)),
            float(_COUNTRY_MAP.get(t.get("merchant_country", "US"), 0)),
        ]
