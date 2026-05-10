import pytest
import numpy as np
from unittest.mock import MagicMock, patch


def test_classifier_returns_label_and_confidence():
    with patch("consumer.classifier.ModelLoader") as mock_loader_cls:
        mock_loader = MagicMock()
        mock_loader.predict.return_value = (True, 0.92)
        mock_loader_cls.return_value = mock_loader

        from consumer.classifier import FraudClassifier
        clf = FraudClassifier()

        transaction = {
            "amount": 5000.0,
            "customer_age": 45,
            "is_international": True,
            "timestamp": "2026-05-10T12:00:00+00:00",
            "currency": "USD",
            "merchant_category": "electronics",
            "card_type": "Visa",
            "merchant_country": "NG",
        }
        label, confidence = clf.classify(transaction)
        assert label is True
        assert 0.0 <= confidence <= 1.0


def test_classifier_extracts_features_correctly():
    with patch("consumer.classifier.ModelLoader") as mock_loader_cls:
        mock_loader = MagicMock()
        mock_loader.predict.return_value = (False, 0.85)
        mock_loader_cls.return_value = mock_loader

        from consumer.classifier import FraudClassifier
        clf = FraudClassifier()

        transaction = {
            "amount": 50.0,
            "customer_age": 30,
            "is_international": False,
            "timestamp": "2026-05-10T14:30:00+00:00",
            "currency": "USD",
            "merchant_category": "grocery",
            "card_type": "Mastercard",
            "merchant_country": "US",
        }
        clf.classify(transaction)
        mock_loader.predict.assert_called_once()
        features = mock_loader.predict.call_args[0][0]
        assert len(features) == 10
        assert features[0] == 50.0
        assert features[1] == 30
