from pydantic import BaseModel, field_validator


class Transaction(BaseModel):
    transaction_id: str
    timestamp: str
    amount: float
    currency: str
    merchant_name: str
    merchant_category: str
    merchant_country: str
    card_last4: str
    card_type: str
    customer_id: str
    customer_age: int
    customer_location: str
    is_international: bool
    is_fraud: bool

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("amount must be positive")
        return v
