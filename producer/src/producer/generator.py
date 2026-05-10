import uuid
from datetime import datetime, timezone
from faker import Faker
from producer.transaction import Transaction

fake = Faker()

_CURRENCIES = ["USD", "EUR", "GBP", "CAD"]
_CARD_TYPES = ["Visa", "Mastercard", "Amex", "Discover"]
_LEGIT_CATEGORIES = ["grocery", "retail", "restaurant", "transport", "healthcare"]
_FRAUD_CATEGORIES = ["electronics", "luxury", "crypto", "wire-transfer", "gambling"]
_COUNTRIES = ["US", "CA", "GB", "AU", "DE"]
_FRAUD_COUNTRIES = ["NG", "RO", "UA", "BR", "PK"]


def generate_transaction(is_fraud: bool) -> Transaction:
    if is_fraud:
        amount = round(fake.pyfloat(min_value=500, max_value=9999, right_digits=2), 2)
        merchant_category = fake.random_element(_FRAUD_CATEGORIES)
        merchant_country = fake.random_element(_FRAUD_COUNTRIES)
        is_international = merchant_country != "US"
    else:
        amount = round(fake.pyfloat(min_value=1, max_value=499, right_digits=2), 2)
        merchant_category = fake.random_element(_LEGIT_CATEGORIES)
        merchant_country = fake.random_element(_COUNTRIES)
        is_international = merchant_country != "US"

    return Transaction(
        transaction_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc).isoformat(),
        amount=amount,
        currency=fake.random_element(_CURRENCIES),
        merchant_name=fake.company(),
        merchant_category=merchant_category,
        merchant_country=merchant_country,
        card_last4=str(fake.random_number(digits=4, fix_len=True)),
        card_type=fake.random_element(_CARD_TYPES),
        customer_id=str(uuid.uuid4()),
        customer_age=fake.random_int(min=18, max=80),
        customer_location=f"{fake.city()}, {fake.country_code()}",
        is_international=is_international,
        is_fraud=is_fraud,
    )
