import random
from datetime import datetime, timedelta
from decimal import Decimal


TRANSACTION_TYPES = [
    "PURCHASE",
    "TRANSFER",
    "WITHDRAWAL",
    "PAYMENT",
]

CHANNELS = [
    "POS",
    "ONLINE",
    "MOBILE",
    "ATM",
]

STATUSES = [
    "SUCCESS",
    "FAILED",
]

CURRENCY_RANGES = {
    "INR": (100, 50000),
    "USD": (10, 5000),
    "GBP": (10, 4000),
    "AED": (50, 20000),
    "SGD": (20, 7000),
}


def generate_transactions(
    accounts: list[dict],
    merchants: list[dict],
    count: int,
    start_time: datetime,
) -> list[dict]:

    transactions = []

    for i in range(1, count + 1):

        account = random.choice(accounts)
        merchant = random.choice(merchants)

        currency = account["currency"]

        min_amount, max_amount = CURRENCY_RANGES[currency]

        amount = Decimal(
            str(
                round(
                    random.uniform(min_amount, max_amount),
                    2,
                )
            )
        )

        transaction = {
            "transaction_id": f"TXN_{i:08d}",
            "account_id": account["account_id"],
            "merchant_id": merchant["merchant_id"],
            "transaction_timestamp": (
                start_time
                + timedelta(
                    seconds=random.randint(
                        0,
                        30 * 24 * 60 * 60,
                    )
                )
            ),
            "amount": amount,
            "currency": currency,
            "transaction_type": random.choice(
                TRANSACTION_TYPES
            ),
            "channel": random.choice(CHANNELS),
            "country": merchant["country"],
            "status": random.choices(
                STATUSES,
                weights=[98, 2],
                k=1,
            )[0],
        }

        transactions.append(transaction)

    return transactions