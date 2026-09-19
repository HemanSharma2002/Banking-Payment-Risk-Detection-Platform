import random
from decimal import Decimal


ACCOUNT_TYPES = [
    "SAVINGS",
    "CURRENT",
    "CREDIT",
]

CURRENCIES = [
    "INR",
    "USD",
    "GBP",
    "AED",
    "SGD",
]

STATUSES = [
    "ACTIVE",
    "BLOCKED",
    "CLOSED",
]


def generate_accounts(
    customers: list[dict],
    count: int,
) -> list[dict]:

    accounts = []

    for i in range(1, count + 1):
        customer = random.choice(customers)

        account = {
            "account_id": f"ACC_{i:06d}",
            "customer_id": customer["customer_id"],
            "account_type": random.choice(ACCOUNT_TYPES),
            "currency": random.choice(CURRENCIES),
            "balance": Decimal(
                str(round(random.uniform(1000, 500000), 2))
            ),
            "status": random.choices(
                STATUSES,
                weights=[95, 3, 2],
                k=1,
            )[0],
        }

        accounts.append(account)

    return accounts