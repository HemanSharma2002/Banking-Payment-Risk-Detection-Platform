import random
from datetime import date, timedelta

from faker import Faker


fake = Faker()


COUNTRIES = [
    "IN",
    "US",
    "GB",
    "AE",
    "SG",
]

RISK_PROFILES = [
    "LOW",
    "MEDIUM",
    "HIGH",
]


def generate_customers(count: int) -> list[dict]:
    customers = []

    for i in range(1, count + 1):
        customer = {
            "customer_id": f"CUST_{i:06d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "date_of_birth": fake.date_of_birth(
                minimum_age=18,
                maximum_age=80,
            ),
            "country": random.choice(COUNTRIES),
            "risk_profile": random.choices(
                RISK_PROFILES,
                weights=[70, 25, 5],
                k=1,
            )[0],
        }

        customers.append(customer)

    return customers