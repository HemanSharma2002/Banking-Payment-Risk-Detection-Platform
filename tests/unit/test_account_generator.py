from src.data_generation.customer_generator import generate_customers
from src.data_generation.account_generator import generate_accounts


def test_generate_accounts():

    customers = generate_customers(10)

    accounts = generate_accounts(
        customers,
        20,
    )

    assert len(accounts) == 20

    assert accounts[0]["account_id"] == "ACC_000001"

    customer_ids = {
        customer["customer_id"]
        for customer in customers
    }

    for account in accounts:
        assert account["customer_id"] in customer_ids

        assert account["account_type"] in {
            "SAVINGS",
            "CURRENT",
            "CREDIT",
        }

        assert account["currency"] in {
            "INR",
            "USD",
            "GBP",
            "AED",
            "SGD",
        }

        assert account["status"] in {
            "ACTIVE",
            "BLOCKED",
            "CLOSED",
        }