from datetime import datetime

from src.data_generation.customer_generator import generate_customers
from src.data_generation.account_generator import generate_accounts
from src.data_generation.merchant_generator import generate_merchants
from src.data_generation.transaction_generator import (
    generate_transactions,
)


def test_generate_transactions():

    customers = generate_customers(10)

    accounts = generate_accounts(
        customers,
        20,
    )

    merchants = generate_merchants(10)

    start_time = datetime(
        2026,
        1,
        1,
    )

    transactions = generate_transactions(
        accounts,
        merchants,
        50,
        start_time,
    )

    assert len(transactions) == 50

    assert transactions[0]["transaction_id"] == "TXN_00000001"

    account_ids = {
        account["account_id"]
        for account in accounts
    }

    merchant_ids = {
        merchant["merchant_id"]
        for merchant in merchants
    }

    for transaction in transactions:

        assert transaction["account_id"] in account_ids

        assert transaction["merchant_id"] in merchant_ids

        assert transaction["amount"] > 0

        assert transaction["currency"] in {
            "INR",
            "USD",
            "GBP",
            "AED",
            "SGD",
        }

        assert transaction["transaction_type"] in {
            "PURCHASE",
            "TRANSFER",
            "WITHDRAWAL",
            "PAYMENT",
        }

        assert transaction["channel"] in {
            "POS",
            "ONLINE",
            "MOBILE",
            "ATM",
        }

        assert transaction["status"] in {
            "SUCCESS",
            "FAILED",
        }