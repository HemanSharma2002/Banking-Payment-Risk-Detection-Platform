from datetime import datetime

from src.common.config import load_config

from src.data_generation.customer_generator import generate_customers
from src.data_generation.account_generator import generate_accounts
from src.data_generation.merchant_generator import generate_merchants
from src.data_generation.transaction_generator import generate_transactions


def generate_banking_data(
    config_path: str = "config/config.yaml",
) -> dict:

    config = load_config(config_path)

    data_config = config["data_generation"]

    customers = generate_customers(
        data_config["customers"]
    )

    accounts = generate_accounts(
        customers,
        data_config["accounts"],
    )

    merchants = generate_merchants(
        data_config["merchants"]
    )

    transactions = generate_transactions(
        accounts,
        merchants,
        data_config["transactions"],
        datetime.now(),
    )

    return {
        "customers": customers,
        "accounts": accounts,
        "merchants": merchants,
        "transactions": transactions,
    }