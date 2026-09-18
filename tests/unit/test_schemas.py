from src.schemas.customer_schema import customer_schema
from src.schemas.account_schema import account_schema
from src.schemas.transaction_schema import transaction_schema


def test_customer_schema():
    assert customer_schema["customer_id"].dataType.typeName() == "string"


def test_account_schema():
    assert account_schema["account_id"].dataType.typeName() == "string"


def test_transaction_schema():
    assert transaction_schema["transaction_id"].dataType.typeName() == "string"

    amount_type = transaction_schema["amount"].dataType

    assert amount_type.typeName() == "decimal"
    assert amount_type.precision == 18
    assert amount_type.scale == 2