from datetime import datetime, timedelta
from decimal import Decimal
import random

from pyspark.sql import SparkSession

from src.schemas.transaction_schema import transaction_schema


RAW_PATH = "/Volumes/banking_risk/bronze/raw/transactions"


spark = (
    SparkSession.builder
    .appName("BankingRisk-TransactionBatchProducer")
    .getOrCreate()
)


# Generate a small new batch
transactions = []

start_time = datetime.now()

for i in range(100):

    transactions.append({
        "transaction_id": (
            f"STREAM_TXN_{int(start_time.timestamp())}_{i:04d}"
        ),
        "account_id": f"ACC_{random.randint(1, 15000):06d}",
        "merchant_id": f"MER_{random.randint(1, 1000):06d}",
        "transaction_timestamp": (
            start_time + timedelta(seconds=i)
        ),
        "amount": Decimal(
            str(round(random.uniform(100, 50000), 2))
        ),
        "currency": "INR",
        "transaction_type": random.choice([
            "PURCHASE",
            "TRANSFER",
            "PAYMENT",
        ]),
        "channel": random.choice([
            "POS",
            "ONLINE",
            "MOBILE",
        ]),
        "country": "IN",
        "status": "SUCCESS",
    })


df = spark.createDataFrame(
    transactions,
    schema=transaction_schema,
)


print(f"Generated {df.count()} transactions.")

(
    df.write
    .mode("append")
    .parquet(RAW_PATH)
)

print("New transaction batch written.")

spark.stop()