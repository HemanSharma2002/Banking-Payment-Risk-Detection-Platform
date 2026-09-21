from datetime import datetime, timedelta
from decimal import Decimal

from pyspark.sql import SparkSession

from src.schemas.transaction_schema import transaction_schema


# ============================================================
# Configuration
# ============================================================

RAW_TRANSACTION_PATH = (
    "/Volumes/banking_risk/bronze/raw/transactions"
)


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-GenerateRiskScenarios")
    .getOrCreate()
)


# ============================================================
# Base timestamp
# ============================================================

base_time = datetime.now()


# ============================================================
# Controlled risk scenarios
# ============================================================

scenario_transactions = []


# ------------------------------------------------------------
# Scenario 1: High-value transaction
# ------------------------------------------------------------

scenario_transactions.append({
    "transaction_id": "RISK_HIGH_VALUE_001",
    "account_id": "ACC_000001",
    "merchant_id": "MER_000001",
    "transaction_timestamp": base_time,
    "amount": Decimal("150000.00"),
    "currency": "INR",
    "transaction_type": "PURCHASE",
    "channel": "ONLINE",
    "country": "IN",
    "status": "SUCCESS",
})


# ------------------------------------------------------------
# Scenario 2: Unusual location
# ------------------------------------------------------------

scenario_transactions.append({
    "transaction_id": "RISK_LOCATION_001",
    "account_id": "ACC_000002",
    "merchant_id": "MER_000002",
    "transaction_timestamp": base_time + timedelta(seconds=10),
    "amount": Decimal("5000.00"),
    "currency": "INR",
    "transaction_type": "PURCHASE",
    "channel": "ONLINE",
    "country": "AE",
    "status": "SUCCESS",
})


# ------------------------------------------------------------
# Scenario 3: Suspicious merchant
# ------------------------------------------------------------

scenario_transactions.append({
    "transaction_id": "RISK_MERCHANT_001",
    "account_id": "ACC_000003",
    "merchant_id": "MER_000003",
    "transaction_timestamp": base_time + timedelta(seconds=20),
    "amount": Decimal("10000.00"),
    "currency": "INR",
    "transaction_type": "PURCHASE",
    "channel": "ONLINE",
    "country": "IN",
    "status": "SUCCESS",
})


# ------------------------------------------------------------
# Scenario 4: Velocity attack
#
# 6 transactions within 10 minutes
# ------------------------------------------------------------

velocity_account = "ACC_000004"

for i in range(6):

    scenario_transactions.append({
        "transaction_id": f"RISK_VELOCITY_{i + 1:03d}",
        "account_id": velocity_account,
        "merchant_id": "MER_000004",
        "transaction_timestamp": (
            base_time + timedelta(minutes=i)
        ),
        "amount": Decimal("5000.00"),
        "currency": "INR",
        "transaction_type": "PURCHASE",
        "channel": "ONLINE",
        "country": "IN",
        "status": "SUCCESS",
    })


# ------------------------------------------------------------
# Scenario 5: Combined high-risk attack
#
# High value
# + velocity
# + unusual location
# + suspicious merchant
# ------------------------------------------------------------

combined_account = "ACC_000005"

for i in range(6):

    scenario_transactions.append({
        "transaction_id": f"RISK_COMBINED_{i + 1:03d}",
        "account_id": combined_account,
        "merchant_id": "MER_000005",
        "transaction_timestamp": (
            base_time + timedelta(minutes=i)
        ),
        "amount": Decimal("150000.00"),
        "currency": "INR",
        "transaction_type": "PURCHASE",
        "channel": "ONLINE",
        "country": "AE",
        "status": "SUCCESS",
    })


# ============================================================
# Create DataFrame
# ============================================================

df = spark.createDataFrame(
    scenario_transactions,
    schema=transaction_schema,
)


# ============================================================
# Inspect generated scenarios
# ============================================================

print(
    f"Generated {df.count()} controlled risk transactions."
)

df.orderBy("transaction_timestamp").show(
    50,
    truncate=False,
)


# ============================================================
# Append to raw transaction data
# ============================================================

(
    df.write
    .mode("append")
    .parquet(RAW_TRANSACTION_PATH)
)


print(
    "Controlled risk scenarios appended to raw transactions."
)


# ============================================================
# Stop Spark
# ============================================================

spark.stop()