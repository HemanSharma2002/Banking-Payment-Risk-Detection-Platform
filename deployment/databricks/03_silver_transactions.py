from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
)

from src.schemas.transaction_schema import transaction_schema


# ============================================================
# Configuration
# ============================================================

BRONZE_TABLE = "banking_risk.bronze.transactions"

SILVER_TABLE = "banking_risk.silver.transactions"

QUARANTINE_TABLE = "banking_risk.silver.transaction_quarantine"

CHECKPOINT_PATH = (
    "/Volumes/banking_risk/bronze/checkpoints/"
    "silver_transactions"
)


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-SilverTransactions")
    .getOrCreate()
)


# ============================================================
# Read Bronze as a stream
# ============================================================

bronze_stream = (
    spark.readStream
    .table(BRONZE_TABLE)
)


# ============================================================
# Data quality rules
# ============================================================

valid_condition = (
    col("transaction_id").isNotNull()
    & col("account_id").isNotNull()
    & col("transaction_timestamp").isNotNull()
    & col("amount").isNotNull()
    & (col("amount") > 0)
    & col("currency").isNotNull()
    & col("status").isNotNull()
)


# ============================================================
# Split valid / invalid records
# ============================================================

valid_transactions = (
    bronze_stream
    .filter(valid_condition)
)


invalid_transactions = (
    bronze_stream
    .filter(~valid_condition)
    .withColumn("_quarantined_at", current_timestamp())
    .withColumn("_quarantine_reason", lit("FAILED_VALIDATION"))
)


# ============================================================
# Deduplication + watermark
# ============================================================

silver_transactions = (
    valid_transactions
    .withWatermark(
        "transaction_timestamp",
        "1 day"
    )
    .dropDuplicates(
        ["transaction_id"]
    )
)


# ============================================================
# Write Silver
# ============================================================

silver_query = (
    silver_transactions.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)


# ============================================================
# Write Quarantine
# ============================================================

quarantine_query = (
    invalid_transactions.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH + "_quarantine"
    )
    .trigger(availableNow=True)
    .toTable(QUARANTINE_TABLE)
)


# ============================================================
# Wait for completion
# ============================================================

silver_query.awaitTermination()
quarantine_query.awaitTermination()


print("Silver transaction pipeline completed.")

spark.stop()