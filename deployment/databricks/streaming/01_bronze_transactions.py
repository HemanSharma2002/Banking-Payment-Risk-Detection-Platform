from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from src.schemas.transaction_schema import transaction_schema


# ============================================================
# Configuration
# ============================================================

RAW_PATH = "/Volumes/banking_risk/bronze/raw/transactions"

BRONZE_TABLE = "banking_risk.bronze.transactions"

CHECKPOINT_PATH = (
    "/Volumes/banking_risk/bronze/checkpoints/"
    "streaming_bronze_transactions"
)


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-Streaming-Bronze-Transactions")
    .getOrCreate()
)


# ============================================================
# Auto Loader
# ============================================================

transactions_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option(
        "cloudFiles.includeExistingFiles",
        "true",
    )
    .schema(transaction_schema)
    .load(RAW_PATH)
)


# ============================================================
# Bronze metadata
# ============================================================

bronze_transactions = (
    transactions_stream
    .withColumn(
        "_ingested_at",
        current_timestamp(),
    )
)


# ============================================================
# Continuous Bronze stream
# ============================================================

query = (
    bronze_transactions.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .toTable(BRONZE_TABLE)
)


# ============================================================
# Keep streaming query alive
# ============================================================

query.awaitTermination()