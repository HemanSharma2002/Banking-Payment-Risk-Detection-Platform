from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


# ============================================================
# Configuration
# ============================================================

CATALOG = "banking_risk"

TRANSACTION_TABLE = f"{CATALOG}.silver.transactions"
ACCOUNT_TABLE = f"{CATALOG}.silver.accounts"
CUSTOMER_TABLE = f"{CATALOG}.silver.customers"
MERCHANT_TABLE = f"{CATALOG}.silver.merchants"

OUTPUT_TABLE = f"{CATALOG}.silver.enriched_transactions"

CHECKPOINT_PATH = (
    "/Volumes/banking_risk/bronze/checkpoints/"
    "silver_enriched_transactions"
)


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-EnrichTransactions")
    .getOrCreate()
)


# ============================================================
# Read transaction stream
# ============================================================

transactions = (
    spark.readStream
    .table(TRANSACTION_TABLE)
)


# ============================================================
# Read reference data as static DataFrames
# ============================================================

accounts = (
    spark.read
    .table(ACCOUNT_TABLE)
    .select(
        "account_id",
        "customer_id",
        "account_type",
        "currency",
        "balance",
        "status",
    )
)

customers = (
    spark.read
    .table(CUSTOMER_TABLE)
    .select(
        "customer_id",
        "country",
        "risk_profile",
    )
)

merchants = (
    spark.read
    .table(MERCHANT_TABLE)
    .select(
        "merchant_id",
        "merchant_name",
        "merchant_category",
        "country",
        "risk_level",
    )
)


# ============================================================
# Stream-static enrichment
# ============================================================

enriched_transactions = (
    transactions.alias("t")

    .join(
        accounts.alias("a"),
        col("t.account_id") == col("a.account_id"),
        "left",
    )

    .join(
        customers.alias("c"),
        col("a.customer_id") == col("c.customer_id"),
        "left",
    )

    .join(
        merchants.alias("m"),
        col("t.merchant_id") == col("m.merchant_id"),
        "left",
    )

    .select(
        col("t.transaction_id"),
        col("t.account_id"),
        col("a.customer_id"),

        col("t.merchant_id"),
        col("m.merchant_name"),
        col("m.merchant_category"),
        col("m.risk_level").alias("merchant_risk_level"),

        col("t.transaction_timestamp"),
        col("t.amount"),
        col("t.currency"),
        col("t.transaction_type"),
        col("t.channel"),
        col("t.country"),
        col("t.status"),

        col("a.account_type"),
        col("a.balance").alias("account_balance"),
        col("a.status").alias("account_status"),

        col("c.country").alias("customer_country"),
        col("c.risk_profile").alias("customer_risk_profile"),

        current_timestamp().alias("_enriched_at"),
    )
)


# ============================================================
# Write enriched stream
# ============================================================

query = (
    enriched_transactions.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .trigger(availableNow=True)
    .toTable(OUTPUT_TABLE)
)


query.awaitTermination()

print("Transaction enrichment completed.")

spark.stop()