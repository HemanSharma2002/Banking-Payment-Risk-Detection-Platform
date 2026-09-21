from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    current_timestamp,
)


# ============================================================
# Configuration
# ============================================================

CATALOG = "banking_risk"

SOURCE_TABLE = (
    f"{CATALOG}.silver.enriched_transactions"
)

OUTPUT_TABLE = (
    f"{CATALOG}.silver.risk_features"
)

CHECKPOINT_PATH = (
    "/Volumes/banking_risk/bronze/checkpoints/"
    "risk_features"
)

# These will eventually come from config/config.yaml
HIGH_VALUE_THRESHOLD = 100000


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-RiskFeatures")
    .getOrCreate()
)


# ============================================================
# Read enriched transactions
# ============================================================

transactions = (
    spark.readStream
    .table(SOURCE_TABLE)
)


# ============================================================
# Risk features
# ============================================================

risk_features = (
    transactions

    # --------------------------------------------------------
    # High-value transaction
    # --------------------------------------------------------

    .withColumn(
        "high_value_flag",
        when(
            col("amount") >= HIGH_VALUE_THRESHOLD,
            lit(1)
        ).otherwise(lit(0))
    )

    # --------------------------------------------------------
    # Suspicious merchant
    # --------------------------------------------------------

    .withColumn(
        "suspicious_merchant_flag",
        when(
            col("merchant_risk_level") == "HIGH",
            lit(1)
        ).otherwise(lit(0))
    )

    # --------------------------------------------------------
    # Unusual location
    # --------------------------------------------------------

    .withColumn(
        "unusual_location_flag",
        when(
            col("country") != col("customer_country"),
            lit(1)
        ).otherwise(lit(0))
    )

    # --------------------------------------------------------
    # Processing metadata
    # --------------------------------------------------------

    .withColumn(
        "_risk_features_calculated_at",
        current_timestamp()
    )
)


# ============================================================
# Write risk features
# ============================================================

query = (
    risk_features.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .trigger(availableNow=True)
    .toTable(OUTPUT_TABLE)
)


query.awaitTermination()

print("Risk feature calculation completed.")

spark.stop()
