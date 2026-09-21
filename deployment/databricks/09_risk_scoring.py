from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    current_timestamp,
)


# ============================================================
# Configuration
# ============================================================

CATALOG = "banking_risk"

SOURCE_TABLE = (
    f"{CATALOG}.silver.transaction_risk_features"
)

OUTPUT_TABLE = (
    f"{CATALOG}.gold.transaction_risk"
)

CHECKPOINT_PATH = (
    "/Volumes/banking_risk/bronze/checkpoints/"
    "risk_scoring"
)


# ============================================================
# Risk scoring configuration
# ============================================================

from src.common.config import load_risk_config


risk_config = load_risk_config()

HIGH_VALUE_SCORE = risk_config["scoring"]["high_value"]
VELOCITY_SCORE = risk_config["scoring"]["velocity"]
UNUSUAL_LOCATION_SCORE = risk_config["scoring"]["unusual_location"]
SUSPICIOUS_MERCHANT_SCORE = risk_config["scoring"]["suspicious_merchant"]

LOW_MAX_SCORE = risk_config["classification"]["low_max_score"]
MEDIUM_MAX_SCORE = risk_config["classification"]["medium_max_score"]

# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-RiskScoring")
    .getOrCreate()
)


# ============================================================
# Read transaction risk features
# ============================================================

transactions = (
    spark.readStream
    .table(SOURCE_TABLE)
)


# ============================================================
# Calculate individual risk scores
# ============================================================

scored_transactions = (
    transactions

    # --------------------------------------------------------
    # High-value score
    # --------------------------------------------------------

    .withColumn(
        "high_value_score",
        when(
            col("high_value_flag") == 1,
            lit(HIGH_VALUE_SCORE)
        ).otherwise(lit(0))
    )

    # --------------------------------------------------------
    # Velocity score
    # --------------------------------------------------------

    .withColumn(
        "velocity_score",
        when(
            col("velocity_flag") == 1,
            lit(VELOCITY_SCORE)
        ).otherwise(lit(0))
    )

    # --------------------------------------------------------
    # Unusual-location score
    # --------------------------------------------------------

    .withColumn(
        "unusual_location_score",
        when(
            col("unusual_location_flag") == 1,
            lit(UNUSUAL_LOCATION_SCORE)
        ).otherwise(lit(0))
    )

    # --------------------------------------------------------
    # Suspicious-merchant score
    # --------------------------------------------------------

    .withColumn(
        "suspicious_merchant_score",
        when(
            col("suspicious_merchant_flag") == 1,
            lit(SUSPICIOUS_MERCHANT_SCORE)
        ).otherwise(lit(0))
    )

    # --------------------------------------------------------
    # Total risk score
    # --------------------------------------------------------

    .withColumn(
        "risk_score",
        (
            col("high_value_score")
            + col("velocity_score")
            + col("unusual_location_score")
            + col("suspicious_merchant_score")
        )
    )

    # --------------------------------------------------------
    # Risk classification
    # --------------------------------------------------------

    .withColumn(
        "risk_classification",
        when(
            col("risk_score") <= LOW_MAX_SCORE,
            lit("LOW")
        )
        .when(
            col("risk_score") <= MEDIUM_MAX_SCORE,
            lit("MEDIUM")
        )
        .otherwise(
            lit("HIGH")
        )
    )

    .withColumn(
        "_risk_scored_at",
        current_timestamp()
    )
)


# ============================================================
# Write Gold transaction risk
# ============================================================

query = (
    scored_transactions.writeStream
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

print("Risk scoring completed.")

spark.stop()