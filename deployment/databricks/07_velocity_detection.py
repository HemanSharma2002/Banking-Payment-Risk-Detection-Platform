from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    count,
    lit,
    current_timestamp,
)


# ============================================================
# Configuration
# ============================================================

CATALOG = "banking_risk"

SOURCE_TABLE = (
    f"{CATALOG}.silver.risk_features"
)

OUTPUT_TABLE = (
    f"{CATALOG}.silver.velocity_features"
)

CHECKPOINT_PATH = (
    "/Volumes/banking_risk/bronze/checkpoints/"
    "velocity_detection"
)

WINDOW_DURATION = "10 minutes"
WATERMARK_DELAY = "1 day"
MAX_TRANSACTIONS = 5


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-VelocityDetection")
    .getOrCreate()
)


# ============================================================
# Read risk-feature stream
# ============================================================

transactions = (
    spark.readStream
    .table(SOURCE_TABLE)
)


# ============================================================
# Event-time velocity detection
# ============================================================

velocity = (
    transactions

    .withWatermark(
        "transaction_timestamp",
        WATERMARK_DELAY
    )

    .groupBy(
        col("account_id"),
        window(
            col("transaction_timestamp"),
            WINDOW_DURATION
        )
    )

    .agg(
        count("*").alias("transaction_count")
    )

    .withColumn(
        "velocity_flag",
        (col("transaction_count") > MAX_TRANSACTIONS)
        .cast("int")
    )

    .withColumn(
        "_velocity_calculated_at",
        current_timestamp()
    )
)


# ============================================================
# Write velocity features
# ============================================================

query = (
    velocity.writeStream
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

print("Velocity detection completed.")

spark.stop()