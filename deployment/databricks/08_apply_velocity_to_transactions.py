from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    coalesce,
    lit,
)


# ============================================================
# Configuration
# ============================================================

CATALOG = "banking_risk"

TRANSACTION_TABLE = (
    f"{CATALOG}.silver.risk_features"
)

VELOCITY_TABLE = (
    f"{CATALOG}.silver.velocity_features"
)

OUTPUT_TABLE = (
    f"{CATALOG}.silver.transaction_risk_features"
)

CHECKPOINT_PATH = (
    "/Volumes/banking_risk/bronze/checkpoints/"
    "transaction_risk_features"
)


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-ApplyVelocity")
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
# Read velocity results
#
# Velocity is currently a Delta table produced by the
# previous streaming job, so we use it as static reference
# data for this pipeline.
# ============================================================

velocity = (
    spark.read
    .table(VELOCITY_TABLE)
    .select(
        col("account_id").alias("velocity_account_id"),
        col("window.start").alias("velocity_window_start"),
        col("window.end").alias("velocity_window_end"),
        col("transaction_count"),
        col("velocity_flag"),
    )
)


# ============================================================
# Add the same 10-minute window to each transaction
# ============================================================

transactions_with_window = (
    transactions
    .withColumn(
        "transaction_window",
        window(
            col("transaction_timestamp"),
            "10 minutes",
        )
    )
)


# ============================================================
# Join transaction with its velocity window
# ============================================================

enriched = (
    transactions_with_window.alias("t")

    .join(
        velocity.alias("v"),

        (
            (col("t.account_id") == col("v.velocity_account_id"))
            &
            (
                col("t.transaction_window.start")
                == col("v.velocity_window_start")
            )
            &
            (
                col("t.transaction_window.end")
                == col("v.velocity_window_end")
            )
        ),

        "left",
    )

    .select(
        "t.*",

        coalesce(
            col("v.transaction_count"),
            lit(0),
        ).alias("velocity_transaction_count"),

        coalesce(
            col("v.velocity_flag"),
            lit(0),
        ).alias("velocity_flag"),
    )

    .drop("transaction_window")
)


# ============================================================
# Write transaction-level risk features
# ============================================================

query = (
    enriched.writeStream
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

print("Velocity successfully applied to transactions.")

spark.stop()