from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    when,
    to_date,
    round,
    current_timestamp,
)


# ============================================================
# Configuration
# ============================================================

CATALOG = "banking_risk"

SOURCE_TABLE = (
    f"{CATALOG}.gold.transaction_risk"
)

OUTPUT_TABLE = (
    f"{CATALOG}.gold.daily_risk_metrics"
)

CHECKPOINT_PATH = (
    "/Volumes/banking_risk/bronze/checkpoints/"
    "daily_risk_metrics"
)


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-DailyRiskMetrics")
    .getOrCreate()
)


# ============================================================
# Read transaction risk stream
# ============================================================

transactions = (
    spark.readStream
    .table(SOURCE_TABLE)
)


# ============================================================
# Daily aggregation
# ============================================================

daily_metrics = (
    transactions

    .withColumn(
        "transaction_date",
        to_date(col("transaction_timestamp")),
    )

    .groupBy("transaction_date")

    .agg(
        # ----------------------------------------------------
        # Transaction volume
        # ----------------------------------------------------

        count("*").alias(
            "total_transactions"
        ),

        # ----------------------------------------------------
        # Transaction amount
        # ----------------------------------------------------

        sum("amount").alias(
            "total_transaction_amount"
        ),

        # ----------------------------------------------------
        # Risk classifications
        # ----------------------------------------------------

        count(
            when(
                col("risk_classification") == "HIGH",
                True,
            )
        ).alias(
            "high_risk_transactions"
        ),

        count(
            when(
                col("risk_classification") == "MEDIUM",
                True,
            )
        ).alias(
            "medium_risk_transactions"
        ),

        count(
            when(
                col("risk_classification") == "LOW",
                True,
            )
        ).alias(
            "low_risk_transactions"
        ),

        # ----------------------------------------------------
        # Risk score
        # ----------------------------------------------------

        round(
            avg("risk_score"),
            2,
        ).alias(
            "average_risk_score"
        ),

        # ----------------------------------------------------
        # Maximum risk score
        # ----------------------------------------------------

        # ----------------------------------------------------
        # Feature-trigger counts
        # ----------------------------------------------------

        count(
            when(
                col("high_value_flag") == 1,
                True,
            )
        ).alias(
            "high_value_transactions"
        ),

        count(
            when(
                col("velocity_flag") == 1,
                True,
            )
        ).alias(
            "velocity_flagged_transactions"
        ),

        count(
            when(
                col("unusual_location_flag") == 1,
                True,
            )
        ).alias(
            "unusual_location_transactions"
        ),

        count(
            when(
                col("suspicious_merchant_flag") == 1,
                True,
            )
        ).alias(
            "suspicious_merchant_transactions"
        ),
    )

    .withColumn(
        "_metrics_calculated_at",
        current_timestamp(),
    )
)


# ============================================================
# Write Gold
# ============================================================

query = (
    daily_metrics.writeStream
    .format("delta")
    .outputMode("complete")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .trigger(availableNow=True)
    .toTable(OUTPUT_TABLE)
)


query.awaitTermination()

print("Daily risk metrics completed.")

spark.stop()