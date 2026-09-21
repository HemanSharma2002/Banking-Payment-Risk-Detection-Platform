from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    when,
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
    f"{CATALOG}.gold.customer_risk"
)

CHECKPOINT_PATH = (
    "/Volumes/banking_risk/bronze/checkpoints/"
    "customer_risk"
)


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-CustomerRisk")
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
# Customer risk aggregation
# ============================================================

customer_risk = (
    transactions
    .groupBy("customer_id")

    .agg(
        count("*").alias("total_transactions"),

        sum("amount").alias(
            "total_transaction_amount"
        ),

        count(
            when(
                col("risk_classification") == "HIGH",
                True
            )
        ).alias("high_risk_transactions"),

        count(
            when(
                col("risk_classification") == "MEDIUM",
                True
            )
        ).alias("medium_risk_transactions"),

        count(
            when(
                col("risk_classification") == "LOW",
                True
            )
        ).alias("low_risk_transactions"),

        round(
            avg("risk_score"),
            2
        ).alias("average_risk_score"),

        max("risk_score").alias(
            "maximum_risk_score"
        ),
    )

    # --------------------------------------------------------
    # High-risk percentage
    # --------------------------------------------------------

    .withColumn(
        "high_risk_percentage",
        round(
            (
                col("high_risk_transactions")
                / col("total_transactions")
            ) * 100,
            2,
        )
    )

    .withColumn(
        "_customer_risk_calculated_at",
        current_timestamp(),
    )
)


# ============================================================
# Write Gold
# ============================================================

query = (
    customer_risk.writeStream
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

print("Customer risk aggregation completed.")

spark.stop()