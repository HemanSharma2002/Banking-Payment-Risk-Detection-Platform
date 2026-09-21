from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


# ============================================================
# Configuration
# ============================================================

CATALOG = "banking_risk"

DATASETS = [
    "customers",
    "accounts",
    "merchants",
]

CHECKPOINT_BASE = (
    "/Volumes/banking_risk/bronze/checkpoints/"
    "silver_reference"
)


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-SilverReferenceData")
    .getOrCreate()
)


# ============================================================
# Process reference datasets
# ============================================================

for dataset in DATASETS:

    bronze_table = f"{CATALOG}.bronze.{dataset}"
    silver_table = f"{CATALOG}.silver.{dataset}"
    checkpoint = f"{CHECKPOINT_BASE}/{dataset}"

    print(f"Processing {bronze_table}")

    stream_df = (
        spark.readStream
        .table(bronze_table)
    )

    silver_df = (
        stream_df
        .dropDuplicates()
        .withColumn(
            "_silver_processed_at",
            current_timestamp()
        )
    )

    query = (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            checkpoint
        )
        .trigger(availableNow=True)
        .toTable(silver_table)
    )

    query.awaitTermination()

    print(f"Completed {silver_table}")


print("Silver reference-data processing completed.")

spark.stop()