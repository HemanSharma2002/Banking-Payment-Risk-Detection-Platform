from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

from src.schemas.customer_schema import customer_schema
from src.schemas.account_schema import account_schema
from src.schemas.merchant_schema import merchant_schema
from src.schemas.transaction_schema import transaction_schema


# ============================================================
# Configuration
# ============================================================

CATALOG = "banking_risk"
BRONZE_SCHEMA = "bronze"

RAW_BASE_PATH = "/Volumes/banking_risk/bronze/raw"
CHECKPOINT_BASE_PATH = "/Volumes/banking_risk/bronze/checkpoints"


DATASETS = {
    "customers": customer_schema,
    "accounts": account_schema,
    "merchants": merchant_schema,
    "transactions": transaction_schema,
}


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("BankingRisk-BronzeIngestion")
    .getOrCreate()
)


# ============================================================
# Bronze ingestion function
# ============================================================

def ingest_to_bronze(dataset_name, schema):

    source_path = f"{RAW_BASE_PATH}/{dataset_name}"
    checkpoint_path = f"{CHECKPOINT_BASE_PATH}/{dataset_name}"
    table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{dataset_name}"

    print(f"Starting Bronze ingestion: {dataset_name}")
    print(f"Source      : {source_path}")
    print(f"Checkpoint  : {checkpoint_path}")
    print(f"Target      : {table_name}")

    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(schema)
        .load(source_path)
    )

    bronze_df = (
        stream_df
        .withColumn("_ingested_at", current_timestamp())
        # .withColumn("_source_file", input_file_name())
    )

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
    )

    query.awaitTermination()

    print(f"Completed Bronze ingestion: {dataset_name}")


# ============================================================
# Run ingestion
# ============================================================

for dataset_name, schema in DATASETS.items():

    ingest_to_bronze(
        dataset_name=dataset_name,
        schema=schema,
    )


print("All Bronze ingestion completed.")

spark.stop()