from datetime import datetime

from pyspark.sql import SparkSession

from src.data_generation.generator import generate_banking_data

from src.schemas.customer_schema import customer_schema
from src.schemas.account_schema import account_schema
from src.schemas.merchant_schema import merchant_schema
from src.schemas.transaction_schema import transaction_schema


# --------------------------------------------------
# Configuration
# --------------------------------------------------

RAW_BASE_PATH = "/Volumes/banking_risk/bronze/raw"

OUTPUTS = {
    "customers": customer_schema,
    "accounts": account_schema,
    "merchants": merchant_schema,
    "transactions": transaction_schema,
}


# --------------------------------------------------
# Spark
# --------------------------------------------------

spark = SparkSession.builder \
    .appName("BankingRisk-RawDataGenerator") \
    .getOrCreate()


# --------------------------------------------------
# Generate data
# --------------------------------------------------

print("Starting banking data generation...")

data = generate_banking_data()

print("Data generation completed.")


# --------------------------------------------------
# Write raw datasets
# --------------------------------------------------

for dataset_name, schema in OUTPUTS.items():

    print(f"Writing {dataset_name}...")

    df = spark.createDataFrame(
        data[dataset_name],
        schema=schema
    )

    output_path = f"{RAW_BASE_PATH}/{dataset_name}"

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(
        f"{dataset_name}: "
        f"{df.count()} records written to {output_path}"
    )


print("All raw datasets generated successfully.")

spark.stop()