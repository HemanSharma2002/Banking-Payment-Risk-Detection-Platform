from pathlib import Path

from pyspark.sql import SparkSession

from src.schemas.customer_schema import customer_schema
from src.schemas.account_schema import account_schema
from src.schemas.merchant_schema import merchant_schema
from src.schemas.transaction_schema import transaction_schema


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def write_datasets(
    spark: SparkSession,
    data: dict,
    output_path: str = "data/sample",
) -> None:

    output_dir = PROJECT_ROOT / output_path

    datasets = {
        "customers": (
            data["customers"],
            customer_schema,
        ),
        "accounts": (
            data["accounts"],
            account_schema,
        ),
        "merchants": (
            data["merchants"],
            merchant_schema,
        ),
        "transactions": (
            data["transactions"],
            transaction_schema,
        ),
    }

    for name, (records, schema) in datasets.items():

        df = spark.createDataFrame(
            records,
            schema=schema,
        )

        path = output_dir / name

        (
            df.write
            .mode("overwrite")
            .parquet(str(path))
        )