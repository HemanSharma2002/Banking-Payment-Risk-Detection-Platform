from pyspark.sql import SparkSession

from src.data_generation.generator import generate_banking_data
from src.data_generation.writer import write_datasets


def main():

    spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("banking-data-generator")
    .config(
        "spark.hadoop.fs.permissions.umask-mode",
        "022",
    )
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem",
    )
    .getOrCreate()
)

    try:
        data = generate_banking_data()

        write_datasets(
            spark,
            data,
        )

        print("Banking datasets generated successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()