from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
)


account_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("balance", DecimalType(18, 2), True),
    StructField("status", StringType(), True),
])