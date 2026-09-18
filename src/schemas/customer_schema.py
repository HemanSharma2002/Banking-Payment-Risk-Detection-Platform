from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    DateType
)
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("country", StringType(), True),
    StructField("risk_profile", StringType(), True),
])