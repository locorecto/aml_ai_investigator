from pyspark.sql.types import StructType, StructField, StringType

SCHEMA = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), False),
    StructField("merchant_category", StringType(), False),
    StructField("country", StringType(), False),
    StructField("state", StringType(), True)
])
