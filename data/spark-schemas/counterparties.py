from pyspark.sql.types import StructType, StructField, StringType

SCHEMA = StructType([
    StructField("counterparty_id", StringType(), False),
    StructField("counterparty_type", StringType(), False),
    StructField("country", StringType(), False)
])
