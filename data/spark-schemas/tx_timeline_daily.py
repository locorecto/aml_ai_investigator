from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, FloatType, BooleanType, ArrayType

SCHEMA = StructType([
    StructField("case_id", StringType(), True),
    StructField("party_id", StringType(), True),
    StructField("txn_date_utc", StringType(), True),
    StructField("instrument_type", StringType(), True),
    StructField("txn_count", LongType(), False),
    StructField("amount_total_usd", DoubleType(), True)
])
