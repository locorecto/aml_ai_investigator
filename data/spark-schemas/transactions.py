from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

SCHEMA = StructType([
    StructField("txn_id", StringType(), False),
    StructField("party_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("instrument_type", StringType(), False),
    StructField("txn_timestamp_ms_utc", LongType(), False),
    StructField("direction", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("counterparty_id", StringType(), False),
    StructField("merchant_id", StringType(), True),
    StructField("channel", StringType(), False),
    StructField("country", StringType(), False),
    StructField("state", StringType(), True),
    StructField("is_international", IntegerType(), False),
    StructField("description", StringType(), False)
])
