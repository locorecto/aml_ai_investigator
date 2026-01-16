from pyspark.sql.types import StructType, StructField, StringType, DoubleType

SCHEMA = StructType([
    StructField("party_id", StringType(), False),
    StructField("party_type", StringType(), False),
    StructField("party_name", StringType(), False),
    StructField("industry", StringType(), True),
    StructField("country", StringType(), False),
    StructField("state", StringType(), True),
    StructField("onboarding_date", StringType(), False),
    StructField("expected_monthly_volume_usd", DoubleType(), False),
    StructField("expected_avg_txn_usd", DoubleType(), False),
    StructField("risk_rating", StringType(), False)
])
