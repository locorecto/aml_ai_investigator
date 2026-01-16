from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType

SCHEMA = StructType([
    StructField("alert_id", StringType(), False),
    StructField("party_id", StringType(), False),
    StructField("party_type", StringType(), True),
    StructField("model_type", StringType(), False),
    StructField("model_version", StringType(), False),
    StructField("scenario_code", StringType(), False),
    StructField("alert_timestamp_ms_utc", LongType(), False),
    StructField("window_start_ms_utc", LongType(), False),
    StructField("window_end_ms_utc", LongType(), False),
    StructField("risk_score", DoubleType(), False),
    StructField("severity", StringType(), False),
    StructField("trigger_summary", StringType(), False),
    StructField("supporting_txn_ids", StringType(), False),
    StructField("amount_total_usd", DoubleType(), False),
    StructField("txn_count", IntegerType(), False),
    StructField("features_json", StringType(), False),
    StructField("data_quality_flags", StringType(), False)
])
