"""PySpark schema for case packet json dataset."""
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, FloatType, BooleanType, ArrayType

SCHEMA = StructType([
    StructField("case_id", StringType(), True),
    StructField("case_packet_json", StringType(), True)
])
