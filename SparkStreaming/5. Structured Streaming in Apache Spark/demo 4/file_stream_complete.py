from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, LongType, DoubleType
)
from pyspark.sql.functions import col, avg

spark = SparkSession.builder \
    .appName("FileStreamCompletedModeDemo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("Rank", IntegerType(), True),
    
])