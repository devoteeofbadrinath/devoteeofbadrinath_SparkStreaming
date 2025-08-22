from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType
)

spark = SparkSession.builder\
    .appName("CheckpointDemo")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("Rank", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Manufacturer", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Segment", StringType(), True),
    StructField("Total_Cores", LongType(), True),
    StructField("Processor_Speed", IntegerType(), True),
    StructField("CoProcessor_Cores", IntegerType(), True),
    StructField("Rmax", DoubleType(), True),
    StructField("Rpeak", DoubleType(), True),
    StructField("Power", DoubleType(), True),
    StructField("Power_Efficiency", DoubleType(), True),
    StructField("Architecture", StringType(), True),
    StructField("Processor_Technology", StringType(), True),
    StructField("Operating_System", StringType(), True),
    StructField("OS_Family", StringType(), True),
])

input_path = "input/"

streaming_df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(input_path)

selected_df = streaming_df.select(
    "Name", "Manufacturer", "Country", "Year"
)

consoleQuery = selected_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "checkpoint/") \
    .start()

consoleQuery.awaitTermination()





