from pyspark.sql  import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)
from pyspark.sql.functions import current_timestamp, col

spark = SparkSession.builder \
    .appName("StreamingSuperComputers") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("Rank", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Manufacturer", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Segment", StringType(), True),
    StructField("Total_Cores", IntegerType(), True),
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

inputPath = "input/"
df = spark.readStream\
    .option("header", "true") \
    .schema(schema) \
    .csv(inputPath)


filtered_df = df.select(
    "Name", "Country", "Year", "Processor_Speed", "Rmax"
).filter(col("Year") > 2015)


# Add a processing timestamp and computre a performance score
transformed_df = filtered_df.withColumn("Processing_Timestamp", current_timestamp()) \
    .withColumn("Performance_Score", col("Processor_Speed") * col("Rmax") / 10**6)

consoleQuery = transformed_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("checkpointLocation", "checkpoint/") \
    .option("path", "output/") \
    .start()

consoleQuery.awaitTermination()