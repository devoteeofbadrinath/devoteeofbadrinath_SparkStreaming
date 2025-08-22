#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, LongType, DoubleType
)
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("FileStreamAppendModeDemo") \
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

    stream_df = spark.readStream.format("csv") \
        .option("header", True) \
        .schema(schema) \
        .load(input_path)

    filtered_df = stream_df.filter(col("Year") > 2017)

    selected_df = filtered_df.select(
        "Rank", "Name", "Manufacturer", "Country", "Processor_Speed", "Rmax", "Year"
    )

    # grouped_df = selected_df.groupBy(
    #    "Manufacturer"
    # ).count()

    # Write streaming data to the console in Append mode (only new rows)
    query = selected_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .queryName("appendQuery") \
        .start()

    # query = grouped_df.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .option("truncate", False) \
    #     .queryName("appendQuery") \
    #     .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()
