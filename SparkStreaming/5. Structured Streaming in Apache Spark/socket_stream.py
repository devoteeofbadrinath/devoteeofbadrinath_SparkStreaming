#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

spark = SparkSession\
    .builder\
    .appName('SocketDemo')\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

lines = spark.readStream\
    .format("socket")\
    .option("host", "localhost")\
    .option("port", 9999)\
    .load()

words = lines.select(
    explode(split(lines.value,' ')).alias("words")
)

word_counts = words.groupBy("words").count()

query = word_counts.writeStream\
    .outputMode("complete")\
    .format("console")\
    .start()

query.awaitTermination()