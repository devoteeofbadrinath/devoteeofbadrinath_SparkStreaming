import json
import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
    
    sparkSession = SparkSession.builder.appName('StreamingStaticJoin').getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    static_schema = StructType([StructField('name', StringType(), True),
                         StructField('year', StringType(), True),
                         StructField('director', StringType(), True),
                         StructField('writer', StringType(), True),
                         StructField('star', StringType(), True)]
                         )
    
    static_data = sparkSession.read\
                        .format("csv")\
                        .option("header", "true")\
                        .schema(static_schema)\
                        .load('/Users/shivammittal/Desktop/Deloitte/SparkStreaming/windowing-join-operations-apache-spark-databricks/final_code/demo-04/datasets/movies/movies_01.csv')
                        
    static_data.show(40,False)

    streaming_schema = StructType([StructField("name", StringType(), True),
                                   StructField("rating", StringType(), True),
                                   StructField("score", FloatType(), True)])
    
    streamingDataDf = sparkSession.readStream\
                                    .format('kafka')\
                                    .option('kafka.bootstrap.servers','localhost:9092')\
                                    .option('subscribe', 'loony-ratings')\
                                    .option('startingOffsets', 'earliest')\
                                    .load()
    
    streamingDataCastDf = streamingDataDf.selectExpr("CAST(value as STRING)")

    streamingData = streamingDataCastDf.select(from_json(col("value").cast("string"),streaming_schema))\
                                            .withColumnRenamed("from_json(CAST(value as String))", "data")

    streamingData = streamingData.select(col('data.*'))
    #streamingData.printSchema()


    outer_join = static_data.join(streamingData)
    outer_join.printSchema()

    right_outer_join = static_data.join(streamingData, on=["name"], how="right_outer")
    #right_outer_join.printSchema()

    #left_outer_join = static_data.join(streamingData, on=["name"], how="left_outer")
    #left_outer_join.printSchema()

    left_outer_join = streamingData.join(static_data, on=["name"], how="left_outer")

    inner_join = static_data.join(streamingData, on=['name'], how="inner")

    selected_join = static_data.join(streamingData, on=['name'], how='inner')\
                                .select("name", "director", "star", "score")

    query = left_outer_join.writeStream\
                .format('console')\
                .outputMode('append')\
                .option('truncate', False)\
                .option('numRows', 40)\
                .start()\
                .awaitTermination()   
    
    #time.sleep(60)
    #query.stop()


if __name__ == '__main__':
    main()
