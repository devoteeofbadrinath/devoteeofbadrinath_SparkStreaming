import json
import time
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():

    sparkSession = SparkSession.builder.appName("StreamingStreamingJoin").getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    streaming_schema_movies = StructType([StructField("name", StringType(), True),
                                          StructField("year", IntegerType(), True),
                                          StructField("director", StringType(), True),
                                          StructField("writer", StringType(), True),
                                          StructField("star", StringType(), True)])
    
    streaming_data_movies = sparkSession.readStream\
                                            .format('kafka')\
                                            .option('kafka.bootstrap.servers', 'localhost:9092')\
                                            .option('subscribe', 'loony-movies')\
                                            .option('startingOffsets', 'earliest')\
                                            .load()
    
    streaming_data_movies_cast = streaming_data_movies.selectExpr("CAST(value as STRING)")

    streaming_data_movies_df= streaming_data_movies_cast.select(from_json(col("value").cast("string"),streaming_schema_movies))\
                                                                .withColumnRenamed("from_json(CAST(value as String))", "data")
    
    streaming_data_movies_df = streaming_data_movies_df.select(col('data.*'))
    streaming_data_movies_df.printSchema()

    streaming_schema_ratings = StructType([StructField("name", StringType(), True),
                                           StructField("rating", StringType(), True),
                                           StructField("score", StringType(), True)])
    
    streaming_data_ratings = sparkSession.readStream\
                                    .format("kafka")\
                                    .option("kafka.bootstrap.servers", "localhost:9092")\
                                    .option("subscribe", "loony-ratings")\
                                    .option("startingOffsets", "earliest")\
                                    .load()
    
    streaming_data_ratings_cast = streaming_data_ratings.selectExpr("CAST(value as STRING)")
    streaming_data_ratings_df = streaming_data_ratings_cast.select(from_json(col("value").cast("string"),streaming_schema_ratings))\
                                                            .withColumnRenamed("from_json(CAST(value as String))", "data")
    
    streaming_data_ratings_df = streaming_data_ratings_df.select(col('data.*'))
    streaming_data_ratings_df.printSchema()
    
    inner_join = streaming_data_movies_df.join(streaming_data_ratings_df, on=['name'], how='inner')

    join_and_filter = streaming_data_movies_df.join(streaming_data_ratings_df, on=['name'], how='inner')\
                                                .where(streaming_data_ratings_df.rating == "PG")
    
    not_supported_join = streaming_data_movies_df.join(streaming_data_ratings_df, on=['name'], how='left_outer')
    
    # query_ratings = streaming_data_ratings_df.writeStream\
    #                         .outputMode('append')\
    #                         .format('console')\
    #                         .option('truncate', False)\
    #                         .start()
    
    # query_movies = streaming_data_movies_df.writeStream\
    #                         .outputMode('append')\
    #                         .format('console')\
    #                         .option('truncate', False)\
    #                         .start()
    
    #time.sleep(60)
    #query_ratings.stop()
    #query_movies.stop()
    
    not_supported_join.writeStream\
                .outputMode('append')\
                .format('console')\
                .option('numRows', 10)\
                .option('truncate', False)\
                .start()\
                .awaitTermination()
    
    

    
if __name__ == '__main__':
    main()

    