import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def main():
    sparkSession = SparkSession.builder.appName("JoinsWithWatermarks")\
                                        .getOrCreate()
    
    sparkSession.sparkContext.setLogLevel("ERROR")

    streaming_schema_movies = StructType([StructField("name", StringType(), True),
                                          StructField("year", IntegerType(), True),
                                          StructField("director", StringType(), True),
                                          StructField("writer", StringType(), True),
                                          StructField("star", StringType(), True)])
    
    streaming_data_movies = sparkSession.readStream\
                                        .format("kafka")\
                                        .option("kafka.bootstrap.servers", "localhost:9092")\
                                        .option("subscribe", "loony-movies")\
                                        .option("startingOffsets", "earliest")\
                                        .load()
    
    streaming_data_movies_cast = streaming_data_movies.selectExpr("CAST(value as STRING)")
    streaming_data_movies_df = streaming_data_movies_cast.select(from_json(col("value").cast("string"), streaming_schema_movies))\
                                                                    .withColumnRenamed("from_json(cast(value as string))","data")
    
    streaming_data_movies_df = streaming_data_movies_df.select(col('data.*'))

    streaming_data_movies_df.printSchema()

    streaming_data_movies_timestamp = streaming_data_movies_df.withColumn("movies_timestamp", current_timestamp())

    streaming_movies_watermark = streaming_data_movies_timestamp.selectExpr("name as movie_name", "year", "movies_timestamp")\
                                                                .withWatermark("movies_timestamp","10 seconds") 

    # streaming_movies_watermark.writeStream\
    #                              .outputMode('append')\
    #                              .format('console')\
    #                              .option('truncate', False)\
    #                              .option('numRows', 40)\
    #                              .start()

    # time.sleep(10)

    streaming_schema_ratings = StructType([StructField('name', StringType(), True),
                                           StructField('rating', StringType(), True),
                                           StructField('score', FloatType(), True)])
    
    streaming_data_ratings = sparkSession.readStream\
                                            .format("kafka")\
                                            .option("kafka.bootstrap.servers", "localhost:9092")\
                                            .option("subscribe", "loony-ratings")\
                                            .option("startingOffsets", "earliest")\
                                            .load()
    
    streaming_data_ratings_cast = streaming_data_ratings.selectExpr("CAST(value as String)")

    streaming_data_ratings_df = streaming_data_ratings_cast.select(from_json(col("value").cast("String"), streaming_schema_ratings))\
                                                                .withColumnRenamed("from_json(cast(value as String))", "data")
    
    streaming_data_ratings_df = streaming_data_ratings_df.select(col('data.*'))

    streaming_data_ratings_df.printSchema()

    streaming_data_ratings_timestamp = streaming_data_ratings_df.withColumn("ratings_timestamp", current_timestamp())

    streaming_ratings_watermark = streaming_data_ratings_timestamp.selectExpr("name", "score", "ratings_timestamp")\
                                                                    .withWatermark("ratings_timestamp", "20 seconds")

    # streaming_ratings_watermark.writeStream\
    #                              .outputMode('append')\
    #                              .format('console')\
    #                              .option('numRows', 40)\
    #                              .option('truncate', False)\
    #                              .start()
    
    # time.sleep(5)

    watermark_join = streaming_movies_watermark.join(streaming_ratings_watermark,expr("""
                                                                                      movie_name = name AND
                                                                                      ratings_timestamp >= movies_timestamp AND
                                                                                      ratings_timestamp <= movies_timestamp + interval 1 minutes
                                                                                      """))
    
    watermark_join_left_outer = streaming_movies_watermark.join(streaming_ratings_watermark,expr("""
                                                                                                 movie_name = name AND
                                                                                                 ratings_timestamp >= movies_timestamp AND
                                                                                                 ratings_timestamp <= movies_timestamp + interval 2 minutes OR
                                                                                                 """
                                                                                                 ),
                                                                                                 "leftOuter")
    
    watermark_join_right_outer = streaming_movies_watermark.join(streaming_ratings_watermark,expr("""
                                                                                                 movie_name = name AND
                                                                                                 movies_timestamp >= ratings_timestamp AND
                                                                                                 movies_timestamp <= ratings_timestamp + interval 2 minutes OR
                                                                                                 """
                                                                                                 ),
                                                                                                 "rightOuter")
    
    watermark_left_semi_join = streaming_movies_watermark.join(streaming_ratings_watermark, expr("""
                                                                                                 movie_name = name AND
                                                                                                 ratings_timestamp >= movies_timestamp AND
                                                                                                 ratings_timestamp <= movies_timestamp + interval 2 minutes
                                                                                                 """),
                                                                                                 "leftSemi")
    
    watermark_join_left_outer.writeStream\
                    .outputMode('append')\
                    .format('console')\
                    .option('numRows', 40)\
                    .option('truncate', False)\
                    .start()\
                    .awaitTermination()
    
    #time.sleep(15)
    
    # watermark_join_right_outer.writeStream\
    #                  .outputMode('append')\
    #                  .format('console')\
    #                  .option('numRows', 40)\
    #                  .option('truncate', False)\
    #                  .start()
    
    # time.sleep(180)
    
if __name__ == '__main__':
    main()

