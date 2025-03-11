from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import current_timestamp, window, col, from_json

def main():

    sparkSession = SparkSession.builder.appName("Watermarking")\
                        .config("spark.sql.session.timezone","UTC")\
                        .getOrCreate()
    
    sparkSession.sparkContext.setLogLevel('ERROR')

    schema = StructType([StructField("State", StringType(), True),
                         StructField("Category", StringType(), True),
                         StructField("Sub-Category", StringType(), True),
                         StructField("Sales", FloatType(), True),
                         StructField("Quantity", IntegerType(), True),
                         StructField("Profit", FloatType(), True),
                         StructField("Timestamp", TimestampType(), True)])
    
    superStoreDf = sparkSession.readStream\
                                .format("kafka")\
                                .option("kafka.bootstrap.servers", "localhost:9092")\
                                .option("subscribe", "loony-sales")\
                                .option("startingOffsets", "earliest")\
                                .load()
    
    superStoreCastDf = superStoreDf.selectExpr("CAST(value as STRING)")

    superStoreData = superStoreCastDf.select(from_json(col("value").cast("string"),schema))\
                                .withColumnRenamed("from_json(CAST(value as String))", "data")
    
    superStoreData = superStoreData.select(col  ('data.*'))

    #windowed_count = superStoreData.groupBy(window(superStoreData.Timestamp, "2 minutes"))\
    #                                .count()
    
    # windowed_count.writeStream\
    #                  .format('memory')\
    #                  .option('truncate', 'false')\
    #                  .queryName("windowed_count")\
    #                  .outputMode('update')\
    #                  .start()

    # import time
    # time.sleep(10)
    # sparkSession.sql("""SELECT * FROM windowed_count""").show(20,False)

    
    windowed_count_withwatermark = superStoreData.withWatermark("Timestamp", "3 minutes")\
                                                    .groupBy(window(superStoreData.Timestamp, "2 minutes"))\
                                                    .count()
    
    windowed_count_withwatermark.writeStream\
                                .outputMode('update')\
                                .format('console')\
                                .option('truncate', False)\
                                .queryName("windowed_count_withwatermark")\
                                .start()\
                                .awaitTermination()
    
    # time.sleep(10)
    # sparkSession.sql("""SELECT * FROM windowed_count_withwatermark""").show(20,False)
        
if __name__ == '__main__':
    main()
    


    
    
    

    