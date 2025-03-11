from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    sparkSession = SparkSession.builder.appName("Wordcount")\
                                .config("spark.streaming.stopGracefullyOnShutdown", "true")\
                                .config("spark.sql.shuffle.partitions",3)\
                                .getOrCreate() 

    sparkSession.sparkContext.setLogLevel("ERROR")

    linesDf = sparkSession.readStream\
                                .format('socket')\
                                .option('host', 'localhost')\
                                .option('port', 9999)\
                                .load()
    
    #wordCountDf.show()
    linesDf.printSchema()
    
    wordsDf = linesDf.selectExpr("explode(split(value,' ')) as word")
    countsDf = wordsDf.groupBy("word").count()

    countsDf.writeStream\
                .outputMode('complete')\
                .format('console')\
                .option('truncate', False)\
                .option('numRows', 40)\
                .start()\
                .awaitTermination()


if __name__ == '__main__':
    main()

