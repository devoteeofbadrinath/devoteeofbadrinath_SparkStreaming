from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
	
	sparkSession = SparkSession.builder.master('local')\
										.appName('Checkpointing')\
										.getOrCreate()

	sparkSession.sparkContext.setLogLevel('ERROR')

	schema = StructType([StructField('Date', TimestampType(), True),
						 StructField('Open', DoubleType(), True),
						 StructField('High', DoubleType(), True),
						 StructField('Low', DoubleType(), True),
						 StructField('Close', DoubleType(), True),
						 StructField('Adjusted Close', DoubleType(), True),
						 StructField('Volume', DoubleType(), True),
						 StructField('Name', StringType(), True)])

	stockPricesDf = sparkSession.readStream\
								.option("header", "true")\
								.option("maxFilesPerTrigger", 10)\
								.schema(schema)\
								.csv('/Users/shivammittal/Desktop/Deloitte/conceptualizing-processing-model-apache-spark-structured-streaming/datasets/stock_data')

	print(' ')
	print("Is the stream ready?")
	print(stockPricesDf.isStreaming)

	print(' ')
	print("The schema of the stream")
	print(stockPricesDf.printSchema())

	stockPricesDf.createOrReplaceTempView('stock_prices')

	avgCloseDf = sparkSession.sql("""SELECT Name, avg(close) FROM stock_prices GROUP BY Name""")

	query = avgCloseDf\
				.writeStream\
				.outputMode('complete')\
				.format('console')\
				.option('numRows', 30)\
				.option('truncate', 'false')\
				.option('checkpointLocation','/Users/shivammittal/Desktop/Deloitte/3. Conceptualizing the Processing Model for Apache Spark Structured Streaming/checkpoint')\
				.start()\
				.awaitTermination()


if __name__ == "__main__":
	main()