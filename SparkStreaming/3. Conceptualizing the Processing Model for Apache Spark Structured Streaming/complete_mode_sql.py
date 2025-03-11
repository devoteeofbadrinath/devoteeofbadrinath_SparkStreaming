from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
	sparkSession = SparkSession.builder.master('local')
										.appName('Aggregations in completed mode')\
										.getOrCreate()

	sparkSession.sparkContext.setLogLevel('ERROR')

	schema = StructType([StructField('Date', DoubleType(), True),
						 StructField('Open', DoubleType(), True),
						 StructField('High', DoubleType(), True),
						 StructField('Low', DoubleType(), True),
						 StructField('Close', DoubleType(), True),
						 StructField('Adjusted Close', DoubleType(), True),
						 StructField('Volume', DoubleType(), True),
						 StructField('Name', StringType(), True)])

	stockPricesDf = sparkSession.readStream\
								.option("header", "true")\
								.option("maxFilesPerTrigger", 2)\
								.schema(schema)\
								.csv("/Users/shivammittal/Desktop/Deloitte/conceptualizing-processing-model-apache-spark-structured-streaming/datasets/stock_data")

	print(' ')
	print('Is the stream ready?')
	print(stockPricesDf.isStreaming)

	print(' ')
	print('Schema of the input stream')
	print(stockPricesDf.printSchema())

	stockPricesDf.createOrReplaceTempView('stock_prices')

	minMaxCloseDf = sparkSession.sql("""SELECT Name, min(close), max(close) FROM stock_prices GROUP BY Name""")

	query = minMaxCloseDf \
			.writeStream.outputMode('completed')\
			.format('console')\
			.option('truncate', 'false')\
			.start()\
			.awaitTermination()

if __name__ == '__main__':
	main()