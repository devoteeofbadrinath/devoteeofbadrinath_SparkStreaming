from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
	sparkSession = SparkSession \
					.builder\
					.master('local')\
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
								.option("maxFilePerTrigger", 10)\
								.schema(schema)\
								.csv('/Users/shivammittal/Desktop/Deloitte/conceptualizing-processing-model-apache-spark-structured-streaming/datasets/stock_data')

	print(' ')
	print('is the stream ready?')
	print(stockPricesDf.isStreaming)

	print(' ')
	print('The schema of the stream')
	print(stockPricesDf.printSchema())


	stockPricesDf.createOrReplaceTempView('stock_prices')

	maxCloseDf = sparkSession.sql("""SELECT Name, MAX(Close) as MaxClose FROM stock_prices GROUP BY Name""")

	query = maxCloseDf\
			.writeStream\
			.outputMode('update')\
			.format('console')\
			.option('truncate', 'false')\
			.option('numRows', 30)\
			.start()\
			.awaitTermination()


if __name__ == '__main__':
	main()