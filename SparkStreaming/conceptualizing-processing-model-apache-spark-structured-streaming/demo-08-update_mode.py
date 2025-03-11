from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
	sparkSession = SparkSession \
		.builder \
		.appName('Aggregations in update mode')\
		.getOrCreate()

	sparkSession.sparkContext.setLogLevel('ERROR')

	schema = StructType([StructField('Date', DoubleType(), True),
						 StructField('Open', DoubleType(), True),
						 StructField('High', DoubleType(), True),
						 StructField('Low', DoubleType(), True),
						 StructField('Close', DoubleType(), True),
						 StructField('Adjusted Close', DoubleType(), True),
						 StructField('Volume', DoubleType(), True),
						 StructField('Name', StringType(), True)
						 ])

	stockPricesDf = sparkSession \
			.readStream \
			.option('header', 'true') \
			.option('maxFilesPerTrigger', 10) \
			.schema(schema) \
			.csv('./datasets/stock_data')



	print(' ')
	print('Is the stream ready?')
	print(stockPricesDf.isStreaming)


	print(' ')
	print('Schema of the input stream')
	print(stockPricesDf.printSchema())


	averageCloseDf = stockPricesDf \
			.groupBy('Name') \
			.agg({'Close': 'avg'}) \
			.withColumnRenamed('avg(Close)', 'Average Close')


	query = averageCloseDf \
			.writeStream.outputMode('update') \
			.format('console') \
			.option('truncate', 'false') \
			.option('numRows', 30) \
			.start() \
			.awaitTermination()



if __name__ == '__main__':
	main()




	