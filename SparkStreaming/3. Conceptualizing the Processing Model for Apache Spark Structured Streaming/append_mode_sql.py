from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
	sparkSession = SparkSession.builder.master('local')\
										.appName('Projections in append mode')\
										.getOrCreate()

	sparkSession.sparkContext.setLogLevel('ERROR')

	schema = StructType([StructField('Date', StringType(), True),
						 StructField('Open', StringType(), True),
						 StructField('High', StringType(), True),
						 StructField('Low', StringType(), True),
						 StructField('Close', StringType(), True),
						 StructField('Adjusted_Close', StringType(), True),
						 StructField('Volume', StringType(), True),
						 StructField('Name', StringType(), True)])

	stockPricesDf = sparkSession \
					.readStream\
					.option('header', 'true')\
					.schema(schema)\
					.csv('/Users/shivammittal/Desktop/Deloitte/conceptualizing-processing-model-apache-spark-structured-streaming/datasets/stock_data')


	print(' ')
	print('Is the stream ready?')
	print(stockPricesDf.isStreaming)

	print(' ')
	print('Schema of the input stream')
	print(stockPricesDf.printSchema())

	stockPricesDf.createOrReplaceTempView('stock_prices')

	selectDf = sparkSession.sql("""SELECT Name, Date, High, Low
									FROM stock_prices
									WHERE High - Low > 10""")

	query = selectDf \
				.writeStream\
				.outputMode('append')\
				.format('console')\
				.option('truncate', 'false')\
				.option('numRows', 5)\
				.start()\
				.awaitTermination()

if __name__ == '__main__':
	main()