from pyspark.sql import SparkSession, Row

from pyspark.sql import SQLContext
from pyspark import SparkContext


def main():
	
	sparkSession = SparkSession \
		.builder \
		.appName('Schema inference')\
		.getOrCreate()

	sparkSession.sparkContext.setLogLevel('ERROR')


	stockPricesDf = sparkSession.read \
		.format('csv') \
		.option('header', 'true') \
		.option('inferSchema', 'true') \
		.option('mode', 'DROPMALFORMED') \
		.load('datasets/stock_data')


	print(' ')
	print('Is the stream ready?')
	print(stockPricesDf.isStreaming)


	print(' ')
	print('Schema of the input stream')
	print(stockPricesDf.printSchema())

	stockPricesDf.select('Date', 'Name', 'Adj Close').show()


	stockPricesDf.groupBy('Name').count().show()


	stockPricesDf.createOrReplaceTempView('stock_prices')


	query = sparkSession.sql("""SELECT Name, avg(Close) 
						        FROM stock_prices
						        GROUP BY Name""")

	query.show()

	
	
if __name__ == '__main__':
	main()


	
