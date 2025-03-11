from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():
	
	sparkSession = SparkSession.builder.master('local').appName('Continous processing').getOrCreate()

	sparkSession.sparkContext.setLogLevel("ERROR")

	streamDf = sparkSession.readStream\
							.format('rate')\
							.option('rowsPerSecond', 2)\
							.option('rampUpTime', 40)\
							.load()

	print(' ')
	print('Is the stream ready?')
	print(streamDf.isStreaming)

	print(' ')
	print('Schema of the input stream')
	print(streamDf.printSchema())

	selectDf = streamDf.selectExpr("*")

	query = selectDf \
				.writeStream \
				.outputMode('append')\
				.format('console')\
				.trigger(continuous='15 second')\
				.option('numrows', 10)\
				.start()

	query.awaitTermination()

if __name__ == '__main__':
	main()