import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, window

if __name__ == "__main__":
	
	host, port = ('localhost', '65432')

	spark = SparkSession\
			.builder\
			.appName("DiabetesTest")\
			.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")

	lines = spark\
			.readStream\
			.format('socket')\
			.option('host', host)\
			.option('port', port)\
			.load()

	CSVData = lines.select(\
					split(lines.value, ',').getItem(0).alias('eventTime').cast('timestamp'),\
					split(lines.value, ',').getItem(1).alias('bloodGlucose').cast('int'),\
					split(lines.value, ',').getItem(2).alias('deviceID'),\
					)

	glucoseByDeviceandTime = CSVData\
								.withWatermark('eventTime', '1 hours')\
								.groupBy('deviceID', window('eventTime', '10 seconds', '10 seconds'))\
								.avg('bloodGlucose')\

	query = glucoseByDeviceandTime\
				.writeStream\
				.outputMode('update')\
				.format('console')\
				.start()\
				.awaitTermination()