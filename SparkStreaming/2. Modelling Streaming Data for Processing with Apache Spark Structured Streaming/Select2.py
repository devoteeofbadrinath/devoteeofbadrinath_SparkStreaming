from pyspark.sql import SparkSession
from pyspark.sql.functions import split, window

if __name__ == "__main__":
	
	host, port = ('localhost', '65432')

	spark = SparkSession\
			.builder\
			.appName("DiabetesTest ")\
			.getOrCreate()

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

	glucoseByDeviceandTime = CSVData.groupBy('deviceID',\
								window('eventTime','30 seconds', '30 seconds'))\
								.avg('bloodGlucose')\
								.sort('deviceID', 'window')

	query = glucoseByDeviceandTime\
				.writeStream\
				.outputMode('complete')\
				.format('console')\
				.start()\
				.awaitTermination()
