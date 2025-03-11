from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import datetime
import sys

if __name__ == "__main__":
	host, port = ("127.0.0.1", "65432")

	spark = SparkSession\
				.builder\
				.appName("DiabetesTese")\
				.getOrCreate()

	lines = spark\
			.readStream\
			.format('socket')\
			.option('host', host)\
			.option('port', port)\
			.load()


	CSVData = lines.select(\
			split(lines.value,',').getItem(0).alias('eventTime').cast("timestamp"),\
			split(lines.value,',').getItem(1).alias('bloodGlucose').cast("int"),\
			split(lines.value,',').getItem(2).alias('deviceID'),\
			)

	
	selectAndFilter = CSVData.select("eventTime", "bloodGlucose")\
						.where("bloodGlucose > 0")

	query = selectAndFilter\
				.writeStream\
				.outputMode('append')\
				.format('console')\
				.start()\
				.awaitTermination()