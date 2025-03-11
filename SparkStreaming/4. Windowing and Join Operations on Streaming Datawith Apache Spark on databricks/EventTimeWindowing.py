from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, window, col, from_json

def main():
	
	sparkSession = SparkSession.builder.appName("EventTimeWindowing").config("spark.sql.session.timeZone", "UTC").getOrCreate()

	sparkSession.sparkContext.setLogLevel('ERROR')

	schema = StructType([StructField("Daily Time Spent on Site", FloatType(), True),
					 StructField("Age", IntegerType(), True),
					 StructField("Area Income", FloatType(), True),
					 StructField("Daily Internet Usage", FloatType(), True),
					 StructField("Male", IntegerType(), True),
					 StructField("Timestamp", TimestampType(), True),
					 StructField("Clicked on Ad", IntegerType(), True)])

	advertising_df = sparkSession.readStream\
							.format("kafka")\
							.option("kafka.bootstrap.servers","localhost:9092")\
							.option("subscribe", "advertising")\
							.option("startingOffsets", "earliest")\
							.load()

	advertising_cast_df = advertising_df.selectExpr("CAST(value as STRING)")

	advertising_data = advertising_cast_df.select(from_json(col("value").cast("string"),schema))\
							.withColumnRenamed("from_json(CAST(value as STRING))", "data") 

	advertising_data = advertising_data.select(col('data.*'))

	min_use = advertising_data.groupBy(window("Timestamp", "1 days"))\
								.agg({"daily Internet Usage" : "min"})
	
	max_use_count = advertising_data.groupBy(window("Timestamp","6 hours"))\
										.agg({"Daily Internet Usage" : "max", "Clicked on Ad":"count"})
	
	avg_income = advertising_data.groupBy(window("Timestamp", "1 days"))\
									.agg({"Area Income" : "avg"})
	
	avg_income_clicked = advertising_data.groupBy(window("Timestamp", "1 days"), advertising_data["Clicked On Ad"])\
											.agg({"Daily Internet Usage":"avg"})\
											.withColumnRenamed("avg(Daily Internet Usage)", "avg_usage")\
											.select("window.start", "window.end", "Clicked on Ad", "avg_usage")\
											.orderBy("window.start")
	
	daily_time_avg = advertising_data.groupBy(window("Timestamp", "5 days", "2 days"), advertising_data["Clicked on Ad"])\
										.agg({"Daily Time Spent on Site": "avg"})\
										.orderBy("window.start")
	
	daily_time_avg.writeStream.outputMode('complete').option('truncate','false').format('console').start().awaitTermination()

if __name__ == '__main__':
	main()





