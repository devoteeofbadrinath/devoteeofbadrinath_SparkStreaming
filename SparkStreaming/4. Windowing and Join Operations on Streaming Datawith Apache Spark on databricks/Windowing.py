from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, window

def main():

	sparkSession = SparkSession.builder.appName("Windowing").getOrCreate()

	sparkSession.sparkContext.setLogLevel('ERROR')

	schema = StructType([StructField("id", IntegerType(), True),
                     StructField("Gender", StringType(), True),
                     StructField("Customer Type", StringType(), True),
                     StructField("Age", IntegerType(), True),
                     StructField("Type of Travel", StringType(), True),
                     StructField("Class", StringType(), True),
                     StructField("Flight Distance", IntegerType(), True),
                     StructField("Inflight wifi service", IntegerType(), True),
                     StructField("Departure/Arrival time convenient", IntegerType(), True),
                     StructField("Ease of Online booking", IntegerType(), True),
                     StructField("Gate location", IntegerType(), True),
                     StructField("Food and drink", IntegerType(), True),
                     StructField("Online boarding", IntegerType(), True),
                     StructField("Seat comfort", IntegerType(), True),
                     StructField("Inflight entertainment", IntegerType(), True),
                     StructField("On-board service", IntegerType(), True),
                     StructField("Leg room service", IntegerType(), True),
                     StructField("Baggage handling", IntegerType(), True),
                     StructField("Checkin service", IntegerType(), True),
                     StructField("Inflight service", IntegerType(), True),
                     StructField("Cleanliness", IntegerType(), True),
                     StructField("Departure Delay in Minutes", IntegerType(), True),
                     StructField("Arrival Delay in Minutes", IntegerType(), True),
                     StructField("satisfaction", StringType(), True)
                    ])

	airline_data_full = sparkSession.readStream\
									.format("csv")\
									.option("header", "true")\
									.schema(schema)\
									.load('/Users/shivammittal/Desktop/Deloitte/SparkStreaming/windowing-join-operations-apache-spark-databricks/final_code/demo-01/datasets/airline_data')

	airline_data = airline_data_full.select("Gender", "Age", "type of Travel", "Class", "Baggage Handling", "Checkin service", "Cleanliness", "Departure Delay In Minutes", "Arrival Delay In Minutes")

	airline_data = airline_data.withColumn("Timestamp", current_timestamp())

	flight_class_age_df = airline_data.groupBy(airline_data.Class)\
										.agg({"Age": "avg"})


	flight_class_baggage_df = airline_data.groupBy(airline_data.Class)\
											.agg({"Baggage Handling" : "avg"})

	avg_dep_delay_window_df = airline_data.groupBy(window(airline_data.Timestamp, "2 minutes"))\
											.agg({"Departure Delay In Minutes" : "avg"})


	avg_checkin_score_window_df = airline_data.groupBy(window(airline_data.Timestamp, "2 minutes"), "Class") \
												.agg({"Checkin service" : "avg"})

	avg_checkin_score_df = airline_data.groupBy(window(airline_data.Timestamp, "30 seconds"), airline_data["Type of Travel"])\
											.agg({"Checkin service" : "avg"})

	avg_age_df = airline_data.groupBy(window(airline_data.Timestamp, "1 minute"), airline_data.Gender)\
										.agg({"Age":"avg"})\
										.withColumnRenamed("avg(Age)", "avg_age")\
										.select ("window.start","window.end","Gender","avg_age")


	avg_clean_df = airline_data.groupBy(window(airline_data.Timestamp, "3 minutes","1 minute"), airline_data.Gender)\
											.agg({"Cleanliness" : "avg"})

	
	avg_baggage_df = airline_data.groupBy(window(airline_data.Timestamp, "2 minutes", "30 seconds"), airline_data["Type of Travel"])\
											.agg({"Baggage Handling" : "avg"})\
											.orderBy("window.start")

	avg_baggage_df.writeStream\
						.outputMode('complete')\
						.format('console')\
						.option("numRows", 40)\
						.option('truncate', 'false')\
						.start()\
						.awaitTermination()


if __name__ == '__main__':
	main()
