
# Set up the windows Finder on the left and console on the right

  - `input/` – Place your CSV files here.

# Remove all the files previously present in input/


# Fixed-interval microbatch

- Show the code in this file

file_stream_fixed.py

- Run the code

spark-submit file_stream_fixed.py

- Place files into input/ and show that each batch gets processed every 60 seconds

# Update the code to write to a file

# Clear the input/ folder

fileQuery = streamingDF.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/fixed/") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "checkpoint/fixed/file") \
    .start()

fileQuery.awaitTermination()

- Place files in the input folder one at a time
- Show the timings of the files at the output

----
# Available-now microbatch

# Have 2-3 files in the input/ folder

- Show the code in this file

file_stream_available_now.py.py

- Run the code

spark-submit file_stream_available_now.py.py

- Run the code

- Show that all 3 files are processed and present in the output


# Notes
- **Fixed Interval Trigger:**  
  - Ensures periodic processing regardless of new data arrivals.
  - Ideal for time-sensitive, continuous streaming applications.
  
- **Available-Now Trigger:**  
  - Processes all available data immediately.
  - Useful for one-off or on-demand processing tasks.

