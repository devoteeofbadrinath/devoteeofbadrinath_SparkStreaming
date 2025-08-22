# Processing streaming data using a file source

# Start 

- Directory structure with:
  - `input/` – Contains your CSV files matching the updated schema.
  - `output/` – (Optional) Destination directory for processed data if file output is enabled.
  - `checkpoint/` – Directory for checkpoint data to ensure fault tolerance.

## Running the Demo

# Show that we have multiple CSV files

# Open up one of the CSV files and show

----

# Show the file (make sure you remove the commented out file sink)

file_stream.py


# Set up a Finder window on the left (increase the font size here) and a terminal window on the right

# On the terminal run (keep the terminal fairly wide)

spark-submit file_stream.py

# On the Finder window be in the input/ folder

# Copy over one file - show the results

# Copy over 1-2 more files and show the results


-------

# In the file_stream.py file

# Now remove the console sink

# Add the code for the file sink

fileQuery = transformed_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("checkpointLocation", "checkpoint/") \
    .option("path", "output/") \
    .start()

fileQuery.awaitTermination()



# On the left have two finder windows open 
 
 input/ # should be empty
 output/


# On the terminal run (keep the terminal fairly wide)

spark-submit file_stream.py

# Copy over one file - show the results in the output/ folder

# Copy over 1-2 more files and show the results

# Open up one of the outputs and show

















