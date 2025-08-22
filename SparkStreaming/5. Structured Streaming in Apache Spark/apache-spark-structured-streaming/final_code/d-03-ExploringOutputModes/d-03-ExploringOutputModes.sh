### Notes Different Output Modes

- **Append Mode (`outputMode("append")`):**
  - **Behavior:** Only new rows are output.
  - **Allowed Transformations:** Selections and filtering.
  - **Limitations:** Aggregations are not supported.

- **Complete Mode (`outputMode("complete")`):**
  - **Behavior:** The entire aggregated state is output each time.
  - **Allowed Transformations:** Aggregations (e.g., average calculations).

- **Update Mode (`outputMode("update")`):**
  - **Behavior:** Only the rows that changed since the last trigger are output.
  - **Allowed Transformations:** Stateful operations.


### Running the demo

 - `input/` – for incoming CSV files.

### Append mode

- Open and show the code in the following file

file_stream_append.py

- Make sure the input/ folder is empty

- Run the code

spark-submit file_stream_append.py

- Add files to the input folder

- Show the output

- Change the code to include the aggregation

    grouped_df = selected_df.groupBy(
       "Manufacturer"
    ).count()


    query = grouped_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .queryName("appendQuery") \
        .start()

- Run the code and show the exception

### Complete mode

- Open and show the code in the following file

file_stream_complete.py

- Make sure the input/ folder has just one file to start off with

- Run the code

spark-submit file_stream_complete.py

- Wait for the first batch to be processed

- Add files to the input folder

- Show the output

# You cannot write the complete output to a file
# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks


### Update mode


- Open and show the code in the following file

file_stream_update.py

- Make sure the input/ folder has just one file to start off with

- Run the code

spark-submit file_stream_update.py

- Wait for the first batch to be processed

- Add files to the input folder

- Show the output


