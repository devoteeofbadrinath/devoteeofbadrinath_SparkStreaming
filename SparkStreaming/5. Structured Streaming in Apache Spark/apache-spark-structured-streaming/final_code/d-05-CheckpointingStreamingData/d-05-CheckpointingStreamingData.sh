# Start in the spark-streaming/ directory

- Create an `input/` directory.
- Delete the existing `checkpoint/` directory

- Show the code in the file

file_stream_checkpoint.py


- Run the following code

spark-submit file_stream_checkpoint.py

- Show that the checkpoint/ folder is created as soon as we run this code

- Show the files in the checkpoint/ folder (show that each subfolder is empty)

- Add 1-2 files to the input/ folder - show that these are processed

- Kill the streaming code

- Show the files in the checkpoint/ folder (show that each subfolder is now no longer empty)

- Remove the files from the input/ folder

# Now restart the stream

- Add the same 1-2 files to input/

- Show that the files are not processed again (they were processed and checkpointed earlier)

- Add a 3rd and 4th file - show that these are processed

- Show the files in the checkpoint/ folder (show that each subfolder now has additional files)



### Notes

- **`commits/`** → Stores information about **successfully completed micro-batches**, ensuring exactly-once processing.  
- **`offsets/`** → Tracks **Kafka (or other sources) offsets** for each micro-batch to avoid reprocessing old data.  
- **`metadata/`** → Contains **query execution metadata**, including schema evolution and configurations.  
- **`state/`** → Stores **intermediate aggregation state** (for stateful operations like windowed aggregations).  
- **`sources/`** → Maintains information about **data sources**, tracking their latest processed offsets.  

