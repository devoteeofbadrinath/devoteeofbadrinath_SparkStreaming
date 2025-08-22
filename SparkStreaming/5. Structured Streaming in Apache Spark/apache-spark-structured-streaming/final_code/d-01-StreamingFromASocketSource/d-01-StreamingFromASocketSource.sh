
### Start in folder

~/spark-streaming/

python --version

pyspark --version


# Preinstalled on MacOS and Linux

nc --version



# For Mac
# brew install netcat


# For Linux

# sudo apt update
# sudo apt install netcat


# For Windows
# https://medium.com/@bonguides25/how-to-install-netcat-on-windows-10-11-f5be1a185611


# Set up two terminals side-by-side (left == sender, right == receiver)

# Open a **Sender Terminal** and run:  

nc -l 9999 


# Receive Messages (Receiver Terminal)  

nc localhost 9999

# Send the following messages (on the left terminal)

Hello!
This is quite fun:-)


# Kill both sender and receiver

---

## Run the Spark Structured Streaming Job  

# Show the `socket_stream.py` Python file

---

# Set up the Sender window on the left and the spark-submit window on the right


# Sender window
nc -l 9999 


# Spark submit window

spark-submit socket_stream.py

Spark will start and wait for data on **`localhost:9999`**.  


---

# On the sender window

Hello Spark Streaming
Streaming is fun
Spark is fun
















