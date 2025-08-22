
# Have 3 tabs in the terminal

## Install Apache Kafka

### Download Kafka
- Visit the [Kafka Downloads page](https://kafka.apache.org/downloads).
- Download the latest Kafka binary (for example, `kafka_2.12-3.9.0.tgz`).
- Make sure the Scala version matches the version you are running

### Extract Kafka

- Extract the Kafka archive to a directory (I have placed it under spark-streaming):

- Run this command to verify version

./bin/kafka-topics.sh --version


## Start Zookeeper (Kafka Dependency)

Kafka uses Zookeeper to manage cluster state. To start Zookeeper:

- Run this command

./bin/zookeeper-server-start.sh config/zookeeper.properties


---

## Start the Kafka Server

./bin/kafka-server-start.sh config/server.properties

- Kafka is now running and ready to process messages.

---

## Create a Kafka Topic

./bin/kafka-topics.sh --create --topic hello-kafka --bootstrap-server localhost:9092


### Verify the Topic

./bin/kafka-topics.sh --list --bootstrap-server localhost:9092

- You should see `hello-kafka` in the list.

---

## Start a Kafka Producer (Send Messages)

./bin/kafka-console-producer.sh --topic hello-kafka --bootstrap-server localhost:9092

- These messages will be sent to the `hello-kafka` topic.

---

## Start a Kafka Consumer (Receive Messages)

./bin/kafka-console-consumer.sh --topic hello-kafka --bootstrap-server localhost:9092 --from-beginning


---

## Send messages from the Producer

- Type messages and press **Enter** after each. For example:
  ```
  Hello Kafka!
  On to Kafka with Spark Streaming!
  ```

- These should be received by the Consumer

---

## Stop the Kafka Consumer (leave the producer running)


## Key Takeaways

- **Real-Time Streaming:** Apache Kafka enables real-time message streaming between producers and consumers.
- **Zookeeper’s Role:** Zookeeper manages Kafka’s metadata and cluster state.
