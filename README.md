# Project - Sky Content Watched

This Application is build to compute how long any particular video content was watched by a particular user.
It's the Spark streaming application developed in Java and it consumes the messages from kafka at real time.
The processed data is then re-published to Kafka to different topic and also persistently stored in MySql database 
for further analysis.

## Prerequisites
You must have below mentioned tools/technologies installed on your PC to run this application locally -
1. Java - 8.0
2. Zookeeper - 3.x
3. Kafka - 2.11
4. Spark - 2.1.0
5. Mysql - 8.x

## Running the Application

1. Install Kafka, Zookeeper and Schema Registry.

2. Start them as follows (On Ubuntu) and let them all run on their default ports.-
Zookeeper       - bin/zookeeper-server-start.sh config/zookeeper.properties
Kafka-Server    - bin/kafka-server-start.sh config/server.properties
Schema Registry - sudo systemctl start confluent-schema-registry

3. Create 2 Topics and 1 Consumer group ID from Kafka command line utility.
- bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic playEventTopic
- bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic watchedContentTopic
- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --consumer-property group.id=skyDataEngineers

4. Install MySql server and do the following - 
- Create user as 'sky' and password as 'password' and grant all the privileges to the user.
- Create Database and tables as mentioned in the CreateMysqlTables.sql in resources folder.

5. The Input csv file (PlayEventsInputData.csv) has 10 sample records to test. You can add data if you like.
    Those 10 sample records mainly covers all the scenarios.

6. Install all the Maven dependencies.

7. Run the Class PlayEventProducer.

8. Run the Class  KafkaSparkStream.
