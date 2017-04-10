<pre>
 ____  _____   _
|_   \|_   _| (_)
  |   \ | |   __   ,--.   .--./) ,--.   _ .--.  ,--.
  | |\ \| |  [  | `'_\ : / /'`\;`'_\ : [ `/'`\]`'_\ :
 _| |_\   |_  | | // | |,\ \._//// | |, | |    // | |,
|_____|\____|[___]\'-;__/.',__` \'-;__/[___]   \'-;__/
                        ( ( __))
</pre>

Niagara is a Fast-Big Data Processing, Machine Learning, and Data-as-a-Service platform, implemented in Scala and built
by SMACK(Spark, Mesos, Akka, Cassandra and Kafka) stack.

It is built on complicated public datasets to evaluate/compare emerging data streaming and machine learning frameworks and libraries.

# Modules

* Kafka Streams

* Spark Streaming

* Akka Streams

* Machine Learning


## Dataset

* The Yelp Dataset contains 4.1 million reviews(3.5GB) by 1 million users(1.2GB) for 144K businesses(115MB).
https://www.yelp.ca/dataset_challenge

* The Stack Exchange Dataset contains 28 million Posts in a 40GB single XML file.
https://archive.org/details/stackexchange

### Tech Stack

* Data Formats: Json, XML, Avro

* Storage Systems: HDFS, Cassandra

* Messaging Systems: Kafka

* Streaming Frameworks/libs: Kafka Streams, Spark Streaming, Akka Streams

* Machine Learning Libs: MLlib, Stanford NLP, TensorFlow

* The other libs: Kafka Connects, Alpakka, Akka-Http

## Prerequisites

#### Install Java8 and Scala in Ubuntu

Please check the steps in my [Big Data Blog](http://alvincjin.blogspot.ca/2017/01/install-java-and-scala-in-ubuntu.html)

#### Install Kafka and Zookeeper

Download [Kafka 0.10.2+](http://mirror.dsrg.utoronto.ca/apache/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz)
```
$ cd kafka_2.11-0.10.2.0/
$ ./bin/zookeeper-server-start.sh config/zookeeper.properties
$ ./bin/kafka-server-start.sh config/server.properties
$ ./bin/kafka-topics --zookeeper localhost:2181 --create --topic post
```

#### Install Cassandra

Download [Cassandra 3.1.0+](http://apache.forsale.plus/cassandra/3.10/apache-cassandra-3.10-bin.tar.gz)
```
$ cd apache-cassandra-3.1.0
$ ./bin/cassandra
```
## Kafka Streams

Utilize Spark to read Json files and convert to Avro messages in Kafka topics.
A Kafka streams Application consumes Review data as KStream from review topic.
At the same time, it consumes Business and User data as GlobalKTable.

KStream is like a fact table, containing large volume transactional records.
While KTable is like dimension table, contains small volume domain snapshot.
Enrich Review Stream by joining with Business and User tables to a new Kafka topic.


## Spark Streaming

Utilize Spark to read Json files and convert to Avro messages in Kafka topics.
A Spark streaming consumer consumes data from Kafka, and then do the transformation and aggregation.
Select features and persist them into Cassandra.



## Akka Streams

The application ingests XML format posts by the file connector in Alpakka.
Akka streams producer app forms a real-time data pipeline to parse, enrich and transform XML files to Avro messages in a Kafka topic.

An Akka stream consumer app consumes avro messages from Kafka, and then persists into Cassandra database.
The data flow DSL is shown below:

```
xmlSource ~> parsing ~> broadcast ~> enrich1 ~> merge ~> fillTitle ~> kafkaSink
                        broadcast ~> enrich2 ~> merge
```

The service layer provides RESTful APIs built by Akka-Http for users to easily interact with data for ad-hoc analytics.
Under the hood, the service calls Cassandra APIs to implement CRUD operations.
