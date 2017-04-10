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
It is built on public datasets to evaluate emerging data streaming and machine learning frameworks and libraries.

Niagara contains below 4 modules.


# Modules

* Akka Streams

* Kafka Streams

* Spark Streaming

* Machine Learning


## Akka Streams

**Dataset:**  The Stack Exchange Dataset contains 28 million Posts in a 40GB single XML file.
https://archive.org/details/stackexchange

The real-time layer utilizes Akka Streams and Alpakka to simulate an infinite streaming source.
Akka streams producer parses Xml files and converts to Avro messages to a Kafka topic simultaneously.
An Akka stream consumer consumes avro messages from Kafka, and then persists into Cassandra database.

The service layer provides RESTful APIs built by Akka-Http for users to easily interact with data for ad-hoc analytics.
Under the hood, the service calls Cassandra APIs to implement CRUD operations.

### Tech Stack

* Data Formats: XML, Avro

* Storage Systems: HDFS, Cassandra

* Messaging Systems: Kafka

* Streaming Libs: Akka Streams, Alpakka, Akka-Http


## Kafka Streams

Utilize Spark to read Json files and convert to Avro messages in Kafka topics.
A Kafka streams Application consumes Review data as KStream from review topic.
At the same time, it consumes Business and User data as GlobalKTable.
Enrich Review Stream by joining with Business and User tables.
Publish results to a new Kafka Topic.

**Dataset:**  Yelp Dataset contains 4.1 million reviews(3.5GB) by 1 million users(1.2GB) for 144K businesses(115MB).
https://www.yelp.ca/dataset_challenge

### Tech Stack

* Data Formats: Json, Avro

* Messaging Systems: Kafka

* Streaming Libs: Kafka Streams

## Spark Streaming

Utilize Spark to read Json files and convert to Avro messages in Kafka topics.
A Spark streaming consumer consumes data from Kafka, and then do the transformation and aggregation.
Select features and persist them into Cassandra.
Build a recommendation engine by Spark MLlibs and Standford NLP.

**Dataset:**  Yelp Dataset contains 4.1 million reviews(3.5GB) by 1 million users(1.2GB) for 144K businesses(115MB).
https://www.yelp.ca/dataset_challenge

### Tech Stack

* Data Formats: Json, Avro

* Storage Systems: HDFS, Cassandra

* Messaging Systems: Kafka

* Streaming Frameworks: Spark Streaming

* Machine Learning Libs: MLlib, Stanford NLP


