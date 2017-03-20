<pre>
 ____  _____   _
|_   \|_   _| (_)
  |   \ | |   __   ,--.   .--./) ,--.   _ .--.  ,--.
  | |\ \| |  [  | `'_\ : / /'`\;`'_\ : [ `/'`\]`'_\ :
 _| |_\   |_  | | // | |,\ \._//// | |, | |    // | |,
|_____|\____|[___]\'-;__/.',__` \'-;__/[___]   \'-;__/
                        ( ( __))
</pre>

Niagara is a Real-time Big Data Processing, Machine Learning, and Data-as-a-Service platform, which are implemented in Scala and built by SMACK(Spark, Mesos, Akka, Cassandra and Kafka) stack.

It is a MVP project on public real-life data sets to evaluate cutting-edge data streaming and machine learning frameworks and libraries.

# Modules

* Akka Streams

* Spark Streaming

* Kafka Streams

* Machine Learning


## Akka Streams

Dataset:

Stack Exchange Dataset contains 28 million posts in a 40GB single XML file.
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

* Frameworks: Akka Streams, Alpakka, Akka-Http


## Spark Streaming

Utilize Spark to read Json files and convert to Avro messages in Kafka topics.
A Spark streaming consumer consumes data from Kafka, and then do the transformation and aggregation.
Select features and persist them into Cassandra.
Build a recommendation engine by Spark MLlibs and Standford NLP.


Dataset:

Yelp Dataset contains Stack Exchange Dataset contains 4.1 million reviews by 1 million users for 144K businesses.
https://www.yelp.ca/dataset_challenge

### Tech Stack

* Data Formats: Json, Avro

* Storage Systems: HDFS, Cassandra

* Messaging Systems: Kafka

* Frameworks: Spark, Spark Streaming, MLlib, Stanford NLP


