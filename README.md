<pre>
 ____  _____   _
|_   \|_   _| (_)
  |   \ | |   __   ,--.   .--./) ,--.   _ .--.  ,--.
  | |\ \| |  [  | `'_\ : / /'`\;`'_\ : [ `/'`\]`'_\ :
 _| |_\   |_  | | // | |,\ \._//// | |, | |    // | |,
|_____|\____|[___]\'-;__/.',__` \'-;__/[___]   \'-;__/
                        ( ( __))
</pre>

Niagara is a open-source Data-as-a-Service platform implemented in Scala and built by SMACK(Spark, Mesos, Akka, Cassandra and Kafka) stack.
It is a MVP project to evaluate cutting-edge real-time processing frameworks, e.g. Spark Streaming, Akka Stream, Kafka Streams, etc.
It utilizes HDFS, MySQL and Cassandra as storage systems, and Kafka as a Pub-Sub system.


# Modules

* Batch Processing

* Real-time Processing

* Data-as-a-Service

## Batch Processing

The batch layer streams the xml files by Spark textfile API.
Then, parses the xml file line by line into a DataSet data structure.
Analyzes the data set by either Dataset API or Spark SQL.
Finally, persists the request on HDFS in Parquet format.

### Tech Stack

* Data Formats: XML, Parquet

* Storage Systems: HDFS

* Frameworks: Spark Core/SQL

## Real-time Processing


The real-time layer utilizes Akka Streams to simulate an infinite streaming source.
Akka streams parses Xml files and converts to Avro messages to a Kafka topic simultaneously.
Three different frameworks are used to build consumers.
1. Spark streaming
2. Kafka Streams
3. Akka Streams

The consumers consume Avro messages from Kafka topic.
Then executes the real-time data analytics.
The ingested data are persisted in Cassandra.

### Tech Stack

* Data Formats: XML, Avro

* Storage Systems: HDFS, Cassandra

* Messaging Systems: Kafka

* Frameworks: Akka Streams, Alpakka, Kafka Streams Spark Streaming

## Data-as-a-Service

The service layer provides RESTful APIs built by Akka-Http for users to easily interact with data for ad-hoc analytics.
Under the hood, the service calls Cassandra APIs to implement CRUD operations.

### Tech Stack

* Data Formats: Json

* Storage Systems: Cassandra, MySQL

* Frameworks: Akka-Http, Slick


## Dataset used in the project:

Stack Exchange Dataset contains 28 million posts in a 40GB single XML file.
https://archive.org/details/stackexchange