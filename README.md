# Niagara

Niagara is a open-source project written in Scala for evaluating cutting-edge
distributed systems, e.g. Spark, Akka, Cassandra, Kafka, etc.

# Modules

* Batch Processing

* Real-time Processing

* Data-as-a-Service

## Batch Processing

The batch layer streams the xml files by Spark textfile API.
It parses the xml file line by line into a DataSet.
Queries the dataset by either Dataset API or Spark SQL.
Finally, persists the dataset on HDFS in Parquet format.

### Tech Stack

* Data Formats: XML, Parquet

* Storage Systems: HDFS

* Frameworks: Spark Core/SQL

## Real-time Processing

The real-time layer utilizes Akka Streams to simulate an infinite streaming producer.
Akka streams in the Xml files and emits Avro messages to Kafka simultaneously.
The consumer is implemented by Spark streaming, which consumes Avro messages from Kafka,
then executes the real-time data analytics.
The ingested data are persisted in Cassandra.

### Tech Stack

* Data Formats: XML, Avro

* Storage Systems: HDFS, Cassandra

* Messaging Systems: Kafka

* Frameworks: Akka Streams, Spark Streaming/SQL,

## Data-as-a-Service

The service layer provides RESTful APIs built by Spray for users to easily interact with data for ad-hoc analytics.
Under the hood, the API triggers a Spark SQL query to run spark jobs on data stored in Cassandra.

### Tech Stack

* Data Formats: Json

* Storage Systems: Cassandra

* Frameworks: Spray, Spark, Akka