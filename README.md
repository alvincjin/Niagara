
Niagara is a Fast & Big Data Processing, Machine Learning, and Data-as-a-Service platform, implemented in Scala with SDACK stack.
It is built on complicated public data sets to evaluate emerging Stateful Stream Processing to build lightweight Streaming Services.

#### SDACK Tech Stack

* The batch analytic engine: Spark (Spark Streaming, SQL, MLlib)

* The lightweight container: Docker (Kubernetes)

* The real-time view: Akka (Akka Streams, Http, Alpakka)

* The scalable storage: Cassandra

* The distributed message broker: Kafka (Kafka Streams, Connects, Schema Registry)


## Dataset

* The Yelp Dataset contains 4.1 million reviews(3.5GB) by 1 million users(1.2GB) for 144K businesses(115MB).
https://www.yelp.ca/dataset_challenge

* The Stack Exchange Dataset contains 28 million Posts in a 40GB single XML file.
https://archive.org/details/stackexchange


## Modules

* Data Streaming (Kafka, Spark, Akka)

* CQRS & Event Sourcing

* Machine Learning


## Prerequisites

#### Install Java 8 and Scala 2.11 in Ubuntu

Please check the steps in my [Big Data Blog](http://alvincjin.blogspot.ca/2017/01/install-java-and-scala-in-ubuntu.html)

#### Create Kafka and Zookeeper Docker Containers

Install Docker for Mac OS, then create 3 containers for Zookeeper, Kafka broker and Schema Registry, respectively.

```
$ cd ~/pathto/Niagara

//Start containers
$ docker-compose up


//Stop containers and remove them entirely
$ docker-compose down

```

#### Install Cassandra

Download and unzip [Cassandra 3.1.0+](http://apache.forsale.plus/cassandra/3.10/apache-cassandra-3.10-bin.tar.gz)
```
$ cd apache-cassandra-3.1.0
$ ./bin/cassandra
```


## Build and Run the App


Use [sbt-avrohugger](https://github.com/julianpeeters/sbt-avrohugger) to automatically generate SpecificRecord case class from avro schema.
```
$ sbt avro:generate-specific
```

Build an uber jar with all the dependencies.
```
$ sbt clean assembly
```

## Kafka Streams

In many use cases, we have to support an event-by-event low latency model rather than one that focuses on microbatches,
and deliver upstream changes to the materialized state store to serve microservice.

A Spark core app ingests Json files and converts Json to Avro messages in Kafka topics, e.g. review, business, user.

A Kafka streams application consumes review messages as KStream from review topic.
In the meanwhile, it consumes Business and User data as GlobalKTable.

KStream works like Fact table, containing large volume immutable transactional records.
While KTable works like Dimension table, contains small volume domain data snapshot.
The pipeline enriches Review Stream by joining with Business and User KTables in real-time to a new Kafka topic.
Then, apply filtering, aggregations and keep the results in local state store to provide Micro-service for Interactive Queries.

For example, KeyValue query on stars summed by city

```
GET /stars/{city}
```

Range Query on stars sumed by business in a time window

```
GET /stars/{business}/{from}/{to}
```


## Spark Streaming

A Spark core app ingests Json files and converts Json to Avro messages in Kafka topics, e.g. review, business, user.
A Spark streaming consumer consumes data from Kafka, and then do the transformation and aggregation.
Select features and persist them into Cassandra.



## Akka Streams

The Akka Streams application ingests XML format posts by the file connector in Alpakka.
Akka streams producer app forms a real-time data pipeline to parse, enrich and transform XML files to Avro messages in a Kafka topic.

An Akka stream consumer app consumes avro messages from Kafka, and then persists data into Cassandra database.
The data flow DSL is shown below:

```
xmlSource ~> parsing ~> broadcast ~> enrich1 ~> merge ~> fillTitle ~> kafkaSink
                        broadcast ~> enrich2 ~> merge
```

The service layer provides RESTful APIs built by Akka-Http for users to easily interact with data for ad-hoc analytics.
Under the hood, the service calls Cassandra APIs to implement CRUD operations.


## CQRS & Event Sourcing

With the distributed guarantees of Exactly Once Processing, Event Driven Services supported by Apache Kafka become reliable, fast and nimble,
blurring the line between transactional business system and big data pipeline.
Please check my [Blog Post](http://alvincjin.blogspot.ca/2017/04/event-sourcing-and-cqrs.html) for the details of CQRS & Event Sourcing.
CDC(Chang Data Capture) is an approach to stream the database changes from binlogs to Kafka State Store.


## Machine Learning

### Alternating Least Squares (ALS) Algorithm
Collaborative Filtering Algorithms: Deciding that two users may both like the same song because they play many other same songs.
