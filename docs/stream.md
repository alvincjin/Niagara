# Streaming Frameworks

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



## Spark Streaming

A Spark core app ingests Json files and converts Json to Avro messages in Kafka topics, e.g. review, business, user.
A Spark streaming consumer consumes data from Kafka, and then do the transformation and aggregation.
Select features and persist them into Cassandra.