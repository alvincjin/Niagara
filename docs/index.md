# Niagara Data Platform <iframe src="https://ghbtns.com/github-btn.html?user=AlvinCJin&repo=Niagara&size=large" frameborder="0" scrolling="0" width="160px" height="30px"></iframe>


Niagara is a Fast & Big Data Processing, Machine Learning, and Data-as-a-Service platform, implemented in Scala with SDACK stack.
It is built on complicated public data sets to evaluate emerging Stateful Stream Processing to build lightweight Streaming Services.

## SDACK Tech Stack

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