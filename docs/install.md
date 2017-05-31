## Installation

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