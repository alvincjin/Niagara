name := "Niagara"

version := "1.0"

val spray = "1.3.3"

val spark = "1.6.1"

val akka = "2.0.4"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
//Spark
"org.apache.spark" %% "spark-streaming" % spark,
"org.apache.spark" %% "spark-core" % spark,
"org.apache.spark" %% "spark-sql" % spark,
//"org.apache.spark" %% "spark-hive" % spark,
"org.apache.spark" %% "spark-streaming-kafka" % spark,
//Avro
"com.databricks" %% "spark-avro" % "2.0.1",
"org.apache.avro" % "avro" % "1.7.7",
"com.databricks" %% "spark-xml" % "0.3.3",
//Kafka
"org.apache.kafka" % "kafka-clients" % "0.10.0.0",
"org.apache.kafka" %% "kafka" % "0.10.0.0",
"io.confluent" % "kafka-avro-serializer" % "1.0.1",
//Test
"org.specs2" %% "specs2-core" % "3.6.4" % "test",
"org.specs2" %%  "specs2-junit" % "3.6.4" % "test",
//"org.specs2" %% "specs2" % "2.4.7",
"org.scalatest" %% "scalatest" % "2.2.6" % "test",
//Cassandra
"com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0",
//Akka
"com.typesafe.akka" %% "akka-stream-experimental" % akka,
"com.typesafe.akka" %% "akka-http-experimental" % akka,
"com.typesafe.akka" %% "akka-http-spray-json-experimental" % akka,
"com.typesafe.akka" %% "akka-http-testkit-experimental" % akka,
"io.gatling.highcharts" % "gatling-charts-highcharts" % "2.1.6"
//Spray
//"io.spray" %% "spray-can" % spray,
//"io.spray" %% "spray-client" % spray,
//"io.spray" %% "spray-routing" % spray,
//"io.spray" %% "spray-testkit" % spray,
//"io.spray" %% "spray-json" % "1.3.1",
//Json
//"org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13"
)

resolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)