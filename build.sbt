name := "Niagra"

version := "1.0"

val spray = "1.3.3"

val spark = "1.6.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
"org.apache.spark" % "spark-streaming_2.10" % spark,
"org.apache.spark" % "spark-core_2.10" % spark,
"org.apache.spark" % "spark-sql_2.10" % spark,
"org.apache.spark" % "spark-hive_2.10" % spark,
"org.apache.spark" % "spark-streaming-kafka_2.10" % spark,
"org.apache.avro" % "avro" % "1.7.7",
"org.apache.kafka" % "kafka-clients" % "0.10.0.0",
"org.apache.kafka" % "kafka_2.10" % "0.10.0.0",
"org.specs2" %% "specs2-core" % "3.6.4" % "test",
"io.confluent" % "kafka-avro-serializer" % "1.0.1",
"com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0",
"com.databricks" %% "spark-avro" % "2.0.1",
"org.scalatest" % "scalatest_2.10" % "2.0" % "test",
"org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
"com.typesafe.akka" % "akka-stream-experimental_2.10" % "2.0",
"com.typesafe.akka" % "akka-http-experimental_2.10" % "2.0",
"io.spray" %% "spray-can" % spray,
  "io.spray" %% "spray-client" % spray,
"io.spray" %% "spray-routing" % spray,
"io.spray" %% "spray-testkit" % spray,
  "io.spray" %% "spray-json" % "1.3.1"
)

