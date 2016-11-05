
name := "Niagara"

version := "1.0.1"

//val spray = "1.3.3"

val spark = "2.0.1"

val akka = "2.4.11"

val kafka = "0.10.1.0"

scalaVersion := "2.11.8"



libraryDependencies ++= Seq(

//Spark
"org.apache.spark" %% "spark-streaming" % spark,
"org.apache.spark" %% "spark-core" % spark,
"org.apache.spark" %% "spark-sql" % spark,
//"org.apache.spark" %% "spark-streaming-kafka" % "1.6.2",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % spark,
  //Avro
  "com.databricks" % "spark-avro_2.10" % "3.0.1",
"org.apache.avro" % "avro" % "1.8.1",
"com.databricks" %% "spark-xml" % "0.3.3",

//Kafka
"org.apache.kafka" % "kafka-clients" % kafka,
"org.apache.kafka" %% "kafka" % kafka,
  //"io.confluent" % "kafka-avro-serializer" % "1.0.1",
//Test
"org.specs2" %% "specs2-core" % "3.6.4" % "test",
"org.specs2" %%  "specs2-junit" % "3.6.4" % "test",
//"org.specs2" %% "specs2" % "2.4.7",
"org.scalatest" %% "scalatest" % "2.2.6" % "test",
//Cassandra
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.2",
//Akka
"com.typesafe.akka" %% "akka-stream" % akka,
"com.typesafe.akka" %% "akka-http-experimental" % akka,
"com.typesafe.akka" %% "akka-http-spray-json-experimental" % akka,
"com.typesafe.akka" %% "akka-http-testkit-experimental" % "2.4.2-RC3"
//"io.gatling.highcharts" % "gatling-charts-highcharts" % "2.1.6"
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
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
"com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
)