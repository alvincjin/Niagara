
name := "Niagara"

version := "1.1.0"


lazy val versions = Map(
  "kafka" -> "0.10.2.0",
  //"confluent" -> "3.2.0",
  "spark" -> "2.1.0",
  "akka" -> "2.4.11"
)

scalaVersion := "2.11.8"



libraryDependencies ++= Seq(

  //Spark
  "org.apache.spark" %% "spark-streaming" % versions("spark"),
  "org.apache.spark" %% "spark-core" % versions("spark"),
  "org.apache.spark" %% "spark-sql" % versions("spark"),
  "org.apache.spark" %% "spark-mllib" % versions("spark"),
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % versions("spark"),


  //Avro
  "com.databricks" % "spark-avro_2.10" % "3.0.1",
  "org.apache.avro" % "avro" % "1.8.1",
  "com.twitter" %% "bijection-avro" % "0.9.5",

  //Kafka
  "org.apache.kafka" % "kafka-clients" % versions("kafka"),
  "org.apache.kafka" % "kafka-streams" % versions("kafka"),
  //"io.confluent" % "kafka-avro-serializer" % versions("confluent"),
  //"io.confluent" % "kafka-schema-registry-client" % versions("confluent"),

  //Test
  "org.specs2" %% "specs2-core" % "3.6.4" % "test",
  "org.specs2" %% "specs2-junit" % "3.6.4" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",

  //Cassandra
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.2",
  "com.tuplejump" %% "kafka-connect-cassandra" % "0.0.7",

  //Akka
  "com.typesafe.akka" %% "akka-stream" % versions("akka"),
  "com.typesafe.akka" %% "akka-http-experimental" % versions("akka"),
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % versions("akka"),
  "com.typesafe.akka" %% "akka-http-testkit-experimental" % "2.4.2-RC3",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.13",
  "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % "0.6",
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.6",

  "org.scala-lang.modules" %% "scala-xml" % "1.0.6",
  "com.typesafe.slick" %% "slick" % "3.1.0",
  "mysql" % "mysql-connector-java" % "5.1.34",
  "com.github.t3hnar" %% "scala-bcrypt" % "2.4",
  "com.jason-goodwin" %% "authentikat-jwt" % "0.4.3",
  "com.github.nscala-time" %% "nscala-time" % "2.4.0",
  "com.danielasfregola" %% "twitter4s" % "5.0"
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case x => MergeStrategy.first
}

scalacOptions := Seq("-unchecked", "-deprecation", "-Xexperimental")


resolvers ++= Seq(
  "confluent" at "http://packages.confluent.io/maven/",
  "Typesafe Simple Repository" at "http://repo.typesafe.com/typesafe/simple/maven-releases/"
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
)