package com.alvin.niagara.akkastreams

/**
  * Created by alvinjin on 2017-02-17.
  */

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import akka.stream.alpakka.cassandra.scaladsl.CassandraSink
import com.alvin.niagara.cassandra.CassandraConfig
import com.alvin.niagara.common.{Post, Setting}
import com.datastax.driver.core.{Cluster, PreparedStatement}
import java.lang.{Long => JLong}

trait AkkaConsumer extends Setting {

  val system = ActorSystem("AkkaConsumer")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer.create(system)

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(brokerList)
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers(brokerList)

  val kafkaProducer = producerSettings.createKafkaProducer()

  implicit val session = Cluster.builder
    .addContactPoint(CassandraConfig.hosts(0))
    .withPort(CassandraConfig.port)
    .build
    .connect()



  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }
}


object Akka2CassandraConsumer extends App with AkkaConsumer {

  type postType = (JLong, Integer, JLong)

  val preparedStatement = session.prepare(
    "INSERT INTO test.post1(postid, typeid, creationdate) VALUES (?,?,?)"
  )
  val statementBinder = (post: postType, statement: PreparedStatement) => statement.bind(post._1, post._2, post._3)
  val cassandraSink = CassandraSink[postType](parallelism = 2, preparedStatement, statementBinder)

  val done = Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
    .map(msg => Post.deserializeToClass(msg.record.value()))
    .map { p => (p.postid: JLong, p.typeid: Integer, p.creationdate: JLong) }
    .runWith(cassandraSink)

  terminateWhenDone(done)

}




