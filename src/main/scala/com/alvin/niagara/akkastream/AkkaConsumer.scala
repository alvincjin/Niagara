package com.alvin.niagara.akkastream

import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.Done
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import akka.stream.alpakka.cassandra.scaladsl.CassandraSink
import com.datastax.driver.core.PreparedStatement
import java.lang.{Long => JLong}

import com.alvin.niagara.cassandra.CassandraDao
import com.alvin.niagara.common.Setting
import com.alvin.niagara.model.PostSede


/**
  * Created by alvin.jin on 3/2/2017.
  */

trait AkkaConsumer extends Setting {

  val system = ActorSystem("AkkaConsumer")
  implicit val ec = system.dispatcher
  implicit val m = ActorMaterializer.create(system)

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(brokerList)
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }

}


object CassandraAkkaConsumer extends App with AkkaConsumer with CassandraDao {

  type postType = (JLong, String, String, JLong)
  //Create keyspace and table if not exist
  createTables(keyspace, table)

  val insertStmt = session.prepare(
    s"INSERT INTO $keyspace.$table(postid, posttype, title, creationdate) VALUES (?,?,?,?)"
  )

  val statementBinder = (post: postType, statement: PreparedStatement) => statement.bind(post._1, post._2, post._3, post._4)
  val cassandraSink = CassandraSink[postType](parallelism = 2, insertStmt, statementBinder)

  val flow = Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
    .map(msg => PostSede.deserialize(msg.record.value()))
    .map { p => (p.postid: JLong, p.posttype: String, p.title: String, p.creationdate: JLong)}
    .runWith(cassandraSink)

  terminateWhenDone(flow)

}




