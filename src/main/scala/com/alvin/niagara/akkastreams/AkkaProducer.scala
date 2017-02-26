package com.alvin.niagara.akkastreams

/**
  * Created by alvinjin on 2017-02-17.
  */

import java.nio.file.{FileSystems, Paths}
import java.text.SimpleDateFormat

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.alpakka.file.scaladsl.FileTailSource
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import akka.stream.{ActorMaterializer, scaladsl}

import scala.concurrent.Future
import akka.{Done, NotUsed}

import scala.concurrent.duration._
import com.alvin.niagara.common.{Post, Setting, Util}

import scala.util.{Failure, Success}

trait AkkaProducer extends Setting {

  val system = ActorSystem("AkkaProducer")

  val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers(brokerList)

  val kafkaProducer = producerSettings.createKafkaProducer()

  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer.create(system)

  def terminateWhenDone(result: Future[Done]): Unit = {
    result.onComplete {
      case Failure(e) =>
        system.log.error(e, e.getMessage)
        system.terminate()
      case Success(_) => system.terminate()
    }
  }

}


object XMLFileAkkaProducer extends App with AkkaProducer {

  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

  val done = FileTailSource.lines(
    path = FileSystems.getDefault.getPath(inputPath),
    maxLineSize = 88192,
    pollingInterval = 250.millis
  ).filter(_.contains("<row"))
    .mapConcat { line => Util.parseXml(line, sdf).toList }
    .map(post => new ProducerRecord[String, Array[Byte]](topic, Post.serializeToAvro(post)))
    .runWith(Producer.plainSink(producerSettings, kafkaProducer))

  terminateWhenDone(done)

}


