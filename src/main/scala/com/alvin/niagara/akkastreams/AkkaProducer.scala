package com.alvin.niagara.akkastreams

/**
  * Created by alvinjin on 2017-02-17.
  */

import java.nio.file.Paths
import java.text.SimpleDateFormat

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{FileIO, Framing}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import akka.stream.ActorMaterializer

import scala.concurrent.Future
import akka.Done
import akka.util.ByteString
import com.alvin.niagara.common.{Post, Setting, Util}

import scala.util.{Failure, Success}

trait AkkaProducer extends Setting {

  val system = ActorSystem("example")

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


object PlainSinkWithAkkaProducer extends App with AkkaProducer {

  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  val filePath = Paths.get(inputPath)

  val done = FileIO.fromPath(filePath)
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
    .map(_.utf8String)
    .filter(_.contains("<row"))
    .mapConcat { line => Util.parseXml(line, sdf).toList }
    .map(post => new ProducerRecord[String, Array[Byte]](topic, Post.serializeToAvro(post)))
    .runWith(Producer.plainSink(producerSettings, kafkaProducer))

  terminateWhenDone(done)

}


