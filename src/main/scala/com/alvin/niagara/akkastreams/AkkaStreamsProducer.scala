package com.alvin.niagara.akkastreams

import java.text.SimpleDateFormat

import akka.actor.ActorSystem
import java.nio.file.Paths

import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Framing, Sink}
import akka.util.ByteString
import com.alvin.niagara.common.{AvroProducer, Setting, Util}

/**
 * Created by jinc4 on 6/1/2016.
 *
 * An AkkaStreams app, which emits posts in avro format messages to Kafka
 * The posts are parsed and streamed from xml files
 * Then sent to Kafka by AvroProducer
 */

object AkkaStreamsProducer extends App with Setting {

    implicit val system = ActorSystem("AkkaStreamsProducer")
    import system.dispatcher

    implicit val materializer = ActorMaterializer()

    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val producer = new AvroProducer
    val filePath = Paths.get(inputPath)


    FileIO.fromPath(filePath)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
      .map(_.utf8String)
      .filter(_.contains("<row"))
      .mapConcat{line => Util.parseXml(line, sdf).toList}
      .runWith(Sink.foreach(producer.send(_)))
      .onComplete(_ => system.terminate())

}