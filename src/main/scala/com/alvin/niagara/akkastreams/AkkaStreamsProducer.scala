package com.alvin.niagara.akkastreams

import java.io.File
import java.text.SimpleDateFormat

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl.{Sink, FileIO}
import akka.util.ByteString
import com.alvin.niagara.common.{Util, Settings}
import com.alvin.niagara.sparkstreaming.AvroDefaultEncodeProducer

/**
 * Created by jinc4 on 6/1/2016.
 */

object AkkaStreamsProducer extends Settings {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("AkkaStreamProducer")
    import system.dispatcher

    implicit val materializer = ActorMaterializer()

    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    val producer = new AvroDefaultEncodeProducer
    val filePath = new File(inputPath)

    FileIO.fromFile(filePath)
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
      .map(_.utf8String)
      .filter(_.contains("<row"))
      .mapConcat {line => Util.parseXml(line, sdf)}
      .runWith(Sink.foreach(producer.send(_)))
      .onComplete(_ => system.shutdown())
  }
}