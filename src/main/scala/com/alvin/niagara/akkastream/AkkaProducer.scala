package com.alvin.niagara.akkastream

import java.nio.file.FileSystems

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.alpakka.file.scaladsl.FileTailSource
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, RunnableGraph}
import com.alvin.niagara.config.Config
import com.alvin.niagara.model.{Post, PostSede, RichPost}
import com.alvin.niagara.util.XmlParser

import scala.concurrent.duration._



/**
  * Created by alvin.jin on 3/2/2017.
  */

trait AkkaProducer extends Config {


  val system = ActorSystem("AkkaProducer")

  val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers(brokerList)

  val kafkaProducer = producerSettings.createKafkaProducer()

  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer.create(system)

}


object XmlFileAkkaProducer extends App with AkkaProducer {


  val xmlSource = FileTailSource.lines(
    path = FileSystems.getDefault.getPath(stackInputPath),
    maxLineSize = 88888,
    pollingInterval = 250.millis)

  val kafkaSink = Producer.plainSink(producerSettings, kafkaProducer)

  val flowGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>

      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[Post](2))
      val merge = builder.add(Merge[RichPost](2))

      val parser = Flow[String].filter(_.contains("<row"))
        .mapConcat{ line => XmlParser.parseXml(line).toList }

      val enrichType1 = Flow[Post]
        .filter(_.typeid == 1)
        .map { p => RichPost(p.postid, "Question", p.title, p.creationdate)}

      val enrichType2 = Flow[Post]
        .filter(_.typeid == 2)
        .map { p => RichPost(p.postid, "Answer", p.title, p.creationdate)}

      val fillTitle = Flow[RichPost]
        .map { p => p.title match {
          case t: String if t != "" => p.copy(title = t.toUpperCase())
          case _ => p.copy(title = "no subject")
        }}
        .map { post => new ProducerRecord[String, Array[Byte]](postTopic, PostSede.serialize(post))}


      xmlSource ~> parser ~> broadcast ~> enrichType1 ~> merge ~> fillTitle ~> kafkaSink
                             broadcast ~> enrichType2 ~> merge

      ClosedShape
    }
  )

  flowGraph.run()


}


