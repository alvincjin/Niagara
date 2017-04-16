package com.alvin.niagara.kafkastream

import javax.ws.rs.NotFoundException
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import org.apache.kafka.streams.{KafkaStreams, KeyValue}
import org.apache.kafka.streams.state._
import java.lang.{Double => JDouble, Long => JLong}
import scala.collection.mutable.ListBuffer
import akka.http.scaladsl.server.Directives._
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._


case class Pair(key: String, value: Long)

trait AkkaJSONProtocol extends DefaultJsonProtocol {
  implicit val entryFormat = jsonFormat2(Pair.apply)

}

class InteractiveQueryService(streams: KafkaStreams) extends AkkaJSONProtocol {

  implicit val actorSystem = ActorSystem("IQ-service")
  implicit val materializer = ActorMaterializer()

  val route =
    path("stars" / Segment) { city =>
      get {

        val count: Long = querybyKey(KStreamApp.STARS_CITY_STORE, city)
        complete(count.toString)
      }

    } ~
      path("stars" / Segment / LongNumber / LongNumber) { (city, from, to) =>
        get {

          val results: List[Pair] = windowedByKey(KStreamApp.STARS_WINDOWED_STORE, city, from, to)
          complete(results)
        }

      }

  def start = {

    import actorSystem.dispatcher
    //ExecutionContext
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
  }

  def querybyKey(storeName: String, key: String): Long = {
    val store: ReadOnlyKeyValueStore[String, Long] = streams.store(storeName, QueryableStoreTypes.keyValueStore[String, Long])
    if (store == null) {
      throw new NotFoundException
    }

    store.get(key)

  }

  //The time range is inclusive and applies to the starting timestamp of the window.
  def windowedByKey(storeName: String, key: String, from: Long, to: Long): List[Pair] = {

    val store: ReadOnlyWindowStore[String, Long] = streams.store(storeName, QueryableStoreTypes.windowStore[String, Long])

    if (store == null) {
      throw new NotFoundException
    }

    val results: WindowStoreIterator[Long] = store.fetch(key, from, to)
    val windowResults = new ListBuffer[Pair]()

    while (results.hasNext) {
      {
        val next: KeyValue[JLong, Long] = results.next
        windowResults += Pair(key, next.value)
      }
    }

    windowResults.toList
  }

}

