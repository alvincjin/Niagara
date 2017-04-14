package com.alvin.niagara.kafkastream

import javax.ws.rs.NotFoundException

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}


/**
  * Created by JINC4 on 6/4/2016.
  *
  * An Akka-Http server starts Spark service
  */
class InteractiveQueryService(streams: KafkaStreams) {

  implicit val actorSystem = ActorSystem("REST-service")
  implicit val materializer = ActorMaterializer()

  val route =
    path("stars" / Segment) { city =>
      get {

        val count: Long = querybyKey(KStreamApp.STARS_CITY_STORE, city)
        complete(count.toString)
      }

    }

  def start = {

    import actorSystem.dispatcher//ExecutionContext
    // start the server
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

  }

  def querybyKey(storeName: String, key: String): Long = {
    val store: ReadOnlyKeyValueStore[String, Long] = streams.store(storeName, QueryableStoreTypes.keyValueStore[String, Long])
    if (store == null) {
      throw new NotFoundException
    }

    store.get(key)

  }

}

