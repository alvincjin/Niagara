package com.alvin.niagara.sparkservice

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http

import scala.concurrent.duration.DurationInt

object SparkServiceBootstrap extends App {

  // we need an ActorSystem to host our application in
  implicit val actorSystem = ActorSystem("spark-services")
  implicit val timeout = 60 seconds

  // create and start our service actor
  val service = actorSystem.actorOf(Props[SparkServices], "spark-services")

  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ! Http.Bind(service, "0.0.0.0", port = 80)

}
