package com.alvin.niagara.sparkservice

import akka.actor.{ActorContext, Actor, ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import scala.concurrent.duration.DurationInt


/**
 * Created by JINC4 on 6/4/2016.
 *
 * A Spray server starts Spark service
 */
object SparkServiceBootstrap extends App {

  implicit val actorSystem = ActorSystem("spark-services")
  implicit val timeout = 60 seconds

  val service = actorSystem.actorOf(Props[SparkServices], "spark-services")

  // start a new HTTP server on port 80 with our service actor as the handler
  IO(Http) ! Http.Bind(service, "0.0.0.0", port = 80)

}

class SparkServices extends Actor with SparkService {

  def actorRefFactory: ActorContext = context

  def receive: Actor.Receive = runRoute(sparkRoutes)
}
