package com.alvin.niagara.cqrs

import java.util.UUID

import akka.actor.ActorSystem
import akka.util.Timeout
import com.alvin.niagara.cqrs.domain.Order
import com.alvin.niagara.cqrs.domain.Order.UserCommand
import akka.pattern.ask
import scala.concurrent.duration._
/**
  * Created by alvinjin on 2017-05-03.
  */
object OrderApp extends App {

  implicit lazy val system = ActorSystem("cqrs-activator")
  implicit val executionContext = system.dispatcher
  implicit val timeout: Timeout = 2 seconds

  lazy val order = system.actorOf(Order.props(100), "Order")
  loadData()

  def loadData() = {
    val orderId = UUID.randomUUID().toString

    (order ? Order.InitializeOrder("Alvin")).onSuccess { case m ⇒ println(m) }
    (order ? Order.AddItem(1, "T-Shirt", 12.50)).onSuccess { case m ⇒ println(m) }
    (order ? Order.AddItem(2, "Sweater", 27.95)).onSuccess { case m ⇒ println(m) }
    (order ? Order.SubmitOrder).onSuccess { case m ⇒ println(m) }

  }
}
