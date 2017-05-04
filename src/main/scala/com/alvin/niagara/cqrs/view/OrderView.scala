package com.alvin.niagara.cqrs.view

import akka.actor.Props
import akka.persistence.PersistentView
import com.alvin.niagara.cqrs.domain.Order.Event
import com.alvin.niagara.cqrs.view.OrderView.Envelope

/**
  * Created by alvinjin on 2017-05-03.
  */

object OrderView {
  case class Envelope(orderId: String, event: Event)

  def props(orderId: String): Props = Props(new OrderView(orderId))
}

class OrderView(orderId: String) extends PersistentView {

  override def persistenceId: String = s"order_$orderId"
  override def viewId: String = s"$persistenceId-view"

  override def receive: Receive = {
    case msg: Event => context.parent forward(Envelope(orderId, msg))
  }
}
