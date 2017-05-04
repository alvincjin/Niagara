package com.alvin.niagara.cqrs.domain

import akka.actor.{ActorLogging, Props, Status}
import akka.persistence.PersistentActor

/**
  * Created by alvinjin on 2017-05-02.
  */

object Order {

  sealed trait Command

  case class AddItem(quantity: Int, productName: String, pricePerItem: Double) extends Command
  case object SubmitOrder extends Command
  case class InitializeOrder(username: String) extends Command
  case object StopOrder extends Command

  case class UserCommand(orderId: String, cmd: Command)

  abstract class CommandException(msg: String) extends Exception(msg)
  case class OrderSubmittedException(orderId: String)
    extends CommandException(s"Can't make changes. Order is submitted: $orderId")

  case class ExceedBudgetException(orderCost: Double, budget: Double)
    extends CommandException(s"Can't add this item. It exceeds the budget $budget. Current order cost $orderCost")
  case class UnknownOrderException() extends CommandException("Unknown order")

  sealed trait Event
  case class ItemAdded(quantity: Int, productName: String, pricePerItem: Double) extends Event
  case object OrderSubmitted extends Event
  case object OrderInitialized extends Event

  case class InitializedOrderAck(id: String, username: String)
  //def persistenceId(orderId: String): String = s"order_$orderId"

  def props(budget: Double): Props = Props(new Order(budget))

}

class Order(budget: Double) extends PersistentActor with ActorLogging {

  import Order._

  val orderId = self.path.name
  override def persistenceId: String = s"order_$orderId"

  var orderPrice: Double = 0.0


  def updateState(event: Event): Unit = event match {
    case OrderInitialized =>
      context become initialized
    case ItemAdded(quantity, productName, pricePerItem) =>
      log.debug(s"Update: $persistenceId")
      orderPrice += quantity * pricePerItem
    case OrderSubmitted =>
      context become submitted

  }


  def uninitialized: Receive = {
    case InitializeOrder(username) =>
      persist(OrderInitialized){ event =>
        log.debug(s"Order initialized {}", persistenceId)
        updateState(event)
        sender ! InitializedOrderAck(orderId, username)
      }
    case StopOrder =>
      context stop self
    case _ => sender ! Status.Failure(UnknownOrderException())
  }


  def initialized: Receive = {
    case AddItem(quantity, productName, pricePerItem) if (orderPrice + quantity*pricePerItem) <= budget =>
      persist(ItemAdded(quantity, productName, pricePerItem)){event =>
        log.debug(s"Item Added {} to {}", persistenceId, event)
        sender ! Status.Success
        updateState(event)
      }
    case AddItem(quantity, productName, pricePerItem) =>
      log.error("Try to add more items when order exceeds budget")
      sender ! Status.Failure(ExceedBudgetException(orderPrice, budget))
    case SubmitOrder if orderPrice > 0 =>
      log.info(s"Order submitted {}", orderId)
      persist(OrderSubmitted){ event =>
        sender ! Status.Success
        updateState(event)
      }
    case StopOrder => context stop self
  }

  def submitted: Receive = {
    case StopOrder => context stop self
    case msg =>
      log.error("Order is completed. Can't accept {}", msg)
      sender ! Status.Failure(OrderSubmittedException(orderId))
  }

  override def receiveCommand: Receive = uninitialized

  override def receiveRecover: Receive = {

    case event: Event =>
      log.debug("Receive recover message: {}", event)
      updateState(event)
  }







}
