package org.chronotics.actor.scala.akka.single

import akka.actor.{Actor, ActorLogging, ActorRef}
import org.chronotics.actor.scala.akka.single.WebsocketMessageActor.{Join, WSMessage}

object WebsocketClientActor {
  case class Connected(outgoing: ActorRef, userName: String = "admin")
  case class IncomingMessage(text: String)
  case class OutgoingMessage(text: String)
}

class WebsocketClientActor(room: ActorRef) extends Actor with ActorLogging {
  import WebsocketClientActor._

  override def receive: Receive = {
    case Connected(outgoing, userName) => context.become(connected(outgoing, userName))
  }

  def connected(outgoing: ActorRef, userName: String): Receive = {
    room ! Join(userName)

    {
      case IncomingMessage(text) => {
        log.debug("New incoming msg: {}", text)
        room ! WSMessage(text)
      }

      case WSMessage(text) => {
        log.debug("New outgoing msg: {}", text)
        outgoing ! OutgoingMessage(text)
      }
    }
  }
}
