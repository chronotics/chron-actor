package org.chronotics.actor.scala.akka.single

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.chronotics.actor.scala.akka.single.WebsocketMessageActor.{Join, WSMessage}
import org.chronotics.silverbullet.scala.akka.util.EnvConfig

import scala.collection.immutable.HashMap
import scala.util.control.Breaks._

object WebsocketMessageActor {
  def props: Props = Props(classOf[WebsocketMessageActor])
  case class Join(userName: String)
  case object Left
  case class WSMessage(strMessage: String)
}

object SystemMesage {
  def apply(text: String) = WSMessage(text)
}

class WebsocketMessageActor extends Actor with ActorLogging {
  import WebsocketMessageActor._
  import context.dispatcher
  implicit val materializer = ActorMaterializer()

  //val log = LoggerFactory.getLogger(getClass());
  var arrConnections: Set[ActorRef] = Set.empty
  var mapUser: HashMap[ActorRef, String] = HashMap.empty

  val config = ConfigFactory.parseFile(EnvConfig.getConfigFile("z2_conn")).resolve()
  val topic = config.getString("kafka.topic")
  var consumerSettings = ConsumerSettings(context.system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(config.getString("kafka.broker"))
    .withGroupId(config.getString("kafka.groupid"))

  consumeKafkaMesage()

  override def receive: Receive = {
    case joiner: Join => {
      arrConnections += sender()
      mapUser += (sender() -> joiner.userName)
      broadcastMsg(SystemMesage("New joiner: " + sender() + " - user: " + joiner.userName + "|" + arrConnections.size))
    }

    case Left => {
      arrConnections -= sender()
      mapUser -= sender()
      broadcastMsg(SystemMesage("Left: " + sender() + "|" + arrConnections.size))
    }

    case Terminated(ref) => {
      arrConnections -= ref
      mapUser -= ref
      broadcastMsg(SystemMesage("Left: " + sender() + "|" + arrConnections.size))
    }

    case msg: WSMessage => {
      //broadcastMsg(msg)
    }
  }

  def consumeKafkaMesage() = {
    Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
      .map(msg => {
        try {
          var strMessage = msg.record.value();

          if (strMessage != null && !strMessage.isEmpty()) {
            if (strMessage.length > 1000) {
              strMessage = strMessage.substring(0, 1000) + "..."
            }

            broadcastMsg(WSMessage(strMessage))
          }
        } catch {
          case e: Throwable => log.error("ERR", e)
        }
      })
      .runWith(Sink.ignore)
  }

  def broadcastMsg(msg: WSMessage) = {
    var arrActorRefUser: Seq[ActorRef] = Seq.empty
    var arrActorRefAdmin: Seq[ActorRef] = Seq.empty
    var arrMsg = msg.strMessage.split("\\]")
    var bIsBroadcast: Boolean = true

    if (arrMsg != null && arrMsg.length > 0 && !msg.strMessage.contains("Left: ")) {
      if (arrMsg(0).contains("-")) {
        var arrUserInfo = arrMsg(0).replace("[", "").split("\\-")

        if (arrUserInfo != null && arrUserInfo.length >= 2) {
          var strCallback = arrUserInfo(0)
          var strUserId = arrUserInfo(1)

          breakable {
            for ((curActor, curUser) <- mapUser) {
              if (strUserId.equals(curUser)) {
                arrActorRefUser = arrActorRefUser :+ curActor

                if (strUserId.equals("admin")) {
                  arrActorRefAdmin = arrActorRefAdmin :+ curActor
                }
              }

              if (curUser.equals("admin") && !strUserId.equals("admin")) {
                arrActorRefAdmin = arrActorRefAdmin :+ curActor
              }
            }
          }
        }
      }
    }

    if (arrActorRefUser != null && arrActorRefAdmin != null
      && arrActorRefUser.length > 0 && arrActorRefAdmin.length > 0) {
      bIsBroadcast = false

      arrActorRefAdmin.foreach(_ ! msg)
      arrActorRefUser.foreach(_ ! msg)
    }

    if (bIsBroadcast) {
      arrConnections.foreach(_ ! msg)
    }
  }
}
