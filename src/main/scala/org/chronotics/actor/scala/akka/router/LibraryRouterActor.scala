package org.chronotics.actor.scala.akka.router

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import org.chronotics.actor.scala.akka.single.StatusManagerActor
import org.chronotics.silverbullet.scala.akka.protocol.Message._

import scala.collection.concurrent.Map
import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._
import scala.util.control.Breaks._

object LibraryRouterActor {
  def props(pId: Int): Props = {
    Props(classOf[LibraryRouterActor], pId)
  }
}

class LibraryRouterActor(pID: Int) extends Actor with ActorLogging {
  var mapAlibPath: Map[String, Map[Int, String]] = new ConcurrentHashMap[String, Map[Int, String]].asScala
  var mapAlibMax: HashMap[String, Int] = HashMap.empty
  var msgCount = 0
  val managerActor: ActorRef = context.actorOf(Props(classOf[StatusManagerActor]))

  override def preStart(): Unit = {
    super.preStart()
  }

  override def postStop(): Unit = {
    super.postStop()
  }

  override def receive: Receive = {
    case askAlib: AskAlibPath => {
      log.info(s"Received ask alib message ${askAlib.libType}")

      if (mapAlibPath.contains(askAlib.libType) && mapAlibPath(askAlib.libType).size > 0) {
        log.info(s"${askAlib.libType} is existing")

        try {
          val path = mapAlibPath(askAlib.libType).head._2
          log.info("Path of " + askAlib.libType + ": " + path);
          sender() ! path
        } catch {
          case ex: Exception => {
            sender() ! ""
          }
        }
      } else {
        sender() ! ""
      }
    }

    case checkAlib: CheckAlib => {
      if (mapAlibMax.contains(checkAlib.libType)) {
        sender ! "1"
      } else {
        sender ! "0"
      }
    }

    case ListAlibPath => {
      sender() ! mapAlibPath
    }

    case update: UpdateAlibActorPath => {
      log.info(s"Received new alib actor ${update.libType} actor: ${update.path}")

      if (!mapAlibPath.contains(update.libType)) {
        log.info("There is no execution actor ref of " + update.libType)

        var intNewId: Int = mapAlibMax.contains(update.libType) match {
          case true => mapAlibMax(update.libType) + 1
          case false => 1
        }

        log.info("Not exist - Increased to: " + intNewId)

        var tempMap: Map[Int, String] = new ConcurrentHashMap[Int, String].asScala
        tempMap.put(intNewId, update.path)

        mapAlibPath.put(update.libType, tempMap)
        mapAlibMax = mapAlibMax + (update.libType -> intNewId)
      } else {
        var intNewId = 1

        if (mapAlibMax.contains(update.libType)) {
          intNewId = mapAlibMax(update.libType) + 1
        }

        if (mapAlibPath(update.libType) != null && mapAlibPath(update.libType).size > 0) {
          breakable {
            while (mapAlibPath(update.libType).contains(intNewId)) {
              intNewId += 1

              if (!mapAlibPath(update.libType).contains(intNewId)) {
                break
              }
            }
          }
        }

        log.info("Increased to: " + intNewId)

        if (mapAlibPath(update.libType) == null) {
          mapAlibPath(update.libType) = new ConcurrentHashMap[Int, String].asScala
        }

        mapAlibPath(update.libType).put(intNewId, update.path)
        mapAlibMax = mapAlibMax + (update.libType -> intNewId)
      }
    }

    case remove: RemoveAlibActorPath => {
      log.info("===Prepare to remove " + remove.libType + " - " + remove.path)
      removeAlibActorPath(remove.libType, remove.path)
    }

    case kill: KilledAlibActor => {
      log.info("===Prepare to kill " + kill.libType + " - " + kill.path)
      removeAlibActorPath(kill.libType, kill.path, true)
    }

    case CheckAlibActorRunning => {
      sender() ! false //isOtherRunning
    }

    case fo: Failover => {
      sender() ! Pong.toString
    }

    case updateAlibMeta: UpdateAlibMetaData => {
      log.debug(s"Update ${updateAlibMeta.libId} meta-data")

      try {
        if (mapAlibPath.contains(updateAlibMeta.libId.toString)) {
          var arrCurAlib: Map[Int, String] = mapAlibPath.get(updateAlibMeta.libId.toString) match {
            case Some(map) => map.asJava.asScala
            case None => new ConcurrentHashMap[Int, String].asScala
          }

          arrCurAlib.foreach(curItem => {
            var curPath = curItem._2

            context.system.actorSelection(curPath) ! UpdateAlibMetaData(updateAlibMeta.libId)
          })
        }
      } catch {
        case e: Throwable => log.error(e, "ERR");
      }
    }

    case health: Alive => {
      mapAlibPath.foreach { curType =>
        curType._2.foreach { curLib =>
          context.system.actorSelection(curLib._2) ! Alive(health.strMsg)
        }
      }
    }
    case Terminated(ref) => {
      context unwatch ref
    }
  }

  def killAlibActor(libType: String, actorPath: String) {
    if (mapAlibPath.contains(libType)) {
      var intNumInstance = 0;

      intNumInstance = mapAlibPath(libType).size

      log.info("NumInstance of " + libType + ": " + intNumInstance)

      if (intNumInstance <= 0) {
        mapAlibMax = mapAlibMax - libType
      }
    } else {
      log.info("No more instance of " + libType + " - Remove")
      mapAlibMax = mapAlibMax - libType
    }
  }

  def removeAlibActorPath(libType: String, actorPath: String, isKill: Boolean = false) = {
    log.info(s"Removed alib actor ${libType} actor")

    if (mapAlibPath.contains(libType)) {
      log.debug(s"${libType} actor existing")

      var intInstanceId = 0;

      mapAlibPath(libType).foreach { item =>
        if (item._2.equals(actorPath)) {
          intInstanceId = item._1
        }
      }

      if (intInstanceId > 0) {
        mapAlibPath(libType).remove(intInstanceId, actorPath)
      }
    }

    if (isKill) {
      killAlibActor(libType, actorPath)
    }
  }
}
