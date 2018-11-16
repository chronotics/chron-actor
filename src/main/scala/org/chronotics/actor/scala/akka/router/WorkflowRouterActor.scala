package org.chronotics.actor.scala.akka.router

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import org.chronotics.actor.scala.akka.single.StatusManagerActor
import org.chronotics.silverbullet.scala.akka.protocol.Message._

import scala.collection.immutable.HashMap
import scala.util.control.Breaks._

object WorkflowRouterActor {
  def props(pId: Int): Props = {
    Props(classOf[WorkflowRouterActor], pId)
  }
}

class WorkflowRouterActor(pID: Int) extends Actor with ActorLogging {
  var workFlowMap: HashMap[Int, String] = HashMap.empty
  var msgCount = 0
  var listWorker = Seq.empty[String]
  val managerActor: ActorRef = context.actorOf(Props(classOf[StatusManagerActor]))

  override def preStart(): Unit = {
    super.preStart()
  }

  override def postStop(): Unit = {
    super.postStop()
  }

  override def receive: Receive = {
    case work: AskWorkFlowPath => {

      if (workFlowMap.contains(work.workFlowId)) {
        log.info(s"${work.workFlowId} is existing")
        sender() ! workFlowMap(work.workFlowId)
      }
      else {
        sender() ! ""
      }
    }

    case ListWorkflowPath => {
      sender() ! workFlowMap
    }

    case update: UpdateWorkFlowActorPath => {
      workFlowMap += (update.workFlowId -> update.path)
      log.info(s"Received new workflow ${update.workFlowId} actor: ${update.path}. Number of workflow actor now is ${workFlowMap.size}")
    }

    case remove: RemoveWorkFlowActorPath if workFlowMap.contains(remove.workFlowId) => {
      workFlowMap -= remove.workFlowId
      log.info(s"Removed new workflow ${remove.workFlowId} actor. Number of workflow actor now is ${workFlowMap.size}")
    }

    case health: Alive => {
      workFlowMap.foreach { curFlow =>
        context.system.actorSelection(curFlow._2) ! Alive(health.strMsg)
      }
    }

    case fo: Failover => {
      sender() ! Pong.toString
    }

    case stop: StopActor => {
      if (stop.strType == "proc") {
        breakable {
          workFlowMap.foreach { curFlow =>
            if (curFlow._1.toString == stop.id) {
              context.system.actorSelection(curFlow._2) ! StopActor(stop.id, stop.strType)

              break
            }
          }
        }
      }
      else {
        workFlowMap.foreach { curFlow =>
          context.system.actorSelection(curFlow._2) ! StopActor(stop.id, stop.strType)
          break
        }
      }
    }

    case Terminated(ref) => {
      context unwatch ref
    }
  }
}
