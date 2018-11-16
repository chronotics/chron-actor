package org.chronotics.actor.scala.akka.router

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.pattern._
import akka.remote.DisassociatedEvent
import akka.routing.Broadcast
import akka.util.Timeout
import org.chronotics.actor.scala.akka.single.StatusManagerActor
import org.chronotics.pandora.java.exception.ExceptionUtil
import org.chronotics.silverbullet.scala.akka.protocol.Message._
import org.chronotics.silverbullet.scala.akka.state.{Initializing, Started, Stopped}
import org.chronotics.silverbullet.scala.akka.util.RemoteAddressExtension

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class RouterProxyActor(val props: Props, val name: String, val pID: Int) extends Actor with ActorLogging {
  var router: ActorRef = context.actorOf(props, name)
  val managerActor: ActorRef = context.actorOf(Props(classOf[StatusManagerActor]))

  val remoteAddr = RemoteAddressExtension(context.system).address
  val strRemotePath = self.path.toStringWithAddress(remoteAddr)

  override def preStart(): Unit = {
    super.preStart()

    context.system.eventStream.subscribe(self, classOf[akka.remote.DisassociatedEvent])
    managerActor ! UpdateActorStatusToManager(strRemotePath, Initializing, "")
  }

  override def postStop(): Unit = {
    super.postStop()

    managerActor ! UpdateActorStatusToManager(strRemotePath, Stopped, "")
    context.system.eventStream.unsubscribe(self)
  }

  override def receive: Receive = {
    case work: AskWorkFlowPath => {
      implicit val timeout = Timeout(5 seconds)
      implicit val execContext = ExecutionContext.Implicits.global

      val curSender = sender()
      val futureResult = router ? AskWorkFlowPath(work.userId, work.intSimulation match { case 1 => 0; case _ => work.workFlowId }, work.strIdentifyId, work.intSimulation)

      futureResult onComplete {
        case Success(result) => {
          val workFlowPath = Await.result(futureResult, timeout.duration)

          curSender ! ReceiveWorkFlowPath(workFlowPath.toString, work.userId, work.workFlowId, work.strIdentifyId)
        }

        case Failure(e) => {
          log.error(ExceptionUtil.getStrackTrace(e));
        }
      }

      context unwatch curSender
    }

    case ListWorkflowPath => {
      implicit val timeout = Timeout(5 seconds)
      implicit val execContext = ExecutionContext.Implicits.global

      val curSender = sender()

      try {
        val futureResult = router ? ListWorkflowPath

        val lstWorkflowPath = Await.result(futureResult, timeout.duration)
        curSender ! lstWorkflowPath
      } catch {
        case ex: Exception => {
          log.error("ERR: " + ExceptionUtil.getStrackTrace(ex))
        }
      }

      context unwatch curSender
    }

    case alib: AskAlibPath => {
      implicit val timeout = Timeout(5 seconds)

      val futureResult = router ? AskAlibPath(alib.libType)
      val alibPath = Await.result(futureResult, timeout.duration)

      sender() ! (alibPath.toString, alib.libType)
      context unwatch sender()
    }

    case checkAlib: CheckAlib => {
      implicit val timeout = Timeout(5 seconds)

      val futureResult = router ? CheckAlib(checkAlib.libType)
      val isStarted = Await.result(futureResult, timeout.duration)

      sender() ! isStarted.toString()
      context unwatch sender()
    }

    case ListAlibPath => {
      implicit val timeout = Timeout(5 seconds)
      implicit val execContext = ExecutionContext.Implicits.global

      val curSender = sender()

      try {
        val futureResult = router ? ListAlibPath

        val lstAlibPath = Await.result(futureResult, timeout.duration)
        curSender ! lstAlibPath
      } catch {
        case ex: Exception => {
          log.error("ERR: " + ExceptionUtil.getStrackTrace(ex))
        }
      }

      context unwatch curSender
    }

    case fo: Failover => {
      implicit val timeout = Timeout(5 seconds)

      if (fo.strMsg.equals("router")) {
        val futureRouter = router ? Failover(fo.strMsg)
        val futureResult = Await.result(futureRouter, timeout.duration)

        sender() ! FailoverAck(fo.strMsg, futureResult.toString)

        context unwatch sender()
      }
    }

    case updateWorkFlow: UpdateWorkFlowActorPath => {
      router ! Broadcast(UpdateWorkFlowActorPath(updateWorkFlow.path, updateWorkFlow.workFlowId))
    }

    case removeWorkFlow: RemoveWorkFlowActorPath => {
      router ! Broadcast(RemoveWorkFlowActorPath(removeWorkFlow.workFlowId))
    }

    case updateAlib: UpdateAlibActorPath => {
      router ! Broadcast(UpdateAlibActorPath(updateAlib.path, updateAlib.libType))
    }

    case removeAlib: RemoveAlibActorPath => {
      router ! Broadcast(RemoveAlibActorPath(removeAlib.path, removeAlib.libType))
    }

    case killAlib: KilledAlibActor => {
      router ! Broadcast(KilledAlibActor(killAlib.path, killAlib.libType))
    }

    case CheckAlibActorRunning => {
      implicit val timeout = Timeout(5 seconds)
      implicit val execContext = ExecutionContext.Implicits.global

      val curSender = sender()
      val futureResult = router ? CheckAlibActorRunning
      val checkResult = Await.result(futureResult, timeout.duration)

      curSender ! checkResult.asInstanceOf[Boolean]
    }

    case updateAlibMeta: UpdateAlibMetaData => {
      log.debug("Update Alib " + updateAlibMeta.libId + " meta-data")
      router ! Broadcast(UpdateAlibMetaData(updateAlibMeta.libId))
    }

    case health: Alive => {
      log.debug("Alive " + strRemotePath)
      managerActor ! UpdateActorStatusToManager(strRemotePath, Started, "")
      router ! Broadcast(Alive(health.strMsg))
    }

    case stop: StopActor => {
      router ! Broadcast(StopActor(stop.id, stop.strType))
    }

    case Terminated(ref) => {
    }

    case evt: DisassociatedEvent => {
    }
  }
}
