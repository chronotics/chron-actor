package org.chronotics.actor.scala.akka.single

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorIdentity, ActorLogging, ActorRef, Identify, Props, Terminated}
import akka.pattern._
import akka.remote.DisassociatedEvent
import akka.util.Timeout
import com.lucidchart.open.xtract.XmlReader
import com.typesafe.config.{Config, ConfigFactory}
import org.chronotics.pandora.java.exception.ExceptionUtil
import org.chronotics.silverbullet.scala.akka.io.ActorRefSelection
import org.chronotics.silverbullet.scala.akka.protocol.Message._
import org.chronotics.silverbullet.scala.akka.state.{Initializing, Started, Stopped}
import org.chronotics.silverbullet.scala.akka.util.EnvConfig
import org.chronotics.silverbullet.scala.model._
import org.chronotics.silverbullet.scala.util.XMLReaderWorkflowTaskDB

import scala.collection.immutable.HashMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.xml.{Elem, XML}

class HttpActor(pID: Int) extends Actor with ActorLogging {
  import context.dispatcher
  implicit val timeout = Timeout(50 seconds)

  var status: AkkaProcessInfo = AkkaProcessInfo("request-received")
  var mapRequest: HashMap[String, UserRequest] = HashMap.empty
  var mapCancelRequest: HashMap[String, GetCancelRequest] = HashMap.empty
  var mapWaitingRequest: HashMap[String, UserRequest] = HashMap.empty

  val managerActor: ActorRef = context.actorOf(Props(classOf[StatusManagerActor]))
  val initActor: ActorRef = context.actorOf(Props(classOf[InitProcessActor]))
  var actorOSProcRouter: ActorRef = null

  val intNumOfReq = new AtomicInteger()
  val config: Config = ConfigFactory.parseFile(EnvConfig.getConfigFile("z2_conn")).resolve()
  val mapActorPath = "akka.tcp://" + config.getString("maphandler.system") + "@" + config.getString("maphandler.host") + ":" + config.getString("maphandler.port") + "/user/map"
  val strAlibRouterActorPath = "akka.tcp://" + config.getString("alibrouterhandler.system") + "@" + config.getString("alibrouterhandler.host") + ":" + config.getString("alibrouterhandler.port") + "/user/alib"
  val mapOSProcActorPath = "akka.tcp://" + config.getString("osprocrouterhandler.system") + "@" + config.getString("osprocrouterhandler.host") + ":" + config.getString("osprocrouterhandler.port") + "/user/osproc"
  val mapDebugActorPath = "akka.tcp://" + config.getString("debugrouterhandler.system") + "@" + config.getString("debugrouterhandler.host") + ":" + config.getString("debugrouterhandler.port") + "/user/debug"

  var bIsHealthScheduling: Boolean = false

  var seedWaitingTime = config.getInt("java_exec.waiting_time_seed")
  var normalWaitingTime = config.getInt("java_exec.waiting_time_normal")
  var routerWaitingTime = config.getInt("java_exec.waiting_time_router")
  var clusterWaitingTime = config.getInt("java_exec.waiting_time_cluster")

  var bIsCheckingWorkflow: Boolean = false
  var mapWorkflowChecking: HashMap[Int, Boolean] = HashMap.empty

  override def preStart(): Unit = {
    super.preStart()

    context.system.eventStream.subscribe(self, classOf[akka.remote.DisassociatedEvent])
    managerActor ! UpdateActorStatusToManager(self.path.toString, Initializing, "")

    context.system.scheduler.schedule(5 seconds, 5 seconds) {
      //scheduleHealthCheckAkkaRemote()
      scheduleToCheckWorkflow()
    }

    scheduleToCheckWorkflow()
  }

  override def postStop(): Unit = {
    super.postStop()

    managerActor ! UpdateActorStatusToManager(self.path.toString, Stopped, "")
    context.system.eventStream.unsubscribe(self)
  }

  override def receive: Receive = {
    case request: GetRequest => {
      implicit val resolveTimeout = Timeout(5 seconds)

      val mapLibParam = convertStringToHashMapLibParam(request.userRequestEntity)
      val intWorkflowIdInEntity: Int = request.userRequestEntity.workflow_id

      sender() ! AkkaProcessResponse(status)

      val strCurrentRequest: String = "[" + request.userId + "][" + request.workFlowId + "]" + "][" + request.requestId + "]"
      managerActor ! UpdateActorStatusToManager(self.path.toString, Started, strCurrentRequest + intNumOfReq.get.toString + " requests")

      val strRequestIdentity = request.requestId.toString + "-" + request.userId + "-" + request.workFlowId
      mapRequest = mapRequest + ((strRequestIdentity + "-map") -> UserRequest(request.userRequestEntity.callback, request.userId, request.workFlowId, request.requestId, 0, 0, mapLibParam, null, null, "", request.taskId, request.bIsStepByStep, request.intSimuation, intWorkflowIdInEntity, request.runInternal))

      context.actorSelection(mapActorPath) ! Identify(strRequestIdentity + "-map")
    }

    case requestCancelled: GetCancelRequest => {
      implicit val resolveTimeout = Timeout(5 seconds)

      sender() ! AkkaProcessResponse(status)

      val strCurrentRequest: String = "[" + requestCancelled.requestId + "][" + requestCancelled.workFlowId + "]"
      managerActor ! UpdateActorStatusToManager(self.path.toString, Started, strCurrentRequest)

      val strRequestIdentity = requestCancelled.requestId.toString + "-" + requestCancelled.workFlowId.toString
      mapCancelRequest = mapCancelRequest + ((strRequestIdentity + "-cancelmap") -> requestCancelled)

      context.actorSelection(mapActorPath) ! Identify(strRequestIdentity + "-cancelmap")
    }

    case health: HealthCheck => {
      sender() ! AkkaProcessResponse(new AkkaProcessInfo("received-healthy"))

      log.debug("Health: {}", health.intId)

      if (health.intId == 1) {
        managerActor ! UpdateActorStatusToManager(self.path.toString, Started, "")
        context.system.actorSelection(mapActorPath) ! Alive(health.intId.toString)
      }
    }

    case request: UpdateLibraryMetaData => {
      log.debug("Received update alib request")
      sender() ! AkkaProcessResponse(new AkkaProcessInfo("received-updating"))

      initActor ! UpdateLibraryMetaData(request.alibId)
    }

    case init: InitAkkaSystem => {
      sender() ! AkkaProcessResponse(new AkkaProcessInfo("received-initakka"))

      log.info("Init: " + init)

      init.cmd match {
        case "1" => initActor ! InitAkkaSystem("http", "0")
        case "2" => initActor ! InitAkkaSystem("consumer", "0")
        case "3" => initActor ! InitAkkaSystem("router", "0")
        case "4" => initActor ! InitAkkaSystem("cluster", "0")
        case "5" => initActor ! InitAkkaSystem("proc", init.id)
        case "6" => initActor ! InitAkkaSystem("alib", init.id)
        case "7" => initActor ! InitAkkaSystem("debug", "0")
        case _ =>
      }
    }

    case stop: StopActor => {
      sender() ! AkkaProcessResponse(new AkkaProcessInfo("received-stopakka"))
      log.debug("Stop: {} - {}", stop.id, stop.strType)

      if ((stop.strType == "5" || stop.strType == "6") && stop.id != "0") {
        context.system.actorSelection(mapActorPath) ! StopActor(stop.id, stop.strType)
      }
    }

    case Terminated(ref) => {
      log.debug("Actor {} is terminated", ref.path)
      managerActor ! UpdateActorStatusToManager(ref.path.toString, Stopped, "")
      context unwatch ref
    }

    case evt: DisassociatedEvent => {
      log.debug("Something was happened: {}", evt)
      managerActor ! UpdateActorStatusToManager(evt.remoteAddress.toString, Stopped, "")
    }

    case ReceiveWorkFlowPath(workFlowPath, userID, workFlowID, strReceivedIdentifyId) => {
      log.info("HttpActor received ProcessActor Path: " + workFlowPath)

      if (workFlowPath != "") {
        if (strReceivedIdentifyId.contains("-map")) {
          val curUserRequest = mapRequest(strReceivedIdentifyId)
          mapRequest = mapRequest - strReceivedIdentifyId

          val strIdentifyWorkflow = strReceivedIdentifyId.replace("-map", "-workflow")
          val objNewUserRequest = UserRequest(curUserRequest.strCallback, curUserRequest.iUserId, curUserRequest.iWorkFlowId, curUserRequest.iRequestId, 0, 0,
            curUserRequest.mapLibParam, null, null, workFlowPath.toString, curUserRequest.intTaskId, curUserRequest.bIsStepByStep, curUserRequest.intSimulation, runInternal = curUserRequest.runInternal)

          mapRequest = mapRequest + (strIdentifyWorkflow -> objNewUserRequest)
          context.actorSelection(workFlowPath.toString) ! Identify(strIdentifyWorkflow)
        } else {
          val curUserRequest = mapCancelRequest(strReceivedIdentifyId)
          mapCancelRequest = mapCancelRequest - strReceivedIdentifyId

          val strIdentifyWorkflow = strReceivedIdentifyId.replace("-cancelmap", "-cancelworkflow")
          val objNewUserRequest = GetCancelRequest(curUserRequest.requestId, curUserRequest.workFlowId, curUserRequest.userId, workFlowPath.toString)

          mapCancelRequest = mapCancelRequest + (strIdentifyWorkflow -> objNewUserRequest)
          context.actorSelection(workFlowPath.toString) ! Identify(strIdentifyWorkflow)
        }
      } else { //If there is no workflow actor, init
        mapWorkflowChecking = mapWorkflowChecking - workFlowID

        initActor ! InitAkkaSystem("proc", workFlowID.toString)

        if (strReceivedIdentifyId.contains("-map")) {
          val curUserRequest = mapRequest(strReceivedIdentifyId)
          mapRequest = mapRequest - strReceivedIdentifyId

          val objNewUserRequest = UserRequest(curUserRequest.strCallback, curUserRequest.iUserId, curUserRequest.iWorkFlowId, curUserRequest.iRequestId, 0, 0,
            curUserRequest.mapLibParam, null, null, workFlowPath.toString, curUserRequest.intTaskId, curUserRequest.bIsStepByStep, curUserRequest.intSimulation, runInternal = curUserRequest.runInternal)

          mapWaitingRequest = mapWaitingRequest + (strReceivedIdentifyId -> objNewUserRequest)
        }
      }
    }

    case ActorIdentity(strIdentifyId, optActorRef) => {
      log.info("Received Identify: " + strIdentifyId.toString)
      log.info("Received ActorRef: " + optActorRef)
      log.info("Map size         : " + mapRequest.size)

      implicit val resolveTimeout = Timeout(5 seconds)

      val strReceivedIdentifyId = strIdentifyId.toString

      if (mapRequest.contains(strReceivedIdentifyId)) {
        log.info("Map contained: " + strReceivedIdentifyId)

        val curUserRequest = mapRequest(strReceivedIdentifyId)

        if (strReceivedIdentifyId.contains("-map")) {
          optActorRef match {
            case Some(actorRefMap) => {
              log.debug(actorRefMap.path.toString)
              actorRefMap ! AskWorkFlowPath(curUserRequest.iUserId, curUserRequest.iWorkFlowId, strReceivedIdentifyId, curUserRequest.intSimulation)
            }

            case None => {
              context.system.scheduler.scheduleOnce(1 seconds) {
                context.actorSelection(mapActorPath) ! Identify(strReceivedIdentifyId)
              }
            }
          }
        } else {
          if (strReceivedIdentifyId.contains("-workflow")) {
            optActorRef match {
              case Some(actorRefProcess) => {
                var bIsAllLibStarted = checkLibraryStatusOfWorkflow(curUserRequest.iWorkFlowId)

                if (bIsAllLibStarted) {
                  mapRequest = mapRequest - strReceivedIdentifyId

                  if (curUserRequest.intTaskId == 0) {
                    val intCurWorkflowId: Int = curUserRequest.intSimulation match {
                      case 1 => curUserRequest.intWorkflowEntity
                      case _ => curUserRequest.iWorkFlowId
                    }

                    context.system.scheduler.scheduleOnce(100 milliseconds) {
                      actorRefProcess ! SendWorkFlow(curUserRequest.strCallback, curUserRequest.iUserId, intCurWorkflowId, curUserRequest.iRequestId, curUserRequest.mapLibParam, curUserRequest.bIsStepByStep, 0, 0, null, null, null, curUserRequest.runInternal)
                    }
                  } else {
                    actorRefProcess ! SendTaskOfWorkFlow(curUserRequest.strCallback, curUserRequest.iUserId, curUserRequest.iWorkFlowId, curUserRequest.iRequestId, curUserRequest.intTaskId, curUserRequest.mapLibParam(curUserRequest.intTaskId))
                  }
                } else {
                  context.system.scheduler.scheduleOnce(2 seconds) {
                    context.actorSelection(curUserRequest.strActorRefPath) ! Identify(strReceivedIdentifyId)
                  }
                }
              }

              case None => {
                context.system.scheduler.scheduleOnce(2 seconds) {
                  context.actorSelection(curUserRequest.strActorRefPath) ! Identify(strReceivedIdentifyId)
                }
              }
            }
          }
        }
      } else if (mapCancelRequest.contains(strReceivedIdentifyId)) {
        log.debug("Cancel map contained: {}", strReceivedIdentifyId)

        var curUserRequest = mapCancelRequest(strReceivedIdentifyId)

        if (strReceivedIdentifyId.contains("-cancelmap")) {
          optActorRef match {
            case Some(actorRefMap) => {
              log.debug(actorRefMap.path.toString)
              actorRefMap ! AskWorkFlowPath(curUserRequest.userId, curUserRequest.workFlowId, strReceivedIdentifyId, 0)
            }

            case None => {
              context.system.scheduler.scheduleOnce(1 seconds) {
                context.actorSelection(mapActorPath) ! Identify(strReceivedIdentifyId)
              }
            }
          }
        } else {
          if (strReceivedIdentifyId.contains("-cancelworkflow")) {
            optActorRef match {
              case Some(actorRefProcess) => {
                mapCancelRequest = mapCancelRequest - strReceivedIdentifyId
                actorRefProcess ! CancelledRequest(curUserRequest.requestId)
              }

              case None => {
                context.system.scheduler.scheduleOnce(1 seconds) {
                  context.actorSelection(curUserRequest.path) ! Identify(strReceivedIdentifyId)
                }
              }
            }
          }
        }
      }
    }
  }

  def checkLibraryStatusOfWorkflow(iWorkflowId: Int): Boolean = {
    var bIsStartedAll: Boolean = true

    if (!mapWorkflowChecking.contains(iWorkflowId) || !mapWorkflowChecking(iWorkflowId)) {
      var routerAlibActorRef = ActorRefSelection.getActorRefOfSelection(strAlibRouterActorPath, context, log)
      val objWorkFlowTaskInfo: WorkFlowTaskInfo = new XMLReaderWorkflowTaskDB(iWorkflowId).readXMLInfo()

      if (objWorkFlowTaskInfo != null) {
        objWorkFlowTaskInfo.arrTask.foreach { objTask =>
          if (objTask != null && objTask.iAlibId > 0) {
            if (routerAlibActorRef != null) {
              try {
                val futureAlibRouter = routerAlibActorRef ? CheckAlib(objTask.iAlibId.toString)
                val futureAlibRouterResult = Await.result(futureAlibRouter, timeout.duration)

                log.info("checkLibraryStatus: " + objTask.iAlibId + " - " + futureAlibRouterResult.toString)

                if (futureAlibRouterResult == null || futureAlibRouterResult.toString.equals("0")) {
                  bIsStartedAll = false
                }
              } catch {
                case ex: Throwable => {
                  log.error("ERR: " + ExceptionUtil.getStrackTrace(ex))
                }
              }
            }
          }
        }
      }

      mapWorkflowChecking = mapWorkflowChecking + (iWorkflowId -> bIsStartedAll)
    }

    bIsStartedAll
  }

  def scheduleToCheckWorkflow() = {
    if (!bIsCheckingWorkflow) {
      bIsCheckingWorkflow = true

      if (mapWaitingRequest != null && mapWaitingRequest.size > 0) {
        var seqIdentity: Seq[String] = Seq.empty

        var mapProcessing: HashMap[String, UserRequest] = HashMap.empty

        for (item <- mapWaitingRequest) {
          mapProcessing = mapProcessing + (item._1 -> item._2)

          var strIdentity = item._1
          var curUserRequest = item._2

          log.info("Waiting: " + strIdentity)

          seqIdentity = seqIdentity :+ strIdentity
        }

        for (strIdentity <- seqIdentity) {
          mapWaitingRequest = mapWaitingRequest - strIdentity
        }

        for (item <- mapProcessing) {
          var strIdentity = item._1
          var curUserRequest = item._2

          log.info("Waiting: " + strIdentity)

          mapRequest = mapRequest + (strIdentity -> UserRequest(curUserRequest.strCallback, curUserRequest.iUserId, curUserRequest.iWorkFlowId, curUserRequest.iRequestId, 0, 0,
            curUserRequest.mapLibParam, null, null, "", curUserRequest.intTaskId, curUserRequest.bIsStepByStep, curUserRequest.intSimulation, runInternal = curUserRequest.runInternal))

          context.actorSelection(mapActorPath) ! Identify(strIdentity)
        }

        mapProcessing = HashMap.empty
      }

      bIsCheckingWorkflow = false
    }
  }

  def convertStringToHashMapLibParam(objRequestEntity: UserRequestEntity): HashMap[Int, Seq[LibParam]] = {
    var mapLibParam: HashMap[Int, Seq[LibParam]] = HashMap.empty

    val xmlMeta: Elem = XML.loadString(objRequestEntity.param_xml)
    val entity: UserRequestXML = XmlReader.of[UserRequestXML].read(xmlMeta).getOrElse(null)

    if (entity != null) {
      entity.arrTaskMetaList.arrTaskMeta.foreach { taskMeta =>
        var arrLibParam: Seq[LibParam] = Seq.empty

        taskMeta.arrParam.arrLibParam.foreach { libParam =>
          var curLibParam: LibParam = LibParam.apply(libParam.iId, "", "", Some(""), Some(0), "", Some(""), Some(""), libParam.strValue, Some(""), Some(""), Some(0))
          arrLibParam = arrLibParam :+ curLibParam
        }

        mapLibParam = mapLibParam + (taskMeta.iId -> arrLibParam)
      }
    }

    mapLibParam
  }
}

object HttpActor {
  def props(pId: Int): Props = Props(classOf[HttpActor], pId)
}