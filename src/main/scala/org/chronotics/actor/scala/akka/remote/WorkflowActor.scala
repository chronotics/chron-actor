package org.chronotics.actor.scala.akka.remote

import java.text.SimpleDateFormat
import java.util.Calendar

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorIdentity, ActorLogging, ActorRef, Identify, OneForOneStrategy, Props, Terminated}
import akka.pattern._
import akka.remote.DisassociatedEvent
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.chronotics.actor.scala.akka.data.XMLActor
import org.chronotics.actor.scala.akka.single.{InitProcessActor, StatusManagerActor}
import org.chronotics.pandora.java.exception.ExceptionUtil
import org.chronotics.pithos.ext.redis.scala.adaptor.RedisPoolConnection
import org.chronotics.silverbullet.scala.akka.io.{ActorRefSelection, LoggingIO}
import org.chronotics.silverbullet.scala.akka.protocol.Message._
import org.chronotics.silverbullet.scala.akka.state._
import org.chronotics.silverbullet.scala.akka.util.{EnvConfig, RemoteAddressExtension}
import org.chronotics.silverbullet.scala.model._
import org.chronotics.silverbullet.scala.util.XMLReaderAlibFromDB

import scala.collection.immutable.{HashMap, Queue}
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.control.Breaks._

class WorkflowActor(workFlowId: Int, pID: Int) extends Actor with ActorLogging {
  import context.dispatcher
  implicit val timeout = Timeout(5 seconds)

  val config: Config = ConfigFactory.parseFile(EnvConfig.getConfigFile("z2_conn")).resolve()
  val redisHost = config.getString("redis.host")
  val redisPort = config.getInt("redis.port")
  val redisDB = config.getInt("redis.db")
  val redisPass = config.getString("redis.pass")
  val redisCli = RedisPoolConnection.getInstance(redisHost, redisPort, redisDB, redisPass)
  val strKeyWorkflowRequestTempalte = config.getString("redis.key-workflow-request")

  val strMapActorPath = "akka.tcp://" + config.getString("maphandler.system") + "@" + config.getString("maphandler.host") + ":" + config.getString("maphandler.port") + "/user/map"
  val strAlibRouterActorPath = "akka.tcp://" + config.getString("alibrouterhandler.system") + "@" + config.getString("alibrouterhandler.host") + ":" + config.getString("alibrouterhandler.port") + "/user/alib"

  val strTimeFormat = "yyyy-MM-dd HH:mm:ss"
  val objSimpleDateFormat: SimpleDateFormat = new SimpleDateFormat(strTimeFormat)

  val remoteAddr = RemoteAddressExtension(context.system).address
  var strRemotePath = self.path.toStringWithAddress(remoteAddr)
  var mapTaskQueue: HashMap[Int, Queue[UserRequest]] = HashMap.empty //Each task will have a request Queue
  var mapTaskStatus: HashMap[Int, TaskState] = HashMap.empty
  var mapTaskOrder: HashMap[Int, Int] = HashMap.empty
  var mapUserRequestStatus: HashMap[UserRequest, UserRequestStatus] = HashMap.empty
  var mapLoop: HashMap[Int, HashMap[Int, Int]] = HashMap.empty //Key: FromTaskID - Value: [Key: ToTaskID, Value: NumberOfLoop]
  var mapRequestLoop: HashMap[UserRequest, HashMap[Int, HashMap[Int, Int]]] = HashMap.empty //Key: UserRequest - Value [Key: FromTaskID - Value: [Key: ToTaskID, Value: NumberOfLoop]]
  var mapEndTaskRequestStatus: HashMap[UserRequest, HashMap[Int, Int]] = HashMap.empty //Key: UserRequest - Value [Key: End TaskID - Value: End Task Status 0 - Not finished, 1 - Finished]
  var mapParentRequest: HashMap[UserRequest, UserRequest] = HashMap.empty
  var mapRequestId: HashMap[Int, UserRequest] = HashMap.empty
  var arrCancelledRequest: HashMap[Int, Int] = HashMap.empty
  var mapTaskEndLoop: HashMap[Int, LoopTask] = HashMap.empty
  var mapUnsatisfiedTask: HashMap[UserRequest, Seq[Int]] = HashMap.empty
  var mapTaskLang: HashMap[Int, String] = HashMap.empty
  var workFlowData: WorkFlowTaskInfo = null
  val actorXMLReader: ActorRef = context.actorOf(Props(classOf[XMLActor]))
  val managerActor: ActorRef = context.actorOf(Props(classOf[StatusManagerActor]))
  val loggingIO: LoggingIO = LoggingIO.getInstance()
  val initActor: ActorRef = context.actorOf(Props(classOf[InitProcessActor]))

  var curName = ""

  val arrActorState: Seq[String] = Seq(Initializing.toString, Started.toString, Stopped.toString, Restarted.toString, Working.toString, Finished.toString, NotSatisfied.toString, Failed.toString)

  var lTimeout: Long = Calendar.getInstance.getTimeInMillis
  var lMaxTimeout: Long = config.getLong("processhandler.idle_time")

  override def supervisorStrategy = OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
    case exOOM: OutOfMemoryError => {
      log.error("ERR", exOOM.getStackTraceString)
      Stop
    }
    case ex: Throwable => {
      log.error("ERR", ex.getStackTraceString)
      Stop
    }
  }

  override def preRestart(reason: Throwable, message: Option[Any]) = {
    log.info("Restarting - Reason: " + reason match {
      case null => ""
      case _ => reason.getStackTraceString
    })

    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable) = {
    println("Restart completed! - Reason: " + reason match {
      case null => ""
      case _ => reason.getStackTraceString
    })

    super.postRestart(reason)
  }

  override def preStart(): Unit = {
    context.actorSelection(strMapActorPath) ! Identify(workFlowId.toString + "-map")

    if (workFlowId > 0) {
      actorXMLReader ! ReadXMLWorkFlowDB(workFlowId)
    }

    lTimeout = Calendar.getInstance.getTimeInMillis

    if (workFlowId != 0) {
      context.system.scheduler.schedule(5 seconds, 1 second) {
        scheduleIdleWorkflow()
      }
    }
  }

  def stopActor() = {
    managerActor ! UpdateActorStatusToManager(strRemotePath, Stopped, curName)

    val actorMap = ActorRefSelection.getActorRefOfSelection(strMapActorPath, context, log)

    if (actorMap != null) {
      actorMap ! RemoveWorkFlowActorPath(workFlowId)
    }

    Thread.sleep(1000)

    context.system.terminate
  }

  override def postStop(): Unit = {
    stopActor()
  }

  override def receive: Receive = {
    case workFlow: SendWorkFlow => { //Receive request from user, add request to queue
      implicit val timeout = Timeout(5 seconds)
      log.info(s"Received request from ${workFlow.userId} and ${workFlow.workFlowId}")

      //Check if run simulate or not
      var bIsXML: Boolean = true

      if (workFlowId <= 0 && workFlow.workFlowId > 0) {
        //Read XML
        bIsXML = false

        try {
          val futureResult = actorXMLReader ? ReadXMLWorkFlowDB(workFlow.workFlowId)
          val curWorkflowResult = Await.result(futureResult, timeout.duration)

          log.info(curWorkflowResult.toString)

          if (curWorkflowResult == null) {
          } else {
            fillXMLReaderResult(curWorkflowResult.asInstanceOf[ReceiveXMLWorkFlowData])
            bIsXML = true
          }
        } catch {
          case ex: Throwable => {
            log.error(ex, "ERR")
            bIsXML = false
          }
        }
      }

      if (bIsXML) {
        val objUserRequest = UserRequest(workFlow.callback, workFlow.userId, workFlow.workFlowId, workFlow.requestId, workFlow.parentTaskId, workFlow.parentWorkflowId, workFlow.mapLibParam, workFlow.arrTaskConnection, workFlow.arrOutConneciton, "", 0, workFlow.bIsStepByStep)

        addWorkflowStatus(objUserRequest, Initializing)

        if (workFlow.parentRequest != null && workFlow.parentTaskId > 0 && workFlow.parentWorkflowId > 0) {
          mapParentRequest = mapParentRequest + (objUserRequest -> workFlow.parentRequest)
        }

        mapRequestId = mapRequestId + (workFlow.requestId -> objUserRequest)

        if (workFlowData != null && workFlowData.arrTask.size > 0) {
          if (mapLoop != null && mapLoop.size > 0) {
            mapRequestLoop = mapRequestLoop + (objUserRequest -> mapLoop)
          }

          log.info("Number of tasks: {}", workFlowData.arrTask.size)

          var arrFirstTask: HashMap[Int, Int] = HashMap.empty
          var mapTaskType: HashMap[Int, String] = HashMap.empty

          workFlowData.arrTask.foreach { objTask =>
            if (mapTaskQueue contains objTask.iId) {
              log.info("MapTaskQueue contains: " + objTask.iId)

              val curQueue = mapTaskQueue(objTask.iId) enqueue objUserRequest
              mapTaskQueue = mapTaskQueue + (objTask.iId -> curQueue)

              objTask.iArrPrevTask match {
                case Some(taskOrder: TaskOrder) => {
                  log.info("Task {} has {}", String.valueOf(objTask.iId), String.valueOf(taskOrder.iArrTaskId.size));

                  //In case task doesn't have any previous task -> First task
                  if (taskOrder.iArrTaskId.size <= 0) {
                    arrFirstTask = arrFirstTask + (objTask.iId -> objTask.iOrder)
                    mapTaskType += (objTask.iId -> objTask.iAlibId.toString)
                  }
                }

                case None => {
                  arrFirstTask = arrFirstTask + (objTask.iId -> objTask.iOrder)
                  mapTaskType += (objTask.iId -> objTask.iAlibId.toString)
                }
              }
            }

            if (mapTaskStatus.contains(objTask.iId) == false) {
              mapTaskStatus += (objTask.iId -> Idle)
            }

            if (mapTaskOrder.contains(objTask.iId) == false) {
              mapTaskOrder += (objTask.iId -> objTask.iOrder)
            }

            getTaskLang(objTask)
          }

          createTimeLogMessage(objUserRequest, 0, 1, Calendar.getInstance, "S", workFlow.workFlowId.toString)

          breakable {
            arrFirstTask.foreach { objTask =>
              log.info("First task: {}", objTask._1)
              log.info("Current Queue of Task {} is {}", String.valueOf(objTask._1), mapTaskStatus(objTask._1))

              if (mapTaskStatus(objTask._1) == Idle) {
                runTask(objTask._1, mapTaskType(objTask._1), objTask._2)

                if (workFlow.bIsStepByStep) {
                  break
                }
              }
            }
          }

          addNewUserRequest(objUserRequest)
          initEndTaskStatus(objUserRequest)
        }
      }
    }

    case taskOfWorkflow: SendTaskOfWorkFlow => {
      log.info("Receive Task of Workflow: " + taskOfWorkflow.taskId + " - " + taskOfWorkflow.workFlowId + " - " + taskOfWorkflow.requestId)

      if (taskOfWorkflow.requestId > 0 && taskOfWorkflow.workFlowId > 0 && taskOfWorkflow.taskId > 0) {
        var arrTaskType = workFlowData.arrTask.filter(p => p.iId == taskOfWorkflow.taskId).map(item => (item.iId, (item.iAlibId.toString, item.iOrder)))

        log.info("taskOfWorkflow.modifiedParams: " + taskOfWorkflow.modifiedParams)

        if (taskOfWorkflow.modifiedParams != null) {
          updateModifiedLibParamOfRequest(taskOfWorkflow.requestId, taskOfWorkflow.taskId, taskOfWorkflow.modifiedParams)
        }

        if (mapTaskQueue.contains(taskOfWorkflow.taskId) && mapTaskQueue(taskOfWorkflow.taskId).size > 0) {
          runTask(taskOfWorkflow.taskId, arrTaskType(0)._2._1, arrTaskType(0)._2._2) //dequeue new request in current task id
        }
      }
    }

    case result: ReceiveXMLWorkFlowData if result != null => { //Receive workflow metadata
      fillXMLReaderResult(result)
    }

    case cancelled: CancelledRequest => {
      log.info("Received cancel request: " + cancelled.intRequestId)
      mapRequestId.get(cancelled.intRequestId) match {
        case Some(objCancelledRequest) => {
          mapUserRequestStatus = mapUserRequestStatus - objCancelledRequest
          arrCancelledRequest = arrCancelledRequest + (cancelled.intRequestId -> cancelled.intRequestId)
        }
        case None =>
      }
    }

    case AlibTaskCompleted(iTaskId, strType, objRequest, objProcSender, objTaskStatus, isEndLoop, isPrevConditionSatisfied) => {
      createTimeLogMessage(objRequest, iTaskId, 2, Calendar.getInstance, "E", strType)
      //log.info("Completed: " + iTaskId + " - " + strType + " - objRequest: " + objRequest + " - isEndLoop: " + isEndLoop + " - isSatisfied: " + isPrevConditionSatisfied)
      updateUserRequest(objRequest, iTaskId)

      if (!isPrevConditionSatisfied) {
        removeTaskFromQueue(iTaskId, objRequest)
      }

      if (mapTaskQueue.contains(iTaskId) && mapTaskQueue(iTaskId).size > 0) {
        log.info("Task Queue of " + iTaskId.toString() + ": " + mapTaskQueue.size)
        log.info("Have another task {}", String.valueOf(iTaskId))

        runTask(iTaskId, strType, mapTaskOrder(iTaskId)) //dequeue new request in current task id
      } else {
        mapTaskStatus = mapTaskStatus + (iTaskId -> Idle)
      }

      if (!checkRequestIsCancelled(objRequest)) {
        if (!objRequest.bIsStepByStep) {
          if (isEndLoop) { //Already End Loop
            runNextTask(objRequest, iTaskId) //run next tasks follow by workflow
            runRemainTasksInSameOrder(objRequest, iTaskId) //run remain task in same order
          } else {
            var objLoop = checkLoopTask(objRequest, iTaskId)
            log.info("objLoop: " + objLoop)

            if (objLoop._1 > 0) {
              log.info("Have Loop Task at: " + objLoop._1)
              runTask(objLoop._1, objLoop._3, objLoop._2, objLoop._4)
            } else {
              runNextTask(objRequest, iTaskId) //run next tasks follow by workflow
              runRemainTasksInSameOrder(objRequest, iTaskId) //run remain task in same order
            }
          }
        }
      }

      if (objRequest.iParentTaskId > 0 && objRequest.iParentWorkflow > 0) {
        updateEndTask(objRequest, iTaskId)

        if (checkAllEndTaskDone(objRequest)) {
          //get a-library router actor reference
          log.info("strWorkflowRouterActor: " + strMapActorPath)
          val actorRefWorkflowRouter = ActorRefSelection.getActorRefOfSelection(strMapActorPath, context, log)
          log.info("actorRef-Received: " + actorRefWorkflowRouter)

          if (actorRefWorkflowRouter != null) {
            log.info(actorRefWorkflowRouter.path.toString)

            var intAskedTime = 0
            var routerActorPath = ""

            breakable {
              do {
                val futureWorkflowActorPath = actorRefWorkflowRouter ? AskWorkFlowPath(workFlowId = objRequest.iParentWorkflow)
                val objResolveWorkflow = Await.result(futureWorkflowActorPath, timeout.duration)

                log.info("ProcessActor received WorkflowActor Path: {}", objResolveWorkflow)

                if (objResolveWorkflow.isInstanceOf[ReceiveWorkFlowPath]) {
                  val curResolve = objResolveWorkflow.asInstanceOf[ReceiveWorkFlowPath]

                  if (curResolve.path != null && !curResolve.path.isEmpty()) {
                    routerActorPath = curResolve.path
                    break
                  } else {
                    intAskedTime = intAskedTime + 1
                    Thread.sleep(200)
                  }
                } else {
                  intAskedTime = intAskedTime + 1
                  Thread.sleep(200)
                }
              } while (intAskedTime < 5)
            }

            //get a-library actor reference
            val actorRefWorkflow = ActorRefSelection.getActorRefOfSelection(routerActorPath.toString, context, log)

            log.info("actorRefWorkflow: {}", actorRefWorkflow)

            if (routerActorPath != "" && actorRefWorkflow != null) {
              var parentRequest: UserRequest = mapParentRequest.get(objRequest) match {
                case Some(parent) => parent
                case None => null
              }

              if (parentRequest != null) {
                actorRefWorkflow ! AlibTaskCompleted(objRequest.iParentTaskId, "0", parentRequest, objProcSender, objTaskStatus, isEndLoop, isPrevConditionSatisfied)
              }
            }
          }
        }
      } else {
        updateEndTask(objRequest, iTaskId)

        if (checkAllEndTaskDone(objRequest)) {
          log.info("All tasks of " + objRequest.iRequestId + " are done")

          createTimeLogMessage(objRequest, iTaskId, 1, Calendar.getInstance, "E", "0")

          //deleteRedisResult(objRequest.iRequestId.toString)
          addWorkflowStatus(objRequest, objTaskStatus)

          var strLog: String = loggingIO.generateLogMessage(objRequest.strCallback, objRequest.iUserId.toString, "W", objRequest.iRequestId, Finished, objRequest.iWorkFlowId.toString, objRequest.iWorkFlowId.toString, objRequest.intTaskId.toString, "")
          managerActor ! UpdateActorStatusToManager(strRemotePath, Finished, strLog)
        }
      }
    }

    case health: Alive => {
      context.system.actorSelection(strAlibRouterActorPath) ! Alive(health.strMsg)
      managerActor ! UpdateActorStatusToManager(strRemotePath, Started, curName)
    }

    case stop: StopActor => {
      if (stop.strType == "proc" && stop.id == workFlowId.toString) {
        context.stop(self)
        managerActor ! UpdateActorStatusToManager(strRemotePath, Stopped, curName)
      } else {
        context.system.actorSelection(strAlibRouterActorPath) ! StopActor(stop.id, stop.strType)
      }
    }

    case Ping => {
      sender() ! Pong
    }

    case Terminated(ref) => {
      log.info("Actor {} is terminated", ref.path.toString)

      context unwatch ref

      if (ref.path.toString.contains(config.getString("maphandler.system"))) {
        context.system.scheduler.scheduleOnce(1 seconds) {
          context.actorSelection(strMapActorPath) ! Identify(workFlowId.toString + "-map")
        }
      }
    }

    case evt: DisassociatedEvent => {
      log.info("Something was happened: {}", evt)
      //managerActor ! Z2Protocol.UpdateActorStatusToManager(evt.remoteAddress.toString, Stopped, "")
      //processInfo.sendToProcessManager(managerActor, pID, evt.remoteAddress.toString, "", PStopped)
    }

    case ActorIdentity(objIdentifyId, optActorRef) => {
      optActorRef match {
        case Some(actorMap) => {
          log.info("Received ActorRouter: {}", actorMap)

          val remoteAddr = RemoteAddressExtension(context.system).address
          strRemotePath = self.path.toStringWithAddress(remoteAddr)

          log.info("RemotePath: {}", strRemotePath)

          actorMap ! UpdateWorkFlowActorPath(strRemotePath, workFlowId) //Update its address to router actor
        }

        case None => {
          log.info("No Received ActorRouter. Try again!")

          context.system.scheduler.scheduleOnce(1 seconds) {
            context.actorSelection(strMapActorPath) ! Identify(workFlowId.toString + "-map")
          }
        }
      }
    }
  }

  def scheduleIdleWorkflow() = {
    var bIsAllEmpty = true

    breakable {
      for (item <- mapTaskQueue) {
        if (item._2.length > 0) {
          bIsAllEmpty = false
          lTimeout = Calendar.getInstance.getTimeInMillis
          break
        }
      }
    }

    var lElapse = Calendar.getInstance.getTimeInMillis - lTimeout

    if (bIsAllEmpty && lElapse > lMaxTimeout) {
      lTimeout = Calendar.getInstance.getTimeInMillis
      log.warning("WARN: Idle Timeout - Actor of workflow " + workFlowId + " will be killed")
      stopActor()
    }
  }

  def getTaskLang(objTask: TaskInfo) = {
    if (objTask.iAlibId > 0) {
      if (!mapTaskLang.contains(objTask.iAlibId)) {
        var objLibMeta: LibMeta = new XMLReaderAlibFromDB(config).parseXMLData(objTask.iAlibId, "")

        if (objLibMeta != null) {
          mapTaskLang = mapTaskLang + (objTask.iId -> (objLibMeta.strLang match { case Some(str) => str case None => "" }))
        }
      }
    } else {
      mapTaskLang += (objTask.iId -> "")
    }
  }

  def updateModifiedLibParamOfRequest(iRequestId: Int, iTaksId: Int, modifiedLibParams: Seq[LibParam]) {
    log.info("modifiedLibParam: " + modifiedLibParams)

    if (mapRequestId.contains(iRequestId)) {
      var objCurRequest = mapRequestId(iRequestId)

      log.info("objCurRequest: " + objCurRequest)

      breakable {
        for ((curTaskId, lstParam) <- objCurRequest.mapLibParam) {
          if (curTaskId.equals(iTaksId)) {
            log.info("iTaksId: " + iTaksId)

            var mapNewLibParam = objCurRequest.mapLibParam
            var lstNewParam = lstParam

            for (iCount <- 0 until lstParam.size) {
              log.info("lstParam(iCount).iId: " + lstParam(iCount).iId)

              for (iCountMod <- 0 until modifiedLibParams.length) {
                if (lstParam(iCount).iId.equals(modifiedLibParams(iCountMod).iId)) {
                  log.info("modifiedLibParam.iId: " + modifiedLibParams(iCountMod).iId)

                  lstNewParam = lstNewParam.patch(iCount, Seq(modifiedLibParams(iCountMod)), 1)
                }
              }
            }

            mapNewLibParam = mapNewLibParam - curTaskId
            mapNewLibParam = mapNewLibParam + (curTaskId -> lstNewParam)

            var objNewRequest = objCurRequest

            objNewRequest.mapLibParam = mapNewLibParam
            mapRequestId = mapRequestId + (iRequestId -> objNewRequest)

            log.info("objNewRequest.mapLibParam: " + objNewRequest.mapLibParam)

            break
          }
        }
      }
    }
  }

  def addWorkflowStatus(objRequest: UserRequest, objStatus: ActorState) = {
    try {
      var strKey = strKeyWorkflowRequestTempalte + objStatus.toString
      var strField: String = StringBuilder.newBuilder.append(objRequest.iRequestId)
        .append("|").append(workFlowId).append("|").append(workFlowData.strName)
        .toString

      //Delete current request in other statuses keys
      for (strActorStatus <- arrActorState) {
        var strCurKey = strKeyWorkflowRequestTempalte + strActorStatus
        redisCli.zScore(strCurKey, strField) match {
          case Some(dbScore) => {
            redisCli.zRemove(strCurKey, strField)
          }
          case None => {
          }
        }
      }

      //Insert into current status key
      redisCli.zAdd(strKey, strField, Calendar.getInstance.getTimeInMillis)
    } catch {
      case ex: Throwable => {
        log.error("ERR", ex.getStackTraceString)
      }
    }
  }

  def deleteRedisResult(requestId: String) {
    try {
      var lstKey: Seq[String] = redisCli.keys("*" + requestId + "*");

      if (lstKey != null && lstKey.length > 0) {
        var lstFilterKey = lstKey.filter(str => !str.contains("pretty") && !str.contains("input"))

        if (lstFilterKey != null && lstFilterKey.length > 0) {
          redisCli.delKeys(lstFilterKey)
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR", ex.getStackTraceString)
      }
    }
  }

  def createTimeLogMessage(objRequest: UserRequest, intTaskId: Int, intType: Int, curTime: Calendar, curTimeType: String, strObjectID: String): String = {
    var strLog = ""
    var strCurTime = objSimpleDateFormat.format(curTime.getTime)
    var curStatus: ActorState = curTimeType match {
      case "S" => Started
      case "E" => Finished
      case _ => Working
    }

    intType match {
      case 1 => strLog = loggingIO.generateLogMessage(objRequest.strCallback, objRequest.iUserId.toString, "W", objRequest.iRequestId, curStatus, objRequest.iWorkFlowId.toString, "0", intTaskId.toString, "")
      case 2 => strLog = loggingIO.generateLogMessage(objRequest.strCallback, objRequest.iUserId.toString, "L", objRequest.iRequestId, curStatus, objRequest.iWorkFlowId.toString, strObjectID, intTaskId.toString, "")
    }

    log.info(strLog)

    managerActor ! UpdateSimpleStatusToManager(strLog, curTime.getTimeInMillis)

    strLog
  }

  def initEndTaskStatus(objRequest: UserRequest) = {
    //If this workflow is inside another workflow
    if (objRequest.seqNextConnection != null && objRequest.seqNextConnection.size > 0) {
      var mapEndTask: HashMap[Int, Int] = HashMap.empty

      objRequest.seqNextConnection.foreach { curConnection =>
        mapEndTask = mapEndTask + (curConnection.fromSubTaskId -> 0)
      }

      mapEndTaskRequestStatus = mapEndTaskRequestStatus + (objRequest -> mapEndTask)
    } else {
      //This workflow is main one
      if (workFlowData != null && workFlowData.arrTask != null && workFlowData.arrTask.length > 0) {
        var mapEndTask: HashMap[Int, Int] = HashMap.empty

        workFlowData.arrTask.foreach { objTask =>
          objTask.iArrNextTask match {
            case None => {
              mapEndTask = mapEndTask + (objTask.iId -> 0)

              log.info("EndTask with None: " + objTask.iId.toString)
            }
            case Some(curTaskOrder) => {
              if (curTaskOrder == null || curTaskOrder.iArrTaskId == null || curTaskOrder.iArrTaskId.size <= 0) {
                mapEndTask = mapEndTask + (objTask.iId -> 0)

                log.info("EndTask with Some: " + objTask.iId.toString)
              }
            }
          }
        }

        mapEndTaskRequestStatus = mapEndTaskRequestStatus + (objRequest -> mapEndTask)
      }
    }
  }

  def updateEndTask(objRequest: UserRequest, iTaskId: Int) = {
    if (mapEndTaskRequestStatus != null && mapEndTaskRequestStatus.contains(objRequest)) {
      mapEndTaskRequestStatus.get(objRequest) match {
        case Some(mapEnd) => {
          if (mapEnd.contains(iTaskId)) {
            var mapUpdate = mapEnd + (iTaskId -> 1)
            mapEndTaskRequestStatus = mapEndTaskRequestStatus + (objRequest -> mapUpdate)
          }
        }

        case None => {}
      }
    }
  }

  def checkAllEndTaskDone(objRequest: UserRequest): Boolean = {
    var bIsAllDone: Boolean = true
    if (mapEndTaskRequestStatus != null && mapEndTaskRequestStatus.contains(objRequest)) {
      mapEndTaskRequestStatus.get(objRequest) match {
        case Some(mapEnd) => {
          breakable {
            mapEnd.foreach { item =>
              if (item._2 == 0) {
                bIsAllDone = false
                break
              }
            }
          }
        }

        case None => {}
      }
    }

    if (bIsAllDone) {
      if (mapRequestId.contains(objRequest.iRequestId)) {
        mapRequestId = mapRequestId - objRequest.iRequestId
      }
    }

    bIsAllDone
  }

  def fillXMLReaderResult(result: ReceiveXMLWorkFlowData) = {
    if (result != null) {
      workFlowData = result.objResult
      curName = workFlowData.strName + "-" + workFlowId

      result.objResult.arrTask.foreach { objTask =>
        mapTaskQueue = mapTaskQueue + (objTask.iId -> Queue[UserRequest]())
        log.info("Number of connection: " + objTask.arrConnection.length)

        if (objTask.arrLoop != null && objTask.arrLoop.length > 0) {
          log.info("Number of Loop: " + objTask.arrLoop.length)

          objTask.arrLoop.foreach { curLoop =>
            log.info("Loop: " + curLoop.fromTaskId + " - " + curLoop.toTaskId)

            var toMap: HashMap[Int, Int] = HashMap.empty
            var curNumOfLoop = curLoop.numOfLoop match {
              case -1 => Int.MaxValue
              case n => n
            }

            var curEndLoopCondition: String = curLoop.endLoopCondition
            var curCounter = curLoop.counter

            var curLoopObj = LoopTask.apply(curLoop.fromTaskId, curEndLoopCondition, curLoop.numOfLoop, curCounter)
            mapTaskEndLoop = mapTaskEndLoop + (curLoop.fromTaskId -> curLoopObj)

            toMap = toMap + (curLoop.toTaskId -> curNumOfLoop)
            mapLoop = mapLoop + (curLoop.fromTaskId -> toMap)
          }
        }

        if (objTask.iAlibId > 0) {
          initLibrary(objTask.iAlibId)
        }
      }
    }

    log.info("mapLoop: " + mapLoop)
    log.info("mapTaskEndLoop: " + mapTaskEndLoop)
  }

  def initLibrary(libId: Int) = {
    initActor ! InitAkkaSystem("alib", libId.toString)
  }

  def addNewUserRequest(objRequest: UserRequest) = {
    if (!mapUserRequestStatus.contains(objRequest)) {
      var objTaskMap: HashMap[Int, Seq[Int]] = HashMap.empty

      workFlowData.arrTask.foreach { objTask =>
        if (objTaskMap.contains(objTask.iOrder)) {
          val arrSeq = objTaskMap(objTask.iOrder) :+ objTask.iId
          objTaskMap = objTaskMap + (objTask.iOrder -> arrSeq)
        } else {
          objTaskMap = objTaskMap + (objTask.iOrder -> Seq(objTask.iId))
        }
      }

      var objStatus: UserRequestStatus = UserRequestStatus.apply(objRequest, objTaskMap)
      mapUserRequestStatus = mapUserRequestStatus + (objRequest -> objStatus)
    }
  }

  def updateUserRequest(objRequest: UserRequest, iCompletedTaskId: Int) = {
    if (mapUserRequestStatus.contains(objRequest)) {
      var iOrder: Int = 0
      var arrNewSeq: Seq[Int] = Seq.empty

      breakable {
        mapUserRequestStatus(objRequest).mapTaskStatus.foreach { objStatus =>
          if (objStatus._2.count(p => p == iCompletedTaskId) == 1) {
            arrNewSeq = objStatus._2.filterNot(p => p == iCompletedTaskId).map(p => p)
            iOrder = objStatus._1
            break
          }
        }
      }

      if (arrNewSeq.size <= 0) {
        val mapCurRequestStatus = mapUserRequestStatus(objRequest).mapTaskStatus - iOrder
        mapUserRequestStatus(objRequest).mapTaskStatus = mapCurRequestStatus

        mapUserRequestStatus = mapUserRequestStatus + (objRequest -> mapUserRequestStatus(objRequest))
      } else {
        val mapCurRequestStatus = mapUserRequestStatus(objRequest).mapTaskStatus + (iOrder -> arrNewSeq)
        mapUserRequestStatus(objRequest).mapTaskStatus = mapCurRequestStatus

        mapUserRequestStatus = mapUserRequestStatus + (objRequest -> mapUserRequestStatus(objRequest))
      }
    }
  }

  def getListResultKeys(iCurrentTaskId: Int, objRequest: UserRequest): (HashMap[String, HashMap[String, String]], Boolean) = {
    var seqResultKey: HashMap[String, HashMap[String, String]] = HashMap.empty
    val keyPattern: String = config.getString("redis.key-task-result") + objRequest.iRequestId
    val parentRequest: UserRequest = mapParentRequest.get(objRequest) match {
      case Some(parent) => parent
      case None => null
    }
    var intNumResultInput: Int = 0
    var intTotalExpectedInput: Int = 0

    log.info("objRequest: " + objRequest)
    log.info("parentRequest: " + parentRequest)

    if (objRequest.iParentTaskId > 0) {
      var mustSubTask: Boolean = true

      if (objRequest.seqConnection != null && objRequest.seqConnection.size > 0) {
        for (curConnection <- objRequest.seqConnection) {
          log.info("CurConnection: " + curConnection)

          if (curConnection.toSubTaskId == iCurrentTaskId && curConnection.isToSubTask == mustSubTask) {
            var curKey = ""

            if (curConnection.isFromSubTask) {
              curKey = keyPattern + "_" + curConnection.fromTaskId + "_" + curConnection.fromSubTaskId
            } else {
              curKey = keyPattern + "_" + parentRequest.iParentTaskId + "_" + curConnection.fromTaskId
            }

            log.info("CurOutKey: " + curKey)

            if (!curKey.isEmpty() && redisCli.checkKey(curKey) && redisCli.hCheckField(curKey, curConnection.fromOutputName)) {
              var curMapField: HashMap[String, String] = HashMap.empty

              if (seqResultKey.contains(curKey)) {
                curMapField = seqResultKey(curKey) + (curConnection.toInputName -> curConnection.fromOutputName)
              } else {
                curMapField = curMapField + (curConnection.toInputName -> curConnection.fromOutputName)
              }

              seqResultKey = seqResultKey + (curKey -> curMapField)

              intNumResultInput = intNumResultInput + 1
            }
          }
        }
      }
    }

    breakable {
      workFlowData.arrTask.foreach { curTask =>
        log.info("curTask: " + curTask)

        if (curTask.iId == iCurrentTaskId) {
          if (curTask.arrConnection != null && curTask.arrConnection.size > 0) {
            intTotalExpectedInput = curTask.arrConnection.size

            curTask.arrConnection.foreach { curInConnection =>
              var curKey = ""

              if (curInConnection.isFromSubTask) {
                curKey = keyPattern + "_" + curInConnection.fromTaskId + "_" + curInConnection.fromSubTaskId
              } else {
                curKey = keyPattern + "_" + objRequest.iParentTaskId + "_" + curInConnection.fromTaskId
              }

              log.info("curKey: " + curKey)
              log.info("fromOutputName: " + curInConnection.fromOutputName)

              if (!curKey.isEmpty() && redisCli.checkKey(curKey) && redisCli.hCheckField(curKey, curInConnection.fromOutputName)) {
                var curMapField: HashMap[String, String] = HashMap.empty

                if (seqResultKey.contains(curKey)) {
                  curMapField = seqResultKey(curKey) + (curInConnection.toInputName -> curInConnection.fromOutputName)
                } else {
                  curMapField = curMapField + (curInConnection.toInputName -> curInConnection.fromOutputName)
                }

                seqResultKey = seqResultKey + (curKey -> curMapField)

                intNumResultInput = intNumResultInput + 1
              }
            }
          }

          break
        }
      }
    }

    log.info("CurrentTask: " + iCurrentTaskId + " - PrevKey: " + seqResultKey)

    (seqResultKey, intNumResultInput >= intTotalExpectedInput)
  }

  def canTaskRun(objRequest: UserRequest, iTaskId: Int, iOrder: Int): (Boolean, HashMap[String, HashMap[String, String]]) = {
    var bCanRun: Boolean = true

    if (!checkRequestIsCancelled(objRequest) && mapUserRequestStatus.contains(objRequest)) {
      var arrPrevTask: Seq[Int] = Seq.empty

      breakable {
        workFlowData.arrTask.foreach { objTask =>
          if (objTask.iId == iTaskId && objTask.iArrPrevTask != null) {
            objTask.iArrPrevTask match {
              case Some(arrTask) => {
                if (arrTask.iArrTaskId != null) {
                  arrTask.iArrTaskId.foreach { iPrevTask =>
                    arrPrevTask = arrPrevTask :+ iPrevTask
                  }
                }
              }
              case None =>
            }

            break
          }
        }
      }

      if (mapUserRequestStatus.contains(objRequest) && mapUserRequestStatus(objRequest).mapTaskStatus != null) {
        breakable {
          mapUserRequestStatus(objRequest).mapTaskStatus.foreach { objStatus =>
            if (objStatus._1 < iOrder && objStatus._2 != null) {
              objStatus._2.foreach { iCurOrderTaskId =>
                if (arrPrevTask.contains(iCurOrderTaskId)) {
                  bCanRun = false
                  break
                }
              }
            }
          }
        }
      }
    } else {
      if (checkRequestIsCancelled(objRequest)) {
        bCanRun = false

        log.info("Can Task Run? Request is cancelled")
      }
    }

    if (bCanRun) {
      //Check Previous Results
      val checkResult = getListResultKeys(iTaskId, objRequest)

      //bCanRun = checkResult._2
      bCanRun = true

      log.info("Can Task Run? " + iTaskId + " - Order: " + iOrder + " - Can: " + bCanRun)

      (bCanRun, checkResult._1)
    } else {
      (bCanRun, null)
    }
  }

  def checkRequestIsCancelled(objRequest: UserRequest): Boolean = {
    var bIsCancelled: Boolean = false

    if (arrCancelledRequest.contains(objRequest.iRequestId)) {
      bIsCancelled = true
    }

    bIsCancelled
  }

  def runTask(iTaskId: Int, strTaskType: String, iOrder: Int, intRemainLoopTime: Int = 0, bIsRecheck: Boolean = false): Boolean = {
    log.info("Run Task {} - Task Type: {}", String.valueOf(iTaskId), strTaskType);
    log.info("Queue size: {}", String.valueOf(mapTaskQueue(iTaskId).size));

    //Remove request from task-queue
    if (mapTaskQueue.contains(iTaskId) && mapTaskQueue(iTaskId).size > 0) {
      var objCurUserRequest = mapTaskQueue(iTaskId) dequeue

      //Check if task request was already cancelled
      breakable {
        var bIsCancelled: Boolean = checkRequestIsCancelled(objCurUserRequest._1)
        var bIsValid: Boolean = mapUnsatisfiedTask.get(objCurUserRequest._1) match {
          case Some(arr) => {
            if (arr.contains(iTaskId)) {
              false
            } else {
              true
            }
          }
          case None => true
        }

        while (bIsCancelled && !bIsValid) {
          if (mapTaskQueue != null && mapTaskQueue(iTaskId).size > 0) {
            objCurUserRequest = mapTaskQueue(iTaskId) dequeue

            bIsCancelled = checkRequestIsCancelled(objCurUserRequest._1)
            bIsValid = mapUnsatisfiedTask.get(objCurUserRequest._1) match {
              case Some(arr) => {
                if (arr.contains(iTaskId)) {
                  false
                } else {
                  true
                }
              }
              case None => true
            }
          } else {
            mapTaskStatus = mapTaskStatus + (iTaskId -> Idle)
            break
          }
        }
      }

      if (objCurUserRequest != null) {
        log.info("User request: " + objCurUserRequest._1.iRequestId)
        mapTaskQueue = mapTaskQueue + (iTaskId -> objCurUserRequest._2)

        log.info("Dequeued - Queue size: {}", String.valueOf(mapTaskQueue(iTaskId).size));

        //Check task can be run?
        var checkTaskRun = canTaskRun(objCurUserRequest._1, iTaskId, iOrder)

        if (checkTaskRun._1) {
          //Check if task contains workflow inside
          var iInWorkflow: Int = 0
          var arrTaskConnection: Seq[TaskConnection] = Seq.empty
          var arrNextTaskConnection: Seq[TaskConnection] = Seq.empty
          var arrPrevCondition: TaskPrevCondition = TaskPrevCondition.apply(Seq.empty)

          breakable {
            workFlowData.arrTask.foreach { curTask =>
              if (curTask.iId == iTaskId && curTask.iWorkflowId > 0) {
                iInWorkflow = curTask.iWorkflowId
                arrTaskConnection = curTask.arrConnection
                break
              }
            }
          }

          breakable {
            workFlowData.arrTask.foreach { curTask =>
              if (curTask.iId == iTaskId) {
                arrPrevCondition = curTask.arrPrevCondition match {
                  case Some(arr) => arr
                  case None => null
                }

                break
              }
            }
          }

          if (iInWorkflow > 0) {
            breakable {
              workFlowData.arrTask.foreach { curTask =>
                if (curTask.iId != iTaskId) {
                  curTask.iArrPrevTask match {
                    case Some(taskOrder: TaskOrder) => {
                      if (taskOrder.iArrTaskId != null && taskOrder.iArrTaskId.size > 0 && taskOrder.iArrTaskId.contains(iTaskId)) {
                        arrNextTaskConnection = arrNextTaskConnection ++ curTask.arrConnection
                      }
                    }

                    case None =>
                  }
                }
              }
            }
          }

          //If there is no inside workflow
          if (iInWorkflow <= 0) {
            //get a-library router actor reference
            log.info("strAlibRouterActorPath: " + strAlibRouterActorPath)
            val actorRefAlibRouter = ActorRefSelection.getActorRefOfSelection(strAlibRouterActorPath, context, log)
            log.info("actorRef-Received: " + actorRefAlibRouter)

            if (actorRefAlibRouter != null) {
              log.info(actorRefAlibRouter.path.toString)

              var intAskedTime = 0
              var routerActorPath = ""

              breakable {
                do {
                  val futureAlibActorPath = actorRefAlibRouter ? AskAlibPath(strTaskType)
                  val (receivedActorPath, taskType) = Await.result(futureAlibActorPath, timeout.duration)

                  log.info("ProcessActor received AlibActor Path: {}", receivedActorPath)

                  if (receivedActorPath != null && receivedActorPath.toString() != "") {
                    routerActorPath = receivedActorPath.toString
                    break
                  } else {
                    intAskedTime = intAskedTime + 1
                    Thread.sleep(200)
                  }
                } while (intAskedTime < 5)
              }

              if (!routerActorPath.isEmpty()) {
                //get a-library actor reference
                val actorRefAlib = ActorRefSelection.getActorRefOfSelection(routerActorPath, context, log)

                log.info("actorRefAlib: {}", actorRefAlib)

                if (!routerActorPath.isEmpty() && actorRefAlib != null) {
                  var objCurLibParam: Seq[LibParam] = Seq.empty

                  log.info("objCurUserRequest._1.mapLibParam: " + objCurUserRequest._1.mapLibParam)
                  log.info("iTaskId: " + iTaskId)

                  if (objCurUserRequest._1.mapLibParam.contains(iTaskId)) {
                    objCurLibParam = objCurUserRequest._1.mapLibParam(iTaskId)
                  }

                  //mapTaskStatus = mapTaskStatus + (iTaskId -> Busy)

                  var sendTask: TaskInfo = null
                  var bIsLastTask: Boolean = false

                  //Get all previous tasks
                  breakable {
                    workFlowData.arrTask.foreach { objTask =>
                      if (objTask.iId == iTaskId) {
                        sendTask = objTask
                        break
                      }
                    }
                  }

                  //Check is last task
                  breakable {
                    workFlowData.arrTask.foreach { objTask =>
                      if (objTask.iId == iTaskId) {
                        objTask.iArrNextTask match {
                          case Some(taskOrder: TaskOrder) => {
                            if (taskOrder != null && taskOrder.iArrTaskId.length > 0) {
                              bIsLastTask = false
                            } else {
                              bIsLastTask = true
                            }
                          }

                          case None => {
                            bIsLastTask = true
                          }
                        }

                        break
                      }
                    }
                  }

                  var curEndLoop: LoopTask = LoopTask.apply(0, "", 0, "")

                  if (mapTaskEndLoop.contains(iTaskId)) {
                    var curTempEndLoop = mapTaskEndLoop.get(iTaskId) match {
                      case Some(x) => x
                      case None => null
                    }

                    if (curTempEndLoop != null) {
                      log.info("NumLoop: " + curTempEndLoop.numLoop)
                      log.info("RemainLoop: " + intRemainLoopTime)

                      curEndLoop = LoopTask.apply(curTempEndLoop.intTaskId, curTempEndLoop.endLoopCondition, curTempEndLoop.numLoop, curTempEndLoop.counter + " = " + (curTempEndLoop.numLoop - intRemainLoopTime).toString())
                    } else {
                      curEndLoop = null
                    }
                  } else {
                    curEndLoop = null
                  }

                  log.info("arrPrevCondition: " + arrPrevCondition)

                  addWorkflowStatus(objCurUserRequest._1, Working)
                  var strLog = createTimeLogMessage(objCurUserRequest._1, iTaskId, 2, Calendar.getInstance, "S", strTaskType)

                  var objNewUserRequest = mapRequestId(objCurUserRequest._1.iRequestId)

                  if (objNewUserRequest == null) {
                    objNewUserRequest = objCurUserRequest._1
                  }

                  var bCanRunInMergeMode = false

                  //actorRefAlib ! SendTaskToAlib(iTaskId, strTaskType, objCurLibParam, objNewUserRequest, checkTaskRun._2, sendTask, bIsLastTask, curEndLoop, arrPrevCondition, bCanRunInMergeMode)
                  actorRefAlib ! SendTaskToAlib(iTaskId, strTaskType, null, objNewUserRequest, checkTaskRun._2, sendTask, bIsLastTask, curEndLoop, arrPrevCondition, bCanRunInMergeMode)

                  strLog = loggingIO.generateLogMessage(objCurUserRequest._1.strCallback, objCurUserRequest._1.iUserId.toString, "L", objCurUserRequest._1.iRequestId, Working, objCurUserRequest._1.iWorkFlowId.toString, strTaskType, iTaskId.toString, strLog)
                  managerActor ! UpdateActorStatusToManager(strRemotePath, Working, strLog)
                } else {
                  var strLog: String = loggingIO.generateLogMessage(objCurUserRequest._1.strCallback, objCurUserRequest._1.iUserId.toString, "W", objCurUserRequest._1.iRequestId, Working,
                    objCurUserRequest._1.iWorkFlowId.toString, strTaskType, iTaskId.toString, "1 - There is no available library execution process.")
                  managerActor ! UpdateActorStatusToManager(strRemotePath, Working, strLog)

                  enqueueAgain(iTaskId, objCurUserRequest._1)

                  checkAndInitAlib(strTaskType, actorRefAlibRouter)
                }
              } else {
                var strLog: String = loggingIO.generateLogMessage(objCurUserRequest._1.strCallback, objCurUserRequest._1.iUserId.toString, "W", objCurUserRequest._1.iRequestId, Working,
                  objCurUserRequest._1.iWorkFlowId.toString, strTaskType, iTaskId.toString, "2 - There is no available library execution process.")
                managerActor ! UpdateActorStatusToManager(strRemotePath, Working, strLog)

                enqueueAgain(iTaskId, objCurUserRequest._1)

                checkAndInitAlib(strTaskType, actorRefAlibRouter)
              }
            } else {
              var strLog: String = loggingIO.generateLogMessage(objCurUserRequest._1.strCallback, objCurUserRequest._1.iUserId.toString, "W", objCurUserRequest._1.iRequestId, Working,
                objCurUserRequest._1.iWorkFlowId.toString, strTaskType, iTaskId.toString, "There is no library router")
              managerActor ! UpdateActorStatusToManager(strRemotePath, Working, strLog)

              enqueueAgain(iTaskId, objCurUserRequest._1)
            }
          } else {
            //get workflow router actor reference
            log.info("strWorkflowRouterActor: " + strMapActorPath)
            val actorRefWorkflowRouter = ActorRefSelection.getActorRefOfSelection(strMapActorPath, context, log)
            log.info("actorRef-Received: " + actorRefWorkflowRouter)

            if (actorRefWorkflowRouter != null) {
              log.info(actorRefWorkflowRouter.path.toString)

              var intAskedTime = 0
              var routerActorPath = ""

              breakable {
                do {
                  val futureWorkflowActorPath = actorRefWorkflowRouter ? AskWorkFlowPath(workFlowId = iInWorkflow)
                  val objResolveWorkflow = Await.result(futureWorkflowActorPath, timeout.duration)

                  log.info("ProcessActor received WorkflowActor Path: {}", objResolveWorkflow)

                  if (objResolveWorkflow.isInstanceOf[ReceiveWorkFlowPath]) {
                    val curResolve = objResolveWorkflow.asInstanceOf[ReceiveWorkFlowPath]

                    if (curResolve.path != null && !curResolve.path.isEmpty()) {
                      routerActorPath = curResolve.path
                      break
                    } else {
                      intAskedTime = intAskedTime + 1
                      Thread.sleep(200)
                    }
                  } else {
                    intAskedTime = intAskedTime + 1
                    Thread.sleep(200)
                  }
                } while (intAskedTime < 5)
              }

              //get a-library actor reference
              val actorRefWorkflow = ActorRefSelection.getActorRefOfSelection(routerActorPath.toString, context, log)

              log.info("actorRefWorkflow: {}", actorRefWorkflow)

              if (routerActorPath != "" && actorRefWorkflow != null) {
                actorRefWorkflow ! SendWorkFlow(objCurUserRequest._1.strCallback, objCurUserRequest._1.iUserId, iInWorkflow, objCurUserRequest._1.iRequestId, objCurUserRequest._1.mapLibParam,
                  objCurUserRequest._1.bIsStepByStep, iTaskId, workFlowId, objCurUserRequest._1, arrTaskConnection, arrNextTaskConnection)
              }
            } else {
              enqueueAgain(iTaskId, objCurUserRequest._1)
            }
          }
        } else {
          var bIsValid = mapUnsatisfiedTask.get(objCurUserRequest._1) match {
            case Some(arr) => {
              if (arr.contains(iTaskId)) {
                false
              } else {
                true
              }
            }
            case None => true
          }

          if (!checkRequestIsCancelled(objCurUserRequest._1) && bIsValid) {
            enqueueAgain(iTaskId, objCurUserRequest._1)

            //Check if there is some tasks that have smaller order and are waiting
            if (iOrder > 1) {
              var iSmallerOrder: Int = iOrder - 1

              if (!bIsRecheck) {
                breakable {
                  workFlowData.arrTask.foreach { objTask =>
                    if (objTask.iOrder == iSmallerOrder) {
                      runTask(objTask.iId, objTask.iAlibId.toString, objTask.iOrder, bIsRecheck = false)

                      break
                    }
                  }
                }
              }
            }
          }
        }
      }
    } else {
      mapTaskStatus = mapTaskStatus + (iTaskId -> Idle)
    }

    true
  }

  def checkAndInitAlib(strLibId: String, actorRefAlibRouter: ActorRef) = {
    try {
      val futureAlibRouter = actorRefAlibRouter ? CheckAlib(strLibId)
      val futureAlibRouterResult = Await.result(futureAlibRouter, timeout.duration)

      if (futureAlibRouterResult == null || futureAlibRouterResult.toString.equals("0")) {
        initActor ! InitAkkaSystem("alib", strLibId)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ExceptionUtil.getStrackTrace(ex))
      }
    }
  }

  def enqueueAgain(iTaskId: Int, objUserRequest: UserRequest) {
    val curQueue = mapTaskQueue(iTaskId) enqueue objUserRequest
    mapTaskQueue = mapTaskQueue + (iTaskId -> curQueue)
  }

  def checkLoopTask(objRequest: UserRequest, iTaskId: Int): (Int, Int, String, Int) = {
    var iBackTaskId: Int = 0
    var iCurOrder: Int = 0
    var strTaskType: String = ""
    var iRemainLoopTime: Int = 0
    var remainMapTo: HashMap[Int, Int] = HashMap.empty
    var remainMapFrom: HashMap[Int, HashMap[Int, Int]] = HashMap.empty

    log.info("mapRequestLoop: " + mapRequestLoop)

    if (mapRequestLoop.contains(objRequest)) {

      mapRequestLoop.get(objRequest) match {
        case Some(mapFrom) => {
          remainMapFrom = mapFrom

          if (mapFrom.contains(iTaskId)) {
            mapFrom.get(iTaskId) match {
              case Some(mapTo) => {
                remainMapTo = mapTo
                breakable {
                  for (curItem: (Int, Int) <- mapTo) {
                    if (curItem._2 > 0) {
                      iBackTaskId = curItem._1
                      iRemainLoopTime = curItem._2

                      break
                    }
                  }
                }
              }
              case None =>
            }
          }
        }
        case None =>
      }

      if (iBackTaskId != 0 && iRemainLoopTime > 0) {
        log.info("Loop Time: " + iRemainLoopTime)

        //Update remain time
        iRemainLoopTime = iRemainLoopTime - 1
        remainMapTo = remainMapTo + (iBackTaskId -> iRemainLoopTime)
        remainMapFrom = remainMapFrom + (iTaskId -> remainMapTo)
        mapRequestLoop = mapRequestLoop + (objRequest -> remainMapFrom)

        breakable {
          workFlowData.arrTask.foreach { objTask =>
            if (objTask.iId == iBackTaskId) {
              iCurOrder = objTask.iOrder
              strTaskType = objTask.iAlibId.toString

              //Add Sequence Task to Queue
              addNextTaskToQueue(iBackTaskId, iTaskId, objRequest)

              break
            }
          }
        }
      } else {
        iBackTaskId = 0
      }
    }

    log.info("iRemainLoopTime: " + iRemainLoopTime)

    (iBackTaskId, iCurOrder, strTaskType, iRemainLoopTime)
  }

  def removeTaskFromQueue(iCurTask: Int, objRequest: UserRequest): Boolean = {
    log.info("Remove task " + iCurTask + " from queue")
    var bIsStop: Boolean = false

    var curUnsatisfiedList = mapUnsatisfiedTask.get(objRequest) match {
      case Some(arr) => arr
      case None => mapUnsatisfiedTask = mapUnsatisfiedTask + (objRequest -> Seq.empty)
    }

    breakable {
      workFlowData.arrTask.foreach { curTask =>
        if (curTask.iId == iCurTask) {
          curTask.iArrNextTask match {
            case Some(arrTask) => {
              arrTask.iArrTaskId.foreach { iNextTaskId =>
                var curSeq = mapUnsatisfiedTask.get(objRequest) match {
                  case Some(arr) => arr
                  case None => Seq.empty
                }

                curSeq = curSeq :+ iNextTaskId
                mapUnsatisfiedTask = mapUnsatisfiedTask + (objRequest -> curSeq)

                bIsStop = removeTaskFromQueue(iNextTaskId, objRequest)

                if (bIsStop) {
                  break
                }
              }
            }
            case None => bIsStop = true
          }
          break
        }
      }
    }

    bIsStop
  }

  def addNextTaskToQueue(iCurTask: Int, iToTask: Int, objRequest: UserRequest): Boolean = {
    log.info("Add Next Task to Queue: curTask: " + iCurTask + " - toTask: " + iToTask)

    var intCurAddTask: Int = iCurTask
    var bIsStop: Boolean = false

    var curTaskQueue = mapTaskQueue.get(intCurAddTask) match {
      case Some(queue) => queue
      case None => Queue[UserRequest]()
    }

    curTaskQueue = curTaskQueue.enqueue(objRequest)
    mapTaskQueue = mapTaskQueue + (intCurAddTask -> curTaskQueue)

    if (iCurTask != iToTask) {
      breakable {
        workFlowData.arrTask.foreach { objTask =>
          if (objTask.iId == iCurTask) {
            objTask.iArrNextTask match {
              case Some(arrTask) => {
                arrTask.iArrTaskId.foreach { iNextTaskId =>
                  addNextTaskToQueue(iNextTaskId, iToTask, objRequest)

                  if (iNextTaskId == iToTask) {
                    bIsStop = true
                    break
                  }
                }
              }
              case None =>
            }

            break
          }
        }
      }
    } else {
      bIsStop = true
    }

    bIsStop
  }

  def runNextTask(objRequest: UserRequest, iTaskId: Int) = {
    var arrNextTask: Seq[Int] = Seq.empty
    var curTaskOrder: Int = 0

    workFlowData.arrTask.foreach { objTask =>
      if (objTask.iId == iTaskId) {
        curTaskOrder = objTask.iOrder

        arrNextTask = objTask.iArrNextTask match {
          case Some(arrTask) => arrTask.iArrTaskId
          case None => null
        }
      }
    }

    if (arrNextTask != null && arrNextTask.size > 0) {
      arrNextTask.foreach { iNextTaskId =>
        log.info("Next Task of Task {} is Task {}", String.valueOf(iTaskId), String.valueOf(iNextTaskId));

        var iRemainLoopTime: Int = 0

        mapRequestLoop.get(objRequest) match {
          case Some(mapFrom) => {
            val remainMapFrom = mapFrom

            if (mapFrom.contains(iNextTaskId)) {
              mapFrom.get(iNextTaskId) match {
                case Some(mapTo) => {
                  val remainMapTo = mapTo
                  breakable {
                    for (curItem: (Int, Int) <- mapTo) {
                      if (curItem._2 > 0) {
                        iRemainLoopTime = curItem._2

                        break
                      }
                    }
                  }
                }
                case None =>
              }
            }
          }
          case None =>
        }

        var arrTaskType = workFlowData.arrTask.filter(p => p.iId == iNextTaskId).map(item => (item.iId, (item.iAlibId.toString, item.iOrder)))
        runTask(iNextTaskId, arrTaskType(0)._2._1, arrTaskType(0)._2._2, iRemainLoopTime)
      }
    }
  }

  def runRemainTasksInSameOrder(objRequest: UserRequest, iTaskId: Int) = {
    var arrRemainTask: Seq[Int] = Seq.empty
    var iCurOrder: Int = 0

    breakable {
      workFlowData.arrTask.foreach { objTask =>
        if (objTask.iId == iTaskId) {
          iCurOrder = objTask.iOrder
          break
        }
      }
    }

    if (iCurOrder > 1) {
      workFlowData.arrTask.foreach { objTask =>
        if (objTask.iOrder == iCurOrder) {
          log.info("Task is same order with " + iTaskId + ": " + objTask.iId)

          if (mapTaskQueue.contains(objTask.iId) && mapTaskQueue(objTask.iId).size > 0) {
            log.info("Still have " + objTask.iId)
            runTask(objTask.iId, objTask.iAlibId.toString, iCurOrder)
          } else {
            log.info("Find next of " + objTask.iId)
            runNextTask(objRequest, objTask.iId)
          }
        }
      }
    }
  }
}

object WorkflowActor {
  def props(workFlowId: Int): Props = Props(classOf[WorkflowActor], workFlowId)
}
