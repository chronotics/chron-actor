package org.chronotics.actor.scala.akka.remote

import java.io.File
import java.util.Calendar

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorIdentity, ActorKilledException, ActorLogging, ActorRef, Identify, OneForOneStrategy, Props, Terminated}
import akka.remote.DisassociatedEvent
import akka.util.Timeout
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.config.{Config, ConfigFactory}
import jep.Jep
import org.apache.commons.lang.SerializationUtils
import org.chronotics.actor.scala.akka.data.XMLActor
import org.chronotics.actor.scala.akka.single.StatusManagerActor
import org.chronotics.pandora.java.exception.ExceptionUtil
import org.chronotics.pandora.scala.file.FileIO
import org.chronotics.pithos.ext.redis.scala.adaptor.RedisPoolConnection
import org.chronotics.silverbullet.scala.akka.io.LoggingIO
import org.chronotics.silverbullet.scala.akka.protocol.Message._
import org.chronotics.silverbullet.scala.akka.state._
import org.chronotics.silverbullet.scala.akka.util.{EnvConfig, RemoteAddressExtension}
import org.chronotics.silverbullet.scala.model.{LibMeta, LibParam, LibParams, UserRequest}
import org.chronotics.silvertongue.scala.engine._
import org.chronotics.silvertongue.scala.listener.REngineOutputListener
import org.rosuda.REngine.JRI.JRIEngine
import org.rosuda.REngine.REngine

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.control.Breaks._

class LibraryActor(strLibType: String, strLang: String, pID: Int, objREngine: REngine, objPythonEngine: Jep) extends Actor with ActorLogging {
  import context.dispatcher

  val config: Config = ConfigFactory.parseFile(EnvConfig.getConfigFile("z2_conn")).resolve()
  val loggingIO: LoggingIO = LoggingIO.getInstance()
  val remoteAddr = RemoteAddressExtension(context.system).address
  val snapShotInterval = 1000
  val alibRouterActorPath = "akka.tcp://" + config.getString("alibrouterhandler.system") + "@" + config.getString("alibrouterhandler.host") + ":" + config.getString("alibrouterhandler.port") + "/user/alib"

  var remotePath = self.path.toStringWithAddress(remoteAddr)
  var alibMetaData: LibMeta = null

  val redisHost = config.getString("redis.host")
  val redisPort = config.getInt("redis.port")
  val redisDB = config.getInt("redis.db")
  val redisPass = config.getString("redis.pass")
  val redisCli = RedisPoolConnection.getInstance(redisHost, redisPort, redisDB, redisPass)

  val managerActor: ActorRef = context.actorOf(Props(classOf[StatusManagerActor]))
  val actorXMLReader: ActorRef = context.actorOf(Props(classOf[XMLActor]))
  var objLocalREngine: REngine = null
  var lElapseTime: Long = 0L
  val lMaxElapseTime: Long = config.getLong("configdir.max_elapsed_time")

  val shBashPath: String = config.getString("sh.bash_path")
  val shBashAnnotation: String = config.getString("sh.bash_annotation")
  val shShPath: String = config.getString("sh.sh_path")
  val shBashInitFile: String = config.getString("sh.bash_init_file")
  val shBashInitLogFile: String = config.getString("sh.bash_init_log")
  val shWaitingTime: Long = config.getLong("sh.bash_waiting_time")
  val fileConnection: FileIO = FileIO.getInstance()

  var curName: String = strLibType
  var lIdleTimeout: Long = Calendar.getInstance.getTimeInMillis
  var lMaxIdleTimeout: Long = config.getLong("alibworkerhandler.idle_time")
  var bIsScheduling: Boolean = false

  override def supervisorStrategy = OneForOneStrategy(maxNrOfRetries = -1, withinTimeRange = Duration.Inf) {
    case exOOM: OutOfMemoryError => {
      log.error(loggingIO.generateLogMessage(Calendar.getInstance.getTimeInMillis.toString, "admin", "L", 0, Finished, "0", strLibType, "", exOOM.getStackTraceString))
      Stop
    }
    case exKilled: ActorKilledException => {
      log.error(loggingIO.generateLogMessage(Calendar.getInstance.getTimeInMillis.toString, "admin", "L", 0, Finished, "0", strLibType, "", exKilled.getStackTraceString))
      Stop
    }
    case ex: Throwable => {
      log.error(loggingIO.generateLogMessage(Calendar.getInstance.getTimeInMillis.toString, "admin", "L", 0, Finished, "0", strLibType, "", ex.getStackTraceString))
      Stop
    }
  }

  def clientSubscribe() {
    val actorAlibRouter = getActorRefOfSelection(alibRouterActorPath, false)

    if (actorAlibRouter != null) {
      val remoteAddr = RemoteAddressExtension(context.system).address
      remotePath = self.path.toStringWithAddress(remoteAddr)

      actorAlibRouter ! UpdateAlibActorPath(remotePath, strLibType)
    }

    Jep.setSharedModulesArgv("")

    if (strLibType.toInt > 0) {
      actorXMLReader ! ReadXMLAlibDB(strLibType.toInt)
    }

    context.system.scheduler.schedule(5 seconds, 5 seconds) {
      scheduleIdleLib()
    }
  }

  override def preRestart(reason: Throwable, message: Option[Any]) = {
    log.info(loggingIO.generateLogMessage(Calendar.getInstance.getTimeInMillis.toString, "admin", "L", 0, Restarted, "0", strLibType, "", "Restarting - Reason: " + reason match {
      case null => ""
      case _ => reason.getStackTraceString
    }))

    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable) = {
    log.info(loggingIO.generateLogMessage(Calendar.getInstance.getTimeInMillis.toString, "admin", "L", 0, Restarted, "0", strLibType, "", "Restart completed! - Reason: " + reason match {
      case null => ""
      case _ => reason.getStackTraceString
    }))

    super.postRestart(reason)
  }

  override def preStart(): Unit = {
    clientSubscribe()
  }

  def stopLibActor(isKill: Boolean = false) = {
    val actorAlibRouter = getActorRefOfSelection(alibRouterActorPath, false)

    val remoteAddr = RemoteAddressExtension(context.system).address
    remotePath = self.path.toStringWithAddress(remoteAddr)

    if (actorAlibRouter != null) {
      managerActor ! UpdateActorStatusToManager(remotePath, Stopped, curName)

      if (isKill) {
        actorAlibRouter ! KilledAlibActor(remotePath, strLibType)
      } else {
        actorAlibRouter ! RemoveAlibActorPath(remotePath, strLibType)
      }
    }
  }

  override def postStop(): Unit = {
    stopLibActor()
  }

  override def receive: Receive = {
    case libMeta: ReceiveXMLAlibData => {
      log.debug("libMeta:" + libMeta)
      alibMetaData = libMeta.objResult

      curName = alibMetaData.strName + "-" + strLibType
    }

    case update: UpdateAlibMetaData => {
      actorXMLReader ! ReadXMLAlibDB(update.libId)
    }

    case task: SendTaskToAlib => { //Receive request from user
      lIdleTimeout = Calendar.getInstance.getTimeInMillis

      val actorAlibRouter = getActorRefOfSelection(alibRouterActorPath, false)
      val remoteAddr = RemoteAddressExtension(context.system).address
      val remotePath = self.path.toStringWithAddress(remoteAddr)
      val strAddrKey = remoteAddr.toString

      val curSender = sender()

      log.info(loggingIO.generateLogMessage(task.objRequest.strCallback, task.objRequest.iUserId.toString, "L", task.objRequest.iRequestId, Started, task.objRequest.iWorkFlowId.toString, task.objTaskInfo.iAlibId.toString, task.iTaskId.toString, s"Received task ${task.iTaskId}, type: ${task.strType}, requestId: ${task.objRequest.iRequestId}"))
      //log.info(task.toString)

      writeRedis(strAddrKey, "[" + task.objRequest.iUserId + "][" + task.objRequest.iWorkFlowId + "]")

      //updateRequestWaiting(CurRequest(task.objRequest, curSender))

      if (actorAlibRouter != null) {
        actorAlibRouter ! RemoveAlibActorPath(remotePath, strLibType) //Remove its address from router actor

        replaceAlibDataWithUserData(task.iTaskId, task.objRequest)

        val curLibType = alibMetaData.strType.toLowerCase
        val strLang: String = alibMetaData.strLang match {
          case Some(txt) => txt
          case None => ""
        }

        var strLogMessage = loggingIO.generateLogMesageTaskAlib(task, Working, "")
        managerActor ! UpdateSimpleStatusToManager(strLogMessage)

        var bIsNewProcess: Boolean = true

        if (lElapseTime < lMaxElapseTime) {
          bIsNewProcess = false
        }

        var taskExecute: SendTaskToExecuteNoActorRef = SendTaskToExecuteNoActorRef(task.iTaskId, task.strType, task.arrParam, task.objRequest, task.mapPrevResultKey, alibMetaData,
          task.objTaskInfo, task.bIsLastTask, task.endLoopCondition, prevCondition = task.prevCondition, isMergeMode = task.isMergeMode)

        if (strLang.equals("R")) {
          startLangEngineProcess(taskExecute, sender(), true)
        } else if (strLang.equals("Python")) {
          startLangEngineProcess(taskExecute, sender(), bIsNewProcess)
        } else if (strLang.equals("Jar")) {
          startLangEngineProcess(taskExecute, sender(), true)
        } else if (strLang.equals("War")) {
          startLangEngineProcess(taskExecute, sender(), true)
        } else if (strLang.equals("Formula")) {
          startLangEngineProcess(taskExecute, sender(), bIsNewProcess)
        } else if (strLang.equals("ES")) {
          startLangEngineProcess(taskExecute, sender(), true)
        }
      } else {
        val strLogging: String = loggingIO.generateLogMesageTaskAlib(task, Failed, "TRACE_REQ " + task.objRequest.iRequestId + " - " + task.iTaskId + " - Can't find the execution router")

        log.info(strLogging)

        managerActor ! UpdateActorStatusToManager(remotePath, Failed, strLogging)
        curSender ! AlibTaskCompleted(task.iTaskId, task.strType, task.objRequest, curSender, Failed, true, true)
      }
    }

    case AlibTaskCompleted(iTaskId, strType, objRequest, objProcSender, objTaskStatus, isEndLoop, isPrevConditionSatisfied) => {
      val strLogging: String = loggingIO.generateLogMessage(objRequest.strCallback, objRequest.iUserId.toString, "L", objRequest.iRequestId, Finished, objRequest.iWorkFlowId.toString, strType, iTaskId.toString, "Task Completed")
      log.info(strLogging)

      val actorAlibRouter = getActorRefOfSelection(alibRouterActorPath, false)
      val remoteAddr = RemoteAddressExtension(context.system).address
      val remotePath = self.path.toStringWithAddress(remoteAddr)

      if (actorAlibRouter != null) {
        actorAlibRouter ! UpdateAlibActorPath(remotePath, strLibType) //Update its address to router actor again
      }

      if (objProcSender != null) {
        objProcSender ! AlibTaskCompleted(iTaskId, strType, objRequest, objProcSender, objTaskStatus, isEndLoop, isPrevConditionSatisfied)
      }

      managerActor ! UpdateActorStatusToManager(remotePath, Finished, strLogging)
    }

    case health: Alive => {
      managerActor ! UpdateActorStatusToManager(remotePath, Started, curName)
    }

    case stop: StopActor => {
      if (stop.strType == "alib" && stop.id == strLibType) {
        context.stop(self)
        managerActor ! UpdateActorStatusToManager(remotePath, Stopped, curName)
      }
    }

    case Ping => {
      sender() ! Pong
    }

    case Terminated(ref) => {
      log.error(strLibType + "-+++Actor " + ref.path.toString + " is terminated")
      context unwatch ref

      if (ref.path.toString.contains(config.getString("alibrouterhandler.system"))) {
        context.system.scheduler.scheduleOnce(1 seconds) {
          context.actorSelection(alibRouterActorPath) ! Identify(strLibType + "-router")
        }
      }
    }

    case evt: DisassociatedEvent => {
      log.debug(strLibType + "-Something was happened: " + evt)
    }

    case ActorIdentity(objIdentifyId, optActorRef) => {
      optActorRef match {
        case Some(actorAlibRouter) => {
          val remoteAddr = RemoteAddressExtension(context.system).address
          remotePath = self.path.toStringWithAddress(remoteAddr)

          log.info(strLibType + "-RemotePath: " + remotePath)

          if (actorAlibRouter != null) {
            actorAlibRouter ! UpdateAlibActorPath(remotePath, strLibType) //Update its address to router actor
          }
        }
        case None => {
          log.info(strLibType + "-No Received ActorRouter. Try again!")

          context.system.scheduler.scheduleOnce(1 seconds) {
            context.actorSelection(alibRouterActorPath) ! Identify(strLibType + "-router")
          }
        }
      }
    }
  }

  def scheduleIdleLib() = {
    if (!bIsScheduling) {
      bIsScheduling = true

      var lElapse = Calendar.getInstance.getTimeInMillis - lIdleTimeout

      if (lElapse > lMaxIdleTimeout) {
        lIdleTimeout = Calendar.getInstance.getTimeInMillis
        log.warning("WARN: Idle Timeout - Actor of alib " + strLibType + " will be killed")
        stopLibActor(true)

        Thread.sleep(1000)

        context.system.terminate
      }

      bIsScheduling = false
    }
  }

  def replaceAlibDataWithUserData(curTaskId: Int, objRequest: UserRequest) = {
    log.info("curTaskId: " + curTaskId)
    log.info("mapLibParam: " + objRequest.mapLibParam)

    breakable {
      objRequest.mapLibParam.foreach { curAlib =>
        log.info("curAlib: " + curAlib)

        if (curAlib._1.equals(curTaskId)) {
          var arrNewLibParams: Seq[LibParam] = Seq.empty
          var arrDuplicateParam: Seq[Int] = Seq.empty

          var intCount = 0
          alibMetaData.arrParam.arrLibParam.foreach { curLibParam =>
            var intCountComp = 0
            alibMetaData.arrParam.arrLibParam.foreach { curLibParamComp =>
              if (intCount != intCountComp) {
                if (curLibParam.iId == curLibParamComp.iId) {
                  arrDuplicateParam = arrDuplicateParam :+ curLibParam.iId
                }
              }

              intCountComp = intCountComp + 1
            }

            intCount = intCount + 1
          }

          arrDuplicateParam = arrDuplicateParam.distinct

          alibMetaData.arrParam.arrLibParam.foreach { metaParam =>
            var bIsExistNew = false
            var bIsAdd = true
            var bIsRuntime = false

            if (arrDuplicateParam.contains(metaParam.iId)) {
              bIsAdd = false

              if (!metaParam.strShow.equals("Runtime")) {
                bIsAdd = true
                bIsRuntime = true
              }
            }

            if (bIsAdd && metaParam.strDataType != "UI List") {
              curAlib._2.foreach { curLibParam =>
                if (metaParam.iId == curLibParam.iId) {
                  log.info(strLibType + "-curLibParam: " + curLibParam.iId)

                  var newMetaParam: LibParam = LibParam.apply(metaParam.iId, metaParam.paramType, metaParam.strName, metaParam.strDesc, metaParam.iParent, bIsRuntime match { case true => "Resource"; case _ => metaParam.strDataType },
                    metaParam.dMinValue, metaParam.dMaxValue, curLibParam.dDefaultValue, metaParam.strShow, metaParam.strDefinedVals, metaParam.intIsResource)
                  arrNewLibParams = arrNewLibParams :+ newMetaParam
                  bIsExistNew = true
                }
              }

              if (!bIsExistNew) {
                var newMetaParam: LibParam = LibParam.apply(metaParam.iId, metaParam.paramType, metaParam.strName, metaParam.strDesc, metaParam.iParent, metaParam.strDataType,
                  metaParam.dMinValue, metaParam.dMaxValue, metaParam.dDefaultValue, metaParam.strShow, metaParam.strDefinedVals, metaParam.intIsResource)
                arrNewLibParams = arrNewLibParams :+ newMetaParam
              }
            }
          }

          var newAlibMetaData: LibMeta = LibMeta.apply(alibMetaData.iId, alibMetaData.strType, alibMetaData.strName, alibMetaData.isSimulate, alibMetaData.strLang, LibParams.apply(arrNewLibParams))
          alibMetaData = newAlibMetaData

          break
        }
      }
    }

    log.info("alibMetaData: " + alibMetaData)
  }

  def createShProcessFromCmd(cmd: String, iRequestId: Int, iTaskId: Int) = {
    try {
      var strFileName = shBashInitFile.replace("{0}", iRequestId.toString).replace("{1}", iTaskId.toString)
      var strProcessCmd = cmd.replace("\r", "").replace("\n", "")
      var objStrBuilder = new StringBuilder();
      objStrBuilder.append(shBashAnnotation).append("\r\n")

      var strCmd = "setsid " + strProcessCmd + " >| \"" + shBashInitLogFile + "\" &"
      objStrBuilder.append(strCmd)

      log.info("createBashFile-Cmd: " + objStrBuilder.toString)

      val objFile = new File(strFileName)
      fileConnection.writeFile(objStrBuilder.toString, strFileName)

      if (new File(strFileName).exists()) {
        log.info("createBashFile-Exist: " + strFileName)

        val objThread = new Thread(new Runnable {
          override def run(): Unit = {
            var strCmd = shShPath + " " + strFileName
            var objRuntime = Runtime.getRuntime
            objRuntime.exec(strCmd.split(" "))
          }
        })

        objThread.start()
      } else {
        log.info("createBashFile-Not Exist: " + strFileName)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ExceptionUtil.getStrackTrace(ex))
      }
    }
  }

  def startLangEngineProcess(task: SendTaskToExecuteNoActorRef, objProcSender: ActorRef, bIsNewProcess: Boolean = true) {
    var loggingIO: LoggingIO = LoggingIO.getInstance()
    //Status
    var objStatus: ActorState = Working
    var bIsEndloop: Boolean = false
    var bIsPrevConditionSatisfied: Boolean = false

    var actorAlibRouter = getActorRefOfSelection(alibRouterActorPath, false)
    var remoteAddr = RemoteAddressExtension(context.system).address
    var remotePath = self.path.toStringWithAddress(remoteAddr)

    //Get JSON String of task SendTaskToExcute
    val mapperJson = new ObjectMapper with ScalaObjectMapper
    mapperJson.registerModule(DefaultScalaModule)
    mapperJson.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    var strTaskJSON: String = "ObjectToJSONString" // mapperJson.writeValueAsString(task)

    if (strTaskJSON != null && !strTaskJSON.equals("")) {
      //Write request to Redis
      val redisKeyTaskRequestTemplate = config.getString("redis.key-task-request")
      val strRedisKeyTaskRequest = redisKeyTaskRequestTemplate + task.objRequest.iRequestId + "_" + task.iTaskId
      val arrTaskByte = SerializationUtils.serialize(task)
      writeRedis(strRedisKeyTaskRequest, arrTaskByte)

      try {
        if (bIsNewProcess) {
          var strLogMessage = loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "Run in New Process")
          managerActor ! UpdateActorStatusToManager(remotePath, Working, strLogMessage)

          //Create and Start News Process
          var strCmdParam: StringBuilder = StringBuilder.newBuilder
          strCmdParam = strCmdParam.append(task.objRequest.iRequestId).append(",").append(task.objRequest.iWorkFlowId)
            .append(",").append(task.iTaskId).append(",").append(task.objTaskInfo.iAlibId)
            .append(",").append(strLang).append(",").append(task.objRequest.strCallback)
            .append(",").append(task.objRequest.iUserId.toString)

          val strPrefix = EnvConfig.getPrefix()
          val strLogbackPath = config.getString("logback.path").replace("[PREFIX]", strPrefix)
          val strJavaExecCp = config.getString("java_exec.cp").replace("[PREFIX]", strPrefix)
          var strJavaCmd = "java -Dlogback.configurationFile=" + strLogbackPath + " -cp "
          var strCmd = strJavaCmd + strJavaExecCp + " brique.z2.engine.lang.LangEngineApp " + strCmdParam.toString

          log.info("Cmd: " + strCmd)

          createShProcessFromCmd(strCmd, task.objRequest.iRequestId, task.iTaskId)
        } else {
          var strLogMessage = loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "Run in Same Process")
          managerActor ! UpdateActorStatusToManager(remotePath, Working, strLogMessage)

          runLangEngine(task.objRequest.strCallback, task.objRequest.iUserId.toString, task.objRequest.iRequestId.toString,
            task.objRequest.iWorkFlowId.toString, task.iTaskId.toString, strLibType, strLang, bIsNewProcess)
        }

        //Read response result data
        val strRedisKeyTaskRequestCompleted = strRedisKeyTaskRequest + "_completed"
        var bIsExistKey = false
        val calWaitingBegin = Calendar.getInstance

        breakable {
          do {
            bIsExistKey = redisCli.checkKey(strRedisKeyTaskRequestCompleted)

            if (!bIsExistKey) {
              Thread.sleep(100);

              val lWaitingTime = (Calendar.getInstance.getTimeInMillis - calWaitingBegin.getTimeInMillis)

              if (lWaitingTime > shWaitingTime) {
                break
              }
            } else {
              break
            }
          } while (!bIsExistKey)
        }

        if (bIsExistKey) {
          log.info("startLangEngineProcess - strRedisKeyTaskRequestCompleted: " + strRedisKeyTaskRequestCompleted)

          redisCli.getKey(strRedisKeyTaskRequestCompleted) match {
            case Some(str) => {
              var arrResult = str.toString().split("\\|")

              if (arrResult != null && arrResult.length >= 1) {
                //strStatus + "|" + isEndLoop.toString + "|" + isPrevConditionSatisfied.toString
                var strStatus = arrResult(0).trim()
                var strIsEndloop = arrResult(1).trim()
                var strIsPrevConditionSatisfied = arrResult(2).trim()

                strStatus match {
                  case "Finished" => objStatus = Finished
                  case "NotSatisfied" => objStatus = NotSatisfied
                  case "Working" => objStatus = Working
                  case "Failed" => objStatus = Failed
                  case "Waiting" => objStatus = Waiting
                }

                strIsEndloop match {
                  case "1" => bIsEndloop = true
                  case "0" => bIsEndloop = false
                }

                strIsPrevConditionSatisfied match {
                  case "1" => bIsPrevConditionSatisfied = true
                  case "0" => bIsPrevConditionSatisfied = false
                }
              }
            }
            case None => {
            }
          }

          redisCli.delKey(strRedisKeyTaskRequestCompleted)
        }
      } catch {
        case ex: Throwable => {
        }
      }
    }

    //Return to actor system
    if (objStatus.equals(Waiting)) {
      log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Waiting, "TRACE_REQ " + task.objRequest.iRequestId.toString + " -Task Completed and Waiting"))
    } else {
      log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Finished, "TRACE_REQ " + task.objRequest.iRequestId.toString + " -Task Completed"))
    }

    actorAlibRouter = getActorRefOfSelection(alibRouterActorPath, false)
    remoteAddr = RemoteAddressExtension(context.system).address
    remotePath = self.path.toStringWithAddress(remoteAddr)

    //val objWaitingSender = mapWaiting.contain(objRequest)
    if (actorAlibRouter != null) {
      actorAlibRouter ! UpdateAlibActorPath(remotePath, strLibType) //Update its address to router actor again
    }

    if (objProcSender != null) {
      log.info("startLangEngineProcess - PrevCondition: " + bIsPrevConditionSatisfied)
      log.info("startLangEngineProcess - IsEndLoop: " + bIsEndloop)

      context.system.scheduler.scheduleOnce(1 second) {
        objProcSender ! AlibTaskCompleted(task.iTaskId, task.strType, task.objRequest, objProcSender, objStatus.equals(Waiting) match { case true => Waiting case false => Finished }, bIsEndloop, bIsPrevConditionSatisfied)
      }
    }

    managerActor ! UpdateActorStatusToManager(remotePath, Finished, loggingIO.generateLogMesageTaskExecuteNoActor(task, objStatus.equals(Waiting) match { case true => Waiting case false => Finished }, ""))
  }

  def runLangEngine(strCallback: String, strUserId: String, strRequestId: String, strWorkflowId: String, strTaskId: String, strLibId: String,
                    strLanguage: String, bIsNewProcess: Boolean = true) {
    var objLangEngine: AbstractLangEngine = null

    log.info("Language: " + strLanguage)

    strLanguage match {
      case "R" => {
        if (objLocalREngine == null) {
          objLocalREngine = startNewREngine(strCallback, strUserId, strRequestId, strWorkflowId, strTaskId, strLibId, false)
        }

        objLangEngine = new RLangEngine(strRequestId, strWorkflowId, strTaskId, strLibId, objLocalREngine)
      }
      case "Python" => {
        objLangEngine = new PythonLangEngine(strRequestId, strWorkflowId, strTaskId, strLibId, null)
      }
      case "War" => {
        objLangEngine = new WarLangEngine(strRequestId, strWorkflowId, strTaskId, strLibId, null)
      }
      case "Jar" => {
        objLangEngine = new JarLangEngine(strRequestId, strWorkflowId, strTaskId, strLibId, null)
      }
//      case "Formula" => {
//        objLangEngine = new FormulaLangEngine(strRequestId, strWorkflowId, strTaskId, strLibId, null)
//      }
      case "ES" => {
        objLangEngine = new ElasticSearchEngine(strRequestId, strWorkflowId, strTaskId, strLibId, null)
      }
    }

    log.info("openKafkaProducer")
    objLangEngine.openKafkaProducer()

    log.info("readDataObjectFromRedis")
    objLangEngine.readDataObjectFromRedis()

    var calBegin: Calendar = Calendar.getInstance
    log.info("executeLanguageEngine")
    objLangEngine.executeLanguageEngine(bIsNewProcess)

    lElapseTime = Calendar.getInstance.getTimeInMillis - calBegin.getTimeInMillis
    log.info("Elapse-Time: " + lElapseTime)

    objLangEngine.closeKafkaProducer()
  }

  def startNewREngine(strCallback: String, strUserId: String, strRequestId: String, strWorkflowId: String, strTaskId: String, strLibId: String, isUseBufferred: Boolean = false): REngine = {
    try {
      if (objREngine != null) {
        objREngine
      } else {
        return REngine.engineForClass(classOf[JRIEngine].getName, Array[String] { "--no-save" }, new REngineOutputListener(strCallback, strUserId, strRequestId, strWorkflowId, strTaskId, strLibId, isUseBufferred), false)
      }
    } catch {
      case e: Throwable => {
        log.error(e.getMessage + " - cause by: " + e.getCause + " - stacktrace: " + e.getStackTraceString, "ERR")
        null
      }
    }
  }

  def getActorRefOfSelection(path: String, isWatch: Boolean): ActorRef = {
    implicit val resolveTimeout = Timeout(10 seconds)
    try {
      val actorRef = Await.result(context.actorSelection(path).resolveOne(), resolveTimeout.duration)

      if (isWatch) {
        context watch actorRef
      }

      actorRef
    } catch {
      case ex: Throwable => {
        log.error(ex, strLibType + "-ERR")
        null
      }
    }
  }

  def writeRedis(strOutKey: String, result: Any) = {
    redisCli.setKey(strOutKey, result)
  }

  def delRedisKey(strKey: String) = {
    redisCli.delKey(strKey)
  }
}

object LibraryActor {
  def props(strLibType: String, strLang: String, processId: Int, objRengine: REngine, objPythonEngine: Jep): Props = {
    Props(classOf[LibraryActor], strLibType, strLang, processId, objRengine, objPythonEngine)
  }
}