package org.chronotics.actor.scala.akka.single

import java.io.File
import java.net.InetAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.chronotics.pandora.java.exception.ExceptionUtil
import org.chronotics.pandora.scala.file.FileIO
import org.chronotics.silverbullet.scala.akka.protocol.Message._
import org.chronotics.silverbullet.scala.akka.state.Started
import org.chronotics.silverbullet.scala.akka.util.EnvConfig
import org.chronotics.silverbullet.scala.model.{DBAlib, DBWorkFlow}
import org.chronotics.silverbullet.scala.util.{DatabaseConnection, XMLReaderWorkflowFromDB}

import scala.collection.immutable.HashMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.sys.process.Process
import scala.util.control.Breaks._

class InitProcessActor extends Actor with ActorLogging {
  import scala.concurrent.ExecutionContext.Implicits.global

  val strPrefix = EnvConfig.getPrefix()
  val config = ConfigFactory.parseFile(EnvConfig.getConfigFile("z2_conn")).resolve()
  var arrProcessCmnd: Seq[String] = Seq[String]()
  var arrAlib: Seq[DBAlib] = Seq.empty
  var arrWorkFlow: Seq[DBWorkFlow] = Seq.empty
  var mapAlib: HashMap[String, Int] = HashMap.empty
  var mapDebug: HashMap[String, Int] = HashMap.empty
  val isJavaCmd = config.getInt("java_exec.is_java")
  val strConfigDirPOM = config.getString("configdir.pom").replace("[PREFIX]", strPrefix)
  val strMavenDir = config.getString("maven.dir").replace("[PREFIX]", strPrefix)
  //val strJavaExecCp = config.getString("java_exec.cp").replace("[PREFIX]", strPrefix)
  val strJavaExecWorkflowCp = config.getString("java_exec.workflow_jar").replace("[PREFIX]", strPrefix)
  val strJavaMainWorkflowClass = config.getString("java_exec.workflow_main")
  val strJavaExecLibraryCp = config.getString("java_exec.library_jar").replace("[PREFIX]", strPrefix)
  val strJavaMainLibraryClass = config.getString("java_exec.library_main")
  val strJavaExecRouterCp = config.getString("java_exec.router_jar").replace("[PREFIX]", strPrefix)
  val strJavaMainRouterClass = config.getString("java_exec.router_main")
  val strJavaExecHttpCp = config.getString("java_exec.http_jar").replace("[PREFIX]", strPrefix)
  val strJavaMainHttpClass = config.getString("java_exec.http_main")
  val strJavaExecConsumerCp = config.getString("java_exec.consumer_jar").replace("[PREFIX]", strPrefix)
  val strJavaMainConsumerClass = config.getString("java_exec.consumer_main")
  val strSQLiteURL = config.getString("sqlitedb.url").replace("[PREFIX]", strPrefix)
  val strLogbackPath = config.getString("logback.path").replace("[PREFIX]", strPrefix)
  val intLimitNumAlib = config.getInt("alibrouterhandler.num-init")

  val int1stSeedPortWorkflow = config.getInt("akka_system.remote.workflow_cluster_seed_port_1st") //3551
  val int2ndSeedPortWorkflow = config.getInt("akka_system.remote.workflow_cluster_seed_port_2nd") //3552
  val int1stSeedPortAlib = config.getInt("akka_system.remote.alib_cluster_seed_port_1st") //6551
  val int2ndSeedPortAlib = config.getInt("akka_system.remote.alib_cluster_seed_port_2nd") //6552
  val int1stSeedPortDebug = config.getInt("akka_system.remote.debug_cluster_seed_port_1st") //7551
  val int2ndSeedPortDebug = config.getInt("akka_system.remote.debug_cluster_seed_port_2nd") //7552

  val managerActor: ActorRef = context.actorOf(Props(classOf[StatusManagerActor]))

  val strPingSeedWorkFlowActor: String = "akka.tcp://" + config.getString("processhandler.system") + "@" + config.getString("processhandler.host") + ":[PORT]/user/handler"
  val strPingSeedAlibActor: String = "akka.tcp://" + config.getString("alibworkerhandler.system") + "@" + config.getString("alibworkerhandler.host") + ":[PORT]/user/alib"
  val strPingSeedDebugActor: String = "akka.tcp://" + config.getString("debugworkerhandler.system") + "@" + config.getString("debugworkerhandler.host") + ":[PORT]/user/map"

  val seqHost = config.getStringList("z2_cluster.host")
  val dataConnect = DatabaseConnection.getInstance(config.getString("database.type"))

  var seedWaitingTime = config.getInt("java_exec.waiting_time_seed")
  var normalWaitingTime = config.getInt("java_exec.waiting_time_normal")
  var routerWaitingTime = config.getInt("java_exec.waiting_time_router")
  var clusterWaitingTime = config.getInt("java_exec.waiting_time_cluster")

  val shBashPath: String = config.getString("sh.bash_path")
  val shBashAnnotation: String = config.getString("sh.bash_annotation")
  val shShPath: String = config.getString("sh.sh_path")
  val shBashInitFile: String = config.getString("sh.bash_init_file")
  val shBashInitLogFile: String = config.getString("sh.bash_init_log")
  val fileConnection: FileIO = FileIO.getInstance()

  val gcOpts: String = config.getString("java_exec.gc_opts")

  override def receive: Receive = {
    case msg: UpdateLibraryMetaData => {
      try {
        val alibRouterActorPath = "akka.tcp://" + config.getString("alibrouterhandler.system") + "@" + config.getString("alibrouterhandler.host") + ":" + config.getString("alibrouterhandler.port") + "/user/alib"
        context.system.actorSelection(alibRouterActorPath) ! UpdateAlibMetaData(msg.alibId)
      } catch {
        case e: Throwable => log.error(e, "ERR");
      }
    }
    case init: InitAkkaSystem => {
      implicit val timeout = Timeout(5 seconds)

      arrProcessCmnd = Seq.empty
      arrAlib = Seq.empty
      arrWorkFlow = Seq.empty
      mapAlib = HashMap.empty
      mapDebug = HashMap.empty

      var bIsSuccees: Boolean = false

      var strJavaCmd = "java " + gcOpts + " -Dlogback.configurationFile=" + strLogbackPath + " -cp "

      try {
        execAlibSQLJDBC()
        execWorkFlowSQLJDBC()
        log.info("Number of alibs: " + arrAlib.length)
        log.info("Number of workflow: " + arrWorkFlow.length)

        if (init.cmd == "" || init.cmd == "consumer") {
          var curCmd = ""

          if (isJavaCmd == 0) {
            curCmd = strMavenDir + "mvn -f " + strConfigDirPOM + " exec:java -Dexec.mainClass=brique.z2.manage.KafkaConsumerApp"
          } else {
            curCmd = strJavaCmd + strJavaExecConsumerCp + " " + strJavaMainConsumerClass
          }

          arrProcessCmnd = arrProcessCmnd :+ curCmd
        }

        var intInitPort = 0

        if (init.cmd == "" || init.cmd == "router") {
          var curCmd = "";

          if (init.id == "0" || init.id == "1") {
            if (isJavaCmd == 0) {
              curCmd = strMavenDir + "mvn -f " + strConfigDirPOM + " exec:java -Dexec.mainClass=brique.z2.route.RouteActorApp -Dexec.args=alib"
            } else {
              curCmd = strJavaCmd + strJavaExecRouterCp + " " + strJavaMainRouterClass + " alib"
            }

            arrProcessCmnd = arrProcessCmnd :+ curCmd
          }

          if (init.id == "0" || init.id == "2") {
            if (isJavaCmd == 0) {
              curCmd = strMavenDir + "mvn -f " + strConfigDirPOM + " exec:java -Dexec.mainClass=brique.z2.route.RouteActorApp -Dexec.args=workflowinfo"
            } else {
              curCmd = strJavaCmd + strJavaExecRouterCp + " " + strJavaMainRouterClass + " workflowinfo"
            }

            arrProcessCmnd = arrProcessCmnd :+ curCmd
          }

          //Add 1 more process-actor will be used for simulate process
          var strPort = "0"

          if (isJavaCmd == 0) {
            curCmd = strMavenDir + "mvn -f " + strConfigDirPOM + " exec:java -Dexec.mainClass=brique.z2.proc.ProcessActorApp -Dexec.args=" + strPort + ",0"
          } else {
            curCmd = strJavaCmd + strJavaExecWorkflowCp + " " + strJavaMainWorkflowClass + " " + strPort + ",0"
          }

          arrProcessCmnd = arrProcessCmnd :+ curCmd

          //Add 1 more alib-actor will be used for dummy alib-actor
          strPort = "0"

          if (isJavaCmd == 0) {
            curCmd = strMavenDir + "mvn -f " + strConfigDirPOM + " exec:java -Dexec.mainClass=brique.z2.alib.AlibTaskActorApp -Dexec.args=" + strPort + ",0,R"
          } else {
            curCmd = strJavaCmd + strJavaExecLibraryCp + " " + strJavaMainLibraryClass + " " + strPort + ",0,R"
          }

          arrProcessCmnd = arrProcessCmnd :+ curCmd
        }

        for (curWorkflow <- arrWorkFlow) {
          val objWorkFlowData = new XMLReaderWorkflowFromDB(config).parseXMLData(curWorkflow.id, curWorkflow.workflow_xml);

          if (objWorkFlowData != null) {
            objWorkFlowData.tasks.arrTask.foreach { objTask =>
              var curNumAlib = 1

              if (mapAlib.contains(objTask.iAlibId.toString)) {
                curNumAlib = mapAlib(objTask.iAlibId.toString) + 1
              }

              mapAlib += (objTask.iAlibId.toString -> curNumAlib)
            }
          }
        }

        var mapNumAlib: HashMap[String, Int] = HashMap.empty
        var strProfileMode = EnvConfig.getRuntimeMode()

        for (curAlib <- mapAlib) {
          var intCurNum = curAlib._2
          if (intCurNum != intLimitNumAlib) {
            intCurNum = intLimitNumAlib
          }

          if (strProfileMode != null && !strProfileMode.isEmpty() && !strProfileMode.contains("docker")) {
            intCurNum = curAlib._2
          }

          mapNumAlib += (curAlib._1 -> intCurNum)
        }

        mapAlib = mapNumAlib

        var arrDebugType = Seq("R", "Python", "Java", "C++", "RMarkdown")

        for (curDebugType <- arrDebugType) {
          mapDebug += (curDebugType -> 1)
        }

        intInitPort = 0
        var curCmd = ""

        var arrReservedPort: Seq[Int] = Seq.empty

        if (init.cmd == "" || init.cmd == "cluster" || init.cmd == "alib") {
          breakable {
            for (item <- mapAlib) {
              var curLang = "";

              breakable {
                for (curDBLib <- arrAlib) {
                  if (curDBLib.id.toString().equals(item._1)) {
                    curLang = curDBLib.lang

                    break
                  }
                }
              }

              if (item._1 == init.id || init.id == "0") {
                for (intCount <- 1 to item._2) {
                  log.info("Alib: " + item._1 + " - " + intCount)

                  var strPort = "0"

                  if (isJavaCmd == 0) {
                    curCmd = strMavenDir + "mvn -f " + strConfigDirPOM + " exec:java -Dexec.mainClass=brique.z2.alib.AlibTaskActorApp -Dexec.args=" + strPort + "," + item._1 + "," + curLang
                  } else {
                    curCmd = strJavaCmd + strJavaExecLibraryCp + " " + strJavaMainLibraryClass + " " + strPort + "," + item._1 + "," + curLang
                  }

                  arrProcessCmnd = arrProcessCmnd :+ curCmd

                  if (init.id != "0" && int2ndSeedPortAlib.toString.equals(strPort)) {
                    if (isJavaCmd == 0) {
                      curCmd = strMavenDir + "mvn -f " + strConfigDirPOM + " exec:java -Dexec.mainClass=brique.z2.alib.AlibTaskActorApp -Dexec.args=0," + item._1 + "," + curLang
                    } else {
                      curCmd = strJavaCmd + strJavaExecLibraryCp + " " + strJavaMainLibraryClass + " 0," + item._1 + "," + curLang
                    }

                    arrProcessCmnd = arrProcessCmnd :+ curCmd
                  }
                }

                if (init.id != "0") {
                  break
                }
              }
            }
          }
        }

        intInitPort = 0

        if (init.cmd == "" || init.cmd == "cluster" || init.cmd == "proc") {
          breakable {
            for (item <- arrWorkFlow) {
              if (item.id.toString == init.id || init.id == "0") {

                var strPort = "0"

                if (isJavaCmd == 0) {
                  curCmd = strMavenDir + "mvn -f " + strConfigDirPOM + " exec:java -Dexec.mainClass=brique.z2.proc.ProcessActorApp -Dexec.args=" + strPort + "," + item.id
                } else {
                  curCmd = strJavaCmd + strJavaExecWorkflowCp + " " + strJavaMainWorkflowClass + " " + strPort + "," + item.id
                }

                arrProcessCmnd = arrProcessCmnd :+ curCmd

                if (item.id.toString == init.id) {
                  break
                }
              }
            }
          }
        }

        intInitPort = 0

        var isNohup = config.getInt("java_exec.is_nohup")

        if (isNohup == 1) {
          createBashProcessFromCmd(arrProcessCmnd, init.cmd)
        } else {
          createProcessFromCmd(arrProcessCmnd, init.cmd)
        }

        managerActor ! UpdateActorStatusToManager("Z2 Akka System was already started!", Started, "")

        bIsSuccees = true
      } catch {
        case ex: Throwable => {
          log.error(ex, "ERR")
        }
      }

      if (bIsSuccees) {
        var status: AkkaProcessInfo = AkkaProcessInfo("Z2 AKKA Sytem is initialized successfully!")
        sender() ! AkkaProcessResponse(status)
      } else {
        var status: AkkaProcessInfo = AkkaProcessInfo("Z2 AKKA Sytem is failed to start!")
        sender() ! AkkaProcessResponse(status)
      }
    }
  }

  def getActorRefOfSelection(path: String): Boolean = {
    implicit val resolveTimeout = Timeout(5 seconds)
    try {
      val actorRef = Await.result(context.actorSelection(path).resolveOne(), resolveTimeout.duration)
      true
    } catch {
      case ex: Throwable => {
        log.error(ex, "ERR")
        false
      }
    }
  }

  def execAlibSQLJDBC() = {
    arrAlib = dataConnect.getAlibList()
    arrAlib
  }

  def execWorkFlowSQLJDBC() = {
    arrWorkFlow = dataConnect.getWorkflowList()
    arrWorkFlow
  }

  def runProcessInThread(arrCmd: Array[String]) = {
    var objThread = new Thread(new Runnable() {
      def run() {
        try {
          var objRuntime = Runtime.getRuntime
          objRuntime.exec(arrCmd)
        } catch {
          case ex: Throwable => {
            log.error(ex, "ERR")
          }
        }
      }
    });

    objThread.start();
  }

  def createBashFile(cmd: String, strClass: String, strParam: String) = {
    try {
      var strFileName = shBashInitFile.replace("{0}", strClass).replace("{1}", strParam.replace(",", "-"))
      var strProcessCmd = cmd.replace("\r", "").replace("\n", "")
      var objStrBuilder = new StringBuilder();
      objStrBuilder.append(shBashAnnotation).append("\r\n")

      var strCmd = "setsid " + strProcessCmd + " >| \"" + shBashInitLogFile + "\" &"
      objStrBuilder.append(strCmd)

      //log.info("createBashFile-Cmd: " + objStrBuilder.toString)

      val objFile = new File(strFileName)
      fileConnection.writeFile(objStrBuilder.toString, strFileName)

      if (new File(strFileName).exists()) {
        //log.info("createBashFile-Exist: " + strFileName)

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

  def createBashProcessFromCmd(arrCmd: Seq[String], strCmd: String) = {
    try {
      var ipLocalHost = InetAddress.getLocalHost.getHostAddress
      var intOS = EnvConfig.getOS()

      var strJavaCmd = "java -Dlogback.configurationFile=" + strLogbackPath + " -cp "

      var terminalCmd = config.getString("configdir.terminal")
      var isNohup = config.getInt("java_exec.is_nohup")

      if (isNohup == 1) {
        terminalCmd = ""
      } else {
        if (intOS > 1) {
          terminalCmd = "cmd.exe"
        }
      }

      var arrKafkaCmd: Seq[String] = Seq.empty
      var arrRouterCmd: Seq[String] = Seq.empty
      var arrSeedNodeCmd: Seq[String] = Seq.empty
      var arrRestNodeCmd: Seq[String] = Seq.empty
      var arrSeedNode2ndCmd: Seq[String] = Seq.empty
      var boolHaveProcSeed = false
      var boolHaveAlibSeed = false
      var boolHaveDebugSeed = false
      var boolHaveProc2ndSeed = false
      var boolHaveAlib2ndSeed = false
      var boolHaveDebug2ndSeed = false

      log.info("Num of Cmds:" + arrCmd.length)

      var curClusterHostIdx = 0

      for (curCmd <- arrCmd) {
        log.info(curCmd)

        val strBaseCmd = curCmd //"setsid " + curCmd + " </dev/null &>/dev/null &"

        if (curCmd.contains("RouteActorApp")) {
          if (isNohup == 1) {
            arrRouterCmd = arrRouterCmd :+ strBaseCmd
          }
        } else if (curCmd.contains(int1stSeedPortAlib.toString) || curCmd.contains(int1stSeedPortWorkflow.toString) || curCmd.contains(int1stSeedPortDebug.toString)) {
          if (isNohup == 1) {
            arrSeedNodeCmd = arrSeedNodeCmd :+ strBaseCmd
          }

          if (curCmd.contains(int1stSeedPortWorkflow.toString)) {
            boolHaveProcSeed = true;
          } else if (curCmd.contains(int1stSeedPortAlib.toString)) {
            boolHaveAlibSeed = true;
          } else {
            boolHaveDebugSeed = true;
          }
        } else if (curCmd.contains(int2ndSeedPortAlib.toString) || curCmd.contains(int2ndSeedPortWorkflow.toString) || curCmd.contains(int2ndSeedPortDebug.toString)) {
          if (isNohup == 1) {
            arrSeedNode2ndCmd = arrSeedNode2ndCmd :+ strBaseCmd
          }

          if (curCmd.contains(int2ndSeedPortWorkflow.toString)) {
            boolHaveProc2ndSeed = true;
          } else if (curCmd.contains(int2ndSeedPortAlib.toString)) {
            boolHaveAlib2ndSeed = true;
          } else {
            boolHaveDebug2ndSeed = true;
          }
        } else if (curCmd.contains("Kafka")) {
          if (isNohup == 1) {
            arrKafkaCmd = arrKafkaCmd :+ strBaseCmd
          }
        } else {
          if (isNohup == 1) {
            if (!curCmd.contains("Cluster")) {
              arrRestNodeCmd = arrRestNodeCmd :+ strBaseCmd
            } else {
              if (curClusterHostIdx < seqHost.size) {
                if (curClusterHostIdx.equals(ipLocalHost)) {
                  arrRestNodeCmd = arrRestNodeCmd :+ strBaseCmd
                } else {
                  arrRestNodeCmd = arrRestNodeCmd :+ ("ssh " + seqHost.get(curClusterHostIdx) + " " + strBaseCmd)
                }

                curClusterHostIdx = curClusterHostIdx + 1

                if (curClusterHostIdx >= seqHost.size) {
                  curClusterHostIdx = 0
                }
              }
            }
          }
        }
      }

      var objRuntime: Runtime = Runtime.getRuntime()

      if (isNohup == 1) {
        if (strCmd == "http") {
          var curCmd = ""
          curCmd = "java -cp " + strJavaExecHttpCp + " " + strJavaMainHttpClass
          log.info("Begin Http Actor")

          createBashFile(curCmd, "HttpActorApp", "")

          Thread.sleep(normalWaitingTime)
        }

        if (strCmd == "" || strCmd == "consumer") {
          log.info("Begin Kafka Consumer")
          arrKafkaCmd.foreach { execCmd =>
            log.info(execCmd)

            createBashFile(execCmd, "Kafka", "")
          }

          Thread.sleep(normalWaitingTime)
        }

        if (strCmd == "" || strCmd == "router") {
          log.info("Begin Router")

          arrRouterCmd.foreach { execCmd =>
            log.info(execCmd)

            var arrSplit = execCmd.trim().split(" ")

            createBashFile(execCmd, "Router", arrSplit(arrSplit.length - 1))
          }

          Thread.sleep(routerWaitingTime)
        }

        if (strCmd == "" || strCmd == "cluster") {
          log.info("Begin Seed Nodes")

          arrSeedNodeCmd.foreach { execCmd =>
            log.info(execCmd)

            var arrSplit = execCmd.trim().split(" ")
            createBashFile(execCmd, "Cluster_Seed_1st", arrSplit(arrSplit.length - 1))
          }

          Thread.sleep(seedWaitingTime)

          log.info("Begin Seed 2nd Nodes")

          arrSeedNode2ndCmd.foreach { execCmd =>
            log.info(execCmd)

            var arrSplit = execCmd.trim().split(" ")
            createBashFile(execCmd, "Cluster_Seed_2nd", arrSplit(arrSplit.length - 1))
          }

          Thread.sleep(seedWaitingTime)

          log.info("Begin Rests")
          arrRestNodeCmd.foreach { execCmd =>
            log.info(execCmd)

            var arrSplit = execCmd.trim().split(" ")
            createBashFile(execCmd, "Cluster_Seed_Rest", arrSplit(arrSplit.length - 1))

            Thread.sleep(clusterWaitingTime)
          }
        }

        if (strCmd == "proc" || strCmd == "router") {
          if (boolHaveProcSeed) {
            log.info("Begin Seed Nodes")

            arrSeedNodeCmd.foreach { execCmd =>
              log.info(execCmd)

              var arrSplit = execCmd.trim().split(" ")
              createBashFile(execCmd, "Proc_Seed_1st", arrSplit(arrSplit.length - 1))
            }

            Thread.sleep(seedWaitingTime)
          }

          if (boolHaveProc2ndSeed) {
            log.info("Begin Seed 2nd Nodes")
            arrSeedNode2ndCmd.foreach { execCmd =>
              log.info(execCmd)

              var arrSplit = execCmd.trim().split(" ")
              createBashFile(execCmd, "Proc_Seed_2nd", arrSplit(arrSplit.length - 1))
            }

            Thread.sleep(seedWaitingTime)
          }

          if (arrRestNodeCmd.length > 0) {
            log.info("Begin Rests")
            //var objProcess = objRuntime.exec(arrRestNodeCmd.toArray)
            //Thread.sleep(3000)

            arrRestNodeCmd.foreach { execCmd =>
              log.info(execCmd)

              var arrSplit = execCmd.trim().split(" ")
              createBashFile(execCmd, "Proc_Seed_Rest", arrSplit(arrSplit.length - 1))

              Thread.sleep(clusterWaitingTime)
            }
          }
        }

        if (strCmd == "alib") {
          if (boolHaveAlibSeed) {
            log.info("Begin Seed Nodes")

            arrSeedNodeCmd.foreach { execCmd =>
              log.info(execCmd)

              var arrSplit = execCmd.trim().split(" ")
              createBashFile(execCmd, "Alib_Seed_1st", arrSplit(arrSplit.length - 1))
            }

            Thread.sleep(seedWaitingTime)
          }

          if (boolHaveAlib2ndSeed) {
            log.info("Begin Seed 2nd Nodes")

            arrSeedNode2ndCmd.foreach { execCmd =>
              log.info(execCmd)

              var arrSplit = execCmd.trim().split(" ")
              createBashFile(execCmd, "Alib_Seed_2nd", arrSplit(arrSplit.length - 1))
            }

            Thread.sleep(seedWaitingTime)
          }

          if (arrRestNodeCmd.length > 0) {
            log.info("Begin Rests")

            arrRestNodeCmd.foreach { execCmd =>
              log.info(execCmd)
              var arrSplit = execCmd.trim().split(" ")
              createBashFile(execCmd, "Alib_Seed_Rest", arrSplit(arrSplit.length - 1))

              Thread.sleep(clusterWaitingTime)
            }
          }
        }
      }
    } catch {
      case ex: Throwable => log.error(ex, "ERR")
    }
  }

  def createProcessFromCmd(arrCmd: Seq[String], strCmd: String) = {
    try {
      var ipLocalHost = InetAddress.getLocalHost.getHostAddress
      var intOS = EnvConfig.getOS()

      var strJavaCmd = "java -Dlogback.configurationFile=" + strLogbackPath + " -cp "

      var terminalCmd = config.getString("configdir.terminal")
      var isNohup = config.getInt("java_exec.is_nohup")

      if (isNohup == 1) {
        terminalCmd = ""
      } else {
        if (intOS > 1) {
          terminalCmd = "cmd.exe"
        }
      }

      var arrKafkaCmd: Seq[String] = Seq.empty
      var arrRouterCmd: Seq[String] = Seq.empty
      var arrSeedNodeCmd: Seq[String] = Seq.empty
      var arrRestNodeCmd: Seq[String] = Seq.empty
      var arrSeedNode2ndCmd: Seq[String] = Seq.empty
      var boolHaveProcSeed = false
      var boolHaveAlibSeed = false
      var boolHaveDebugSeed = false
      var boolHaveProc2ndSeed = false
      var boolHaveAlib2ndSeed = false
      var boolHaveDebug2ndSeed = false

      if (isNohup != 1) {
        arrKafkaCmd = arrKafkaCmd :+ terminalCmd
        arrRouterCmd = arrRouterCmd :+ terminalCmd
        arrSeedNodeCmd = arrSeedNodeCmd :+ terminalCmd //:+ "export" :+ ("R_HOME=" + config.getString("rserve.r_home")) :+ "export" :+ ("LD_LIBRARY_PATH=" + config.getString("rserve.r_jri"))
        arrRestNodeCmd = arrRestNodeCmd :+ terminalCmd //:+ "export" :+ ("R_HOME=" + config.getString("rserve.r_home")) :+ "export" :+ ("LD_LIBRARY_PATH=" + config.getString("rserve.r_jri"))
        arrSeedNode2ndCmd = arrSeedNode2ndCmd :+ terminalCmd //:+ "export" :+ ("R_HOME=" + config.getString("rserve.r_home")) :+ "export" :+ ("LD_LIBRARY_PATH=" + config.getString("rserve.r_jri"))
      }

      log.info("Num of Cmds:" + arrCmd.length)

      var curClusterHostIdx = 0

      for (curCmd <- arrCmd) {
        log.info(curCmd)

        val strBaseCmd = "setsid " + curCmd + " </dev/null &>/dev/null &"

        if (curCmd.contains("RouteActorApp")) {
          if (isNohup == 1) {
            arrRouterCmd = arrRouterCmd :+ strBaseCmd
          } else {
            if (intOS <= 1) {
              arrRouterCmd = arrRouterCmd :+ "--tab"
              arrRouterCmd = arrRouterCmd :+ "-e"
            } else {
              arrRouterCmd = arrRouterCmd :+ "/c"
              arrRouterCmd = arrRouterCmd :+ "start"
            }

            arrRouterCmd = arrRouterCmd :+ curCmd
          }
        } else if (curCmd.contains(int1stSeedPortAlib.toString)
          || curCmd.contains(int1stSeedPortWorkflow.toString) || curCmd.contains(int1stSeedPortDebug.toString)) {
          if (isNohup == 1) {
            arrSeedNodeCmd = arrSeedNodeCmd :+ strBaseCmd
          } else {
            if (intOS <= 1) {
              arrSeedNodeCmd = arrSeedNodeCmd :+ "--tab"
              arrSeedNodeCmd = arrSeedNodeCmd :+ "-e"
            } else {
              arrSeedNodeCmd = arrSeedNodeCmd :+ "/c"
              arrSeedNodeCmd = arrSeedNodeCmd :+ "start"
            }

            arrSeedNodeCmd = arrSeedNodeCmd :+ curCmd
          }

          if (curCmd.contains(int1stSeedPortWorkflow.toString)) {
            boolHaveProcSeed = true;
          } else if (curCmd.contains(int1stSeedPortAlib.toString)) {
            boolHaveAlibSeed = true;
          } else {
            boolHaveDebugSeed = true;
          }
        } else if (curCmd.contains(int2ndSeedPortAlib.toString) || curCmd.contains(int2ndSeedPortWorkflow.toString) || curCmd.contains(int2ndSeedPortDebug.toString)) {
          if (isNohup == 1) {
            arrSeedNode2ndCmd = arrSeedNode2ndCmd :+ strBaseCmd
          } else {
            if (intOS <= 1) {
              arrSeedNode2ndCmd = arrSeedNode2ndCmd :+ "--tab"
              arrSeedNode2ndCmd = arrSeedNode2ndCmd :+ "-e"
            } else {
              arrSeedNode2ndCmd = arrSeedNode2ndCmd :+ "/c"
              arrSeedNode2ndCmd = arrSeedNode2ndCmd :+ "start"
            }

            arrSeedNode2ndCmd = arrSeedNode2ndCmd :+ curCmd
          }

          if (curCmd.contains(int2ndSeedPortWorkflow.toString)) {
            boolHaveProc2ndSeed = true;
          } else if (curCmd.contains(int2ndSeedPortAlib.toString)) {
            boolHaveAlib2ndSeed = true;
          } else {
            boolHaveDebug2ndSeed = true;
          }
        } else if (curCmd.contains("Kafka")) {
          if (isNohup == 1) {
            arrKafkaCmd = arrKafkaCmd :+ strBaseCmd
          } else {
            if (intOS <= 1) {
              arrKafkaCmd = arrKafkaCmd :+ "--tab"
              arrKafkaCmd = arrKafkaCmd :+ "-e"
            } else {
              arrKafkaCmd = arrKafkaCmd :+ "/c"
              arrKafkaCmd = arrKafkaCmd :+ "start"
            }

            arrKafkaCmd = arrKafkaCmd :+ curCmd
          }
        } else {
          if (isNohup == 1) {
            if (!curCmd.contains("Cluster")) {
              arrRestNodeCmd = arrRestNodeCmd :+ strBaseCmd
            } else {
              if (curClusterHostIdx < seqHost.size) {
                if (curClusterHostIdx.equals(ipLocalHost)) {
                  arrRestNodeCmd = arrRestNodeCmd :+ strBaseCmd
                } else {
                  arrRestNodeCmd = arrRestNodeCmd :+ ("ssh " + seqHost.get(curClusterHostIdx) + " " + strBaseCmd)
                }

                curClusterHostIdx = curClusterHostIdx + 1

                if (curClusterHostIdx >= seqHost.size) {
                  curClusterHostIdx = 0
                }
              }
            }
          } else {
            if (intOS <= 1) {
              arrRestNodeCmd = arrRestNodeCmd :+ "--tab"
              arrRestNodeCmd = arrRestNodeCmd :+ "-e"
            } else {
              arrRestNodeCmd = arrRestNodeCmd :+ "/c"
              arrRestNodeCmd = arrRestNodeCmd :+ "start"
            }

            arrRestNodeCmd = arrRestNodeCmd :+ curCmd
          }
        }
      }

      if (strCmd == "http") {
        var curCmd = ""
        curCmd = "setsid java -cp " + strJavaExecHttpCp + " " + strJavaMainHttpClass + " </dev/null &>/dev/null &"
        log.info("Begin Http Actor")

        val objProcess = Process(curCmd.trim())
        objProcess.run()

        Thread.sleep(normalWaitingTime)
      }

      if (strCmd == "" || strCmd == "consumer") {
        log.info("Begin Kafka Consumer")
        arrKafkaCmd.foreach { execCmd =>
          log.info(execCmd)

          runProcessInThread(execCmd.split(" "))
        }

        Thread.sleep(normalWaitingTime)
      }

      if (strCmd == "" || strCmd == "router") {
        log.info("Begin Router")

        arrRouterCmd.foreach { execCmd =>
          log.info(execCmd)
          runProcessInThread(execCmd.split(" "))
        }

        Thread.sleep(routerWaitingTime)
      }

      if (strCmd == "" || strCmd == "cluster") {
        log.info("Begin Seed Nodes")

        arrSeedNodeCmd.foreach { execCmd =>
          log.info(execCmd)
          runProcessInThread(execCmd.split(" "))
        }

        Thread.sleep(seedWaitingTime)

        log.info("Begin Seed 2nd Nodes")

        arrSeedNode2ndCmd.foreach { execCmd =>
          log.info(execCmd)
          runProcessInThread(execCmd.split(" "))
        }

        Thread.sleep(seedWaitingTime)

        log.info("Begin Rests")
        arrRestNodeCmd.foreach { execCmd =>
          log.info(execCmd)
          runProcessInThread(execCmd.split(" "))

          Thread.sleep(clusterWaitingTime)
        }
      }

      if (strCmd == "proc" || strCmd == "router") {
        if (boolHaveProcSeed) {
          log.info("Begin Seed Nodes")

          arrSeedNodeCmd.foreach { execCmd =>
            log.info(execCmd)
            runProcessInThread(execCmd.split(" "))
          }

          Thread.sleep(seedWaitingTime)
        }

        if (boolHaveProc2ndSeed) {
          log.info("Begin Seed 2nd Nodes")
          arrSeedNode2ndCmd.foreach { execCmd =>
            log.info(execCmd)
            runProcessInThread(execCmd.split(" "))
          }

          Thread.sleep(seedWaitingTime)
        }

        if (arrRestNodeCmd.length > 0) {
          log.info("Begin Rests")

          arrRestNodeCmd.foreach { execCmd =>
            log.info(execCmd)
            runProcessInThread(execCmd.split(" "))

            Thread.sleep(clusterWaitingTime)
          }
        }
      }

      if (strCmd == "alib") {
        if (boolHaveAlibSeed) {
          log.info("Begin Seed Nodes")

          arrSeedNodeCmd.foreach { execCmd =>
            log.info(execCmd)
            runProcessInThread(execCmd.split(" "))
          }

          Thread.sleep(seedWaitingTime)
        }

        if (boolHaveAlib2ndSeed) {
          log.info("Begin Seed 2nd Nodes")

          arrSeedNode2ndCmd.foreach { execCmd =>
            log.info(execCmd)
            runProcessInThread(execCmd.split(" "))
          }

          Thread.sleep(seedWaitingTime)
        }

        if (arrRestNodeCmd.length > 0) {
          log.info("Begin Rests")

          arrRestNodeCmd.foreach { execCmd =>
            log.info(execCmd)
            runProcessInThread(execCmd.split(" "))

            Thread.sleep(clusterWaitingTime)
          }
        }
      }

      if (strCmd == "debug") {
        if (boolHaveDebugSeed) {
          log.info("Begin Seed Nodes")

          arrSeedNodeCmd.foreach { execCmd =>
            log.info(execCmd)
            runProcessInThread(execCmd.split(" "))
          }

          Thread.sleep(seedWaitingTime)
        }

        if (boolHaveDebug2ndSeed) {
          log.info("Begin Seed 2nd Nodes")

          arrSeedNode2ndCmd.foreach { execCmd =>
            log.info(execCmd)
            runProcessInThread(execCmd.split(" "))
          }

          Thread.sleep(seedWaitingTime)
        }

        if (arrRestNodeCmd.length > 0) {
          log.info("Begin Rests")

          arrRestNodeCmd.foreach { execCmd =>
            log.info(execCmd)
            runProcessInThread(execCmd.split(" "))

            Thread.sleep(clusterWaitingTime)
          }
        }
      }
    } catch {
      case ex: Throwable => log.error(ex, "ERR")
    }
  }
}
object InitProcessActor {
  def props: Props = Props(classOf[InitProcessActor])
}
