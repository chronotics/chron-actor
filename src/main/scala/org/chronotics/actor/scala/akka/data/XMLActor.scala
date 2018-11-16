package org.chronotics.actor.scala.akka.data

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import org.chronotics.silverbullet.scala.akka.protocol.Message._
import org.chronotics.silverbullet.scala.akka.util.EnvConfig
import org.chronotics.silverbullet.scala.model.{LibMeta, WorkFlowTaskInfo}
import org.chronotics.silverbullet.scala.util.{XMLReaderAlibFromDB, XMLReaderWorkflow, XMLReaderWorkflowTaskDB}

class XMLActor extends Actor {
  val strPrefix = EnvConfig.getPrefix()
  val config: Config = ConfigFactory.parseFile(EnvConfig.getConfigFile("z2_conn")).resolve()
  val xmlDir: String = config.getString("configdir.dir").replace("[PREFIX]", strPrefix)

  override def receive: Receive = {
    case read: ReadXMLWorkFlowFile => {
      val strWorkFlowFile = xmlDir + "/" + config.getString("configdir.workflow") + read.intWorkFlowId + ".xml"
      val objWorkFlowTaskInfo: WorkFlowTaskInfo = new XMLReaderWorkflow(strWorkFlowFile, false).readXMLInfo(xmlDir)

      sender() ! ReceiveXMLWorkFlowData(objWorkFlowTaskInfo)
    }

    case readDB: ReadXMLWorkFlowDB => {
      val objWorkFlowTaskInfo: WorkFlowTaskInfo = new XMLReaderWorkflowTaskDB(readDB.intWorkFlowId).readXMLInfo()
      sender() ! ReceiveXMLWorkFlowData(objWorkFlowTaskInfo)
    }

    case readAlibDB: ReadXMLAlibDB => {
      var objAlibXMLInfo: LibMeta = new XMLReaderAlibFromDB(config).parseXMLData(readAlibDB.intAlibId, "")
      sender() ! ReceiveXMLAlibData(objAlibXMLInfo)
    }
  }

}
