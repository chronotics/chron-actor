package org.chronotics.actor.scala.akka.single

import akka.actor.{Actor, ActorLogging}
import akka.kafka.ProducerSettings
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.chronotics.silverbullet.scala.akka.protocol.Message._
import org.chronotics.silverbullet.scala.akka.util.EnvConfig

class StatusManagerActor extends Actor with ActorLogging {
  val config = ConfigFactory.parseFile(EnvConfig.getConfigFile("z2_conn")).resolve()
  val topic = config.getString("kafka.topic")
  val partition = 0
  val key = null

  val osProcActorPath = "akka.tcp://" + config.getString("osprocrouterhandler.system") + "@" + config.getString("osprocrouterhandler.host") + ":" + config.getString("osprocrouterhandler.port") + "/user/osproc"

  val kafkaProducer = ProducerSettings(context.system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(config.getString("kafka.broker"))
    .createKafkaProducer();

  override def receive: Receive = {
    case update: UpdateActorStatusToManager => {
      val strMessage = update.actorPath + "|" + update.status.toString + "|" + update.msg
      sendToKafkaProducer(update.timestamp, strMessage)
    }

    case simpleUpdate: UpdateSimpleStatusToManager => {
      val strMessage = simpleUpdate.msg
      sendToKafkaProducer(simpleUpdate.timestamp, strMessage)
    }

    case simpleArrayUpdate: UpdateSimpleArrayStatusToManager => {
      val arrMessage = simpleArrayUpdate.msg

      arrMessage.foreach { objMessage =>
        sendToKafkaProducer(objMessage._2, objMessage._1, false)
      }
    }
  }

  def sendToKafkaProducer(timestamp: Long, message: String, isFlush: Boolean = true) {
    var record = new ProducerRecord[Array[Byte], String](topic, partition, timestamp, key, message)

    kafkaProducer.send(record, new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, ex: Exception): Unit = {
      }
    })

    if (isFlush) {
      kafkaProducer.flush
    }
  }
}
