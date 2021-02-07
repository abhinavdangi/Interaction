package com.mymall.analytics.util

import java.io.File
import java.util.Properties

import com.mymall.analytics.constants.KafkaZKConstants
import kafka.server.{KafkaConfig, KafkaServerStartable}

object KafkaUtil {

  /**
    * Sets up kafka properties and starts the broker.
    * It has a single broker.
    */
  def startKafka(): Unit = {

    val kafkaDir = File.createTempFile(KafkaZKConstants.KAFKA_TEMP_FILE_PREFIX, KafkaZKConstants.TEMP_FILE_SUFFIX)
    kafkaDir.delete()
    kafkaDir.mkdir()

    val props = new Properties()
    props.put(KafkaZKConstants.BROKER_ID, KafkaZKConstants.BROKER_ID_DEFAULT)
    props.put(KafkaZKConstants.BROKER_PORT, KafkaZKConstants.BROKER_PORT_DEFAULT)
    props.put(KafkaZKConstants.ZK_CONNECT, KafkaZKConstants.ZK_HOST_DEFAULT + ":" + KafkaZKConstants.ZK_PORT_DEFAULT)
    props.put(KafkaZKConstants.BROKER_HOST, KafkaZKConstants.BROKER_HOST_DEFAULT)
    props.put(KafkaZKConstants.AUTO_CREATE_TOPICS, KafkaZKConstants.AUTO_CREATE_TOPICS_DEFAULT)
    props.put(KafkaZKConstants.LOG_DIR, kafkaDir.getAbsolutePath)

    val conf = new KafkaConfig(props)
    val server = new KafkaServerStartable(conf)
    server.startup()
    try {
      Thread.sleep(1000)
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }
  }
}
