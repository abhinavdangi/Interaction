package com.mymall.analytics.constants

object KafkaZKConstants {
  val ZK_TEMP_FILE_PREFIX = "zookeeper"
  val KAFKA_TEMP_FILE_PREFIX: String = "kafka"
  val TEMP_FILE_SUFFIX: String = "test"
  val BROKER_HOST = "host.name"
  val BROKER_HOST_DEFAULT = "localhost"
  val BROKER_PORT = "port"
  val BROKER_PORT_DEFAULT = "9092"
  val BROKER_ID = "broker.id"
  val BROKER_ID_DEFAULT = "0"
  val ZK_CONNECT = "zookeeper.connect"
  val ZK_PORT_DEFAULT = "2181"
  val ZK_HOST_DEFAULT: String = "localhost"
  val AUTO_CREATE_TOPICS: String = "auto.create.topics.enable"
  val AUTO_CREATE_TOPICS_DEFAULT: String = "true"
  val LOG_DIR = "log.dir"

}
