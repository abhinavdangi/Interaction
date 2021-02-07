package com.mymall.analytics.constants

/**
  * Contains constants required for EventGenerator
  */
object EventGeneratorConstants {

  val BOOTSTRAP_SERVERS = "bootstrap.servers"
  val BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092"
  val KEY_SERIALIZER = "key.serializer"
  val KEY_SERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringSerializer"
  val VALUE_SERIALIZER = "value.serializer"
  val VALUE_SERIALIZER_DEFAULT = "org.apache.kafka.common.serialization.StringSerializer"
  val ACKS = "acks"
  val ACKS_DEFAULT = "1"
  val TOPIC_NAME = "person_info"

}
