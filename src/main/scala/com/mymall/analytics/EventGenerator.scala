package com.mymall.analytics

import java.util.Properties

import com.mymall.analytics.constants.EventGeneratorConstants
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.DateTime

import scala.util.Random

object EventGenerator {
  private def getProperties = {
    val props = new Properties()
    props.put(EventGeneratorConstants.BOOTSTRAP_SERVERS, EventGeneratorConstants.BOOTSTRAP_SERVERS_DEFAULT)
    props.put(EventGeneratorConstants.KEY_SERIALIZER, EventGeneratorConstants.KEY_SERIALIZER_DEFAULT)
    props.put(EventGeneratorConstants.VALUE_SERIALIZER, EventGeneratorConstants.VALUE_SERIALIZER_DEFAULT)
    props.put(EventGeneratorConstants.ACKS, EventGeneratorConstants.ACKS_DEFAULT)
    props
  }

  def main(args: Array[String]): Unit = {
    val topic = EventGeneratorConstants.TOPIC_NAME
    val random = new Random()
    val producer = new KafkaProducer[String, String](getProperties)
    while (true) {
      // kafka record contains two values: personId and distance from the mall
      val value = random.nextInt(500) + "," + random.nextInt(500) + "," + random.nextInt(10)
      val key = DateTime.now().toString()
      println(key + ":::" + value)
      producer.send(new ProducerRecord(
        topic, key, value))
      producer.flush()
      Thread.sleep(50)
    }

  }
}
