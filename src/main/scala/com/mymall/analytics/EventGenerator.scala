package com.mymall.analytics

import java.util.Properties

import com.mymall.analytics.constants.EventGeneratorConstants
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.DateTime

import scala.util.Random

/**
  * Class to generate random events. The event structure is:
  * <personId>,<xCoordinate>,<yCoordinate>
 */
object EventGenerator {

  val SLEEP_TIME_MS = 50

  /**
    * Returns properties for creating a Kafka Producer
    * @return Properties object
    */
  private def getProperties = {
    val props = new Properties()
    props.put(EventGeneratorConstants.BOOTSTRAP_SERVERS, EventGeneratorConstants.BOOTSTRAP_SERVERS_DEFAULT)
    props.put(EventGeneratorConstants.KEY_SERIALIZER, EventGeneratorConstants.KEY_SERIALIZER_DEFAULT)
    props.put(EventGeneratorConstants.VALUE_SERIALIZER, EventGeneratorConstants.VALUE_SERIALIZER_DEFAULT)
    props.put(EventGeneratorConstants.ACKS, EventGeneratorConstants.ACKS_DEFAULT)
    props
  }

  /**
    * Creates a Kafka Producer and starts producing messages until the process is killed
    * The speed can be controlled using sleep time.
    * personId is generated between 0 and 500.
    * xCoordinate is number generated between 0 and 200
    * yCoordinate is number generated between 0 and 200
    * Assumed that Mall is at (100,100)
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val topic = EventGeneratorConstants.TOPIC_NAME
    val random = new Random()
    val producer = new KafkaProducer[String, String](getProperties)
    while (true) {
      // kafka record contains following values: personId, distance from the mall, mallId
      val x = random.nextInt(200)
      val y = random.nextInt(200)
      val value = random.nextInt(500) + "," + x + "," + y
      val key = DateTime.now().toString()
      producer.send(new ProducerRecord(
        topic, key, value))
      producer.flush()
      Thread.sleep(SLEEP_TIME_MS)
    }

  }
}
