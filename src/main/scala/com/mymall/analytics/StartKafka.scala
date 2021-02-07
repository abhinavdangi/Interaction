package com.mymall.analytics

import com.mymall.analytics.util.{KafkaUtil, ZookeeperUtil}

/**
  * Starts kafka in local.
  */
object StartKafka {

  def main(args: Array[String]): Unit = {
    ZookeeperUtil.startZookeeper()
    KafkaUtil.startKafka()
  }

}
