package com.mymall.analytics

import com.mymall.analytics.util.{KafkaUtil, ZookeeperUtil}

object StartKafka {
  def main(args: Array[String]): Unit = {
    ZookeeperUtil.startZookeeper()
    KafkaUtil.startKafka()
  }

}
