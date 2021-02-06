package com.mymall.analytics.util

import java.io.File

import com.mymall.analytics.constants.KafkaZKConstants
import org.apache.zookeeper.server.ZooKeeperServerMain

object ZookeeperUtil {

  def startZookeeper(): Unit = {

    val zkTmpDir = File.createTempFile(KafkaZKConstants.ZK_TEMP_FILE_PREFIX, KafkaZKConstants.TEMP_FILE_SUFFIX)
    zkTmpDir.delete()
    zkTmpDir.mkdir()

    new Thread(new Runnable {
      override def run(): Unit = {
        ZooKeeperServerMain.main(Array[String](
          KafkaZKConstants.ZK_PORT_DEFAULT, zkTmpDir.getAbsolutePath))
      }
    }).start()

    try {
      Thread.sleep(1000)
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }

  }
}
