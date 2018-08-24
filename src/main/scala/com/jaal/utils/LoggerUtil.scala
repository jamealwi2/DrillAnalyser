package com.jaal.utils

import java.util.Calendar

import org.apache.log4j.Level

class LoggerUtil {
  ClassLoader.getSystemResource("log4j.properties")

  val log =  org.apache.log4j.LogManager.getLogger("drillbitaggregatelogger")
  log.setLevel(Level.INFO)

 def loginfo(message: String)= {
    log.info(message)
  }

  def logwarn(message: String)= {
    log.warn(message)
  }

  def logerror(message: String)= {
    log.error(message)
  }
}
