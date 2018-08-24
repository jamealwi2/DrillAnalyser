package com.jaal.utils

import java.util.Properties
import java.io.FileInputStream
import com.jaal.utils.LoggerUtil

object Constants {

  val log = new LoggerUtil

  val Grep = "grep"
  val Extract = "extract"
  val QueryLog = "query_log"
  val LogExtractorJobName = "LogExtractor"
  val LogExtractorJobOutPutLocation = "/process_date"
  val LogRangeExtractorJobName = "LogRangeExtractor"
  val LogRangeExtractorJobOutPutLocation = "/logs_between"
  val LogGrepperJobName = "LogGrepper"
  val LogGrepperJobOutPutLocation = "/grepped"
  val QueryLogAggregatorJobName = "QueryLogAggregator"
  val QueryLogAggregatorOutPutLocation = "/query_logs/"

  val (s3AccessKey, s3SecretKey, master, coalesce) =
    try {
      val prop = new Properties()
      prop.load(new FileInputStream("conf/config.properties"))

      (
        prop.getProperty("s3.access.key"),
        prop.getProperty("s3.secret.key"),
        prop.getProperty("spark.master"),
	prop.getProperty("spark.coalesce.number")
      )
    } catch {
      case e: Exception =>
        e.printStackTrace()
        log.logerror(e.getMessage)
    }


}
