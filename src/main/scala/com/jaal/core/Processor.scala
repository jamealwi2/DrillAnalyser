package com.jaal.core

import com.jaal.utils.{DataQuality, LoggerUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
import com.jaal.utils.LoadData
import com.jaal.utils.Constants

class Processor {

  val log = new LoggerUtil
  val ld = new LoadData

  // Function to extract logs for the time period greater than the specified time - 'process_time'
  // The logs will be stored as a textfile in the 'output_location' directory under 'process_date' folder.
  def processExtract(path: String, output_location: String, process_time: String) = {
    val dq = new DataQuality
    log.loginfo("Creating spark object")
    val spark = SparkSession.builder().master(Constants.master.asInstanceOf[String]).appName(Constants.LogExtractorJobName).getOrCreate()
    log.loginfo("Reading the log files")
    val logDF = ld.loadDataFromS3(spark, path)

    log.loginfo("Extracting logs from time - " + process_time)
    val logwithInputFileRDD = logDF.select(input_file_name(), logDF.col("*")).rdd
    val filteredLogwithInputFileRDD = logwithInputFileRDD.filter(line => dq.isValid(line.getString(0), line.getString(1), process_time))
    log.loginfo("Writing to output file.")
    filteredLogwithInputFileRDD.map(line => dq.addFileName(line.getString(0).substring(path.length), line.getString(1))).saveAsTextFile(output_location + Constants.LogExtractorJobOutPutLocation)
    log.loginfo("Successfully wrote the logs to output location.")
  }

  // Function to extract logs for the time period between specified time - 'start_process_time' and 'end_process_time'
  // The logs will be stored as a textfile in the 'output_location' directory under 'logs_between' folder.
  def processExtractRange(path: String, output_location: String, start_process_time: String, end_process_time: String) = {
    val dq = new DataQuality
    log.loginfo("Creating spark object")
    val spark = SparkSession.builder().master(Constants.master.asInstanceOf[String]).appName(Constants.LogRangeExtractorJobName).getOrCreate()
    log.loginfo("Reading the log files")
    val logDF = ld.loadDataFromS3(spark, path)

    log.loginfo("Extracting logs between timestamp - " + start_process_time + " and " + end_process_time)
    log.loginfo("Convert into RDD.")
    val logwithInputFileRDD = logDF.select(input_file_name(), logDF.col("*")).rdd.coalesce((Constants.coalesce.asInstanceOf[String]).toInt)
    log.loginfo("Check for validity.")
    val filteredLogwithInputFileRDD = logwithInputFileRDD.filter(line => dq.isValidRange(line.getString(0), line.getString(1), start_process_time, end_process_time))
    log.loginfo("Writing to output file.")
    filteredLogwithInputFileRDD.map(line => dq.addFileName(line.getString(0).substring(path.length), line.getString(1))).saveAsTextFile(output_location + Constants.LogRangeExtractorJobOutPutLocation)
    log.loginfo("Successfully wrote the logs to output location.")
  }


  // Function to extract all log lines with the specified keyword.
  // The logs will be stored as a textfile in the 'output_location' directory under 'grepped' folder.
  def processGrep(path: String, output_location: String, grep_key: String) = {
    log.loginfo("Creating spark object")
    val spark = SparkSession.builder().master(Constants.master.asInstanceOf[String]).appName(Constants.LogGrepperJobName).getOrCreate()
    log.loginfo("Reading the log files")
    val logDF = ld.loadDataFromS3(spark, path)

    val logDFwithInputFile = logDF.withColumn("filename", input_file_name)
    logDFwithInputFile.createOrReplaceTempView("tmp_logdetails")
    log.loginfo("Grepping logs for key - " + grep_key)
    val data = spark.sql("select filename, value from tmp_logdetails where value like '%" + grep_key + "%' group by filename,value").rdd
    data.coalesce(1).saveAsTextFile(output_location + Constants.LogGrepperJobOutPutLocation)
    log.loginfo("Successfully wrote the logs to output location.")
  }

  // Function to aggregate all logs for a specific query id.
  // The logs will be stored as a textfile in the 'output_location' directory under 'grepped' folder.
  def processQueryLogExtraction(path: String, output_location: String, query_id: String) = {
    val dq = new DataQuality
    log.loginfo("Creating spark object")
    val spark = SparkSession.builder().master(Constants.master.asInstanceOf[String]).appName(Constants.QueryLogAggregatorJobName).getOrCreate()
    log.loginfo("Reading the log files")
    val logDF = ld.loadDataFromS3(spark, path)

    log.loginfo("Extracting logs for query id - " + query_id)
    val logwithInputFileRDD = logDF.select(input_file_name(), logDF.col("*")).rdd
    val filteredLogwithInputFileRDD = logwithInputFileRDD.filter(line => dq.isPartOfQueryLog(line.getString(1), query_id))
    log.loginfo("Writing to output file - " + query_id + ".log")
    filteredLogwithInputFileRDD.map(line => dq.addFileName(line.getString(0).substring(path.length), line.getString(1))).saveAsTextFile(output_location + Constants.QueryLogAggregatorOutPutLocation + query_id)
    log.loginfo("Successfully wrote the logs to output location.")
  }


}
