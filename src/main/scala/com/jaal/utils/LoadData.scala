package com.jaal.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

class LoadData {
  def loadDataFromS3(spark: SparkSession, path: String): DataFrame = {
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Constants.s3AccessKey.asInstanceOf[String])
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Constants.s3SecretKey.asInstanceOf[String])
    spark.read.text(path)
  }
}
