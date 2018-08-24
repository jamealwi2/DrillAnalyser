package com.jaal.utils

import java.time.format.DateTimeFormatter

class DateUtils extends Serializable{

  def convertStringToDate(dateString: String) = {
    val datetime_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    datetime_format.format(datetime_format.parse(dateString))
  }

  def checkIfValidDate(dateString: String) = {
    val datetime_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    try {
      datetime_format.parse(dateString)
      true
    } catch {
      case e: Exception => false
    }
  }
}
