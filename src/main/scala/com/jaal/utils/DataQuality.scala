package com.jaal.utils

class DataQuality extends Serializable {

  var flag = false;
  var validRangeflag = false;
  var fn = ""
  var afn = ""
  val du = new DateUtils

  // Function to check if the log line is written on or after the specified process_time
  def isValid(filename: String, line: String, process_time: String): Boolean = {
    if (!flag && line.contains(process_time)) {
      flag = true;
    } else if (!flag) {
      try {
        // Checking for the timestamp
        val dtime = line.substring(0, 19)
        if (checkWhetherTimestamp(dtime)) {
          // Set flag as true if the timestamp is greater or equal to the process date
          if (dtime.compareTo(process_time) >= 0) {
            flag = true
          }
        }
      } catch {
        case e: Exception => {}
      }
    }
    else if (flag) {
      // Check if the task is processing a different file
      if (!fn.equalsIgnoreCase(filename)) {
        flag = false;
        if (line.contains(process_time)) {
          flag = true;
        } else {
          try {
            // Checking for the timestamp
            val dtime = line.substring(0, 19)
            if (checkWhetherTimestamp(dtime)) {
              // Set flag as true if the timestamp is greater or equal to the process date
              if (dtime.compareTo(process_time) >= 0) {
                flag = true
              }
            }
          } catch {
            case e: Exception => {}
          }
        }
        fn = filename
      }
    }

    flag;
  }

  // Function to check if the log line is written between the specified time range
  def isValidRange(filename: String, line: String, start_process_time: String, end_process_time: String): Boolean = {
    if (!validRangeflag && line.contains(start_process_time)) {
      validRangeflag = true;
    } else if (!validRangeflag) {
      try {
        // Checking for the timestamp
        val dtime = line.substring(0, 19)
        if (checkWhetherTimestamp(dtime)) {
          // Set flag as true if the timestamp is greater or equal to the process date
          if ((dtime.compareTo(start_process_time) >= 0) && (dtime.compareTo(end_process_time) <= 0)) {
            validRangeflag = true
          }
        }
      } catch {
        case e: Exception => {}
      }
    }
    else if (validRangeflag) {
      // Check if the task is processing a different file
      if (!fn.equalsIgnoreCase(filename)) {
        validRangeflag = false;
        if (line.contains(start_process_time)) {
          validRangeflag = true;
        } else {
          try {
            // Checking for the timestamp
            val dtime = line.substring(0, 19)
            if (checkWhetherTimestamp(dtime)) {
              // Set flag as true if the timestamp is greater or equal to the process date
              if ((dtime.compareTo(start_process_time) >= 0) && (dtime.compareTo(end_process_time) <= 0)) {
                validRangeflag = true
              }
            }
          } catch {
            case e: Exception => {}
          }
        }
        fn = filename
      }else{
        try {
          // Checking for the timestamp
          val dtime = line.substring(0, 19)
          if (checkWhetherTimestamp(dtime)) {
            // Set flag as true if the timestamp is greater or equal to the process date
            if ((dtime.compareTo(start_process_time) < 0) || (dtime.compareTo(end_process_time) > 0)) {
              validRangeflag = false
            }
          }
        } catch {
          case e: Exception => {}
        }
      }
    }

    validRangeflag;
  }

  // Function to check if the log line is part of current query_id
  def isPartOfQueryLog(line: String, query_id: String): Boolean = {
    if (line.contains(query_id))
      flag = true
    else {
      try {
        if (line.split(" ")(2).split(":")(0).length == 37) {
          flag = false
        }
      } catch {
        case e: Exception => {}
      }

      try {
        if (line.split(" ")(2).split("]")(0).equalsIgnoreCase("[main")) {
          flag = false
        }
      } catch {
        case e: Exception => {}
      }
      if (line.contains("Starting drillbit")) {
        flag = false
      }
    }
    flag
  }

  def addFileName(filename: String, line: String) = {
    var rline = ""
    if (!afn.equalsIgnoreCase(filename)) {
      rline = "\n\n" + filename + "\n=========================================================================================== \n" + line
      afn = filename
    } else {
      rline = line;
    }
    rline
  }

  def checkWhetherTimestamp(line: String) = {
    du.checkIfValidDate(line)
  }
}
