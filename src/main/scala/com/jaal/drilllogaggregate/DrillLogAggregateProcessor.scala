package com.jaal.drilllogaggregate
import com.jaal.utils.{DataQuality, DirUtils, LoggerUtil}
import com.jaal.core.Processor
import com.jaal.utils.Constants

class DrillLogAggregateProcessor {

  // The function with core logic.
  def processor(args: Array[String]) = {

    // Path to parent directory containing the input files
    val input_log_location = args(0)

    // Path to the parent output directory
    val output_location = args(2)

    // Variable to store the grep key/ query id/ process date
    val grep_key = args(3)
    val query_id = args(3)
    val extract_key = args(3)

    val log = new LoggerUtil
    val dirutils = new DirUtils
    log.loginfo("Input path - "+input_log_location)
    log.loginfo("Files processed "+dirutils.getListOfFiles(input_log_location))

    val processorOb = new Processor

    val option = args(1)
    option match {
      case Constants.Grep => {
        log.loginfo("Option set to - grep.")
        processorOb.processGrep(input_log_location, output_location, grep_key)
      }
      case Constants.Extract => {
        if(args.length==5){
          val extract_key_end = args(4)
          log.loginfo("Option set to - extraction. Range :- "+extract_key+" - "+extract_key_end)
          processorOb.processExtractRange(input_log_location, output_location, extract_key, extract_key_end)
        }else {
          log.loginfo("Option set to - extraction. Range :- "+extract_key+" - END")
          processorOb.processExtract(input_log_location, output_location, extract_key)
        }
      }
      case Constants.QueryLog => {
        log.loginfo("Option set to - query_log.")
        processorOb.processQueryLogExtraction(input_log_location, output_location, query_id)
      }

    }
}
}
