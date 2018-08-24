package com.jaal.drillanalyser

import com.jaal.drilllogaggregate.DrillLogAggregateProcessor

object DrillLogsAggregator {

  def usage()={
    println("")
    println("Please find the usage : ")
    println("[1] <Path_to_Input_Directory> <Type_of_Operation (extract/grep/query_log)> <Path_to_Output_Directory> <Query_Profile_ID/Timestamp/Grep_Key>")
    println("[2] <Path_to_Input_Directory> <Type_of_Operation (extract/grep/query_log)> <Path_to_Output_Directory> <Start_Timestamp> <End_Timestamp>")
    println("")
    println("Examples :-  ")
    println("--------------------------------------------------------------------------------------------------------------------------------------")
    println("1. Aggregate Logs for a query ID          : /tmp/DrillLog query_log /tmp/DrillLogOut/output 254572d8-713f-7909-0c34-8a8950721b68")
    println("2. Aggregate Logs on or after given time  : /tmp/DrillLog extract /tmp/DrillLogOut/output '2018-03-21 18:04:16'")
    println("3. Aggregate Logs between given period    : /tmp/DrillLog extract /tmp/DrillLogOut/output '2018-03-21 18:04:16' '2018-04-18 13:24:34'")
    println("4. Extract Logs for a given key           : /tmp/DrillLog grep /tmp/DrillLogOut/output OutOfMemory")
    println("--------------------------------------------------------------------------------------------------------------------------------------")
  }

  // Main function
  def main(args: Array[String]): Unit = {

    // Check if the input arguments are present
    if(!(args.length==4 || args.length==5)) {
      usage()
    }else {
      val obj = new DrillLogAggregateProcessor
      obj.processor(args)
    }
  }

}
