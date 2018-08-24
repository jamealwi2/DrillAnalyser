name := "DrillAnalyser"

version := "0.1"

scalaVersion := "2.11.0"

// Adding MapR repository
resolvers += "MapR Repository" at "http://repository.mapr.com/maven/"

// https://mvnrepository.com/artifact/xerces/xercesImpl
libraryDependencies += "xerces" % "xercesImpl" % "2.11.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
// libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

// For MapR Spark 2.2.1
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1-mapr-1803"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
// libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"

// For MapR Spark 2.2.1
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1-mapr-1803"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"

