package config

import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.ImplicitUtils.Ternary

import scala.collection.JavaConverters.mapAsScalaMapConverter

object SparkObject {

  lazy val spark: SparkSession = getSparkSession
  //lazy val ssc: StreamingContext = getStreamingContext //entry point of spark streaming program

  def getSparkSession: SparkSession = {
    var builder = SparkSession.builder()
    val master = AppConfig.getProperty("spark.master")
    val sparkAppName = s"${AppConfig.getProperty("spark.appName")}-$getMainClass"
    if(master.equalsIgnoreCase("local")) {
      builder = builder.master("local[*]")
        .appName(sparkAppName)
        //.config("spark.driver.host", "127.0.0.1")
        //.config("spark.testing.memory", "2147480000")
        //.config("spark.sql.shuffle.partitions", 2)
        //.config("spark.broadcast.compress", "false")
        .config("spark.shuffle.compress", "false")
        .config("spark.shuffle.spill.compress", "false")
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.hive.metastore.client.socket.timeout","3000")
        .enableHiveSupport()
    } else {
      builder = builder
        .appName(sparkAppName)
        //.config("spark.sql.warehouse.dir", warehouseLocation)
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.hadoop.hive.metastore.client.socket.timeout","3000")
        .enableHiveSupport()
    }
    builder.getOrCreate()
  }

  private def getMainClass: String = {
    val allTraceMap = Thread.getAllStackTraces.asScala
    val trace = allTraceMap.values.flatMap(_.toSeq)
    val mainClass = trace.find(t => t.getClassName.toLowerCase().contains("main"))
    mainClass.isEmpty ?? ("Unknown", mainClass.get.getClassName.split("\\$")(0).split("\\.").reverse.head)
  }

}
