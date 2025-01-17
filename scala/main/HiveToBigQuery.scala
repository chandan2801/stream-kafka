package main

import config.SparkObject.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object HiveToBigQuery extends App {
  val df = spark.read.table("shared_gcs.wikipedia_recent_changes").filter(col("ts") === lit("20250109"))

  val newDf = df.withColumn("formatted_timestamp", to_timestamp(from_unixtime(col("timestamp"))))
    .withColumn("is_bot", when(col("bot") === lit("false"), lit("0")).otherwise(lit("1")))
    .withColumn("ts", to_timestamp(col("ts"),"yyyyMMdd"))
    .groupBy("title"
    ,"title_url"
    , "formatted_timestamp"
    , "is_bot"
    , "ts")
    .agg(count_distinct(col("comment")).cast("int").as("count_comments"))
    .select("title"
    ,"title_url"
    , "formatted_timestamp"
    , "is_bot"
    , "count_comments"
    , "ts")

  newDf.show(false)

  overwriteBQTable(newDf,"employee_data.wikipedia_formatted_changes","DAY", "ts", "20250109")

  def overwriteBQTable(sourceDf: DataFrame, bqTbl:String, bqPtnType:String = "", bqPtnField:String = "",ptnVal:String = "",clsFields:String = ""): Unit ={

    val gcsBucket = "project-employee-data/bq_temp_location"
    val gcpProject = "imposing-kite-438605-q1"

    if(bqPtnField != "") {
      sourceDf.write
        .format("bigquery")
        .option("parentProject", gcpProject)
        .option("temporaryGcsBucket", gcsBucket)
        .option("OUTPUT_TABLE_WRITE_DISPOSITION_KEY", "WRITE_TRUNCATE")
        .option("table", bqTbl)
        .option("partitionType", bqPtnType)
        .option("partitionField", bqPtnField)
        .option("datePartition", ptnVal)
        //.option("clusteredFields", clsFields)
        .mode("overwrite")
        .save()
    }
    else {
      sourceDf.write
        .format("bigquery")
        .option("parentProject", gcpProject)
        .option("temporaryGcsBucket", gcsBucket)
        .option("OUTPUT_TABLE_WRITE_DISPOSITION_KEY", "WRITE_TRUNCATE")
        .option("table", bqTbl)
        .mode("overwrite")
        .save()
    }

  }
}
