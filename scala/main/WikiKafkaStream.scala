package main

import config.SparkObject
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import schema.wikiSchema

object WikiKafkaStream extends App with Logging {

  val spark = SparkObject.spark
  val kafkaTopic = "wikipedia-topic-4"
  val kafkaBootstrapServers = "34.138.94.155:9092,34.58.41.125:9092"

  // Read streaming data from Kafka
  val kafkaStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
    .option("subscribe", kafkaTopic)
    .option("startingOffsets", "earliest") // Start consuming from the latest offset
    .load()

  val parsedStream = kafkaStream
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), getJsonSchema).as("data"))
    .select(col("data.*"))

  import spark.implicits._
  // Map each row to the schema
  val transformedStream = parsedStream.map(row => {
    val metaRow = row.getAs[Row]("meta")
    wikiSchema(
      schema = row.getAs[String]("$schema"),
      meta_uri = Option(metaRow).map(_.getAs[String]("uri")).orNull,
      meta_request_id = Option(metaRow).map(_.getAs[String]("request_id")).orNull,
      meta_id = Option(metaRow).map(_.getAs[String]("id")).orNull,
      meta_dt = Option(metaRow).map(_.getAs[String]("dt")).orNull,
      meta_domain = Option(metaRow).map(_.getAs[String]("domain")).orNull,
      meta_stream = Option(metaRow).map(_.getAs[String]("stream")).orNull,
      meta_topic = Option(metaRow).map(_.getAs[String]("topic")).orNull,
      meta_partition = Option(metaRow).map(_.getAs[String]("partition")).orNull,
      meta_offset = Option(metaRow).map(_.getAs[String]("offset")).orNull,
      id = row.getAs[String]("id"),
      `type` = row.getAs[String]("type"),
      namespace = row.getAs[String]("namespace"),
      title = row.getAs[String]("title"),
      title_url = row.getAs[String]("title_url"),
      comment = row.getAs[String]("comment"),
      timestamp = row.getAs[String]("timestamp"),
      user = row.getAs[String]("user"),
      bot = row.getAs[String]("bot"),
      notify_url = row.getAs[String]("notify_url"),
      server_url = row.getAs[String]("server_url"),
      server_name = row.getAs[String]("server_name"),
      server_script_path = row.getAs[String]("server_script_path"),
      wiki = row.getAs[String]("wiki"),
      parsedcomment = row.getAs[String]("parsedcomment")
    )
  }).withColumn("ts", date_format(current_timestamp(), "yyyyMMdd"))

  transformedStream.printSchema()

  val hiveTableLocation = s"gs://imposing-kite-438605-q1/shared_gcs/wikipedia_recent_changes/"

  //write to hive location
  val query = transformedStream.writeStream
    .format("parquet")
    .option("path", hiveTableLocation)
    .option("checkpointLocation", "gs://imposing-kite-438605-q1/wiki_recent_changes_checkpoint-3")
    .partitionBy("ts")
    .outputMode("append")
    .start()

  //val query = transformedStream.writeStream.format("console").start()

  query.awaitTermination()

  def getJsonSchema: StructType = {
    StructType(Seq(
      StructField("bot", StringType, nullable = true),
      StructField("comment", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("length", StructType(Seq(
        StructField("new", StringType, nullable = true),
        StructField("old", StringType, nullable = true)
      )), nullable = true),
      StructField("meta", StructType(Seq(
        StructField("domain", StringType, nullable = true),
        StructField("dt", StringType, nullable = true),
        StructField("id", StringType, nullable = true),
        StructField("offset", StringType, nullable = true),
        StructField("partition", StringType, nullable = true),
        StructField("request_id", StringType, nullable = true),
        StructField("stream", StringType, nullable = true),
        StructField("topic", StringType, nullable = true),
        StructField("uri", StringType, nullable = true)
      )), nullable = true),
      StructField("minor", StringType, nullable = true),
      StructField("namespace", StringType, nullable = true),
      StructField("parsedcomment", StringType, nullable = true),
      StructField("patrolled", StringType, nullable = true),
      StructField("revision", StructType(Seq(
        StructField("new", StringType, nullable = true),
        StructField("old", StringType, nullable = true)
      )), nullable = true),
      StructField("log_id", StringType, nullable = true),
      //StructField("log_type", StringType, nullable = true),
      StructField("log_action", StringType, nullable = true),
      StructField("log_params", StructType(Seq(
        StructField("action", StringType, nullable = true),
        StructField("filter", StringType, nullable = true),
        StructField("actions", StringType, nullable = true),
        StructField("log", StringType, nullable = true),
      )), nullable = true),
      StructField("log_action_comment", StringType, nullable = true),
      StructField("$schema", StringType, nullable = true),
      StructField("server_name", StringType, nullable = true),
      StructField("server_script_path", StringType, nullable = true),
      StructField("server_url", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("title_url", StringType, nullable = true),
      StructField("notify_url", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("type", StringType, nullable = true),
      StructField("user", StringType, nullable = true),
      StructField("wiki", StringType, nullable = true)
    ))
  }

}
