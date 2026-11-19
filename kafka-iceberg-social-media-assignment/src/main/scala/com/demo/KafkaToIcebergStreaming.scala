package com.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object KafkaToIcebergStreaming {

  def main(args: Array[String]): Unit = {
    // Windows: folder containing bin\winutils.exe and bin\hadoop.dll
    System.setProperty("hadoop.home.dir", "C:/hadoop")

    println("========== HADOOP DIAGNOSTICS ==========")
    println("Hadoop version: " + org.apache.hadoop.util.VersionInfo.getVersion)
    println("Hadoop home: " + System.getProperty("hadoop.home.dir"))
    println("Hadoop DLL exists: " + new java.io.File("C:/hadoop/bin/hadoop.dll").exists())
    println("========================================")

    // STEP 1 - Spark session with Iceberg catalog
    val spark = SparkSession.builder()
      .appName("KafkaToIcebergStreaming")
      .master("local[*]")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "file:///C:/iceberg/warehouse")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // STEP 2 - Create the destination Iceberg table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.sales")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.sales.linkedin_events (
        event_id STRING,
        post_id STRING,
        user_id STRING,
        event_type STRING,
        event_time STRING,
        views BIGINT,
        likes BIGINT,
        comments BIGINT,
        event_timestamp TIMESTAMP,
        ingested_at TIMESTAMP
      )
      USING iceberg
    """)

    // STEP 3 - Explicit JSON schema
    val eventSchema = new StructType()
      .add("event_id", StringType)
      .add("post_id", StringType)
      .add("user_id", StringType)
      .add("event_type", StringType)
      .add("event_time", StringType)
      .add("views", LongType)
      .add("likes", LongType)
      .add("comments", LongType)

    // STEP 4 - Read the Kafka stream
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "social-media")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    // STEP 5 - Kafka value bytes to string
    val jsonMessages = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json_text")

    // STEP 6 - Parse JSON into columns
    val parsedEvents = jsonMessages
      .select(from_json(col("json_text"), eventSchema).as("event"))
      .select("event.*")

    // STEP 7 - Clean and enrich
    val cleanEvents = parsedEvents
      .withColumn("event_timestamp", to_timestamp(col("event_time")))
      .withColumn("ingested_at", current_timestamp())
      .filter(col("event_id").isNotNull)
      .filter(col("event_timestamp").isNotNull)
      .select(
        col("event_id"), col("post_id"), col("user_id"), col("event_type"),
        col("event_time"), col("views"), col("likes"), col("comments"),
        col("event_timestamp"), col("ingested_at")
      )

    // STEP 8 - Append micro-batches to Iceberg with a checkpoint
    val checkpointLocation =
      "file:///C:/iceberg/checkpoints/social-media-to-iceberg"

    val streamingQuery = cleanEvents.writeStream
      .format("iceberg")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .toTable("local.sales.linkedin_events")

    // STEP 9 - Keep running until stopped
    println("Streaming started: Kafka topic social-media -> Iceberg table local.sales.linkedin_events")
    streamingQuery.awaitTermination()
    spark.stop()
  }
}