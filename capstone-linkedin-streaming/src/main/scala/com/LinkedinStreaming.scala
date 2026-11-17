package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LinkedinStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LinkedIn Kafka Spark Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val eventSchema = new StructType()
      .add("eventId", StringType)
      .add("memberId", StringType)
      .add("postId", StringType)
      .add("eventType", StringType)
      .add("eventTime", StringType)

    // Step A: Read the Kafka topic. Kafka's value arrives as binary.
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "linkedin-events")
      .option("startingOffsets", "earliest")
      .load()

    // Step B: Convert the message value to text and parse its JSON.
    val parsedDF = kafkaDF
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), eventSchema).as("data"))
      .select("data.*")
      .withColumn("event_ts", to_timestamp(col("eventTime")))
      .filter(col("eventId").isNotNull &&
        col("memberId").isNotNull &&
        col("postId").isNotNull &&
        col("event_ts").isNotNull)
      .withWatermark("event_ts", "2 minutes")

    // Step C: Total views, likes and comments per post.
    val postTotals = parsedDF
      .groupBy(col("postId"))
      .agg(
        sum(when(col("eventType") === "view", 1).otherwise(0)).as("views"),
        sum(when(col("eventType") === "like", 1).otherwise(0)).as("likes"),
        sum(when(col("eventType") === "comment", 1).otherwise(0)).as("comments"),
        sum(when(col("eventType") === "connection_request", 1).otherwise(0))
          .as("connection_requests")
      )

    val totalsQuery = postTotals.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoints/post-totals")
      .start()

    // Step D: Count unique members per post.
    // Deduplicate each member/post pair before counting. The watermark bounds
    // the amount of deduplication state retained by Spark.
    val uniqueMembers = parsedDF
      .dropDuplicates("postId", "memberId")
      .groupBy(col("postId"))
      .count()
      .withColumnRenamed("count", "unique_members")

    val uniqueQuery = uniqueMembers.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoints/unique-members")
      .start()

    // Step E: One-minute window metrics per post.
    val windowMetrics = parsedDF
      .groupBy(
        window(col("event_ts"), "1 minute"),
        col("postId")
      )
      .agg(
        sum(when(col("eventType") === "view", 1).otherwise(0)).as("views"),
        sum(when(col("eventType") === "like", 1).otherwise(0)).as("likes"),
        sum(when(col("eventType") === "comment", 1).otherwise(0)).as("comments"),
        sum(when(col("eventType") === "connection_request", 1).otherwise(0))
          .as("connection_requests")
      )

    val windowQuery = windowMetrics.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoints/one-minute-window")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
