
package com.training.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaSparkStreaming {

  def main(args: Array[String]): Unit = {

    // 1. Create Spark Session
    val spark = SparkSession.builder()
      .appName("KafkaSparkStreaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 2. Define JSON schema
    val eventSchema = StructType(Seq(
      StructField("user", StringType, true),
      StructField("page", StringType, true),
      StructField("ts", StringType, true)
    ))

    // 3. Read streaming data from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "clickstream")
      .option("startingOffsets", "earliest")
      .load()

    // 4. Convert Kafka value into a string
    val jsonDF = kafkaDF.selectExpr(
      "CAST(value AS STRING) AS json"
    )

    // 5. Parse JSON and convert timestamp
    val parsedDF = jsonDF
      .select(
        from_json(col("json"), eventSchema).as("data")
      )
      .select("data.*")
      .withColumn("event_time", to_timestamp(col("ts")))
      .filter(
        col("user").isNotNull &&
          col("page").isNotNull &&
          col("event_time").isNotNull
      )

    // 6. Apply watermark and 5-minute window aggregation
    val aggregatedDF = parsedDF
      .withWatermark("event_time", "10 minutes")
      .groupBy(
        window(col("event_time"), "5 minutes"),
        col("page")
      )
      .agg(
        count("*").as("total_clicks"),
        approx_count_distinct("user").as("unique_users")
      )

    // 7. Display streaming results
    val query = aggregatedDF.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        "checkpoints/clickstream-windowed"
      )
      .start()

    // 8. Keep application running
    query.awaitTermination()
  }
}
