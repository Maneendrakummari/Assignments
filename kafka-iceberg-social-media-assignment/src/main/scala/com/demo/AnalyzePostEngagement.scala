package com.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AnalyzePostEngagement {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AnalyzePostEngagement")
      .master("local[*]")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "file:///C:/iceberg/warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val events = spark.table("local.sales.linkedin_events")

    // Data-quality check: if these differ, an event was ingested more than once
    println("TOTAL ROWS: " + events.count())
    println("DISTINCT EVENT IDS: " + events.select("event_id").distinct().count())

    // Post-wise engagement
    println("POST-WISE ENGAGEMENT:")
    val engagement = events
      .groupBy("post_id")
      .agg(
        sum("views").as("total_views"),
        sum("likes").as("total_likes"),
        sum("comments").as("total_comments")
      )
      .orderBy("post_id")

    engagement.show(100, truncate = false)

    // Posts ranked by views + likes + comments
    println("POSTS RANKED BY TOTAL ENGAGEMENT:")
    engagement
      .withColumn("total_engagement",
        col("total_views") + col("total_likes") + col("total_comments"))
      .orderBy(desc("total_engagement"), asc("post_id"))
      .show(100, truncate = false)

    spark.stop()
  }
}