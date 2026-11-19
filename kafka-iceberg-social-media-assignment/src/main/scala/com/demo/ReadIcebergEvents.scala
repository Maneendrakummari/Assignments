package com.demo

import org.apache.spark.sql.SparkSession

object ReadIcebergEvents {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ReadIcebergEvents")
      .master("local[*]")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "file:///C:/iceberg/warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val events = spark.table("local.sales.linkedin_events")

    println("ALL EVENTS STORED IN ICEBERG:")
    events.orderBy("event_timestamp").show(100, truncate = false)

    println("TOTAL EVENT COUNT: " + events.count())

    spark.stop()
  }
}