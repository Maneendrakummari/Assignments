package com.demo


import org.apache.spark.sql.SparkSession

object IcebergDemo {

  def main(args: Array[String]): Unit = {

    println("Starting Spark Session...")

    val spark = SparkSession.builder()
      .appName("Scala-Spark-Iceberg-Demo")
      .master("local[*]")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config("spark.sql.catalog.local.type", "hadoop")
      .config(
        "spark.sql.catalog.local.warehouse",
        "file:///C:/iceberg/warehouse"
      )
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      import spark.implicits._

      spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS local.sales
      """)

      spark.sql("""
        CREATE TABLE IF NOT EXISTS local.sales.orders (
          order_id BIGINT,
          customer STRING,
          amount DOUBLE,
          city STRING
        )
        USING iceberg
      """)

      // Clear previous demo rows, then insert fresh sample data
      spark.sql("DELETE FROM local.sales.orders")

      val ordersDF = Seq(
        (101L, "Raj", 2500.0, "Hyderabad"),
        (102L, "Kiran", 3500.0, "Bangalore"),
        (103L, "Arun", 4500.0, "Chennai"),
        (104L, "John", 5500.0, "Mumbai"),
        (105L, "David", 6500.0, "Pune")
      ).toDF("order_id", "customer", "amount", "city")

      println("Source DataFrame:")
      ordersDF.show(false)

      ordersDF.writeTo("local.sales.orders").append()

      println("Data after INSERT:")
      spark.table("local.sales.orders").show(false)

      println("Orders greater than 4000:")
      spark.sql("""
        SELECT *
        FROM local.sales.orders
        WHERE amount > 4000
      """).show(false)

      spark.sql("""
        UPDATE local.sales.orders
        SET amount = 8000
        WHERE order_id = 101
      """)

      println("Data after UPDATE:")
      spark.table("local.sales.orders").show(false)

      spark.sql("""
        DELETE FROM local.sales.orders
        WHERE order_id = 105
      """)

      println("Data after DELETE:")
      spark.table("local.sales.orders").show(false)

      println("Iceberg snapshot history:")
      spark.sql("""
        SELECT committed_at, snapshot_id, operation
        FROM local.sales.orders.snapshots
        ORDER BY committed_at
      """).show(false)

      println("Iceberg table history:")
      spark.sql("""
        SELECT made_current_at, snapshot_id, parent_id,
               is_current_ancestor
        FROM local.sales.orders.history
        ORDER BY made_current_at
      """).show(false)

      val finalCount = spark.table("local.sales.orders").count()
      println(s"Final record count: $finalCount")
      println("ICEBERG INTEGRATION SUCCESSFUL")

    } finally {
      spark.stop()
      println("Stopping Spark Session...")
    }
  }
}
