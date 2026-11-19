package com.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class CustomerOrder(
                          order_id: Int,
                          customer_id: String,
                          customer_name: String,
                          product: String,
                          category: String,
                          quantity: Int,
                          order_amount: Long,
                          status: String
                        )

object CustomerOrdersShuffleAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CustomerOrders-RDD-vs-DataFrame-Shuffle")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val orders = Seq(
      CustomerOrder(1001, "C101", "Anita",  "Laptop",     "Electronics", 1, 55000, "Delivered"),
      CustomerOrder(1002, "C102", "Bharat", "Phone",      "Electronics", 2, 30000, "Delivered"),
      CustomerOrder(1003, "C101", "Anita",  "Mouse",      "Accessories", 2, 1000,  "Delivered"),
      CustomerOrder(1004, "C103", "Charan", "Chair",      "Furniture",   1, 7000,  "Cancelled"),
      CustomerOrder(1005, "C104", "Divya",  "Desk",       "Furniture",   1, 12000, "Delivered"),
      CustomerOrder(1006, "C102", "Bharat", "Headphones", "Accessories", 1, 2500,  "Returned"),
      CustomerOrder(1007, "C105", "Esha",   "Phone",      "Electronics", 1, 15000, "Delivered"),
      CustomerOrder(1008, "C103", "Charan", "Desk",       "Furniture",   2, 24000, "Delivered"),
      CustomerOrder(1009, "C101", "Anita",  "Keyboard",   "Accessories", 1, 2000,  "Returned"),
      CustomerOrder(1010, "C104", "Divya",  "Laptop",     "Electronics", 1, 55000, "Delivered"),
      CustomerOrder(1011, "C102", "Bharat", "Mouse",      "Accessories", 3, 1500,  "Delivered"),
      CustomerOrder(1012, "C105", "Esha",   "Chair",      "Furniture",   2, 14000, "Cancelled")
    )

    val ordersRdd = sc.parallelize(orders, 4)
    val ordersDf  = spark.createDataFrame(ordersRdd)

    val p1Rdd = ordersRdd
      .map(o => ((o.customer_id, o.customer_name), o.order_amount))
      .reduceByKey(_ + _)
      .map { case ((id, name), total) => (id, name, total) }
      .sortBy { case (id, _, total) => (-total, id) }
    val p1RddResult = p1Rdd.collect()

    val p1Df = ordersDf
      .groupBy("customer_id", "customer_name")
      .agg(sum("order_amount").as("total_order_value"))
      .orderBy(desc("total_order_value"), asc("customer_id"))
    val p1DfResult = p1Df.collect()
      .map(r => (r.getString(0), r.getString(1), r.getLong(2)))

    val p2Rdd = ordersRdd
      .map(o => (o.product, o.quantity.toLong))
      .reduceByKey(_ + _)
      .sortBy { case (product, units) => (-units, product) }
    val p2RddResult = p2Rdd.collect()

    val p2Df = ordersDf
      .groupBy("product")
      .agg(sum("quantity").as("units_sold"))
      .orderBy(desc("units_sold"), asc("product"))
    val p2DfResult = p2Df.collect()
      .map(r => (r.getString(0), r.getLong(1)))

    val p3Rdd = ordersRdd
      .map(o => (o.status, 1L))
      .reduceByKey(_ + _)
      .sortBy { case (status, cnt) => (-cnt, status) }
    val p3RddResult = p3Rdd.collect()

    val p3CountByValue = ordersRdd.map(_.status).countByValue()
    assert(p3CountByValue.toMap == p3RddResult.toMap, "countByValue disagrees with reduceByKey")

    val p3Df = ordersDf
      .groupBy("status")
      .agg(count(lit(1)).as("order_count"))
      .orderBy(desc("order_count"), asc("status"))
    val p3DfResult = p3Df.collect()
      .map(r => (r.getString(0), r.getLong(1)))

    def show[T](title: String, rows: Array[T]): Unit = {
      println(s"\n=== $title ===")
      rows.foreach(println)
    }
    show("P1 RDD (customer_id, customer_name, total_order_value)", p1RddResult)
    show("P1 DF",  p1DfResult)
    show("P2 RDD (product, units_sold)", p2RddResult)
    show("P2 DF",  p2DfResult)
    show("P3 RDD (status, order_count)", p3RddResult)
    show("P3 DF",  p3DfResult)

    assert(p1RddResult.toSeq == p1DfResult.toSeq, "Problem 1 mismatch")
    assert(p2RddResult.toSeq == p2DfResult.toSeq, "Problem 2 mismatch")
    assert(p3RddResult.toSeq == p3DfResult.toSeq, "Problem 3 mismatch")

    assert(p1RddResult.toSeq == Seq(
      ("C104", "Divya", 67000L), ("C101", "Anita", 58000L), ("C102", "Bharat", 34000L),
      ("C103", "Charan", 31000L), ("C105", "Esha", 29000L)))
    assert(p2RddResult.toSeq == Seq(
      ("Mouse", 5L), ("Chair", 3L), ("Desk", 3L), ("Phone", 3L),
      ("Laptop", 2L), ("Headphones", 1L), ("Keyboard", 1L)))
    assert(p3RddResult.toSeq == Seq(("Delivered", 8L), ("Cancelled", 2L), ("Returned", 2L)))
    println("\nAll assertions passed: RDD and DataFrame results are identical.")

    println("\n--- P1 RDD lineage ---\n" + p1Rdd.toDebugString)
    println("\n--- P2 RDD lineage ---\n" + p2Rdd.toDebugString)
    println("\n--- P3 RDD lineage ---\n" + p3Rdd.toDebugString)

    println("\n--- P1 DF physical plan ---"); p1Df.explain()
    println("\n--- P2 DF physical plan ---"); p2Df.explain()
    println("\n--- P3 DF physical plan ---"); p3Df.explain()

    spark.stop()
  }
}