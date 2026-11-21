package com.demo

import org.apache.spark.sql.SparkSession

object VerifyProjectNexusIceberg {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/hadoop")
    System.load("C:/hadoop/bin/hadoop.dll")

    val warehousePath = "file:///C:/kafkastreamingicebergdemo/spark-warehouse"

    val spark = SparkSession.builder()
      .appName("VerifyProjectNexusIceberg")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", warehousePath)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=== 1. ROW COUNTS ACROSS ICEBERG TABLES ===")
    println(s"Bronze Events:     ${spark.table("local.retail_nexus.bronze_order_events").count()}")
    println(s"Silver Orders:     ${spark.table("local.retail_nexus.silver_order_events").count()}")
    println(s"Quarantine Events: ${spark.table("local.retail_nexus.quarantine_order_events").count()}")

    println("\n=== 2. EVOLVED SILVER SCHEMA ===")
    spark.table("local.retail_nexus.silver_order_events").printSchema()

    println("\n=== 3. SILVER ORDERS SAMPLE (INCLUDING UPDATES, CANCELLATIONS, LATE ARRIVALS & V2 SCHEMA) ===")
    spark.sql("""
      SELECT order_id, last_event_id, customer_id, product_id, quantity, unit_price,
             order_status, last_event_type, is_late_arrival, sales_channel, discount_amount
      FROM local.retail_nexus.silver_order_events
      ORDER BY order_id
    """).show(50, truncate = false)

    println("\n=== 4. QUARANTINED EVENTS & REJECTION REASONS ===")
    spark.sql("""
      SELECT event_id, order_id, customer_id, product_id, quantity, unit_price, event_type, rejection_reason
      FROM local.retail_nexus.quarantine_order_events
      ORDER BY event_id
    """).show(50, truncate = false)

    println("\n=== 5. GOLD 10-MINUTE WINDOWED CATEGORY REVENUE (EXCLUDING CANCELLED) ===")
    spark.sql("""
      SELECT window_start, window_end, category, valid_orders, total_units, net_revenue
      FROM local.retail_nexus.gold_streaming_sales_window
      ORDER BY window_start, net_revenue DESC
    """).show(50, truncate = false)

    println("\n=== 6. ICEBERG SNAPSHOTS & HISTORY ON silver_order_events ===")
    spark.sql("SELECT committed_at, snapshot_id, operation, summary['added-records'] AS added, summary['deleted-records'] AS deleted FROM local.retail_nexus.silver_order_events.snapshots").show(truncate = false)
  }
}
