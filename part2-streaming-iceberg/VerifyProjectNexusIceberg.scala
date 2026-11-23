package com.demo

import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}

object VerifyProjectNexusIceberg {
  def main(args: Array[String]): Unit = {
    val isWindows = System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("win")
    val windowsHadoopDll = Paths.get(
      sys.env.getOrElse("HADOOP_HOME", "C:/hadoop"),
      "bin",
      "hadoop.dll"
    )
    if (isWindows && Files.exists(windowsHadoopDll)) {
      System.setProperty("hadoop.home.dir", windowsHadoopDll.getParent.getParent.toString)
      System.load(windowsHadoopDll.toAbsolutePath.toString)
    }

    val defaultWarehouse = if (isWindows) {
      "file:///C:/kafkastreamingicebergdemo/spark-warehouse"
    } else {
      "file:///lakehouse/warehouse"
    }
    val warehousePath = sys.env.getOrElse("ICEBERG_WAREHOUSE", defaultWarehouse)
    val deltaWarehousePath = sys.env.getOrElse(
      "DELTA_WAREHOUSE",
      if (isWindows) "file:///C:/kafkastreamingicebergdemo/delta-warehouse/retail_nexus"
      else "file:///lakehouse/delta/retail_nexus"
    )
    val sparkMaster = sys.env.getOrElse("SPARK_MASTER", "local[*]")

    val spark = SparkSession.builder()
      .appName("VerifyProjectNexusIceberg")
      .master(sparkMaster)
      .config("spark.sql.catalogImplementation", "in-memory")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
    val snapshotHistory = spark.sql("""
      SELECT committed_at, snapshot_id, operation,
             summary['added-records'] AS added,
             summary['deleted-records'] AS deleted
      FROM local.retail_nexus.silver_order_events.snapshots
      ORDER BY committed_at
    """)
    snapshotHistory.show(truncate = false)

    val earliestSnapshotId = snapshotHistory
      .select("snapshot_id")
      .limit(1)
      .collect()
      .headOption
      .map(_.getLong(0))
      .getOrElse(throw new IllegalStateException("No Iceberg snapshots found for silver_order_events"))
    val earliestSnapshotRows = spark.sql(
      s"SELECT count(*) FROM local.retail_nexus.silver_order_events VERSION AS OF $earliestSnapshotId"
    ).first().getLong(0)
    println(s"Iceberg time travel -> earliest snapshot $earliestSnapshotId has $earliestSnapshotRows silver rows")

    println("\n=== 7. GOLD AGGREGATE SNAPSHOTS & HISTORY ===")
    val goldSnapshotHistory = spark.sql("""
      SELECT committed_at, snapshot_id, operation,
             summary['added-records'] AS added,
             summary['deleted-records'] AS deleted
      FROM local.retail_nexus.gold_streaming_sales_window.snapshots
      ORDER BY committed_at
    """)
    goldSnapshotHistory.show(truncate = false)

    val earliestGoldSnapshotId = goldSnapshotHistory
      .select("snapshot_id")
      .limit(1)
      .collect()
      .headOption
      .map(_.getLong(0))
      .getOrElse(throw new IllegalStateException("No Iceberg snapshots found for gold_streaming_sales_window"))
    val earliestGoldSnapshotRows = spark.sql(
      s"SELECT count(*) FROM local.retail_nexus.gold_streaming_sales_window VERSION AS OF $earliestGoldSnapshotId"
    ).first().getLong(0)
    println(s"Iceberg time travel -> earliest Gold snapshot $earliestGoldSnapshotId has $earliestGoldSnapshotRows rows")

    println("\n=== 8. DELTA COPIES OF STREAMING TABLES ===")
    Seq("bronze_order_events", "silver_order_events", "quarantine_order_events", "gold_streaming_sales_window")
      .foreach { tableName =>
        val deltaRows = spark.read.format("delta").load(s"$deltaWarehousePath/$tableName").count()
        println(s"$tableName Delta rows: $deltaRows")
      }
  }
}
