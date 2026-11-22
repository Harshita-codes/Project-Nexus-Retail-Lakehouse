package com.demo

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class ProjectNexusStreamingAppSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("ProjectNexusStreamingAppSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      super.afterAll()
    }
  }

  test("quarantines duplicate IDs, invalid quantities/prices, bad timestamps, and future events") {
    val events = Seq(
      event("EVT000001", "ORD000001", 2, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00")),
      event("EVT000002", "ORD000002", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00")),
      event("EVT000002", "ORD000003", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:02:00")),
      event("EVT000003", "ORD000004", 0, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00")),
      event("EVT000004", "ORD000005", 1, 0.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00")),
      event("EVT000005", "ORD000006", 1, 10.0, null, timestamp("2026-11-20 10:01:00")),
      event("EVT000006", "ORD000007", 1, 10.0, timestamp("2026-11-25 10:00:00"), timestamp("2026-11-20 10:01:00")),
      event("EVT000009", "ORD000008", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00")),
      event("BAD_ID", "ORD000009", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00")),
      event("EVT000010", "BAD_ORDER", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00")),
      event("EVT000011", "ORD000011", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00"), eventType = "ORDER_ERR"),
      event("EVT000012", "ORD000012", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00"), customerId = "CUST999"),
      event("EVT000013", "ORD000013", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00"), productId = "PRD999"),
      event("EVT000014", "ORD000014", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00"), salesChannel = "PHONE"),
      event("EVT000015", "ORD000015", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00"), discountAmount = -1.0)
    )
    val parsed = createParsedEvents(events)
    val existingIds = createDataFrame(
      Seq(Row("EVT000009")),
      StructType(Seq(StructField("event_id", StringType, nullable = true)))
    )
    val customers = createDataFrame(
      Seq(Row("CUST001")),
      StructType(Seq(StructField("customer_id", StringType, nullable = false)))
    )
    val products = createDataFrame(
      Seq(Row("PRD001")),
      StructType(Seq(StructField("product_id", StringType, nullable = false)))
    )

    val reasons = ProjectNexusStreamingApp
      .evaluateEvents(parsed, existingIds, customers, products)
      .select("order_id", "rejection_reason")
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toMap

    assert(reasons("ORD000001").isEmpty)
    assert(reasons("ORD000003").contains("Duplicate event_id"))
    assert(reasons("ORD000004").contains("Invalid quantity"))
    assert(reasons("ORD000005").contains("Invalid unit_price"))
    assert(reasons("ORD000006").contains("Invalid event_time/ingest_time"))
    assert(reasons("ORD000007").contains("Future-skewed event_time"))
    assert(reasons("ORD000008").contains("Duplicate event_id"))
    assert(reasons("ORD000009").contains("Invalid/missing event_id"))
    assert(reasons("BAD_ORDER").contains("Invalid/missing order_id"))
    assert(reasons("ORD000011").contains("Invalid event_type"))
    assert(reasons("ORD000012").contains("Invalid/orphan customer_id"))
    assert(reasons("ORD000013").contains("Invalid/orphan product_id"))
    assert(reasons("ORD000014").contains("Invalid sales_channel"))
    assert(reasons("ORD000015").contains("Negative discount_amount"))
  }

  test("calculates discounted revenue in ten-minute windows and excludes cancelled orders") {
    val silverSchema = StructType(Seq(
      StructField("order_id", StringType, nullable = false),
      StructField("product_id", StringType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
      StructField("unit_price", DoubleType, nullable = false),
      StructField("discount_amount", DoubleType, nullable = true),
      StructField("order_status", StringType, nullable = false),
      StructField("event_time", TimestampType, nullable = false)
    ))
    val silver = createDataFrame(Seq(
      Row("ORD000001", "PRD001", 2, 10.0, 1.0, "PLACED", timestamp("2026-11-20 10:01:00")),
      Row("ORD000002", "PRD001", 1, 5.0, 0.0, "UPDATED", timestamp("2026-11-20 10:08:00")),
      Row("ORD000003", "PRD001", 1, 100.0, 0.0, "CANCELLED", timestamp("2026-11-20 10:05:00"))
    ), silverSchema)
    val products = createDataFrame(
      Seq(Row("PRD001", "Electronics")),
      StructType(Seq(
        StructField("product_id", StringType, nullable = false),
        StructField("category", StringType, nullable = false)
      ))
    )

    val result = ProjectNexusStreamingApp.aggregateGoldWindow(silver, products).first()

    assert(result.getAs[Long]("valid_orders") == 2L)
    assert(result.getAs[Long]("total_units") == 3L)
    assert(result.getAs[java.math.BigDecimal]("net_revenue").compareTo(new java.math.BigDecimal("24.00")) == 0)
    assert(result.getAs[String]("category") == "Electronics")
  }

  private val parsedEventSchema = StructType(Seq(
    StructField("raw_payload", StringType, nullable = true),
    StructField("batch_id", LongType, nullable = false),
    StructField("event_id", StringType, nullable = true),
    StructField("order_id", StringType, nullable = true),
    StructField("customer_id", StringType, nullable = true),
    StructField("product_id", StringType, nullable = true),
    StructField("quantity", IntegerType, nullable = true),
    StructField("unit_price", DoubleType, nullable = true),
    StructField("event_type", StringType, nullable = true),
    StructField("event_time_str", StringType, nullable = true),
    StructField("ingest_time_str", StringType, nullable = true),
    StructField("sales_channel", StringType, nullable = true),
    StructField("discount_amount", DoubleType, nullable = true),
    StructField("event_ts", TimestampType, nullable = true),
    StructField("ingest_ts", TimestampType, nullable = true)
  ))

  private def event(
      eventId: String,
      orderId: String,
      quantity: Int,
      unitPrice: Double,
      eventTime: Timestamp,
      ingestTime: Timestamp,
      eventType: String = "ORDER_CREATED",
      customerId: String = "CUST001",
      productId: String = "PRD001",
      salesChannel: String = "WEB",
      discountAmount: Double = 0.0
  ): Row = Row(
    "{}",
    1L,
    eventId,
    orderId,
    customerId,
    productId,
    quantity,
    unitPrice,
    eventType,
    Option(eventTime).map(_.toString).orNull,
    Option(ingestTime).map(_.toString).orNull,
    salesChannel,
    discountAmount,
    eventTime,
    ingestTime
  )

  private def timestamp(value: String): Timestamp = Timestamp.valueOf(value)

  private def createParsedEvents(events: Seq[Row]): DataFrame =
    createDataFrame(events, parsedEventSchema)

  private def createDataFrame(rows: Seq[Row], schema: StructType): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
}
