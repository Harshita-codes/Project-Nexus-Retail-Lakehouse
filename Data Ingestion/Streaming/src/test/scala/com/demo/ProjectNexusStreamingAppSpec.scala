package com.demo

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

class ProjectNexusStreamingAppSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var icebergWarehouse: Path = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
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

    icebergWarehouse = Files.createTempDirectory("nexus-iceberg-test")
    spark = SparkSession.builder()
      .appName("ProjectNexusStreamingAppSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory", "2147480000")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", icebergWarehouse.toUri.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      if (icebergWarehouse != null) deleteRecursively(icebergWarehouse)
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

  test("quarantines events older than the persisted event-time watermark") {
    val parsed = createParsedEvents(Seq(
      event("EVT000020", "ORD000020", 1, 10.0, timestamp("2026-11-20 09:59:59"), timestamp("2026-11-20 10:01:00")),
      event("EVT000021", "ORD000021", 1, 10.0, timestamp("2026-11-20 10:00:00"), timestamp("2026-11-20 10:01:00"))
    ))
    val existingIds = createDataFrame(
      Seq.empty[Row],
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
      .evaluateEvents(parsed, existingIds, customers, products, Some(timestamp("2026-11-20 10:00:00")))
      .select("order_id", "rejection_reason")
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toMap

    assert(reasons("ORD000020").contains("older than event-time watermark"))
    assert(reasons("ORD000021").isEmpty)
  }

  test("persists a monotonic event-time watermark across app restarts") {
    val root = Files.createTempDirectory("event-time-watermark")
    val path = root.resolve("state.txt")

    try {
      val store = new ProjectNexusStreamingApp.EventTimeWatermarkStore(path, 600L)
      assert(store.currentWatermark.isEmpty)

      store.observe(Some(timestamp("2026-11-20 10:20:00")))
      assert(store.currentWatermark.contains(timestamp("2026-11-20 10:10:00")))

      store.observe(Some(timestamp("2026-11-20 10:15:00")))
      val restored = new ProjectNexusStreamingApp.EventTimeWatermarkStore(path, 600L)
      assert(restored.maxObservedEventTime.contains(timestamp("2026-11-20 10:20:00")))
      assert(restored.currentWatermark.contains(timestamp("2026-11-20 10:10:00")))
    } finally {
      deleteRecursively(root)
    }
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
    assert(ProjectNexusStreamingApp.aggregateGoldWindow(silver, products, 300L).count() == 2L)
  }

  test("Delta append writes are idempotent for a retried micro-batch") {
    val root = Files.createTempDirectory("delta-idempotent-append")
    val path = root.resolve("bronze").toUri.toString
    val schema = StructType(Seq(
      StructField("event_id", StringType, nullable = false),
      StructField("batch_id", LongType, nullable = false)
    ))
    val firstBatch = createDataFrame(Seq(Row("EVT000001", 0L)), schema)
    val secondBatch = createDataFrame(Seq(Row("EVT000002", 1L)), schema)

    try {
      ProjectNexusStreamingApp.appendDeltaBatch(firstBatch, path, "test-bronze", 0L)
      ProjectNexusStreamingApp.appendDeltaBatch(firstBatch, path, "test-bronze", 0L)
      assert(spark.read.format("delta").load(path).count() == 1L)

      ProjectNexusStreamingApp.appendDeltaBatch(secondBatch, path, "test-bronze", 1L)
      assert(spark.read.format("delta").load(path).count() == 2L)
    } finally {
      deleteRecursively(root)
    }
  }

  test("Delta snapshots replace current state and support schema evolution and time travel") {
    val root = Files.createTempDirectory("delta-current-snapshot")
    val path = root.resolve("silver").toUri.toString
    val initial = createDataFrame(
      Seq(Row("ORD000001", 2)),
      StructType(Seq(
        StructField("order_id", StringType, nullable = false),
        StructField("quantity", IntegerType, nullable = false)
      ))
    )
    val evolved = createDataFrame(
      Seq(
        Row("ORD000001", 3, "WEB"),
        Row("ORD000002", 1, "STORE")
      ),
      StructType(Seq(
        StructField("order_id", StringType, nullable = false),
        StructField("quantity", IntegerType, nullable = false),
        StructField("sales_channel", StringType, nullable = false)
      ))
    )

    try {
      ProjectNexusStreamingApp.overwriteDeltaSnapshot(initial, path)
      ProjectNexusStreamingApp.overwriteDeltaSnapshot(evolved, path)

      val current = spark.read.format("delta").load(path)
      assert(current.count() == 2L)
      assert(current.columns.contains("sales_channel"))
      assert(spark.read.format("delta").option("versionAsOf", 0).load(path).count() == 1L)
    } finally {
      deleteRecursively(root)
    }
  }

  test("initializes missing Delta copies from existing Iceberg tables") {
    val root = Files.createTempDirectory("delta-iceberg-bootstrap")
    val tableNames = Seq(
      "bronze_order_events",
      "silver_order_events",
      "quarantine_order_events",
      "gold_streaming_sales_window"
    )
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.retail_nexus")
    tableNames.foreach { tableName =>
      spark.sql(s"CREATE TABLE local.retail_nexus.$tableName (id INT) USING iceberg")
      spark.sql(s"INSERT INTO local.retail_nexus.$tableName VALUES (1)")
    }

    try {
      ProjectNexusStreamingApp.initializeDeltaCopies(spark, root.toUri.toString)

      tableNames.foreach { tableName =>
        val path = s"${root.toUri}$tableName"
        assert(spark.read.format("delta").load(path).count() == 1L)
      }
    } finally {
      deleteRecursively(root)
    }
  }

  test("Iceberg bronze merge ignores a replayed Kafka partition and offset") {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.test")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.test.bronze_events (
        kafka_key STRING,
        raw_payload STRING,
        kafka_partition INT,
        kafka_offset BIGINT,
        kafka_timestamp TIMESTAMP,
        batch_id BIGINT,
        _ingested_at TIMESTAMP
      ) USING iceberg
    """)
    val schema = StructType(Seq(
      StructField("kafka_key", StringType, nullable = true),
      StructField("raw_payload", StringType, nullable = true),
      StructField("kafka_partition", IntegerType, nullable = false),
      StructField("kafka_offset", LongType, nullable = false),
      StructField("kafka_timestamp", TimestampType, nullable = true),
      StructField("batch_id", LongType, nullable = false),
      StructField("_ingested_at", TimestampType, nullable = true)
    ))
    val first = createDataFrame(
      Seq(Row("ORD000001", "payload-1", 0, 100L, null, 0L, null)),
      schema
    )
    val next = createDataFrame(
      Seq(Row("ORD000002", "payload-2", 0, 101L, null, 1L, null)),
      schema
    )

    ProjectNexusStreamingApp.mergeBronzeEvents(spark, first, "local.test.bronze_events")
    ProjectNexusStreamingApp.mergeBronzeEvents(spark, first, "local.test.bronze_events")
    assert(spark.table("local.test.bronze_events").count() == 1L)
    val firstSnapshotId = spark.sql("""
      SELECT snapshot_id
      FROM local.test.bronze_events.snapshots
      ORDER BY committed_at
      LIMIT 1
    """).first().getLong(0)
    assert(
      spark.sql(s"SELECT count(*) FROM local.test.bronze_events VERSION AS OF $firstSnapshotId")
        .first().getLong(0) == 1L
    )

    ProjectNexusStreamingApp.mergeBronzeEvents(spark, next, "local.test.bronze_events")
    assert(spark.table("local.test.bronze_events").count() == 2L)
  }

  test("Iceberg quarantine merge maps and deduplicates Kafka offsets") {
    spark.sql("""
      CREATE TABLE IF NOT EXISTS local.test.quarantine_events (
        event_id STRING,
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        unit_price DOUBLE,
        event_type STRING,
        event_time STRING,
        ingest_time STRING,
        sales_channel STRING,
        discount_amount DOUBLE,
        rejection_reason STRING,
        raw_payload STRING,
        batch_id BIGINT,
        _quarantined_at TIMESTAMP,
        kafka_partition INT,
        kafka_offset BIGINT
      ) USING iceberg
    """)
    val schema = StructType(Seq(
      StructField("event_id", StringType, nullable = true),
      StructField("order_id", StringType, nullable = true),
      StructField("customer_id", StringType, nullable = true),
      StructField("product_id", StringType, nullable = true),
      StructField("quantity", IntegerType, nullable = true),
      StructField("unit_price", DoubleType, nullable = true),
      StructField("event_type", StringType, nullable = true),
      StructField("event_time", StringType, nullable = true),
      StructField("ingest_time", StringType, nullable = true),
      StructField("sales_channel", StringType, nullable = true),
      StructField("discount_amount", DoubleType, nullable = true),
      StructField("rejection_reason", StringType, nullable = false),
      StructField("raw_payload", StringType, nullable = false),
      StructField("batch_id", LongType, nullable = false),
      StructField("_quarantined_at", TimestampType, nullable = true),
      StructField("kafka_partition", IntegerType, nullable = false),
      StructField("kafka_offset", LongType, nullable = false)
    ))
    val invalidEvent = createDataFrame(
      Seq(Row(
        "EVT000099", "ORD000099", "CUST999", "PRD001", 1, 10.0,
        "ORDER_CREATED", "2026-11-20T10:00:00", "2026-11-20T10:01:00",
        "WEB", 0.0, "Invalid/orphan customer_id", "{}", 2L,
        timestamp("2026-11-20 10:01:00"), 1, 200L
      )),
      schema
    )

    ProjectNexusStreamingApp.mergeQuarantineEvents(
      spark,
      invalidEvent,
      "local.test.quarantine_events"
    )
    ProjectNexusStreamingApp.mergeQuarantineEvents(
      spark,
      invalidEvent,
      "local.test.quarantine_events"
    )

    assert(spark.table("local.test.quarantine_events").count() == 1L)
    assert(spark.table("local.test.quarantine_events").first().getLong(16) == 200L)
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

  private def deleteRecursively(root: Path): Unit = {
    if (Files.exists(root)) {
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists(_))
      finally paths.close()
    }
  }
}
