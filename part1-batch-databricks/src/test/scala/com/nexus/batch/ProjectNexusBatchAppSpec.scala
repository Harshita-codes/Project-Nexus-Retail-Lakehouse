package com.nexus.batch

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.Comparator

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class ProjectNexusBatchAppSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("ProjectNexusBatchAppSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.testing.memory", "2147480000")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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

  test("writes Bronze, Silver, Quarantine, and Gold Delta outputs from flat CSV inputs") {
    val root = Files.createTempDirectory("nexus-batch-app-test")
    val input = Files.createDirectories(root.resolve("input"))
    val output = root.resolve("output")

    try {
      writeCsv(
        input.resolve("products.csv"),
        """product_id,product_name,category,unit_price,active
          |PRD001,Notebook,Electronics,10.00,Y
          |PRD001,Duplicate,Electronics,-1.00,N
          |PRD002,Toy,Toys,5.00,Y
          |""".stripMargin
      )
      writeCsv(
        input.resolve("customers.csv"),
        """customer_id,customer_name,email,city,signup_date
          |CUST001,Alice,alice@example.com,New York,2018-01-01
          |CUST001,Alice Old,alice@example.com,New York,2017-01-01
          |CUST002,Bob,not-an-email,Boston,2018-01-01
          |""".stripMargin
      )
      writeCsv(
        input.resolve("orders.csv"),
        """order_id,customer_id,product_id,quantity,order_date,order_status,last_updated
          |ORD000001,CUST001,PRD001,2,2019-01-01,PLACED,2019-01-01 10:00:00
          |ORD000001,CUST001,PRD001,1,2019-01-01,PLACED,2018-12-31 10:00:00
          |ORD000002,CUST999,PRD001,1,2019-01-01,SHIPPED,2019-01-01 11:00:00
          |ORD000003,CUST001,PRD001,0,2019-01-01,PLACED,2019-01-01 12:00:00
          |""".stripMargin
      )

      val result = ProjectNexusBatchApp.execute(
        spark,
        BatchConfig(input.toString, output.toString, "2020-01-01")
      )

      assert(result.counts("bronze.products") == 3)
      assert(result.counts("silver.products") == 1)
      assert(result.counts("quarantine.products") == 2)
      assert(result.counts("silver.customers") == 1)
      assert(result.counts("quarantine.customers") == 2)
      assert(result.counts("silver.orders") == 1)
      assert(result.counts("quarantine.orders") == 3)
      assert(result.counts("gold.category_sales") == 1)
      assert(result.counts("gold.customer_spend") == 1)
      assert(result.durationMillis >= 0)

      val silverOrder = spark.read.format("delta")
        .load(output.resolve("silver/orders").toString)
        .head()
      assert(silverOrder.getAs[Int]("quantity") == 2)

      val goldRevenue = spark.read.format("delta")
        .load(output.resolve("gold/category_sales").toString)
        .head()
        .getAs[java.math.BigDecimal]("total_net_revenue")
      assert(goldRevenue.compareTo(new java.math.BigDecimal("20.00")) == 0)
    } finally {
      deleteRecursively(root)
    }
  }

  test("rejects an invalid run date and unsupported output format") {
    val root = Files.createTempDirectory("nexus-batch-invalid-config")
    val input = Files.createDirectories(root.resolve("input"))
    writeCsv(input.resolve("products.csv"), "product_id,product_name,category,unit_price,active\n")
    writeCsv(input.resolve("customers.csv"), "customer_id,customer_name,email,city,signup_date\n")
    writeCsv(input.resolve("orders.csv"), "order_id,customer_id,product_id,quantity,order_date,order_status,last_updated\n")

    try {
      intercept[java.time.format.DateTimeParseException] {
        ProjectNexusBatchApp.execute(spark, BatchConfig(input.toString, root.resolve("out").toString, "not-a-date"))
      }
      intercept[IllegalArgumentException] {
        ProjectNexusBatchApp.execute(
          spark,
          BatchConfig(input.toString, root.resolve("out").toString, "2020-01-01", "unsupported")
        )
      }
    } finally {
      deleteRecursively(root)
    }
  }

  private def writeCsv(path: Path, contents: String): Unit =
    Files.write(path, contents.getBytes(UTF_8))

  private def deleteRecursively(root: Path): Unit = {
    if (Files.exists(root)) {
      val paths = Files.walk(root)
      try {
        paths.iterator().asScala.toVector
          .sortWith((left, right) => left.getNameCount > right.getNameCount)
          .foreach(path => Files.deleteIfExists(path))
      } finally {
        paths.close()
      }
    }
  }
}
