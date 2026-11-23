package com.nexus.batch

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class BatchHelpersSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("BatchHelpersSpec")
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

  test("raw CSV schemas preserve fields as strings so invalid values can be quarantined") {
    assert(BatchHelpers.productsSchema.fieldNames.toSeq ==
      Seq("product_id", "product_name", "category", "unit_price", "active"))
    assert(BatchHelpers.customersSchema.fieldNames.contains("signup_date"))
    assert(BatchHelpers.ordersSchema.fields.forall(_.dataType == StringType))
    assert(BatchHelpers.ordersV2Schema.fieldNames.takeRight(2).toSeq ==
      Seq("sales_channel", "discount_amount"))
  }

  test("reusable validation rules retain all rejection reasons") {
    val schema = StructType(Seq(
      StructField("product_id", StringType, nullable = true),
      StructField("unit_price", StringType, nullable = true)
    ))
    val source: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("PRD001", "12.50"),
        Row("bad-id", "-1"),
        Row("PRD003", "not-a-price")
      )),
      schema
    )

    val checked = BatchHelpers.withRejectionReasons(source, Seq(
      (!BatchHelpers.matchesPattern(col("product_id"), "^PRD\\d{3}$"), "Invalid product_id"),
      (!BatchHelpers.positiveDecimal(col("unit_price")), "Invalid unit_price")
    ))
    val reasons = checked.select("rejection_reason").collect().map(_.getString(0))

    assert(reasons(0).isEmpty)
    assert(reasons(1).contains("Invalid product_id"))
    assert(reasons(1).contains("Invalid unit_price"))
    assert(reasons(2).contains("Invalid unit_price"))
  }
}
