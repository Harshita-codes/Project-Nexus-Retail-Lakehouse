package com.nexus.batch

import java.nio.file.{Files, Path, Paths}
import java.sql.Date
import java.time.{Duration, Instant, LocalDate}
import java.util.Locale

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

final case class BatchConfig(
    inputPath: String,
    outputPath: String,
    runDate: String,
    outputFormat: String = "delta"
)

final case class BatchRunSummary(
    counts: Map[String, Long],
    durationMillis: Long
)

object ProjectNexusBatchApp {
  private val allowedCategories =
    Seq("Electronics", "Apparel", "Home", "Grocery", "Beauty", "Sports")
  private val allowedStatuses =
    Seq("PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED")
  private val emailRegex = "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"

  def main(args: Array[String]): Unit = {
    require(args.isEmpty, "Configure the batch app with BATCH_* environment variables; positional arguments are unsupported")
    val config = BatchConfig(
      inputPath = requiredEnvironment("BATCH_INPUT_PATH"),
      outputPath = requiredEnvironment("BATCH_OUTPUT_PATH"),
      runDate = requiredEnvironment("BATCH_RUN_DATE"),
      outputFormat = sys.env.getOrElse("BATCH_OUTPUT_FORMAT", "delta")
    )
    val master = sys.env.getOrElse("SPARK_MASTER", "local[*]")

    val spark = SparkSession.builder()
      .appName("ProjectNexusBatchApp")
      .master(master)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", sys.env.getOrElse("SPARK_SHUFFLE_PARTITIONS", "4"))
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    try {
      execute(spark, config)
    } finally {
      spark.stop()
    }
  }

  def execute(spark: SparkSession, config: BatchConfig): BatchRunSummary = {
    val startedAt = Instant.now()
    val parsedRunDate = LocalDate.parse(config.runDate)
    val format = config.outputFormat.toLowerCase(Locale.ROOT)
    require(format == "delta" || format == "parquet", "BATCH_OUTPUT_FORMAT must be 'delta' or 'parquet'")
    require(config.inputPath.trim.nonEmpty, "BATCH_INPUT_PATH must not be empty")
    require(config.outputPath.trim.nonEmpty, "BATCH_OUTPUT_PATH must not be empty")

    println(
      s"Batch start: run_date=$parsedRunDate input=${config.inputPath} " +
        s"output=${config.outputPath} format=$format"
    )

    val productsBronze = readCsv(
      spark,
      config.inputPath,
      "products.csv",
      "raw/products/products.csv",
      BatchHelpers.productsSchema
    ).withColumn("_run_date", to_date(lit(parsedRunDate.toString)))
      .withColumn("_ingested_at", current_timestamp())

    val customersBronze = readCsv(
      spark,
      config.inputPath,
      "customers.csv",
      "raw/customers/customers.csv",
      BatchHelpers.customersSchema
    ).withColumn("_run_date", to_date(lit(parsedRunDate.toString)))
      .withColumn("_ingested_at", current_timestamp())

    val ordersBronze = readCsv(
      spark,
      config.inputPath,
      "orders.csv",
      "raw/orders/orders.csv",
      BatchHelpers.ordersSchema
    ).withColumn("_run_date", to_date(lit(parsedRunDate.toString)))
      .withColumn("_ingested_at", current_timestamp())

    val counts = scala.collection.mutable.LinkedHashMap.empty[String, Long]
    def writeOutput(name: String, frame: DataFrame, relativePath: String): Unit = {
      val count = frame.count()
      val path = outputPath(config.outputPath, relativePath)
      frame.write.format(format).mode("overwrite").save(path)
      counts += name -> count
      println(s"$name rows=$count path=$path format=$format")
    }

    writeOutput("bronze.products", productsBronze, "bronze/products")
    writeOutput("bronze.customers", customersBronze, "bronze/customers")
    writeOutput("bronze.orders", ordersBronze, "bronze/orders")

    val productsTyped = productsBronze
      .withColumn("product_id", upper(trim(col("product_id"))))
      .withColumn("product_name", trim(col("product_name")))
      .withColumn("category", initcap(trim(col("category"))))
      .withColumn("unit_price_dec", col("unit_price").cast(DecimalType(10, 2)))
      .withColumn("active", upper(trim(col("active"))))

    val productWindow = Window.partitionBy("product_id").orderBy(
      when(col("unit_price_dec") > 0, 1).otherwise(2).asc,
      when(col("active") === "Y", 1).otherwise(2).asc
    )
    val productsDeduped = productsTyped
      .withColumn(
        "rn",
        when(BatchHelpers.nonBlank(col("product_id")), row_number().over(productWindow)).otherwise(1)
      )
      .withColumn("is_dup", col("rn") > 1)

    val productsEvaluated = BatchHelpers.withRejectionReasons(productsDeduped, Seq(
      col("is_dup") -> "Duplicate product_id",
      !BatchHelpers.matchesPattern(col("product_id"), "^PRD\\d{3}$") -> "Invalid/missing product_id",
      !BatchHelpers.nonBlank(col("product_name")) -> "Missing product_name",
      notIn(col("category"), allowedCategories) -> "Invalid/missing category",
      !BatchHelpers.positiveDecimal(col("unit_price_dec")) -> "Invalid unit_price (<= 0 or non-numeric)",
      notIn(col("active"), Seq("Y", "N")) -> "Invalid active flag"
    ))

    val productsSilver = productsEvaluated
      .filter(col("rejection_reason") === "")
      .select(
        col("product_id"),
        col("product_name"),
        col("category"),
        col("unit_price_dec").alias("unit_price"),
        col("active"),
        current_timestamp().alias("_silver_processed_at")
      )
    val productsQuarantine = productsEvaluated
      .filter(col("rejection_reason") =!= "")
      .drop("rn", "is_dup", "unit_price_dec")
      .withColumn("_quarantined_at", current_timestamp())

    writeOutput("silver.products", productsSilver, "silver/products")
    writeOutput("quarantine.products", productsQuarantine, "quarantine/products")

    val customersTyped = customersBronze
      .withColumn("customer_id", upper(trim(col("customer_id"))))
      .withColumn("customer_name", trim(col("customer_name")))
      .withColumn("email", lower(trim(col("email"))))
      .withColumn("city", initcap(regexp_replace(trim(col("city")), "\\s+", " ")))
      .withColumn(
        "signup_date_parsed",
        expr("to_date(try_to_timestamp(trim(signup_date), 'yyyy-MM-dd'))")
      )

    val customerWindow = Window.partitionBy("customer_id").orderBy(col("signup_date_parsed").desc_nulls_last)
    val customersDeduped = customersTyped
      .withColumn(
        "rn",
        when(BatchHelpers.nonBlank(col("customer_id")), row_number().over(customerWindow)).otherwise(1)
      )
      .withColumn("is_dup", col("rn") > 1)

    val customersEvaluated = BatchHelpers.withRejectionReasons(customersDeduped, Seq(
      col("is_dup") -> "Duplicate customer_id",
      !BatchHelpers.matchesPattern(col("customer_id"), "^CUST\\d{3}$") -> "Invalid/missing customer_id",
      !BatchHelpers.nonBlank(col("customer_name")) -> "Missing customer_name",
      !BatchHelpers.matchesPattern(col("email"), emailRegex) -> "Invalid/missing email",
      !BatchHelpers.nonBlank(col("city")) -> "Missing city",
      (
        col("signup_date_parsed").isNull ||
          col("signup_date_parsed") > lit(Date.valueOf(parsedRunDate))
      ) -> "Invalid or future signup_date"
    ))

    val customersSilver = customersEvaluated
      .filter(col("rejection_reason") === "")
      .select(
        col("customer_id"),
        col("customer_name"),
        col("email"),
        col("city"),
        col("signup_date_parsed").alias("signup_date"),
        current_timestamp().alias("_silver_processed_at")
      )
    val customersQuarantine = customersEvaluated
      .filter(col("rejection_reason") =!= "")
      .drop("rn", "is_dup", "signup_date_parsed")
      .withColumn("_quarantined_at", current_timestamp())

    writeOutput("silver.customers", customersSilver, "silver/customers")
    writeOutput("quarantine.customers", customersQuarantine, "quarantine/customers")

    val validCustomerIds = customersSilver.select("customer_id").distinct()
    val validProductIds = productsSilver.select("product_id").distinct()
    val ordersTyped = ordersBronze
      .withColumn("order_id", upper(trim(col("order_id"))))
      .withColumn("customer_id", upper(trim(col("customer_id"))))
      .withColumn("product_id", upper(trim(col("product_id"))))
      .withColumn("quantity_int", expr("try_cast(trim(quantity) as int)"))
      .withColumn(
        "order_date_parsed",
        expr(
          "coalesce(" +
            "to_date(try_to_timestamp(trim(order_date), 'yyyy-MM-dd')), " +
            "to_date(try_to_timestamp(trim(order_date), 'dd-MM-yyyy')))"
        )
      )
      .withColumn("order_status_clean", normalizedOrderStatus(col("order_status")))
      .withColumn(
        "last_updated_ts",
        expr("try_to_timestamp(trim(last_updated), 'yyyy-MM-dd HH:mm:ss')")
      )

    val orderWindow = Window.partitionBy("order_id").orderBy(col("last_updated_ts").desc_nulls_last)
    val ordersDeduped = ordersTyped
      .withColumn(
        "rn",
        when(BatchHelpers.nonBlank(col("order_id")), row_number().over(orderWindow)).otherwise(1)
      )
      .withColumn("is_dup", col("rn") > 1)

    val ordersWithForeignKeys = ordersDeduped
      .join(validCustomerIds.withColumn("_customer_exists", lit(true)), Seq("customer_id"), "left")
      .join(broadcast(validProductIds).withColumn("_product_exists", lit(true)), Seq("product_id"), "left")

    val ordersEvaluated = BatchHelpers.withRejectionReasons(ordersWithForeignKeys, Seq(
      col("is_dup") -> "Duplicate/stale order_id version",
      !BatchHelpers.matchesPattern(col("order_id"), "^ORD\\d{6}$") -> "Invalid/missing order_id",
      (col("customer_id").isNull || col("_customer_exists").isNull) -> "Orphan/missing customer_id",
      (col("product_id").isNull || col("_product_exists").isNull) -> "Orphan/missing product_id",
      (col("quantity_int").isNull || col("quantity_int") <= 0) -> "Invalid quantity (<= 0)",
      (
        col("order_date_parsed").isNull ||
          col("order_date_parsed") > lit(Date.valueOf(parsedRunDate))
      ) -> "Invalid or future order_date",
      notIn(col("order_status_clean"), allowedStatuses) -> "Invalid order_status",
      col("last_updated_ts").isNull -> "Invalid/missing last_updated timestamp"
    ))

    val ordersSilver = ordersEvaluated
      .filter(col("rejection_reason") === "")
      .select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("quantity_int").alias("quantity"),
        col("order_date_parsed").alias("order_date"),
        col("order_status_clean").alias("order_status"),
        col("last_updated_ts").alias("last_updated"),
        current_timestamp().alias("_silver_processed_at")
      )
    val ordersQuarantine = ordersEvaluated
      .filter(col("rejection_reason") =!= "")
      .drop(
        "rn",
        "is_dup",
        "quantity_int",
        "order_date_parsed",
        "order_status_clean",
        "last_updated_ts",
        "_customer_exists",
        "_product_exists"
      )
      .withColumn("_quarantined_at", current_timestamp())

    writeOutput("silver.orders", ordersSilver, "silver/orders")
    writeOutput("quarantine.orders", ordersQuarantine, "quarantine/orders")

    val enrichedSales = ordersSilver
      .filter(col("order_status") =!= "CANCELLED")
      .join(broadcast(productsSilver), Seq("product_id"), "inner")
      .join(customersSilver, Seq("customer_id"), "inner")
      .withColumn("gross_amount", col("quantity") * col("unit_price"))
      .withColumn("discount_applied", lit(0.00).cast(DecimalType(10, 2)))
      .withColumn("net_revenue", col("gross_amount") - col("discount_applied"))

    val goldCategorySales = enrichedSales
      .groupBy(col("category"), lit("LEGACY_BATCH").alias("sales_channel"))
      .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("quantity").alias("total_units_sold"),
        round(sum("net_revenue"), 2).alias("total_net_revenue")
      )
      .orderBy(col("total_net_revenue").desc)

    val goldCustomerSpend = enrichedSales
      .groupBy("customer_id", "customer_name", "city")
      .agg(
        countDistinct("order_id").alias("valid_orders"),
        round(sum("net_revenue"), 2).alias("lifetime_net_spend")
      )
      .orderBy(col("lifetime_net_spend").desc)

    writeOutput("gold.category_sales", goldCategorySales, "gold/category_sales")
    writeOutput("gold.customer_spend", goldCustomerSpend, "gold/customer_spend")

    val finishedAt = Instant.now()
    val durationMillis = Duration.between(startedAt, finishedAt).toMillis
    println(s"Batch completed: run_date=$parsedRunDate duration_ms=$durationMillis")
    println(s"Valid products=${counts("silver.products")}; quarantined products=${counts("quarantine.products")}")
    println(s"Valid customers=${counts("silver.customers")}; quarantined customers=${counts("quarantine.customers")}")
    println(s"Valid orders=${counts("silver.orders")}; quarantined orders=${counts("quarantine.orders")}")
    println(
      s"Gold category rows=${counts("gold.category_sales")}; " +
        s"gold customer rows=${counts("gold.customer_spend")}"
    )

    BatchRunSummary(counts.toMap, durationMillis)
  }

  private def requiredEnvironment(name: String): String =
    sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(s"Required environment variable $name is not set")
    }

  private def readCsv(
      spark: SparkSession,
      inputRoot: String,
      flatFileName: String,
      nestedFileName: String,
      schema: org.apache.spark.sql.types.StructType
  ): DataFrame = {
    val root = Paths.get(inputRoot)
    val path = Seq(flatFileName, nestedFileName)
      .map(root.resolve(_))
      .find(candidate => Files.isRegularFile(candidate))
      .getOrElse {
        throw new IllegalArgumentException(
          s"Missing $flatFileName under $inputRoot; expected either " +
            s"$inputRoot/$flatFileName or $inputRoot/$nestedFileName"
        )
      }

    spark.read
      .option("header", "true")
      .schema(schema)
      .csv(path.toAbsolutePath.toString)
  }

  private def outputPath(outputRoot: String, relativePath: String): String =
    Paths.get(outputRoot, relativePath.split("/"): _*).toString

  private def notIn(value: Column, allowed: Seq[String]): Column =
    !coalesce(value.isin(allowed: _*), lit(false))

  private def normalizedOrderStatus(rawStatus: Column): Column = {
    val value = upper(trim(rawStatus))
    when(value.isin("PLACED", "PLACE"), lit("PLACED"))
      .when(value.isin("SHIPPED", "SHIP"), lit("SHIPPED"))
      .when(value.isin("DELIVERED", "DELIVER"), lit("DELIVERED"))
      .when(value.isin("CANCELLED", "CANCELED", "CANCEL"), lit("CANCELLED"))
      .when(value.isin("RETURNED", "RETURN"), lit("RETURNED"))
      .otherwise(value)
  }
}
