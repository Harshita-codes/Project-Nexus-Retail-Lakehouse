// Databricks notebook source
package com.nexus.batch

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BatchHelpers {
  private def stringSchema(fields: String*): StructType =
    StructType(fields.map(name => StructField(name, StringType, nullable = true)))

  val productsSchema: StructType =
    stringSchema("product_id", "product_name", "category", "unit_price", "active")

  val customersSchema: StructType =
    stringSchema("customer_id", "customer_name", "email", "city", "signup_date")

  val ordersSchema: StructType =
    stringSchema("order_id", "customer_id", "product_id", "quantity", "order_date", "order_status", "last_updated")

  val ordersV2Schema: StructType =
    stringSchema(
      "order_id", "customer_id", "product_id", "quantity", "order_date", "order_status",
      "last_updated", "sales_channel", "discount_amount"
    )

  def nonBlank(value: Column): Column =
    coalesce(value.isNotNull && length(trim(value)) > 0, lit(false))

  def matchesPattern(value: Column, pattern: String): Column =
    coalesce(value.isNotNull && trim(value).rlike(pattern), lit(false))

  def positiveDecimal(value: Column, precision: Int = 10, scale: Int = 2): Column =
    coalesce(value.cast(DecimalType(precision, scale)) > 0, lit(false))

  def withRejectionReasons(df: DataFrame, rules: Seq[(Column, String)]): DataFrame =
    df.withColumn(
      "rejection_reason",
      concat_ws("; ", rules.map { case (condition, reason) => when(condition, lit(reason)) }: _*)
    )
}
