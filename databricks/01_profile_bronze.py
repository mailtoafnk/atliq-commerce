# Databricks notebook source
# MAGIC %md
# MAGIC # 01_profile_bronze: measure the mess before cleaning it
# MAGIC Profile in Spark, not only in SQL Server: Azure SQL's default collation ignores case and trailing spaces
# MAGIC (it showed 10 order statuses), Spark compares exactly and shows all 25.

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE = "abfss://lakehouse@atliqlakemeet.dfs.core.windows.net/bronze"
customers = spark.read.parquet(f"{BRONZE}/customers")
products  = spark.read.parquet(f"{BRONZE}/products")
orders    = spark.read.parquet(f"{BRONZE}/orders")
items     = spark.read.parquet(f"{BRONZE}/order_items")
payments  = spark.read.parquet(f"{BRONZE}/payments")

def distinct(df, col):
    return df.select(col).distinct().count()

print("ROWS      customers", customers.count(), "| products", products.count(),
      "| orders", orders.count(), "| order_items", items.count(), "| payments", payments.count())
print("SPELLINGS city", distinct(customers, "city"), "| category", distinct(products, "category"),
      "| status", distinct(orders, "status"), "| method", distinct(payments, "method"))
print("QA test accounts        :", customers.filter(F.lower("email").contains("@atliq-test.com")).count())
print("emails not lower/trimmed:", customers.filter(F.col("email") != F.lower(F.trim("email"))).count())
print("orders dated 1900-01-01 :", orders.filter(F.col("order_date") == "1900-01-01").count())
dup_items = items.count() - items.dropDuplicates(["order_id","product_id","quantity","item_price","created_at"]).count()
print("double-submitted lines  :", dup_items)
extra_pay = payments.count() - payments.select("order_id").distinct().count()
print("retried payments (extra):", extra_pay)

# COMMAND ----------

def variants(df, col):
    # [brackets] make leading/trailing spaces visible
    return (df.groupBy(F.concat(F.lit("["), F.col(col), F.lit("]")).alias(col))
              .count().orderBy(col))

display(variants(orders, "status"))
display(variants(customers, "city"))
display(variants(products, "category"))
display(variants(payments, "method"))

# COMMAND ----------

def norm(col):
    """trimmed, lower-case, single-spaced text: the key every lookup map uses"""
    return F.lower(F.regexp_replace(F.trim(col), r"\s+", " "))

STATUS_MAP = {"placed": "Placed", "shipped": "Shipped", "delivered": "Delivered",
              "cancelled": "Cancelled", "canceled": "Cancelled", "returned": "Returned"}
status_map = F.create_map(*[F.lit(x) for kv in STATUS_MAP.items() for x in kv])

print("Step 1: normalised keys")
display(orders.groupBy(norm(F.col("status")).alias("key")).count().orderBy("key"))
print("Step 2: mapped to the 5 official statuses")
display(orders.groupBy(status_map[norm(F.col("status"))].alias("status")).count().orderBy("status"))
