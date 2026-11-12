# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver_full: customers, products, supplier CSV, marketing CSV
# MAGIC Full tables: read the latest Bronze snapshot, clean it, **overwrite** the Silver table.
# MAGIC Every rule logs how many rows it touched to `atliq.silver.dq_log`.
# MAGIC Order matters: products must exist before the supplier CSV (unknown products are dropped).

# COMMAND ----------

# MAGIC %run ./cleaning_rules

# COMMAND ----------

run_date = get_run_date()
print("run_date:", run_date)

def save(df, table):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{table}"))
    print(f"saved {SILVER}.{table}: {spark.table(f'{SILVER}.{table}').count()} rows")

# COMMAND ----------

# MAGIC %md ## customers: city map, email lower+trim, flag QA test accounts (flag, do not delete)

# COMMAND ----------

raw = spark.read.parquet(f"{BRONZE}/customers")
city = clean_text(F.col("city"), CITY_MAP)
email = F.lower(F.trim("email"))

log_dq(run_date, "customers", "city: spelling/case/old name fixed", count_where(raw, F.col("city") != city))
log_dq(run_date, "customers", "city: unmapped (NULL) - must be 0", count_where(raw, city.isNull()))
log_dq(run_date, "customers", "email: lower-cased and trimmed", count_where(raw, F.col("email") != email))
log_dq(run_date, "customers", "is_test: QA account flagged (@atliq-test.com)", count_where(raw, email.endswith("@atliq-test.com")))

customers = (raw
    .withColumn("city", city)
    .withColumn("email", email)
    .withColumn("is_test", F.col("email").endswith("@atliq-test.com"))
    .withColumn("signup_date", F.to_date("signup_date"))
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"]))
save(customers, "customers")

# COMMAND ----------

# MAGIC %md ## products: tidy names, category map

# COMMAND ----------

raw = spark.read.parquet(f"{BRONZE}/products")
category = clean_text(F.col("category"), CATEGORY_MAP)

log_dq(run_date, "products", "product_name: spaces trimmed/collapsed", count_where(raw, F.col("product_name") != tidy_text(F.col("product_name"))))
log_dq(run_date, "products", "category: spelling/case/'and' fixed", count_where(raw, F.col("category") != category))
log_dq(run_date, "products", "category: unmapped (NULL) - must be 0", count_where(raw, category.isNull()))

products = (raw
    .withColumn("product_name", tidy_text(F.col("product_name")))
    .withColumn("category", category)
    .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"]))
save(products, "products")

# COMMAND ----------

# MAGIC %md ## supplier_price_list (CSV, all text): parse amounts and dates, drop exact repeats, drop unknown products
# MAGIC The *current* cost (latest effective_date) is picked later, in dbt staging.

# COMMAND ----------

raw = spark.read.option("header", True).csv(f"{BRONZE}/supplier_price_list")
parsed = (raw
    .withColumn("product_id", F.expr("try_cast(product_id AS INT)"))
    .withColumn("product_name", tidy_text(F.col("product_name")))
    .withColumn("supplier_name", tidy_text(F.col("supplier_name")))
    .withColumn("supplier_cost", parse_amount(F.col("supplier_cost")))
    .withColumn("effective_date", parse_date(F.col("effective_date"))))

deduped = parsed.dropDuplicates()
known = deduped.join(spark.table(f"{SILVER}.products").select("product_id"), "product_id", "left_semi")

log_dq(run_date, "supplier_price_list", "amount: currency symbol / separators parsed", count_where(raw, ~F.col("supplier_cost").rlike(r"^-?[0-9]+(\.[0-9]+)?$")))
log_dq(run_date, "supplier_price_list", "date: non-ISO format parsed", count_where(raw, ~F.col("effective_date").rlike(r"^\d{4}-\d{2}-\d{2}$")))
log_dq(run_date, "supplier_price_list", "unparseable amount or date (NULL) - must be 0", count_where(parsed, F.col("supplier_cost").isNull() | F.col("effective_date").isNull()))
log_dq(run_date, "supplier_price_list", "exact duplicate rows removed", parsed.count() - deduped.count())
log_dq(run_date, "supplier_price_list", "rows for unknown/discontinued products dropped", deduped.count() - known.count())
save(known, "supplier_price_list")

# COMMAND ----------

# MAGIC %md ## marketing_spend (CSV, all text): channel map, parse numbers, drop repeats and blank spend, keep negative corrections

# COMMAND ----------

raw = spark.read.option("header", True).csv(f"{BRONZE}/marketing_spend")
deduped = raw.dropDuplicates()
channel = clean_text(F.col("channel"), CHANNEL_MAP)
parsed = (deduped
    .withColumn("spend_date", F.to_date("spend_date"))
    .withColumn("channel", channel)
    .withColumn("spend_amount", parse_amount(F.col("spend_amount")))
    .withColumn("clicks", parse_int("clicks")))
kept = parsed.filter(F.col("spend_amount").isNotNull())

log_dq(run_date, "marketing_spend", "exact duplicate rows removed", raw.count() - deduped.count())
log_dq(run_date, "marketing_spend", "rows with blank spend dropped", parsed.count() - kept.count())
log_dq(run_date, "marketing_spend", "channel: spelling/case/alias fixed", count_where(deduped, F.col("channel") != channel))
log_dq(run_date, "marketing_spend", "channel: unmapped (NULL) - must be 0", count_where(kept, F.col("channel").isNull()))
log_dq(run_date, "marketing_spend", "clicks: thousands separator removed", count_where(deduped, F.col("clicks").contains(",")))
log_dq(run_date, "marketing_spend", "negative corrections kept (they net off)", count_where(kept, F.col("spend_amount") < 0))
save(kept, "marketing_spend")
