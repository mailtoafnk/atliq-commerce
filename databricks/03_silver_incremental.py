# Databricks notebook source
# MAGIC %md
# MAGIC # 03_silver_incremental: orders, order_items, payments
# MAGIC Read **only this run's Bronze batch** (`run_date` widget), keep the latest version of each key,
# MAGIC clean it, then **MERGE** (upsert) into Silver. MERGE on the business key is what makes a re-run idempotent.
# MAGIC
# MAGIC `run_date`: `YYYY-MM-DD` = one ADF batch · `ALL` = rebuild from every batch · empty = today (UTC).
# MAGIC A night with no batch is skipped cleanly (no PATH_NOT_FOUND failure).
# MAGIC
# MAGIC Business duplicates (double-submitted lines, retried payments) **stay in Silver** and are only counted here;
# MAGIC they are removed in dbt staging (M4), so Silver remains a faithful copy of the source.

# COMMAND ----------

# MAGIC %run ./cleaning_rules

# COMMAND ----------

run_date = get_run_date()
print("run_date:", run_date)

def read_batch(table):
    path = f"{BRONZE}/{table}" if run_date.upper() == "ALL" else f"{BRONZE}/{table}/ingest_date={run_date}"
    if not batch_exists(path):
        print(f"No new {table} batch for {run_date}: Silver unchanged.")
        return None
    df = spark.read.parquet(path)
    return df.drop("ingest_date") if "ingest_date" in df.columns else df

def latest_per_key(df, key, ts):
    w = Window.partitionBy(key).orderBy(F.col(ts).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def merge(src, table, key, update_when_newer=True):
    target = f"{SILVER}.{table}"
    DeltaTable.createIfNotExists(spark).tableName(target).addColumns(src.schema).execute()
    m = DeltaTable.forName(spark, target).alias("t").merge(src.alias("s"), f"t.{key} = s.{key}")
    if update_when_newer:
        m = m.whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
    m.whenNotMatchedInsertAll().execute()
    print(f"merged into {target}: now {spark.table(target).count()} rows")

# COMMAND ----------

# MAGIC %md ## orders: status map, fix 1900-01-01 dates, MERGE on order_id (update when newer)

# COMMAND ----------

batch = read_batch("orders")
if batch is not None:
    status = clean_text(F.col("status"), STATUS_MAP)
    log_dq(run_date, "orders", "status: spelling/case/spaces fixed", count_where(batch, F.col("status") != status))
    log_dq(run_date, "orders", "status: unmapped (NULL) - must be 0", count_where(batch, status.isNull()))
    log_dq(run_date, "orders", "order_date 1900-01-01 replaced by created_at date", count_where(batch, F.col("order_date") == F.lit("1900-01-01").cast("date")))
    log_dq(run_date, "orders", "older versions of the same order in batch collapsed", batch.count() - batch.select("order_id").distinct().count())

    src = (latest_per_key(batch, "order_id", "updated_at")
        .withColumn("status", status)
        .withColumn("order_date", F.when(F.col("order_date") == F.lit("1900-01-01").cast("date"), F.to_date("created_at"))
                                   .otherwise(F.col("order_date")))
        .withColumn("order_amount", F.col("order_amount").cast("decimal(12,2)"))
        .filter(F.col("order_id").isNotNull()))
    merge(src, "orders", "order_id")

# COMMAND ----------

# MAGIC %md ## order_items: insert-only MERGE on order_item_id (line items never change)

# COMMAND ----------

batch = read_batch("order_items")
if batch is not None:
    dup = batch.count() - batch.dropDuplicates(["order_id", "product_id", "quantity", "item_price", "created_at"]).count()
    log_dq(run_date, "order_items", "double-submitted lines detected (removed in staging)", dup)

    src = (batch.dropDuplicates(["order_item_id"])
        .withColumn("item_price", F.col("item_price").cast("decimal(10,2)"))
        .filter(F.col("order_item_id").isNotNull()))
    merge(src, "order_items", "order_item_id", update_when_newer=False)

# COMMAND ----------

# MAGIC %md ## payments: method map, MERGE on payment_id (update when newer)

# COMMAND ----------

batch = read_batch("payments")
if batch is not None:
    method = clean_text(F.col("method"), METHOD_MAP)
    log_dq(run_date, "payments", "method: spelling/case/alias fixed", count_where(batch, F.col("method") != method))
    log_dq(run_date, "payments", "method: unmapped (NULL) - must be 0", count_where(batch, method.isNull()))
    log_dq(run_date, "payments", "retried payments detected (first kept in staging)", batch.count() - batch.select("order_id").distinct().count())

    src = (latest_per_key(batch, "payment_id", "updated_at")
        .withColumn("method", method)
        .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
        .filter(F.col("payment_id").isNotNull()))
    merge(src, "payments", "payment_id")
