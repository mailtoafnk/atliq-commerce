# Databricks notebook source
# MAGIC %md
# MAGIC # 04_silver_clickstream: Bronze JSON lines -> atliq.silver.web_events
# MAGIC Read raw lines as text, parse with an explicit schema, **quarantine** unparseable lines instead of failing,
# MAGIC then: parse both timestamp forms, normalise event_type, one row per event_id, drop events without a session,
# MAGIC drop **whole bot sessions**, drop events for unknown products. Events are dated by `event_ts`, not by file date.
# MAGIC Rebuilt in full each night (fine at this size).

# COMMAND ----------

# MAGIC %run ./cleaning_rules

# COMMAND ----------

run_date = get_run_date()
print("run_date:", run_date)

SCHEMA = ("event_id STRING, session_id STRING, customer_id BIGINT, event_type STRING, "
          "event_ts STRING, product_id BIGINT, order_id BIGINT, device STRING, "
          "user_agent STRING, utm_source STRING")

lines  = spark.read.text(f"{BRONZE}/clickstream")          # raw lines, exactly as ADF copied them
parsed = lines.withColumn("e", F.from_json("value", SCHEMA))

# COMMAND ----------

# MAGIC %md ## 1. Quarantine lines that are not valid JSON events

# COMMAND ----------

bad = parsed.filter("e.event_id IS NULL").select("value")
bad.write.mode("overwrite").saveAsTable(f"{SILVER}.web_events_quarantine")
log_dq(run_date, "web_events", "unparseable lines quarantined", spark.table(f"{SILVER}.web_events_quarantine").count())

# COMMAND ----------

# MAGIC %md ## 2. Clean the parsed events

# COMMAND ----------

ts = (F.when(F.col("event_ts").rlike(r"^\d+$"), F.timestamp_millis(F.col("event_ts").cast("bigint")))
       .otherwise(F.try_to_timestamp("event_ts", F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX"))))

ev0 = parsed.filter("e.event_id IS NOT NULL").select("e.*")
log_dq(run_date, "web_events", "event_ts given as epoch milliseconds (parsed)", count_where(ev0, F.col("event_ts").rlike(r"^\d+$")))
log_dq(run_date, "web_events", "event_type: case/spaces fixed", count_where(ev0, F.col("event_type") != norm(F.col("event_type"))))

ev1 = (ev0.withColumn("event_ts", ts)
          .withColumn("event_type", norm(F.col("event_type"))))
log_dq(run_date, "web_events", "event_ts unparseable (NULL) - must be 0", count_where(ev1, F.col("event_ts").isNull()))
log_dq(run_date, "web_events", "event_type not one of the 5 (must be 0)", count_where(ev1, ~F.col("event_type").isin(EVENT_TYPES)))

ev2 = ev1.dropDuplicates(["event_id"])
log_dq(run_date, "web_events", "re-sent events removed (one row per event_id)", ev1.count() - ev2.count())

ev3 = ev2.filter(F.col("session_id").isNotNull() & (F.trim("session_id") != ""))
log_dq(run_date, "web_events", "events without a session dropped", ev2.count() - ev3.count())

# COMMAND ----------

# MAGIC %md ## 3. Drop whole bot sessions and events for unknown (retired) products

# COMMAND ----------

bots = (ev3.filter(F.col("user_agent").rlike(r"(?i)(bot|crawler|spider|python-requests)"))
           .select("session_id").distinct())
ev4 = ev3.join(bots, "session_id", "left_anti")
log_dq(run_date, "web_events", "bot sessions dropped (sessions)", bots.count())
log_dq(run_date, "web_events", "bot sessions dropped (events)", ev3.count() - ev4.count())

known = spark.table(f"{SILVER}.products").select(F.col("product_id").alias("_pid"))
ev5 = (ev4.join(known, F.col("product_id") == F.col("_pid"), "left")
          .filter(F.col("product_id").isNull() | F.col("_pid").isNotNull())
          .drop("_pid"))
log_dq(run_date, "web_events", "events for unknown products dropped", ev4.count() - ev5.count())

web_events = ev5.withColumn("event_date", F.to_date("event_ts"))   # dated by event_ts, not by file date
(web_events.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{SILVER}.web_events"))
print("saved", f"{SILVER}.web_events:", spark.table(f"{SILVER}.web_events").count(), "events,",
      spark.table(f"{SILVER}.web_events").select("session_id").distinct().count(), "sessions")
