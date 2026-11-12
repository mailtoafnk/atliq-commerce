# Databricks notebook source
# MAGIC %md
# MAGIC # cleaning_rules
# MAGIC Shared settings, lookup maps and helper functions for every Silver notebook.
# MAGIC Load it at the top of a notebook with `%run ./cleaning_rules`.
# MAGIC
# MAGIC **Rule of thumb:** normalise text (`norm`), then map it. Anything a map does not know becomes NULL,
# MAGIC so a brand-new spelling fails a dbt test in M4 instead of disappearing silently.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

LAKE    = "abfss://lakehouse@atliqlakemeet.dfs.core.windows.net"
BRONZE  = f"{LAKE}/bronze"
SILVER  = "atliq.silver"

# COMMAND ----------

# MAGIC %md ## Text normalisation and lookup maps (keys are already normalised: trimmed, lower-case, single-spaced)

# COMMAND ----------

def norm(col):
    """trimmed, lower-case, single-spaced text: the key every lookup map uses"""
    return F.lower(F.regexp_replace(F.trim(col), r"\s+", " "))

def to_map(d):
    return F.create_map(*[F.lit(x) for kv in d.items() for x in kv])

def clean_text(col, mapping):
    """normalise, then look up; unknown values become NULL"""
    return to_map(mapping)[norm(col)]

def tidy_text(col):
    """trim and collapse repeated spaces, keep the original casing (product names)"""
    return F.regexp_replace(F.trim(col), r"\s+", " ")

STATUS_MAP = {"placed": "Placed", "shipped": "Shipped", "delivered": "Delivered",
              "cancelled": "Cancelled", "canceled": "Cancelled", "returned": "Returned"}

CITY_MAP = {"bengaluru": "Bengaluru", "bangalore": "Bengaluru",
            "mumbai": "Mumbai", "bombay": "Mumbai",
            "delhi": "Delhi",
            "hyderabad": "Hyderabad",
            "chennai": "Chennai", "madras": "Chennai",
            "pune": "Pune",
            "kolkata": "Kolkata", "calcutta": "Kolkata",
            "ahmedabad": "Ahmedabad", "jaipur": "Jaipur", "surat": "Surat"}

CATEGORY_MAP = {"electronics": "Electronics",
                "home & kitchen": "Home & Kitchen", "home and kitchen": "Home & Kitchen",
                "fashion": "Fashion", "books": "Books", "beauty": "Beauty", "sports": "Sports"}

METHOD_MAP = {"upi": "UPI",
              "credit card": "Credit Card", "creditcard": "Credit Card",
              "debit card": "Debit Card", "debitcard": "Debit Card",
              "net banking": "Net Banking", "netbanking": "Net Banking",
              "wallet": "Wallet",
              "cod": "COD", "cash on delivery": "COD"}

CHANNEL_MAP = {"meta ads": "Meta Ads", "facebook ads": "Meta Ads",
               "google ads": "Google Ads", "googleads": "Google Ads",
               "email": "Email", "e-mail": "Email",
               "seo": "SEO",
               "influencer": "Influencer", "influencer marketing": "Influencer"}

EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "checkout", "purchase"]

# COMMAND ----------

# MAGIC %md ## Parsing helpers (safe under ANSI mode: bad text becomes NULL instead of failing the job)

# COMMAND ----------

def parse_amount(col):
    """extract the number: 'Rs. 1202.45' -> 1202.45, '₹1,298.26' -> 1298.26, '-132.98' -> -132.98, '' -> NULL"""
    amount = F.regexp_replace(F.regexp_extract(col, r"-?[0-9][0-9,]*(\.[0-9]+)?", 0), ",", "")
    return F.when(amount != "", amount.cast("decimal(12,2)"))

def parse_date(col, formats=("yyyy-MM-dd", "dd/MM/yyyy", "dd-MMM-yyyy")):
    """try each date format in turn; the first that fits wins"""
    return F.coalesce(*[F.to_date(F.try_to_timestamp(col, F.lit(f))) for f in formats])

def parse_int(col):
    """'1,234' -> 1234, '' -> NULL"""
    return F.expr(f"try_cast(regexp_replace({col}, ',', '') AS BIGINT)")

# COMMAND ----------

# MAGIC %md ## Run date, batch checks and the data quality log

# COMMAND ----------

def get_run_date():
    """run_date widget: 'YYYY-MM-DD' (one ADF batch), 'ALL' (rebuild from every batch), empty = today (UTC)"""
    dbutils.widgets.text("run_date", "")
    value = dbutils.widgets.get("run_date").strip()
    return value or datetime.now(timezone.utc).strftime("%Y-%m-%d")

def batch_exists(path: str) -> bool:
    """True when the folder exists and holds a Parquet file or a sub-folder (ingest_date=...)."""
    try:
        return any(f.name.endswith(".parquet") or f.name.endswith("/") for f in dbutils.fs.ls(path))
    except Exception as e:
        if "FileNotFound" in str(e) or "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e):
            return False
        raise  # anything else (e.g. permissions) should still fail the run

def log_dq(run_date, table_name, rule, rows_affected):
    (spark.createDataFrame([(run_date, table_name, rule, int(rows_affected or 0))],
                           "run_date STRING, table_name STRING, rule STRING, rows_affected LONG")
          .withColumn("logged_at", F.current_timestamp())
          .write.mode("append").saveAsTable(f"{SILVER}.dq_log"))
    print(f"  dq | {table_name:<20} | {rule:<55} | {int(rows_affected or 0):>7}")

def count_where(df, condition):
    return df.filter(condition).count()

print("cleaning_rules loaded")
