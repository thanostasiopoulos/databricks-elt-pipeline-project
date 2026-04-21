# Databricks notebook source
# Gold Layer — gld_delivery_performance
#
# Aggregates delivery performance metrics by state and month.
# Measures actual vs estimated delivery, on-time rate, and average
# fulfilment cycle time — useful for identifying regional logistics issues.
#
# Source tables:
#   - silver.fact_orders    (delivery metrics, order_status)
#   - silver.dim_customer   (customer_state)
#
# Full overwrite on each run.

# COMMAND ----------

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F

# COMMAND ----------

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG = "olist"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.gld_delivery_performance"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

# ── Extract ───────────────────────────────────────────────────────────────────

fact_orders_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.fact_orders")
dim_customer_df = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.dim_customer")

# COMMAND ----------

# ── Transform ─────────────────────────────────────────────────────────────────

# Filter to delivered orders only — only these have meaningful delivery dates
delivered_df = (
    fact_orders_df
    .filter(F.col("order_status") == "delivered")
    .join(
        dim_customer_df.select("customer_unique_id", "customer_state"),
        on="customer_unique_id",
        how="left",
    )
    .withColumn(
        "customer_state",
        F.coalesce(F.col("customer_state"), F.lit("unknown")),
    )
)

delivery_perf_df = (
    delivered_df
    .groupBy("order_purchase_month", "customer_state")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum(F.col("is_delivered_on_time").cast("integer")).alias("on_time_orders"),
        F.round(
            F.sum(F.col("is_delivered_on_time").cast("integer")) / F.count("order_id") * 100, 2
        ).alias("on_time_rate_pct"),
        F.round(F.avg("delivery_delta_days"), 2).alias("avg_delivery_delta_days"),
        F.round(F.avg("purchase_to_delivery_days"), 2).alias("avg_purchase_to_delivery_days"),
        F.round(F.min("delivery_delta_days"), 2).alias("min_delivery_delta_days"),
        F.round(F.max("delivery_delta_days"), 2).alias("max_delivery_delta_days"),
    )
    .orderBy("order_purchase_month", "customer_state")
    .withColumn("_transformed_at", F.current_timestamp())
)

print(f"Gold row count : {delivery_perf_df.count():,}")

# COMMAND ----------

# ── Data Quality Assertions ───────────────────────────────────────────────────

assert delivery_perf_df.count() > 0, "gld_delivery_performance is empty"

null_rate = delivery_perf_df.filter(F.col("on_time_rate_pct").isNull()).count()
assert null_rate == 0, f"Null on_time_rate_pct values detected — {null_rate} rows affected"

invalid_rate = delivery_perf_df.filter(
    (F.col("on_time_rate_pct") < 0) | (F.col("on_time_rate_pct") > 100)
).count()
assert invalid_rate == 0, f"on_time_rate_pct out of range [0-100] — {invalid_rate} rows affected"

print("  ✓ All data quality checks passed")

# COMMAND ----------

# ── Load ──────────────────────────────────────────────────────────────────────

(
    delivery_perf_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

final_count = spark.table(TARGET_TABLE).count()
print(f"  ✓ {TARGET_TABLE} written — {final_count:,} rows")
