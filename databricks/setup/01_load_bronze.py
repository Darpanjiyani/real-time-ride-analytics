# Databricks notebook source
# MAGIC %md
# MAGIC # Load bronze lookup tables + historical backfill
# MAGIC
# MAGIC Reads the JSON files from the `rides.bronze.raw` Volume (no SAS tokens in code)
# MAGIC and writes them as Delta tables in `rides.bronze`.
# MAGIC
# MAGIC * `map_*` lookup tables are overwritten on every run (small, static reference data).
# MAGIC * `bulk_rides` is written **only once** - it is the one-time backfill that the
# MAGIC   pipeline's `rides_backfill` append flow streams into silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "rides")
dbutils.widgets.text("source_path", "/Volumes/rides/bronze/raw")

CATALOG = dbutils.widgets.get("catalog")
SOURCE_PATH = dbutils.widgets.get("source_path").rstrip("/")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

LOOKUP_TABLES = [
    "map_cities",
    "map_cancellation_reasons",
    "map_payment_methods",
    "map_ride_statuses",
    "map_vehicle_makes",
    "map_vehicle_types",
]

for name in LOOKUP_TABLES:
    df = spark.read.option("multiLine", "true").json(f"{SOURCE_PATH}/{name}.json")
    if "updated_at" in df.columns:
        df = df.withColumn("updated_at", to_timestamp("updated_at"))
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.bronze.{name}")
    )
    print(f"Loaded {CATALOG}.bronze.{name}: {df.count()} rows")

# COMMAND ----------

bulk_table = f"{CATALOG}.bronze.bulk_rides"

if spark.catalog.tableExists(bulk_table):
    print(f"{bulk_table} already exists - skipping (backfill is a one-time load)")
else:
    bulk = spark.read.option("multiLine", "true").json(f"{SOURCE_PATH}/bulk_rides.json")
    for ts in ["booking_timestamp", "pickup_timestamp", "dropoff_timestamp"]:
        bulk = bulk.withColumn(ts, to_timestamp(col(ts)))
    bulk.write.format("delta").saveAsTable(bulk_table)
    print(f"Created {bulk_table}: {bulk.count()} rows")
