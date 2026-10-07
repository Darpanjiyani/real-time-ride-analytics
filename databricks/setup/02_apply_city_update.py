# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: trigger an SCD Type 2 change
# MAGIC
# MAGIC `map_cities.json` ships with a typo ("Los Angelas"). This notebook applies
# MAGIC `updates/map_cities_update.json`, which corrects the name with a newer `updated_at`.
# MAGIC
# MAGIC 1. Run this notebook.
# MAGIC 2. Send a few new rides (`python -m producer.event_hub_producer --count 200`).
# MAGIC 3. Run the pipeline, then query `rides.gold.dim_location` for `city_id = 2`:
# MAGIC    the old row gets an `__END_AT`, and a new current row appears.

# COMMAND ----------

dbutils.widgets.text("catalog", "rides")
dbutils.widgets.text("source_path", "/Volumes/rides/bronze/raw")
CATALOG = dbutils.widgets.get("catalog")
SOURCE_PATH = dbutils.widgets.get("source_path").rstrip("/")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import to_timestamp

updates = (
    spark.read.option("multiLine", "true")
    .json(f"{SOURCE_PATH}/updates/map_cities_update.json")
    .withColumn("updated_at", to_timestamp("updated_at"))
)

(
    DeltaTable.forName(spark, f"{CATALOG}.bronze.map_cities").alias("t")
    .merge(updates.alias("s"), "t.city_id = s.city_id")
    .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
    .whenNotMatchedInsertAll()
    .execute()
)

display(spark.table(f"{CATALOG}.bronze.map_cities").orderBy("city_id"))
