# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC Creates the Unity Catalog structure for the project: one schema per layer
# MAGIC (landing, bronze, silver, gold, ops) and the `raw_files` volume where source files land.
# MAGIC Idempotent: safe to run multiple times.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schemas

# COMMAND ----------

CATALOG = "workspace"

schemas = ["landing", "bronze", "silver", "gold", "ops"]

for schema in schemas: 
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

print(f"Schemas created or already exists in {CATALOG}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create volumes

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.raw_files")

print(f"Volume raw_files created or already exists in {CATALOG}.landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display results

# COMMAND ----------

display(spark.sql("SHOW CATALOGS"))

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.landing"))