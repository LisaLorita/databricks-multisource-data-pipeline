# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 - Bronze ingestion
# MAGIC Ingest source files from systems A, B and C to bronze tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

CATALOG = "workspace"
BASE_PATH = f"/Volumes/{CATALOG}/landing/raw_files"
BATCH = "batch_01"
BRONZE_SCHEMA = f"{CATALOG}.bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## System A - Define schema

# COMMAND ----------

schema_a = StructType([
  StructField("id_cliente", StringType(), True),
  StructField("nombre", StringType(), True),
  StructField("apellido", StringType(), True),
  StructField("email", StringType(), True),
  StructField("telefono", StringType(), True),
  StructField("fecha_alta", StringType(), True),
  StructField("ciudad", StringType(), True),
  StructField("updated_at", StringType(), True),
])

# COMMAND ----------

path_a = f"{BASE_PATH}/system_a/{BATCH}"

df_a_raw = (
    spark.read
    .option("header", True)
    .option("enforceSchema", False)
    .schema(schema_a)
    .csv(path_a)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### System A - Add metadata columns

# COMMAND ----------

df_a_bronze = (
    df_a_raw
    .withColumn("_source_system", lit("system_a"))
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_ingestion_ts", current_timestamp())
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### System A - Write delta table

# COMMAND ----------

table_a = f"{BRONZE_SCHEMA}.system_a_customers"

df_a_bronze.write.mode("overwrite").saveAsTable(table_a)


# COMMAND ----------

# MAGIC %md
# MAGIC ### System A - Verification

# COMMAND ----------

display(spark.table(table_a))
print(spark.table(table_a).count())
spark.table(table_a).printSchema()

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {table_a}"))

# COMMAND ----------

