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

# MAGIC %md
# MAGIC ## System B - Define schema

# COMMAND ----------

schema_b = StructType([
    StructField("customerId", StringType(), True),
    StructField("fullName", StringType(), True),
    StructField("contact", StructType([
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
    ]), True),
    StructField("signupDate", StringType(), True),
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True)
    ]), True),
    StructField("lastModified", StringType(), True),
])


# COMMAND ----------

path_b = f"{BASE_PATH}/system_b/{BATCH}"

df_b_raw = (
    spark.read
    .schema(schema_b)
    .json(path_b)
)



# COMMAND ----------

# MAGIC %md
# MAGIC ### System B - Add metadata columns

# COMMAND ----------

df_b_bronze = (
    df_b_raw
    .withColumn("_source_system", lit("system_b"))
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_ingestion_ts", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### System B - Write delta table

# COMMAND ----------

table_b = f"{BRONZE_SCHEMA}.system_b_customers"

df_b_bronze.write.mode("overwrite").saveAsTable(table_b)


# COMMAND ----------

# MAGIC %md
# MAGIC ### System B - Verification

# COMMAND ----------

display(spark.table(table_b))
print(spark.table(table_b).count())
spark.table(table_b).printSchema()

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {table_b}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## System C - Define schema

# COMMAND ----------

schema_c = StructType([
  StructField("order_id", StringType(), True),
  StructField("customer_ref", StringType(), True),
  StructField("order_date", StringType(), True), 
  StructField("amount", StringType(), True), 
  StructField("currency", StringType(), True),
  StructField("status", StringType(), True),
  StructField("updated_at", StringType(), True),
])

# COMMAND ----------

path_c = f"{BASE_PATH}/system_c/{BATCH}"

df_c_raw = (
    spark.read
    .option("header", True)
    .option("enforceSchema", False)
    .schema(schema_c)
    .csv(path_c)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### System C - Add metadata columns

# COMMAND ----------

df_c_bronze = (
    df_c_raw
    .withColumn("_source_system", lit("system_c"))
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_ingestion_ts", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### System C - Write delta table

# COMMAND ----------

table_c = f"{BRONZE_SCHEMA}.system_c_orders"

df_c_bronze.write.mode("overwrite").saveAsTable(table_c)

# COMMAND ----------

# MAGIC %md
# MAGIC ### System C - Verification

# COMMAND ----------

display(spark.table(table_c))
print(spark.table(table_c).count())
spark.table(table_c).printSchema()

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {table_c}"))