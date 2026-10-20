# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 - Bronze ingestion
# MAGIC Ingests the source files of systems A, B and C from the landing volume into bronze Delta tables.
# MAGIC Data is stored as received (all columns as string) plus ingestion metadata columns.

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
# MAGIC ## Schemas
# MAGIC Explicit schemas for each source system. All fields are strings: typing happens in silver.

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
        StructField("country", StringType(), True),
    ]), True),
    StructField("lastModified", StringType(), True),
])

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

# MAGIC %md
# MAGIC ## Sources config
# MAGIC One entry per source system. Adding a new system = adding a new entry here.

# COMMAND ----------

SOURCES = {
    "system_a": {
        "format": "csv",
        "options": {"header": "true", "enforceSchema": "false"},
        "schema": schema_a,
        "table": "system_a_customers",
    },
    "system_b": {
        "format": "json",
        "options": {},
        "schema": schema_b,
        "table": "system_b_customers",
    },
    "system_c": {
        "format": "csv",
        "options": {"header": "true", "enforceSchema": "false"},
        "schema": schema_c,
        "table": "system_c_orders",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def read_source(system, config):
    """Read the raw files of a source system using its explicit schema."""
    path = f"{BASE_PATH}/{system}/{BATCH}"
    return (
        spark.read
        .format(config["format"])
        .options(**config["options"])
        .schema(config["schema"])
        .load(path)
    )

def add_metadata(df, system):
    """Add ingestion metadata columns to a raw DataFrame."""
    return (
        df
        .withColumn("_source_system", lit(system))
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingestion_ts", current_timestamp())
    )

def write_bronze(df, table):
    """Write a DataFrame to a bronze Delta table (full overwrite)."""
    full_table_name = f"{BRONZE_SCHEMA}.{table}"
    df.write.mode("overwrite").saveAsTable(full_table_name)
    return full_table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run ingestion

# COMMAND ----------

for system, config in SOURCES.items():
    df_raw = read_source(system, config)
    df_bronze = add_metadata(df_raw, system)
    table_name = write_bronze(df_bronze, config["table"])
    print(f"{system}: {spark.table(table_name).count()} rows -> {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

for config in SOURCES.values():
    table_name = f"{BRONZE_SCHEMA}.{config['table']}"
    print(table_name)
    spark.table(table_name).printSchema()
    display(spark.table(table_name).limit(5))

# COMMAND ----------

for config in SOURCES.values():
    table_name = f"{BRONZE_SCHEMA}.{config['table']}"
    display(spark.sql(f"DESCRIBE HISTORY {table_name}"))
