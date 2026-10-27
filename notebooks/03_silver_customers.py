# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Silver customers
# MAGIC Process and prepares data from the bronze tables of systems A and B into silver Delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

CATALOG = "workspace"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import col, lit, trim, lower, to_date, to_timestamp, split

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read bronze

# COMMAND ----------

df_a_bronze = spark.table(f"{BRONZE_SCHEMA}.system_a_customers")
df_b_bronze = spark.table(f"{BRONZE_SCHEMA}.system_b_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform System A

# COMMAND ----------

def transform_a(df):
    """Map system A bronze customers to the canonical customer model."""
    return df.select(
        col("_source_system").alias("source_system"),
        col("id_cliente").alias("source_customer_id"),
        col("nombre").alias("first_name"),
        col("apellido").alias("last_name"),
        lower(trim(col("email"))).alias("email"),
        lit(None).cast("string").alias("phone_country_code"),
        col("telefono").alias("phone_number"),
        to_date(col("fecha_alta"), "dd/MM/yyyy").alias("signup_date"),
        col("ciudad").alias("city"),
        lit(None).cast("string").alias("country"),
        to_timestamp(col("updated_at")).alias("updated_at"),
        col("_source_file"),
        col("_ingestion_ts"),
    )

# COMMAND ----------

df_a_silver = transform_a(df_a_bronze)
display(df_a_silver)
df_a_silver.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform System B

# COMMAND ----------

def transform_b(df):
    """Map system B bronze customers to the canonical customer model."""
    return df.select(
        col("_source_system").alias("source_system"),
        col("customerId").alias("source_customer_id"),
        split(col("fullName"), " ").getItem(0).alias("first_name"),
        split(col("fullName"), " ").getItem(1).alias("last_name"),
        lower(trim(col("contact.email"))).alias("email"),
        split(col("contact.phone"), " ").getItem(0).alias("phone_country_code"),
        split(col("contact.phone"), " ").getItem(1).alias("phone_number"),
        to_date(col("signupDate"), "yyyy-MM-dd").alias("signup_date"),
        col("address.city").alias("city"),
        col("address.country").alias("country"),
        to_timestamp(col("lastModified")).alias("updated_at"),
        col("_source_file"),
        col("_ingestion_ts"),
    )


# COMMAND ----------

df_b_silver = transform_b(df_b_bronze)
display(df_b_silver)
df_b_silver.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Union

# COMMAND ----------

df_customers = df_a_silver.unionByName(df_b_silver)
print(df_customers.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver

# COMMAND ----------

def write_silver(df, table):
    """Write a DataFrame to silver Delta table (full overwrite)."""
    full_table_name = f"{SILVER_SCHEMA}.{table}"
    df.write.mode("overwrite").saveAsTable(full_table_name)
    return full_table_name

# COMMAND ----------

table_name = write_silver(df_customers, "customers")
print(f"{df_customers.count()} rows -> {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

duplicates = (
    spark.table(f"{SILVER_SCHEMA}.customers")
    .groupBy("source_system", "source_customer_id")
    .count()
    .filter(col("count") > 1)
)
print(duplicates.count())