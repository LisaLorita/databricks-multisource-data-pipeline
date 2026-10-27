# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 - Silver orders ingestion
# MAGIC Processes and prepares system C orders from bronze into a silver Delta table.

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

from pyspark.sql.functions import col, trim, upper, to_date, to_timestamp, when

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze

# COMMAND ----------

df_c_bronze = spark.table(f"{BRONZE_SCHEMA}.system_c_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform

# COMMAND ----------

def transform_c(df):
    """Map system C bronze orders to the canonical order model."""
    return df.select(
        col("order_id"),
        col("_source_system").alias("source_system"),
        when(col("customer_ref").startswith("B-"), "system_b")
            .when(col("customer_ref").startswith("A"), "system_a")
            .otherwise(None)
            .alias("customer_source_system"),
        col("customer_ref").alias("customer_source_id"),
        to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
        col("amount").cast("decimal(10,2)").alias("amount"),
        upper(trim(col("currency"))).alias("currency"),
        upper(trim(col("status"))).alias("status"),
        to_timestamp(col("updated_at")).alias("updated_at"),
        col("_source_file"),
        col("_ingestion_ts"),
    )

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

df_orders = transform_c(df_c_bronze)
table_name = write_silver(df_orders, "orders")
print(f"{df_orders.count()} rows -> {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

orders = spark.table(f"{SILVER_SCHEMA}.orders").alias("o")
customers = spark.table(f"{SILVER_SCHEMA}.customers").alias("c")

# COMMAND ----------

duplicates = (
    orders
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
)
print("Duplicate order_ids:", duplicates.count())

# COMMAND ----------

orders.printSchema()
display(orders.limit(5))

for c in ["order_date", "amount", "updated_at", "customer_source_system"]:
    print(c, orders.filter(col(c).isNull()).count())

display(orders.groupBy("customer_source_system").count())


# COMMAND ----------

join_condition = (
    (col("o.customer_source_system") == col("c.source_system"))
    & (col("o.customer_source_id") == col("c.source_customer_id"))
)

# COMMAND ----------

orphan_orders = orders.join(customers, join_condition, "left_anti")
print("Orphan orders:", orphan_orders.count())

# COMMAND ----------

early_orders = (
    orders.join(customers, join_condition, "inner")
    .filter(col("o.order_date") < col("c.signup_date"))
)
print("Orders before customer signup:", early_orders.count())
display(early_orders.select("o.order_id", "o.order_date", "c.source_customer_id", "c.signup_date").limit(10))