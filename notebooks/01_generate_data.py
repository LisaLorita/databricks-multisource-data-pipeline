# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 - Generate synthetic data
# MAGIC Generates synthetic source files for systems A, B and C and writes them to the landing volume,
# MAGIC simulating batch deliveries from three different source systems.

# COMMAND ----------

import random
from datetime import date, datetime, timedelta

CATALOG = "workspace"
BASE_PATH = f"/Volumes/{CATALOG}/landing/raw_files"
BATCH = "batch_01"
SEED = 42

N_CUSTOMERS_A = 500
N_CUSTOMERS_B = 500
N_ORDERS_C = 2000

random.seed(SEED)

# COMMAND ----------

FIRST_NAMES = ["Anna", "Lisa", "John", "Charles", "Peter", "Mary", "Tom", "Victoria", "Martha", "David", "William", "Michael"]
LAST_NAMES = ["Holland", "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez"]
CITIES = ["Paris", "Madrid", "London", "Rome", "Berlin", "Tokyo", "New York", "Sydney", "Melbourne", "Toronto", "Vancouver"]

# COMMAND ----------

customers_a = []

for i in range(1, N_CUSTOMERS_A + 1):
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)                               
    signup_date = date(2023, 1, 1) + timedelta(days=random.randint(0, 900))
    updated_at = datetime(2026, 1, 1) + timedelta(minutes=random.randint(0, 60 * 24 * 30))

    customers_a.append({
        "id_cliente": f"A{i:04d}",
        "nombre": first_name,
        "apellido": last_name,                             
        "email": f"{first_name}.{last_name}{i}@email.com",
        "telefono": "6" + str(random.randint(10000000, 99999999)),
        "fecha_alta": signup_date.strftime("%d/%m/%Y"),
        "ciudad": random.choice(CITIES),
        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
    })

print(len(customers_a))
print(customers_a[0])

# COMMAND ----------

COLUMNS_A = ["id_cliente", "nombre", "apellido", "email", "telefono", "fecha_alta", "ciudad", "updated_at"]

df_a = spark.createDataFrame(customers_a).select(*COLUMNS_A)

display(df_a)
df_a.printSchema()


# COMMAND ----------

path_a = f"{BASE_PATH}/system_a/{BATCH}"

df_a.coalesce(1).write.mode("overwrite").option("header", True).csv(path_a)

df_a_check = spark.read.option("header", True).csv(path_a)
display(df_a_check.limit(5))
print(df_a_check.count())
df_a_check.printSchema()

# COMMAND ----------

display(dbutils.fs.ls(path_a))