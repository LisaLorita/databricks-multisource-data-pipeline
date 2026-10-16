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
CITY_COUNTRY = {"Paris": "FR", "Madrid": "ES", "London": "GB", "Rome": "IT", "Berlin": "DE", "Tokyo": "JP", "New York": "US", "Sydney": "AU", "Melbourne": "AU", "Toronto": "CA", "Vancouver": "CA"}
COUNTRY_CODE = {"FR": "+33", "ES": "+34", "GB": "+44", "IT": "+39", "DE": "+49", "JP": "+81", "US": "+1", "AU": "+61", "CA": "+1"}
CITIES = list(CITY_COUNTRY.keys())
ORDER_STATUS = ["PENDING", "DELIVERED", "SHIPPED", "CANCELLED"]
CURRENCY = ["EUR"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions 

# COMMAND ----------

def random_signup_date():
    """Random date between 2023-01-01 and ~mid 2025."""
    return date(2023, 1, 1) + timedelta(days=random.randint(0, 900))

def random_updated_at():
    """Random timestamp within January 2026."""
    return datetime(2026, 1, 1) + timedelta(minutes=random.randint(0, 60 * 24 * 30))

def random_order_date():
    """Random date between 2025-01-01 and 2025-12-31."""
    return date(2025, 1, 1) + timedelta(days=random.randint(0, 364))



# COMMAND ----------

# MAGIC %md
# MAGIC ## System A - Customers CSV

# COMMAND ----------

customers_a = []

for i in range(1, N_CUSTOMERS_A + 1):
    customer_id = f"A{i:04d}"
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)  
    email = f"{first_name}.{last_name}{i}@email.com"
    phone = "6" + str(random.randint(10000000, 99999999))  
    city = random.choice(CITIES)         
    signup_date = random_signup_date()
    updated_at = random_updated_at()

    customers_a.append({
        "id_cliente": customer_id,
        "nombre": first_name,
        "apellido": last_name,                             
        "email": email,
        "telefono": phone,
        "fecha_alta": signup_date.strftime("%d/%m/%Y"),
        "ciudad": city,
        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
    })

print(len(customers_a))
print(customers_a[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### System A - Dataframe 

# COMMAND ----------

COLUMNS_A = ["id_cliente", "nombre", "apellido", "email", "telefono", "fecha_alta", "ciudad", "updated_at"]

df_a = spark.createDataFrame(customers_a).select(*COLUMNS_A)

display(df_a)
df_a.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ### System A - Write & verify

# COMMAND ----------

path_a = f"{BASE_PATH}/system_a/{BATCH}"

df_a.coalesce(1).write.mode("overwrite").option("header", True).csv(path_a)

df_a_check = spark.read.option("header", True).csv(path_a)
display(df_a_check.limit(5))
print(df_a_check.count())
df_a_check.printSchema()

# COMMAND ----------

display(dbutils.fs.ls(path_a))

# COMMAND ----------

# MAGIC %md
# MAGIC ## System B - Customers JSON 

# COMMAND ----------

customers_b = []

for i in range(1, N_CUSTOMERS_B + 1):
    customer_id = f"B-{i:04d}"
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    full_name = f"{first_name} {last_name}"
    email = f"{first_name}.{last_name}{i}@email.com".lower()
    city = random.choice(CITIES)
    country = CITY_COUNTRY[city]
    phone = f"{COUNTRY_CODE[country]} {random.randint(10000000, 99999999)}"
    signup_date = random_signup_date()
    last_modified = random_updated_at()

    customers_b.append({
        "customerId": customer_id,
        "fullName": full_name, 
        "contact": {
            "email": email,             
            "phone": phone,                                     
        },
        "signupDate": signup_date.isoformat(),
        "address": {
            "city": city,
            "country": country,
        },
        "lastModified": last_modified.isoformat(),
    })

print(len(customers_b))
print(customers_b[0])


# COMMAND ----------

# MAGIC %md
# MAGIC ### System B - Dataframe 

# COMMAND ----------

COLUMNS_B = ["customerId", "fullName", "contact", "signupDate", "address", "lastModified"]

df_b = spark.createDataFrame(customers_b).select(*COLUMNS_B)

display(df_b)
df_b.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### System B - Write & verify

# COMMAND ----------

path_b = f"{BASE_PATH}/system_b/{BATCH}"

df_b.coalesce(1).write.mode("overwrite").json(path_b)

df_b_check = spark.read.json(path_b)
display(df_b_check.limit(5))
print(df_b_check.count())
df_b_check.printSchema()

# COMMAND ----------

display(dbutils.fs.ls(path_b))

# COMMAND ----------

# MAGIC %md
# MAGIC ## System C - Orders

# COMMAND ----------

customer_ids_a = [customer["id_cliente"] for customer in customers_a]
customer_ids_b = [customer["customerId"] for customer in customers_b]
all_customer_ids = customer_ids_a + customer_ids_b


# COMMAND ----------

orders_c = []

for i in range(1, N_ORDERS_C + 1):
    order_id = f"ORD-{i:06d}"
    customer_ref = random.choice(all_customer_ids)
    order_date = random_order_date()
    amount = round(random.uniform(5, 300), 2)
    currency = random.choice(CURRENCY)
    status = random.choice(ORDER_STATUS)
    updated_at = random_updated_at()

    orders_c.append({
        "order_id": order_id, 
        "customer_ref": customer_ref, 
        "order_date": order_date.isoformat(), 
        "amount": amount, 
        "currency": currency, 
        "status": status,
        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
    })

print(len(orders_c))
print(orders_c[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### System C - Dataframe

# COMMAND ----------

COLUMNS_C = ["order_id", "customer_ref", "order_date", "amount", "currency", "status", "updated_at"]

df_c = spark.createDataFrame(orders_c).select(*COLUMNS_C)

display(df_c)
df_c.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### System C - Write & verify

# COMMAND ----------

path_c = f"{BASE_PATH}/system_c/{BATCH}"

df_c.coalesce(1).write.mode("overwrite").option("header", True).csv(path_c)

df_c_check = spark.read.option("header", True).csv(path_c)
display(df_c_check.limit(5))
print(df_c_check.count())
df_c_check.printSchema()

# COMMAND ----------

display(dbutils.fs.ls(path_c))