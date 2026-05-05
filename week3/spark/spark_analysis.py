from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, round, when, month, year,
    datediff, to_timestamp, desc
)

# --- Start Spark Session ---
spark = SparkSession.builder \
    .appName("Olist Analytics") \
    .config("spark.jars", "/home/ahmed_usman/data-engineering-portfolio/week3/spark/postgresql-42.7.3.jar") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark session started")
print(f"Spark version: {spark.version}")

# --- Load data from Postgres ---
print("\nLoading data from Postgres...")

jdbc_url = "jdbc:postgresql://localhost:5432/olist_db"
jdbc_props = {
    "user": "ahmad",
    "password": "ahmad123",
    "driver": "org.postgresql.Driver"
}

orders = spark.read.jdbc(jdbc_url, "orders", properties=jdbc_props)
payments = spark.read.jdbc(jdbc_url, "payments", properties=jdbc_props)
customers = spark.read.jdbc(jdbc_url, "customers", properties=jdbc_props)

print(f"Orders loaded: {orders.count()} rows")
print(f"Payments loaded: {payments.count()} rows")
print(f"Customers loaded: {customers.count()} rows")

# --- Transformations ---
print("\nRunning Spark transformations...")

# Join orders with payments and customers
df = orders.join(payments, "order_id", "left") \
           .join(customers, "customer_id", "left")

# Revenue by city
print("\n--- Revenue by City ---")
df.groupBy("customer_city") \
  .agg(
      count("order_id").alias("total_orders"),
      round(sum("payment_value"), 2).alias("total_revenue"),
      round(avg("payment_value"), 2).alias("avg_order_value")
  ) \
  .orderBy(desc("total_revenue")) \
  .show()

# Orders by status
print("--- Orders by Status ---")
df.groupBy("order_status") \
  .agg(
      count("order_id").alias("total_orders"),
      round(sum("payment_value"), 2).alias("total_revenue")
  ) \
  .orderBy(desc("total_orders")) \
  .show()

# Revenue by payment type
print("--- Revenue by Payment Type ---")
df.groupBy("payment_type") \
  .agg(
      count("order_id").alias("total_orders"),
      round(sum("payment_value"), 2).alias("total_revenue")
  ) \
  .orderBy(desc("total_revenue")) \
  .show()

print("✅ Spark analysis complete!")
spark.stop()
