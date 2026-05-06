from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, round, rank, desc,
    month, year, datediff, to_timestamp
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Olist Advanced Analytics") \
    .master("local[*]") \
    .config("spark.jars", "/home/ahmed_usman/data-engineering-portfolio/week3/spark/postgresql-42.7.3.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark session started")

jdbc_url = "jdbc:postgresql://localhost:5432/olist_db"
jdbc_props = {"user": "ahmad", "password": "ahmad123", "driver": "org.postgresql.Driver"}

orders = spark.read.jdbc(jdbc_url, "orders", properties=jdbc_props)
payments = spark.read.jdbc(jdbc_url, "payments", properties=jdbc_props)
customers = spark.read.jdbc(jdbc_url, "customers", properties=jdbc_props)

# Join and calculate delivery_days and month/year from timestamps
df = orders.join(payments, "order_id", "left") \
           .join(customers, "customer_id", "left") \
           .withColumn("delivery_days", datediff(
               col("order_delivered_timestamp"),
               col("order_purchase_timestamp")
           )) \
           .withColumn("order_year", year(col("order_purchase_timestamp"))) \
           .withColumn("order_month", month(col("order_purchase_timestamp")))

print(f"✅ Loaded {df.count()} rows")

# --- Window 1: Top 5 customers by spending ---
print("\n--- Top 5 Customers by Total Spending ---")
customer_window = Window.orderBy(desc("total_spent"))
df.groupBy("customer_id", "customer_city") \
  .agg(
      count("order_id").alias("total_orders"),
      round(sum("payment_value"), 2).alias("total_spent")
  ) \
  .withColumn("spending_rank", rank().over(customer_window)) \
  .filter(col("spending_rank") <= 5) \
  .show()

# --- Window 2: Monthly revenue with running total ---
print("--- Monthly Revenue with Running Total ---")
monthly_window = Window.orderBy("order_year", "order_month") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_revenue = df.groupBy("order_year", "order_month") \
    .agg(round(sum("payment_value"), 2).alias("monthly_revenue")) \
    .withColumn("running_total", round(sum("monthly_revenue").over(monthly_window), 2)) \
    .orderBy("order_year", "order_month")
monthly_revenue.show(12)

# --- Window 3: Cities ranked by revenue ---
print("--- Cities Ranked by Revenue ---")
city_window = Window.orderBy(desc("total_revenue"))
city_revenue = df.groupBy("customer_city") \
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("payment_value"), 2).alias("total_revenue"),
        round(avg("delivery_days"), 1).alias("avg_delivery_days")
    ) \
    .withColumn("revenue_rank", rank().over(city_window))
city_revenue.show()

# --- Save results to Postgres ---
print("Saving results to Postgres...")
city_revenue.write.jdbc(jdbc_url, "spark_city_analysis", mode="overwrite", properties=jdbc_props)
monthly_revenue.write.jdbc(jdbc_url, "spark_monthly_revenue", mode="overwrite", properties=jdbc_props)
print("✅ Results saved to Postgres!")

spark.stop()
print("✅ Spark advanced analysis complete!")
