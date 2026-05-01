import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://ahmad:ahmad123@localhost:5432/olist_db')

print("Loading data from Postgres...")
orders = pd.read_sql('SELECT * FROM orders', engine)
payments = pd.read_sql('SELECT * FROM payments', engine)
customers = pd.read_sql('SELECT * FROM customers', engine)

print(f"Orders loaded: {len(orders)} rows")

# --- Transformations ---

# 1. Convert timestamps
orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
orders['order_delivered_timestamp'] = pd.to_datetime(orders['order_delivered_timestamp'])
orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])

# 2. Delivery time in days
orders['delivery_days'] = (
    orders['order_delivered_timestamp'] - orders['order_purchase_timestamp']
).dt.days

# 3. Late delivery flag
orders['is_late'] = orders['order_delivered_timestamp'] > orders['order_estimated_delivery_date']

# 4. Extract month and year
orders['order_month'] = orders['order_purchase_timestamp'].dt.month
orders['order_year'] = orders['order_purchase_timestamp'].dt.year

# 5. Join with payments
df = orders.merge(payments, on='order_id', how='left')

# 6. Categorize order value
def categorize_value(val):
    if val < 100:
        return 'low'
    elif val < 500:
        return 'medium'
    else:
        return 'high'

df['order_value_category'] = df['payment_value'].apply(categorize_value)

# 7. Join with customers
df = df.merge(customers, on='customer_id', how='left')

print("\n--- Transformation Summary ---")
print(f"Total orders: {len(df)}")
print(f"Late deliveries: {df['is_late'].sum()} ({df['is_late'].mean()*100:.1f}%)")
print(f"Avg delivery days: {df['delivery_days'].mean():.1f}")
print(f"\nOrder value breakdown:")
print(df['order_value_category'].value_counts())
print(f"\nOrders by city:")
print(df['customer_city'].value_counts())

# 8. Save transformed data back to Postgres
df.to_sql('orders_transformed', engine, if_exists='replace', index=False)
print(f"\nTransformed table saved to Postgres: {len(df)} rows")
