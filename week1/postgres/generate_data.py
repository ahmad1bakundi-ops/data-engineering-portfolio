import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

statuses = ['delivered', 'shipped', 'canceled', 'processing']
customers = [f'cust_{i:04d}' for i in range(1, 501)]

orders = []
for i in range(1, 10001):
    purchase = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
    delivered = purchase + timedelta(days=random.randint(3, 30))
    estimated = purchase + timedelta(days=random.randint(7, 25))
    orders.append({
        'order_id': f'ord_{i:05d}',
        'customer_id': random.choice(customers),
        'order_status': random.choice(statuses),
        'order_purchase_timestamp': purchase,
        'order_delivered_timestamp': delivered,
        'order_estimated_delivery_date': estimated
    })

df = pd.DataFrame(orders)
df.to_csv('olist_orders_dataset.csv', index=False)
print(f"Generated {len(df)} orders")
print(df.head())
