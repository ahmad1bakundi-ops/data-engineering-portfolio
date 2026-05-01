import pandas as pd
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine

random.seed(42)
engine = create_engine('postgresql://ahmad:ahmad123@localhost:5432/olist_db')

# --- Customers ---
cities = ['Lagos', 'Abuja', 'Dubai', 'London', 'Nairobi', 'Cairo', 'Accra']
states = ['LA', 'AB', 'DU', 'LN', 'NR', 'CR', 'AC']
customers = []
for i in range(1, 501):
    customers.append({
        'customer_id': f'cust_{i:04d}',
        'customer_city': random.choice(cities),
        'customer_state': random.choice(states),
        'customer_zip_code': f'{random.randint(10000, 99999)}'
    })
df_customers = pd.DataFrame(customers)
df_customers.to_sql('customers', engine, if_exists='replace', index=False)
print(f"Loaded {len(df_customers)} customers")

# --- Products ---
categories = ['electronics', 'fashion', 'food', 'furniture', 'sports', 'beauty', 'books']
products = []
for i in range(1, 201):
    products.append({
        'product_id': f'prod_{i:04d}',
        'product_category': random.choice(categories),
        'product_weight_g': random.randint(100, 5000),
        'product_length_cm': random.randint(10, 100),
        'product_height_cm': random.randint(5, 50),
        'product_width_cm': random.randint(5, 50)
    })
df_products = pd.DataFrame(products)
df_products.to_sql('products', engine, if_exists='replace', index=False)
print(f"Loaded {len(df_products)} products")

# --- Payments ---
payment_types = ['credit_card', 'debit_card', 'voucher', 'bank_transfer']
payments = []
for i in range(1, 10001):
    payments.append({
        'order_id': f'ord_{i:05d}',
        'payment_sequential': 1,
        'payment_type': random.choice(payment_types),
        'payment_installments': random.randint(1, 12),
        'payment_value': round(random.uniform(10, 5000), 2)
    })
df_payments = pd.DataFrame(payments)
df_payments.to_sql('payments', engine, if_exists='replace', index=False)
print(f"Loaded {len(df_payments)} payments")

print("All tables loaded successfully!")
