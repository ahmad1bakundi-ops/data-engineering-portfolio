import pandas as pd
from sqlalchemy import create_engine

# Connection
engine = create_engine('postgresql://ahmad:ahmad123@localhost:5432/olist_db')

# Load CSV
df = pd.read_csv('olist_orders_dataset.csv')
print(f"Loaded {len(df)} rows from CSV")
print(df.head())

# Push to Postgres
df.to_sql('orders', engine, if_exists='replace', index=False)
print(f"Successfully loaded {len(df)} rows into Postgres!")
