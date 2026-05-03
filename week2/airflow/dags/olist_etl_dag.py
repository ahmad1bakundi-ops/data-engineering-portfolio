from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
from sqlalchemy import create_engine

# --- Default arguments ---
default_args = {
    'owner': 'ahmad',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Task functions ---
def generate_orders():
    print("Generating new orders...")
    random.seed()
    statuses = ['delivered', 'shipped', 'canceled', 'processing']
    customers = [f'cust_{i:04d}' for i in range(1, 501)]
    
    orders = []
    for i in range(1, 101):
        orders.append({
            'order_id': f'ord_{datetime.now().strftime("%Y%m%d%H%M%S")}_{i:03d}',
            'customer_id': random.choice(customers),
            'order_status': random.choice(statuses),
            'order_purchase_timestamp': datetime.now(),
            'order_delivered_timestamp': datetime.now() + timedelta(days=random.randint(3, 30)),
            'order_estimated_delivery_date': datetime.now() + timedelta(days=random.randint(7, 25))
        })
    
    df = pd.DataFrame(orders)
    df.to_csv('/tmp/new_orders.csv', index=False)
    print(f"Generated {len(df)} new orders")

def load_to_postgres():
    print("Loading orders into Postgres...")
    engine = create_engine('postgresql://ahmad:ahmad123@172.18.0.2:5432/olist_db')
    df = pd.read_csv('/tmp/new_orders.csv')
    df.to_sql('orders', engine, if_exists='append', index=False)
    print(f"Loaded {len(df)} orders into Postgres")

def transform_orders():
    print("Transforming orders...")
    engine = create_engine('postgresql://ahmad:ahmad123@172.18.0.2:5432/olist_db')
    df = pd.read_sql('SELECT * FROM orders', engine)
    
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['order_delivered_timestamp'] = pd.to_datetime(df['order_delivered_timestamp'])
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'])
    df['delivery_days'] = (df['order_delivered_timestamp'] - df['order_purchase_timestamp']).dt.days
    df['is_late'] = df['order_delivered_timestamp'] > df['order_estimated_delivery_date']
    df['order_month'] = df['order_purchase_timestamp'].dt.month
    df['order_year'] = df['order_purchase_timestamp'].dt.year
    
    df.to_sql('orders_transformed', engine, if_exists='replace', index=False)
    print(f"Transformed {len(df)} orders successfully")

# --- DAG definition ---
with DAG(
    dag_id='olist_etl_pipeline',
    default_args=default_args,
    description='Daily Olist ETL Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['olist', 'etl', 'portfolio']
) as dag:

    task_generate = PythonOperator(
        task_id='generate_orders',
        python_callable=generate_orders
    )

    task_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    task_transform = PythonOperator(
        task_id='transform_orders',
        python_callable=transform_orders
    )

    # Pipeline order
    task_generate >> task_load >> task_transform
