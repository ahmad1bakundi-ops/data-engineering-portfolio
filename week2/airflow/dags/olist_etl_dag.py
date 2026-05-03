from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os

DB_CONN = 'postgresql://ahmad:ahmad123@postgres-de:5432/olist_db'
GCP_PROJECT = 'olist-de-portfolio'
BQ_TABLE = 'olist-de-portfolio.olist_dataset.orders_transformed'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/gcp-key.json'

default_args = {
    'owner': 'ahmad',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_orders():
    import random
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

def check_data_quality(**context):
    print("Running data quality checks...")
    df = pd.read_csv('/tmp/new_orders.csv')
    issues = []
    if df.isnull().sum().any():
        issues.append("Null values found")
    if df['order_id'].duplicated().sum() > 0:
        issues.append("Duplicate order IDs found")
    valid_statuses = ['delivered', 'shipped', 'canceled', 'processing']
    if len(df[~df['order_status'].isin(valid_statuses)]) > 0:
        issues.append("Invalid statuses found")
    if len(df) < 10:
        issues.append(f"Too few rows: {len(df)}")
    if issues:
        for issue in issues:
            print(f"❌ {issue}")
        return 'data_quality_failed'
    print(f"✅ All quality checks passed — {len(df)} rows")
    return 'load_to_postgres'

def load_to_postgres():
    print("Loading orders into Postgres...")
    engine = create_engine(DB_CONN)
    df = pd.read_csv('/tmp/new_orders.csv')
    df.to_sql('orders', engine, if_exists='append', index=False)
    print(f"✅ Loaded {len(df)} orders into Postgres")

def transform_orders():
    print("Transforming orders...")
    engine = create_engine(DB_CONN)
    orders = pd.read_sql('SELECT * FROM orders', engine)
    payments = pd.read_sql('SELECT * FROM payments', engine)
    customers = pd.read_sql('SELECT * FROM customers', engine)

    df = orders.merge(payments, on='order_id', how='left')
    df = df.merge(customers, on='customer_id', how='left')

    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'], format='mixed')
    df['order_delivered_timestamp'] = pd.to_datetime(df['order_delivered_timestamp'], format='mixed')
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'], format='mixed')
    df['delivery_days'] = (df['order_delivered_timestamp'] - df['order_purchase_timestamp']).dt.days
    df['is_late'] = df['order_delivered_timestamp'] > df['order_estimated_delivery_date']
    df['order_month'] = df['order_purchase_timestamp'].dt.month
    df['order_year'] = df['order_purchase_timestamp'].dt.year

    def categorize_value(val):
        if pd.isna(val): return 'unknown'
        if val < 100: return 'low'
        elif val < 500: return 'medium'
        else: return 'high'

    df['order_value_category'] = df['payment_value'].apply(categorize_value)
    df.to_sql('orders_transformed', engine, if_exists='replace', index=False)
    print(f"✅ Transformed {len(df)} orders successfully")

def load_to_bigquery():
    print("Loading to BigQuery...")
    from google.cloud import bigquery
    engine = create_engine(DB_CONN)
    df = pd.read_sql('SELECT * FROM orders_transformed', engine)
    client = bigquery.Client(project=GCP_PROJECT)
    job = client.load_table_from_dataframe(
        df, BQ_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
    )
    job.result()
    print(f"✅ Loaded {len(df)} rows to BigQuery")

def log_pipeline_success():
    engine = create_engine(DB_CONN)
    df = pd.read_sql('SELECT COUNT(*) as total FROM orders_transformed', engine)
    print(f"✅ Pipeline complete — {df['total'].iloc[0]} total orders in database")

def log_quality_failure():
    print("❌ Pipeline stopped due to data quality failure")
    raise ValueError("Data quality check failed")

with DAG(
    dag_id='olist_etl_pipeline',
    default_args=default_args,
    description='Daily Olist ETL Pipeline with BigQuery',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['olist', 'etl', 'portfolio', 'bigquery']
) as dag:

    task_generate = PythonOperator(task_id='generate_orders', python_callable=generate_orders)
    task_quality = BranchPythonOperator(task_id='check_data_quality', python_callable=check_data_quality, provide_context=True)
    task_load = PythonOperator(task_id='load_to_postgres', python_callable=load_to_postgres)
    task_transform = PythonOperator(task_id='transform_orders', python_callable=transform_orders)
    task_bigquery = PythonOperator(task_id='load_to_bigquery', python_callable=load_to_bigquery)
    task_success = PythonOperator(task_id='log_pipeline_success', python_callable=log_pipeline_success)
    task_failed = PythonOperator(task_id='data_quality_failed', python_callable=log_quality_failure)

    task_generate >> task_quality >> [task_load, task_failed]
    task_load >> task_transform >> task_bigquery >> task_success
