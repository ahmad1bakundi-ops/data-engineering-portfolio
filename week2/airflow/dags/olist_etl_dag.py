from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

DB_CONN = 'postgresql://ahmad:ahmad123@postgres-de:5432/olist_db'

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
    
    # Check 1: Null values
    null_counts = df.isnull().sum()
    if null_counts.any():
        issues.append(f"Null values found: {null_counts[null_counts > 0].to_dict()}")
    
    # Check 2: Duplicate order IDs
    duplicates = df['order_id'].duplicated().sum()
    if duplicates > 0:
        issues.append(f"Duplicate order IDs found: {duplicates}")
    
    # Check 3: Invalid order status
    valid_statuses = ['delivered', 'shipped', 'canceled', 'processing']
    invalid = df[~df['order_status'].isin(valid_statuses)]
    if len(invalid) > 0:
        issues.append(f"Invalid statuses found: {len(invalid)} rows")
    
    # Check 4: Row count
    if len(df) < 10:
        issues.append(f"Too few rows: {len(df)} (expected at least 10)")
    
    if issues:
        print("DATA QUALITY ISSUES FOUND:")
        for issue in issues:
            print(f"  ❌ {issue}")
        return 'data_quality_failed'
    else:
        print(f"✅ All quality checks passed — {len(df)} rows, no issues")
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
    df = pd.read_sql('SELECT * FROM orders', engine)
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'], format='mixed')
    df['order_delivered_timestamp'] = pd.to_datetime(df['order_delivered_timestamp'], format='mixed')
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'], format='mixed')
    df['delivery_days'] = (df['order_delivered_timestamp'] - df['order_purchase_timestamp']).dt.days
    df['is_late'] = df['order_delivered_timestamp'] > df['order_estimated_delivery_date']
    df['order_month'] = df['order_purchase_timestamp'].dt.month
    df['order_year'] = df['order_purchase_timestamp'].dt.year
    df.to_sql('orders_transformed', engine, if_exists='replace', index=False)
    print(f"✅ Transformed {len(df)} orders successfully")

def log_pipeline_success():
    engine = create_engine(DB_CONN)
    df = pd.read_sql('SELECT COUNT(*) as total FROM orders_transformed', engine)
    total = df['total'].iloc[0]
    print(f"✅ Pipeline completed successfully — {total} total orders in database")

def log_quality_failure():
    print("❌ Pipeline stopped due to data quality failure")
    print("Action required: Check /tmp/new_orders.csv for issues")
    raise ValueError("Data quality check failed — pipeline halted")

with DAG(
    dag_id='olist_etl_pipeline',
    default_args=default_args,
    description='Daily Olist ETL Pipeline with Data Quality',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['olist', 'etl', 'portfolio']
) as dag:

    task_generate = PythonOperator(
        task_id='generate_orders',
        python_callable=generate_orders
    )

    task_quality = BranchPythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
        provide_context=True
    )

    task_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    task_transform = PythonOperator(
        task_id='transform_orders',
        python_callable=transform_orders
    )

    task_success = PythonOperator(
        task_id='log_pipeline_success',
        python_callable=log_pipeline_success
    )

    task_failed = PythonOperator(
        task_id='data_quality_failed',
        python_callable=log_quality_failure
    )

    # Pipeline flow
    task_generate >> task_quality >> [task_load, task_failed]
    task_load >> task_transform >> task_success
