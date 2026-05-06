from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/ahmed_usman/data-engineering-portfolio/keys/gcp-key.json'

default_args = {
    'owner': 'ahmad',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_world_bank_data():
    import wbgapi as wb
    print("Fetching Nigerian economic data from World Bank API...")

    indicators = {
        'NY.GDP.MKTP.KD.ZG': 'gdp_growth_pct',
        'FP.CPI.TOTL.ZG': 'inflation_rate_pct',
        'SL.UEM.TOTL.ZS': 'unemployment_rate_pct',
        'NY.GDP.PCAP.CD': 'gdp_per_capita_usd',
        'BX.KLT.DINV.WD.GD.ZS': 'foreign_direct_investment_pct',
    }

    dfs = []
    for code, name in indicators.items():
        try:
            df = wb.data.DataFrame(code, 'NGA', mrv=15)
            df = df.T.reset_index()
            df.columns = ['year', name]
            df['year'] = df['year'].str.replace('YR', '').astype(int)
            dfs.append(df.set_index('year'))
        except Exception as e:
            print(f"Warning: {name} failed: {e}")

    combined = pd.concat(dfs, axis=1).reset_index()
    combined['country'] = 'Nigeria'
    combined['country_code'] = 'NGA'
    combined['ingested_at'] = datetime.now().isoformat()

    os.makedirs('/tmp/nigeria', exist_ok=True)
    combined.to_csv('/tmp/nigeria/raw.csv', index=False)
    print(f"✅ Ingested {len(combined)} rows")

def transform_data():
    print("Transforming data...")
    df = pd.read_csv('/tmp/nigeria/raw.csv')
    df = df.dropna(subset=['gdp_growth_pct', 'inflation_rate_pct'], how='all')
    df = df[df['year'] >= 2010].sort_values('year').reset_index(drop=True)

    def classify_economy(row):
        if pd.isna(row['gdp_growth_pct']): return 'Unknown'
        if row['gdp_growth_pct'] > 5: return 'High Growth'
        elif row['gdp_growth_pct'] > 2: return 'Moderate Growth'
        elif row['gdp_growth_pct'] > 0: return 'Slow Growth'
        else: return 'Recession'

    def classify_inflation(val):
        if pd.isna(val): return 'Unknown'
        if val < 10: return 'Low'
        elif val < 20: return 'Moderate'
        elif val < 30: return 'High'
        else: return 'Crisis'

    df['economic_period'] = df.apply(classify_economy, axis=1)
    df['inflation_severity'] = df['inflation_rate_pct'].apply(classify_inflation)
    df['gdp_yoy_change'] = df['gdp_growth_pct'].diff().round(2)
    df['hardship_index'] = (df['inflation_rate_pct'] - df['gdp_growth_pct']).round(2)

    df.to_csv('/tmp/nigeria/transformed.csv', index=False)
    print(f"✅ Transformed {len(df)} rows")

def load_to_bigquery():
    from google.cloud import bigquery
    print("Loading to BigQuery...")
    df = pd.read_csv('/tmp/nigeria/transformed.csv')
    client = bigquery.Client(project='olist-de-portfolio')
    dataset = bigquery.Dataset('olist-de-portfolio.nigeria_economic')
    dataset.location = 'US'
    client.create_dataset(dataset, exists_ok=True)
    job = client.load_table_from_dataframe(
        df, 'olist-de-portfolio.nigeria_economic.raw_indicators',
        job_config=bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
    )
    job.result()
    print(f"✅ Loaded {len(df)} rows to BigQuery")

with DAG(
    dag_id='nigeria_economic_pipeline',
    default_args=default_args,
    description='Weekly Nigerian Economic Data Pipeline',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nigeria', 'economics', 'worldbank', 'portfolio']
) as dag:

    task_ingest = PythonOperator(task_id='ingest_world_bank_data', python_callable=ingest_world_bank_data)
    task_transform = PythonOperator(task_id='transform_data', python_callable=transform_data)
    task_load = PythonOperator(task_id='load_to_bigquery', python_callable=load_to_bigquery)

    task_ingest >> task_transform >> task_load
