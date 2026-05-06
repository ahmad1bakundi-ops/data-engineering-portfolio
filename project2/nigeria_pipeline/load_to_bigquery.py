import pandas as pd
from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.expanduser('~/data-engineering-portfolio/keys/gcp-key.json')

def load_to_bigquery():
    print("Loading Nigerian economic data to BigQuery...")

    client = bigquery.Client(project='olist-de-portfolio')

    # Create dataset if not exists
    dataset_id = 'nigeria_economic'
    dataset = bigquery.Dataset(f'olist-de-portfolio.{dataset_id}')
    dataset.location = 'US'
    client.create_dataset(dataset, exists_ok=True)
    print(f"✅ Dataset {dataset_id} ready")

    # Load transformed data
    df = pd.read_csv('data/nigeria_economic_transformed.csv')

    table_id = 'olist-de-portfolio.nigeria_economic.raw_indicators'
    job = client.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
    )
    job.result()
    print(f"✅ Loaded {len(df)} rows to BigQuery: {table_id}")

if __name__ == '__main__':
    load_to_bigquery()
