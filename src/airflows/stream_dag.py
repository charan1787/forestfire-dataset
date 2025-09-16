from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Paths inside Airflow container (mapped via docker-compose)
RAW_FILE = "/opt/airflow/data/stream/stream_data.csv"
PROCESSED_FILE = "/opt/airflow/data/processed/processed_stream.csv"

def clean_stream_data():
    if not os.path.exists(RAW_FILE):
        print(f"[DAG] No stream file found at {RAW_FILE}")
        return
    
    df = pd.read_csv(RAW_FILE)
    print(f"[DAG] Loaded {len(df)} rows")

    # Simple cleaning: drop duplicates & fill missing
    df = df.drop_duplicates()
    df = df.fillna(0)

    # Ensure numeric columns are correct type
    numeric_cols = ["Temperature","RH","Ws","Rain","FFMC","DMC","ISI","Classes","Region"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Save processed
    os.makedirs(os.path.dirname(PROCESSED_FILE), exist_ok=True)
    df.to_csv(PROCESSED_FILE, index=False)
    print(f"[DAG] Processed data saved to {PROCESSED_FILE} with {len(df)} rows")

# Define DAG
with DAG(
    dag_id="stream_cleaning_dag",
    start_date=datetime(2025, 9, 16),
    schedule_interval="@hourly",  # runs every hour
    catchup=False,
    tags=["stream","kafka","etl"]
) as dag:

    clean_task = PythonOperator(
        task_id="clean_stream_data",
        python_callable=clean_stream_data
    )

    clean_task
