from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import shutil

# Paths inside Airflow container (we'll mount project "data" folder to /opt/airflow/data)
STREAM_FILE = "/opt/airflow/data/stream/stream_data.csv"
MASTER_FILE = "/opt/airflow/data/master_data_new.csv"

FEATURES = ["Temperature", "RH", "Ws", "Rain", "FFMC", "DMC","ISI","Classes", "Region", "FWI"]

def etl_task():
    if not os.path.exists(STREAM_FILE):
        print("⚠️ No stream file found, skipping ETL.")
        return

    # Read stream data
    stream_df = pd.read_csv(STREAM_FILE)

    # Make sure schema matches master dataset (skip rows missing FWI if needed)
    available_cols = [c for c in FEATURES if c in stream_df.columns]
    stream_df = stream_df[available_cols]

    # Append to master
    if os.path.exists(MASTER_FILE):
        master_df = pd.read_csv(MASTER_FILE)
        master_df = pd.concat([master_df, stream_df], ignore_index=True)
    else:
        master_df = stream_df

    master_df.to_csv(MASTER_FILE, index=False)

    # Optionally clear stream file after merge
    shutil.move(STREAM_FILE, STREAM_FILE + ".bak")

    print(f"✅ ETL complete: {len(stream_df)} new rows appended.")

# Define DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fwi_etl_dag",
    default_args=default_args,
    description="ETL DAG for merging streaming Kafka data into master dataset",
    schedule_interval="* * * * *",  # runs once per day (can adjust)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["forest_fire", "ETL"],
) as dag:

    etl = PythonOperator(
        task_id="merge_stream_to_master",
        python_callable=etl_task
    )
