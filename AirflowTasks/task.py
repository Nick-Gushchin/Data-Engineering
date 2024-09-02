from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import pendulum

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'nyc_airbnb_etl',
    default_args=default_args,
    description='A simple ETL pipeline for NYC Airbnb data',
    schedule=timedelta(days=1),
)

# Define parameters
RAW_DATA_PATH = 'AirflowTasks/raw/AB_NYC_2019.csv'
TRANSFORMED_DATA_PATH = 'AirflowTasks/transformed/AB_NYC_2019_transformed.csv'

# Ingest Data
def ingest_data(**kwargs):
    try:
        # Check if the file exists
        if not os.path.exists(RAW_DATA_PATH):
            raise FileNotFoundError(f"File {RAW_DATA_PATH} does not exist.")
        
        # Read the data
        df = pd.read_csv(RAW_DATA_PATH)
        print(df)
        
        # Store the data in XCom for later tasks
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_dict())
    
    except Exception as e:
        print(f"Error reading the file: {e}")
        raise

ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

# Transform Data
def transform_data(**kwargs):
    try:
        # Retrieve the raw data from XCom
        raw_data = kwargs['ti'].xcom_pull(key='raw_data')
        df = pd.DataFrame.from_dict(raw_data)
        
        # Transformation steps
        df = df[df['price'] > 0]
        df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')
        df['last_review'].fillna(df['last_review'].min(), inplace=True)
        df['reviews_per_month'].fillna(0, inplace=True)
        df.dropna(subset=['latitude', 'longitude'], inplace=True)
        
        # Save the transformed data
        df.to_csv(TRANSFORMED_DATA_PATH, index=False)
    
    except Exception as e:
        print(f"Error during data transformation: {e}")
        raise

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Set task dependencies
ingest_task >> transform_task