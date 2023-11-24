from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from sqlalchemy import text
from sqlalchemy import create_engine
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import glob
import io
import os
import json
from dotenv import load_dotenv

# Construct the full path to the .env file
#dotenv_path = os.path.join('/Users/66831/DE/Projects/internet_usage', '.env')

# Load environment variables from .env file
#load_dotenv(dotenv_path)

# Postgres Config
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('POSTGRES_HOST')
db_port = int(os.getenv('POSTGRES_PORT'))
db_name = os.getenv('POSTGRES_DB_NAME')

# API path
credentials_path = os.path.abspath('/home/airflow/credentials/ServiceKey_Internetusage1.json')

# Local path where csv will be placed when export from sale platform
local_path_folder = '/Users/66831/DE/Projects/internet_usage/import_data'

# Google cloud platform path
gcs_bucket_name = 'internet_usage_project_cp'
gcs_location = 'asia-southeast1'
gcs_raw_data_folder = 'internet_usage_raw_data'
gcs_transformed_data_folder = 'internet_usage_transformed_data'

# Table name
data_table_name = 'internet_session'

def import_csv_to_postgres(local_path):

    # CSV file paths using glob will return path in list.
    csv_file_paths = glob.glob(local_path + '/*.csv')
    # Creat engine
    db_connection = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(db_connection)
    
    for csv_file_path in csv_file_paths:
        
        # Get the table name from the CSV file name
        table_name = os.path.basename(csv_file_path).split(".")[0] 

        # Read data from CSV to a pandas DataFrame
        df = pd.read_csv(csv_file_path)

        with engine.connect() as connection:
            # Create the table and insert data
            df.to_sql(table_name, connection,if_exists='replace', index=False)
            connection.commit()
        
        print(f"Table: {table_name} created on PostgreSQL table Successfully!")

def fetch_from_postgres_to_gcs(blob_raw_folder, pg_table_name, gcs_bucket_name, gcs_credentials):
    
    # Define connection to postgresql with sqlalchemy
    db_connection = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(db_connection)
    
    # Create a database connection engine using SQLAlchemy and establish a connection
    with engine.connect() as engine :
        # Query data from the PostgreSQL table and store it in a DataFrame
        # need to add text() from sqlalchemy bz sqlalchemy treats query as string
        sql = f"SELECT * FROM {pg_table_name}"
        data_df = pd.read_sql(text(sql), engine)
    
    # Write the data from the DataFrame to a CSV file based on the PostgreSQL table name
    file_name = f"{pg_table_name}.csv"
    data_df.to_csv(file_name, index=False)
    
    # Upload the CSV data to GCS bucket
    with open('/home/airflow/credentials/ServiceKey_Internetusage1.json') as source:
        info = json.load(source)
    storage_credentials = service_account.Credentials.from_service_account_info(info)
    storage_client = storage.Client(credentials=storage_credentials)
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(f"{blob_raw_folder}/{file_name}")
    blob.upload_from_filename(file_name, content_type='text/csv')
    
    print(f"{file_name} from PostgreSQL uploaded to {gcs_bucket_name} Successfully!")

def data_transform(gcs_credentials,gcs_bucket_name,table_name,blob_raw_folder,
    blob_transformed_folder):

    storage_client = storage.Client.from_service_account_json(gcs_credentials)
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(f"{blob_raw_folder}/{table_name}.csv")
    content = blob.download_as_text()
    data_df = pd.read_csv(io.StringIO(content))

    # Revise all columns to lower case
    data_df.columns = data_df.columns.str.lower()
    # Revise column name from seession_break_reason to session_break_reason
    data_df.columns = data_df.columns.str.replace('seession_break_reason', 'session_break_reason')
    # Drop NA on column
    data_df = data_df.dropna().copy()
    # Convert data column start_time from object to datetime
    data_df['start_time'] = pd.to_datetime(data_df['start_time'])
    # Convert data column usage_time to datetime and revise format
    data_df['usage_time'] = data_df['usage_time'].str.replace('00:', '', 1)
    data_df['usage_time'] = pd.to_datetime(data_df['usage_time'], format='%H:%M:%S').dt.strftime('%H:%M:%S')
    # Convert data column upload, download to float and create new column to convert data usage KB to GB
    data_df['upload'] = data_df['upload'].str.extract(r'(\d+\.\d+)').astype(float)
    data_df['download'] = data_df['download'].str.extract(r'(\d+\.\d+)').astype(float)
    data_df['upload_gb'] = (data_df['upload'] / (1024 * 1024)).round(2)
    data_df['download_gb'] = (data_df['download'] / (1024 * 1024)).round(2)
    data_df['total_transfer_gb'] = (data_df['total_transfer'] / (1024 * 1024)).round(2)   

    # Output csv
    file_name = f"{table_name}_transformed.csv"
    data_df.to_csv(file_name, index=False)

    # Upload to GCS
    blob = bucket.blob(f"{blob_transformed_folder}/{file_name}")
    blob.upload_from_filename(file_name, content_type='text/csv')

    print(f"{file_name} uploaded to {gcs_bucket_name}/{blob_transformed_folder} Successfully.")
default_args = {
    'owner': 'Chawanwit',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Set the working directory to the directory of the DAG script
os.chdir(os.path.dirname(__file__))

# Create a DAG
with DAG(
    'internet_usage_gcp_pipeline',
    default_args=default_args,
    schedule_interval='@weekly'
    )as dag:

    t1 = PythonOperator(
        task_id="csv_to_postgres",
        python_callable=import_csv_to_postgres,
        op_kwargs={"local_path":local_path_folder},
    )

    t2 = PythonOperator(
        task_id="postgres_to_gcs",
        python_callable=fetch_from_postgres_to_gcs,
        op_kwargs={
            "blob_raw_folder":gcs_raw_data_folder,
            "gcs_bucket_name":gcs_bucket_name,
            "gcs_credentials":credentials_path,
            "pg_table_name":data_table_name
            },
    )

    t3 = PythonOperator(
        task_id="data_transform",
        python_callable=data_transform,
        op_kwargs={
            "gcs_credentials":credentials_path,
            "gcs_bucket_name":gcs_bucket_name,
            "table_name":data_table_name,
            "blob_raw_folder":gcs_raw_data_folder,
            "blob_transformed_folder":gcs_transformed_data_folder,
            },
    )

    t4 = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=gcs_bucket_name,
        source_objects=[f"{gcs_transformed_data_folder}/{data_table_name}_transformed.csv"],
        destination_project_dataset_table=f"de-project-internetusage.internet_usage_datasets.{data_table_name}",
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",  # Modify based on your requirements
    )

    t1 >> t2 >> t3 >> t4