import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
from google.cloud.bigquery import SchemaField
import os
from os import getenv
from dotenv import load_dotenv

load_dotenv

# Define default_args for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize Airflow DAG
dag = DAG(
    'postgres_to_bigquery',
    default_args=default_args,
    description='ETL from Postgres to BigQuery',
    schedule_interval=timedelta(days=1)
)

# PostgreSQL connection setup
postgres_conn_string = 'postgresql://lawalson:lawalson@my_etl_project-ingestion-postgres-1:5432/ecommerce'
engine = create_engine(postgres_conn_string)

# BigQuery connection setup
credentials = service_account.Credentials.from_service_account_file(f"/opt/airflow/data/key/{getenv('key')}")
client = bigquery.Client(credentials=credentials, project=f'{getenv('key')}')

def execute_sql_script(file_path):
    with open(file_path, 'r') as sql_file:
        sql_script = sql_file.read()
    client.query(sql_script).result()  # Execute the SQL script
    logging.info(f"Executed SQL script: {file_path}")

def infer_bq_schema_from_df(df):
    """
    Infer BigQuery schema from the DataFrame's dtypes.
    """
    schema = []
    for column, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            field_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            field_type = "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = "TIMESTAMP"
        else:
            field_type = "STRING"
        
        schema.append(SchemaField(column, field_type))
    return schema

def load_postgres_to_bigquery(**kwargs):
    try:
        # Fetch all table names from the PostgreSQL schema
        table_names = engine.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';").fetchall()
        table_names = [name[0] for name in table_names]

        # Loop through the tables and convert them to dataframes
        for table in table_names:
            # Convert table to DataFrame
            df = pd.read_sql_table(table, con=engine)

            # Log the dataframe info to inspect column types
            logging.info(f"Loading table: {table}")
            logging.info(f"DataFrame types: {df.dtypes}")

            # Define the BigQuery table ID
            table_id = f"my-etl-project-434409.etl_dataset.{table}"

            # Define the BigQuery schema for the current table dynamically based on DataFrame dtypes
            schema = infer_bq_schema_from_df(df)

            # Fetch the existing schema from BigQuery
            try:
                bigquery_table = client.get_table(table_id)
                existing_schema = bigquery_table.schema
                existing_schema_dict = {field.name: field.field_type for field in existing_schema}
            except bigquery.exceptions.NotFound:
                existing_schema_dict = {}
                logging.info(f"Table {table_id} does not exist in BigQuery. Creating table.")

            # Create the table if it does not exist
            if not existing_schema_dict:
                job_config = bigquery.LoadJobConfig(schema=schema)
                client.create_table(bigquery.Table(table_id, schema=schema))
                logging.info(f"Created table {table_id} in BigQuery.")

            # Convert DataFrame column types to match BigQuery schema
            for column in df.columns:
                dtype = df[column].dtype
                bq_type = dict((field.name, field.field_type) for field in schema).get(column, 'STRING')

                if bq_type == 'INTEGER':
                    df[column] = pd.to_numeric(df[column], errors='coerce', downcast='integer')
                elif bq_type == 'FLOAT':
                    df[column] = pd.to_numeric(df[column], errors='coerce', downcast='float')
                elif bq_type == 'TIMESTAMP':
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                else:
                    df[column] = df[column].astype(str)
            
            # Load DataFrame to BigQuery
            job_config = bigquery.LoadJobConfig(schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete

            logging.info(f"Loaded {table} to BigQuery")

    except Exception as e:
        logging.error(f"Failed to load table {table}: {e}")
        raise

# Define SQL script file path
sql_script_path = '/opt/airflow/data/sql_script/init1.sql'

# Airflow PythonOperator to execute SQL script
init_task = PythonOperator(
    task_id='execute_sql_script',
    python_callable=execute_sql_script,
    op_kwargs={'file_path': sql_script_path},
    dag=dag
)

# Airflow PythonOperator to run the load function
load_task = PythonOperator(
    task_id='load_postgres_to_bigquery',
    python_callable=load_postgres_to_bigquery,
    dag=dag
)

# Define the DAG structure
init_task >> load_task
