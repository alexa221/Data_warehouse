from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
import pandas as pd

default_args = {
   'owner': 'your_name',
   'depends_on_past': False,
   'start_date': datetime(2023, 1, 1),
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
}

dag = DAG(
   'dag_with_postgres_operator_2',
   schedule_interval='@daily',
   start_date=datetime(2023, 12, 24),
   # other parameters...
)

# SQL statement to create the car_info table
create_table_sql = """
CREATE TABLE IF NOT EXISTS car_info (
 track_id SERIAL PRIMARY KEY,
 type VARCHAR(255),
 traveled_d VARCHAR(255),
 avg_speed DOUBLE PRECISION,
 lat DOUBLE PRECISION,
 lon DOUBLE PRECISION,
 speed DOUBLE PRECISION,
 lon_acc DOUBLE PRECISION,
 lat_acc DOUBLE PRECISION,
 time DOUBLE PRECISION
);

"""

# SQL statement to copy data from CSV to the car_info table
# SQL statement to copy data from CSV to the car_info table with casting 'traveled_d' to DOUBLE PRECISION
# SQL statement to copy data from CSV to the car_info table with casting 'traveled_d' to DOUBLE PRECISION
# SQL statement to copy data from CSV to the car_info table with casting 'traveled_d' to DOUBLE PRECISION
copy_csv_to_table_sql = """
COPY car_info FROM '/docker-entrypoint-initdb.d/preprocess.csv' WITH CSV HEADER;
"""



# Preprocess the CSV file to replace invalid entries in the 'time' column
#default_timestamp = datetime.now()
#df = pd.read_csv('/docker-entrypoint-initdb.d/cleaned_data.csv')
#df['time'] = pd.to_datetime(df['time'], errors='coerce', default=default_timestamp)
#df.to_csv('/docker-entrypoint-initdb.d/cleaned_data_fixed.csv', index=False)

import_csv_task = PostgresOperator(
   task_id='import_csv_task',
   sql=[create_table_sql, copy_csv_to_table_sql],
   postgres_conn_id='postgres_localhost',
   dag=dag,
)

# Set up the DAG structure
import_csv_task
