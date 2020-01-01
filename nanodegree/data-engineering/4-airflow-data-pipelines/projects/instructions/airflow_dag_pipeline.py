import datetime
import logging
import pandas as pd 
import numpy as np

from airflow import DAG
from airflow.operators.python_operator import python_operator
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook 
from airflow.contrib.hooks.aws_hook import AwsHook


################### 
# Defining the task
###################
def greet():
    logging.info('Hey how is it going!')

def hello_world():
    logging.info("Hello World")

def current_time():
    logging.info(f"Current time is {datetime.datetime.utcnow().isoformat()}")

def working_dir():
    logging.info(f"Working directory is {os.getcwd()}")

def complete():
    logging.info("Congrats, your first multi-task pipeline is complete!")

def list_keys():
    hook = S3Hook(aws_conn_id = 'aws_credentials')
    bucket = Variable.get('S3_bucket')
    logging.info(f"Listing Keys from {bucket}")
    keys = hook.list_keys(bucket)
    for key in keys:
        logging.info(f" - s3://{bucket}/{key}")

def log_details(*args, **kwargs):
    logging.info(f"My execution date is {kwargs['ds']}")
    logging.info(f"My execution date is {kwargs['execution_date']}")
    logging.info(f"My run id is {kwargs[run_id]}")
    if previous_ds:
        logging.info(f'My previous run was on {previous_ds}')
    if next_ds:
        logging.info(f"My next run will be {next_ds}")

def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql.COPY_STATIONS_SQL.format(credentials.access_key, credentials.secret_key)
    redshift_hook.run(sql_stmt)

def check_greater_than_zero(*args, **kwargs):
    table = kwargs['params']['table']
    redshift_hook = PostgresHook('redshift')
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    
    if records is None or len(records[0]) < 1:
        logging.error(f"No records present in destination table {table}")
        raise ValueError(f"No records present in destination table {table}")

    logging.info(f'Data quality on table {table} check passed with {records[0][0]} records')

def load_and_analyze(*args, **kwargs):
    redshift_hook = PostgresHook('redshift')

    # Find all trips where the rider was over 80
    redshift_hook.run("""
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE brithyear > 0 AND birthyear <= 1945
       );
       COMMIT;
       """)
       records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
       """)
    if len(records) > 0 and len(records[0] > 0:
        logging.info(f'Oldest rider was born in {records[0][0]}')

    # Find all trips where the rider was under 18
    redshift_hook.run("""
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000

        );
        COMMIT;
    """)
    records = redshift_hook.get_records("""
        SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
    """)    
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f'Youngest rider was born in {record[0][0]}')


#####################
# Initalizing the DAG 
#####################
dag = DAG(
        'lesson.demo1',
        start_date = datetime.datetime.now() - datetime.timedelta(days = 60),
        schedule_interval = "@monthly")


###########
# Operators
###########
create_table = PostgresOperator(
    task_id = 'create_table'
    postgres_conn_id = 'redshift',
    sql = sql.CREATE_STATIONS_TABLE_SQL,
    dag = dag)

copy_task = PythonOperator(
    task_id = 'copy_to_redshift',
    python_callable = load_data_to_redshift,
    dag = dag)

copy_stations_task = PythonOperator(
    task_id='load_stations_from_s3_to_redshift',
    dag=dag,
    python_callable=load_station_data_to_redshift)

create_trips_table = PostgresOperator(
    task_id = 'create trips table',
    dag = dag, 
    postgres_conn_id = 'redshift',
    sql = sql.CREATE_TRIPS_TABLE_SQL)

copy_trips_task = PythonOperator(
    task_id = 'load_trips_from_s3_to_redshift',
    dag = dag,
    python_callable = load_trip_data_to_redshift,
    provide_context = True 
    # Setting an SLA
    sla = datetime.timedelta(hours = 1))

location_traffic_task = PostgresOperator(
    task_id = 'calculate_location_traffic',
    # python_callable = calculate_location_traffic,
    postgres_conn_id = 'redshift',
    # previous ds and next ds in globa variables - python format and then airflow statement
    sql = f"""
          {sql.LOCATION_TRAFFIC_SQL}
          WHERE end_time > {{{{prev_ds}}}} AND end_time < {{{{next_ds}}}} 
          """
    dag = dag)

# Refactored from function
oldest_riders_table = PostgresOperator(
    task_id = 'oldest_riders_table'
    dag = dag,
    sql = """
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <> 1945
        );
        COMMIT;
    """,
    postgres_conn_id = 'redshit' 
)


#######################################
# Assigning tasks and operators to DAGs
#######################################
greet_talk = PythonOperator(
    task_id = 'greet',
    python_callable = greet,
    dag = dag
    )

hello_world_task = PythonOperator(
    task_id = 'hello_world',
    python_callable = hello_world,
    dag = dag   
    )

current_time_task = PythonOperator(
    task_id = 'current_time',
    python_callable = current_time,
    dag = dag
    )

complete_task = PythonOperator(
    task_id = 'complete',
    python_operator = complete
    dag = dag
    )

list_task = PythonOperator(
    task_id = 'list_keys',
    python_callable = list_keys,
    dag = dag, 
    provide_context = True # Providing context for list task
    )

check_trips = PythonOperator(
    task_id = 'check_trips_data'
    dag = dag,
    python_callable = check_greater_than_zero,
    provide_context = True,
    params = {
        'table': 'trips'
            }
    )


#################################
# Configure the task dependencies
#################################
hello_world_task >> current_time_task
hello_world_task >> working_dir_task
current_time_task >> complete_task
working_dir_task >> complete_task 
create_trip_table >> copy_trips_task
copy_trips_task >> location_traffic_task


##########################
# Custom Plugins and hooks 
##########################
class HasRowsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 *args, **kwargs):

        super(HasRowsOperator, self).__init__(*args, **kwargs)
        self.table = table 
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {self.table}')
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Data quality check failed. {self.table} returned no results')
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f'Data quality check failed. {self.table} contained 0 rows')
        logging.info(f'Data quality on table {self.table} check passed with {records[0][0]} records')


class S3toRedshiftOperator(BaseOperator):
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM {}
        ACCESS_KEY_ID {}
        SECRET_ACCESS_KEY {}
        IGNOREHEADER {}
        DELIMITED {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 s3_bucket = '',
                 s3_key = '',
                 delimiter = '',
                 ignore_headings = 1,
                 *args, **kwargs):
        super(S3toRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter 
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from destination table')
        redshift.run('DELETE FROM {}'.format(self.table))

        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(**context)
        formatted_sql = S3toRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers
        )
        redshift.run(formatted_sql)


##############
# init file 
##############
from airflow.plugins_manager import AirflowPlugin
import operators

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = 'udacity_plugin',
    operators = [operators.HasRowsOperator,
                operators.S3toRedshiftOperator]




