from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries


AWS_KEY = AwsHook('aws_credentials').get_credentials().access_key
AWS_SECRET = AwsHook('aws_credentials').get_credentials().secret_key

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'catchup':False,
}

dag = DAG('etl_airflow_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.sql
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_events',
    access_key=AWS_KEY,
    secret_key=AWS_SECRET,
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='log_data',
    region='us-west-2',
    json_path=f"s3://{Variable.get('s3_bucket')}/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_songs',
    access_key=AWS_KEY,
    secret_key=AWS_SECRET,
    s3_bucket=Variable.get('s3_bucket'),
    s3_key='song_data',
    region='us-west-2',
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    select_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    select_stmt=SqlQueries.user_table_insert,
    truncate_mode=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    select_stmt=SqlQueries.song_table_insert,
    truncate_mode=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    select_stmt=SqlQueries.artist_table_insert,
    truncate_mode=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    select_stmt=SqlQueries.time_table_insert,
    truncate_mode=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    qc = [
        {'query':'SELECT COUNT(*) FROM public.songplays WHERE sessionid IS NULL','expectation':0},
        {'query':'SELECT COUNT(*) FROM public.users WHERE userid IS NULL','expectation':0},
        {'query':'SELECT COUNT(*) FROM public.songs WHERE song_id IS NULL','expectation':0},
        {'query':'SELECT COUNT(*) FROM public.artists WHERE artist_id IS NULL','expectation':0},
        {'query':'SELECT COUNT(*) FROM public."time" WHERE start_time IS NULL','expectation':0},
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables \
               >> [stage_events_to_redshift, stage_songs_to_redshift] \
               >> load_songplays_table \
               >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
               >> run_quality_checks \
               >> end_operator
