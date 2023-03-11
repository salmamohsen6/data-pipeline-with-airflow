import os
from airflow import conf
from airflow import DAG

from airflow.operators.subdag_operator import SubDagOperator

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import ( DataQualityOperator,LoadFactOperator)
from helpers import SqlQueries
from helping_dags import (redshift_tables,Load_Dimensions)
import datetime
from datetime import timedelta


default_args = {
    'owner': 'Salma',
    'start_date': datetime.datetime.now(),
    'end_date': None,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past':False
}

dag = DAG(
    'main_dag',
    default_args = default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',  
    end_date = None,  
    max_active_runs=4,  # 4 paralell DAGs run at a time
)

f=open('/home/workspace/airflow/create_tables.sql')
create_tables_sql = f.read()

create_tables = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift= SubDagOperator(subdag=redshift_tables('main_dag', default_args , "staging_events", "s3://udacity-dend/log_data", "s3://udacity-dend/log_json_path.json"))

stage_songs_to_redshift= SubDagOperator(subdag=redshift_tables('main_dag',default_args, 'staging_songs', "s3://udacity-dend/song_data", "auto"))


load_songplays_table = LoadFactOperator( task_id='Load_songplays_fact_table', dag=dag, redshift_conn_id="redshift", table="songplays", sql=SqlQueries.songplay_table_insert)
                                         
load_songs_table= SubDagOperator( subdag= Load_Dimensions( 'main_dag', default_args,'songs' ,SqlQueries.song_table_insert))

load_users_table= SubDagOperator( subdag= Load_Dimensions( 'main_dag',default_args, 'artists', SqlQueries.user_table_insert))

load_artists_table= SubDagOperator(subdag=Load_Dimensions( 'main_dag',default_args, 'users', SqlQueries.artist_table_insert))

load_time_table= SubDagOperator (subdag=Load_Dimensions( 'main_dag',default_args, 'time', SqlQueries.time_table_insert))

run_quality_checks = DataQualityOperator( task_id='Run_data_quality_checks', dag=dag, redshift_conn_id="redshift", tables=[ "songplays", "songs", "artists",  "time", "users"] )


end_operator = DummyOperator(task_id='stop_execution',  dag=dag)



                                 
                                 
start_operator >> create_tables >> [stage_songs_to_redshift, stage_events_to_redshift]
### 
    
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
###
    
    
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, 
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
####
    
run_quality_checks >> end_operator
### end of dag