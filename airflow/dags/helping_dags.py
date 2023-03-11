from datetime import datetime, timedelta
import os
from airflow import conf
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueries

def redshift_tables(parent_dag_name, args, table, S3,json_path) -> DAG:
    dag=  DAG( f"{parent_dag_name}.{table}",
             start_date = datetime.now(),
             default_args = args)
    
    stage_to_redshift = StageToRedshiftOperator(
        task_id=table,
        dag=dag,    
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",    
        table = table,
        #s3_bucket="udacity-dend"
        s3_path = S3,
        json_path=json_path
    )

    return redshift_tables 


def Load_Dimensions(parent_dag_name,args, table, Query) -> DAG :
    
    dag=  DAG(f"{parent_dag_name}.{table}",
             start_date = datetime.now(),
             default_args = args)
    
    dim= LoadDimensionOperator(
        task_id=f"Load_{table}",
        dag=dag,   
        redshift_conn_id="redshift",
        table=table,
        sql=Query)
    
    return dim