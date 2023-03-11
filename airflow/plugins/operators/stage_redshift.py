from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator (BaseOperator):
   
    copy_sql="""
    copy {}
    from '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    region '{}'
    json '{}'
    
    """
    
    @apply_defaults
    def __init__(self,
                 aws_credential="",
                 red_shift_conn="",
                  table="",
                  s3_key="",
                  s_bucket="",
                  region="",
                  json_path="",
                
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
       
        self.aws_credential= aws_credential
        self.red_shift_conn= red_shift_conn
        self.table=table
        self.region=region
        self.s3_key=s3_key
        self.s_bucket=s_bucket
        self.json_path=json_path
        
    def execute(self, context):
        aws_hook=AwsHook(self.aws_credential)
        credential=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.red_shift_conn)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info('StageToRedshiftOperator not implemented yet')
        
        render_key=self.s3_key.format(**context)
        s3_path="s3://{}/{}".format(self.s_bucket,render_key)
        formated_sql=StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credential.access_key,
            credential.secret_key,
            self.region,
            self.json_path
     
            
        )
        redshift.run(formated_sql)