from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table = "",
                 truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate


    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.truncate:
            self.log.info("Truncating table before inserting new data...")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info("Insert data from staging tables into {} fact table".format(self.table))
        
        self.log.info(f"Running sql: \n{insert_statement}")
        redshift.run("insert into {} {}".format(self.table,self.sql_help))
        self.log.info("Successfully completed insert into{}".format (self.table))