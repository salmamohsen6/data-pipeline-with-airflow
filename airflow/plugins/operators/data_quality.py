from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
   
    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 tables: List[str] = [],
                *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
                                                                           
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql_query = f"SELECT COUNT(*) FROM {table}"
                                                                                     
 
        for table in self.tables:
            self.log.info(f"Running data quality check on table {table}")
            records = redshift_hook.get_records(sql_query)

            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"Data quality check failed. {table} returned no results")
                                                                                                 num_records = records[0][0]
            if num_records < 1:
                self.log.error(f"Data quality check failed on {table} contained 0 rows")

            self.log.info(f"Data quality on table {table} check passed with expected number of rows")
 