from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        for table in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))        
            if len(records[0]) < 1 or len(records) < 1:
                raise ValueError(f"Data quality check failed. Table {table} has no results")
            num_records = records[0][0]
            if num_records == 0:
                raise ValueError("Data quality check failed. Table {} has 0 rows".format(table))
            self.log.info("Data quality check successful on table {} passed with {} records".format(table, num_records))