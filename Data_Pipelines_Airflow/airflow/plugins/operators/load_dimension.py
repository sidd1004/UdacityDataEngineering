from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql_query = "",  
                 truncate_table = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")