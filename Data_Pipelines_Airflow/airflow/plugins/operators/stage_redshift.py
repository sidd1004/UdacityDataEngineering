from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id,
                 aws_credentials,
                 s3_bucket,
                 s3_path,
                 json_path,
                 data_format,
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.delimiter = delimiter
        self.aws_credentials = aws_credentials
        self.data_format = data_format
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.json_path = json_path

    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        rendered_key = self.s3_path.format(**context)
        s3_path_full = f"s3://{self.s3_bucket}/{rendered_key}"
        
        self.log.info("Copying data from S3 to Redshift")

        if self.data_format == "json":
            cmd = f"COPY {self.table} FROM '{s3_path_full}' ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY" \
                f" '{credentials.secret_key}' JSON '{self.json_path}' COMPUPDATE OFF"
            redshift.run(cmd)
            
        if self.data_format == "csv":

            cmd = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' " \
                f"SECRET_ACCESS_KEY '{credentials.secret_key}' IGNOREHEADER {self.ignore_headers} " \
                f"DELIMITER '{self.delimiter}'"
            redshift.run(cmd)

            self.log.info(f"Successfully copied {self.table} from S3 to Redshift")

