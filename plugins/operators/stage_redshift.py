from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    staging_sql_template = """
    COPY {table}
    FROM '{s3_source}'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    REGION AS '{region}'
    JSON '{path_file}'
    """

    @apply_defaults
    def __init__(self,
                 # Defining operators params (with defaults)
                 redshift_conn_id='',
                 table='',
                 access_key='',
                 secret_key='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.access_key = access_key
        self.secret_key = secret_key
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path
        

    def execute(self, context):
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        final_sql_string = StageToRedshiftOperator.staging_sql_template.format(
            table = self.table,
            s3_source = self.s3_path,
            access_key = self.access_key,
            secret_key = self.secret_key,
            region = self.region,
            path_file = self.json_path
        )
        
        self.log.info(f'Copying data from {self.s3_path} to {self.table} ')
        redshift_hook.run(final_sql_string)
        



