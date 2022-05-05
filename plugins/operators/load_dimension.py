from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    dim_table_insert_template = """
    INSERT INTO {table}
    {select_stmt}
    """
    
    dim_table_truncate_template = """
    TRUNCATE TABLE {table}
    """

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='',
                 select_stmt='',
                 truncate_mode=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_stmt = select_stmt
        self.truncate_mode = truncate_mode

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_mode:
            self.log.info(f'Truncate Mode selected: Truncating {self.table}')
            truncate_query = LoadDimensionOperator.dim_table_truncate_template.format(
                table=self.table
            )
            redshift_hook.run(truncate_query)
            
        final_sql_string = LoadDimensionOperator.dim_table_insert_template.format(
            table=self.table,
            select_stmt=self.select_stmt
        )
        self.log.info(f'Loading Dimension table {self.table} into Redshift')
        redshift_hook.run(final_sql_string)
        