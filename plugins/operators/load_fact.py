from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    fact_table_sql_template = """
    INSERT INTO {table}
    {select_stmt}
    """

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='',
                 select_stmt='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_stmt = select_stmt

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        final_sql_string = LoadFactOperator.fact_table_sql_template.format(
            table=self.table,
            select_stmt=self.select_stmt
        )
        self.log.info(f'Loading Fact table {self.table} into Redshift')
        redshift_hook.run(final_sql_string)