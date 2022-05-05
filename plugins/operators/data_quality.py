from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 qc='',
                 redshift_conn_id='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.qc = qc
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        num_errors = 0
        failed_queries = []
        for criteria in qc:
            query = criteria['query']
            expectation = criteria['expectation']
            
            result = redshift_hook.get_records(query)[0][0]
            
            if result != expectation:
                num_errors += 1
                failed_queries.append(query)
        
        if num_errors > 0:
            self.log.info(f'The following tests failed: {failed_queries}')
            raise ValueError('Data Quality check failed')
        else:
            self.log.info('Data Quality check succeeded. No errors found!')