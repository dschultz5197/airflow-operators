import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mssql_hook import MsSqlHook


class MsSQLQueryOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 mssql_conn_id,
                 mssql_query,
                 s3_conn_id=None,
                 s3_bucket=None,
                 s3_key=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.mssql_query = mssql_query
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        logging.info('MS SQL Query Operator Starting')
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        if isinstance(self.s3_conn_id, str):
            self.execute_results_to_s3(hook=hook)
        else:
            self.execute_no_results(hook=hook)
        logging.info('MS SQL Query Operator Complete')

    def execute_results_to_s3(self, hook):
        logging.info('Executing query : {}'.format(self.mssql_query))
        results = hook.get_pandas_df(self.mssql_query)
        path = 's3://{}/{}'.format(self.s3_bucket, self.s3_key)
        logging.info('Writing file to : {}'.format(path))
        results.to_csv(path, index=False)
        logging.info('File written to : {}'.format(path))

    def execute_no_results(self, hook):
        # Not really needed because standard sql operator could do this, just added functionality for fun.
        logging.info('Executing query : {}'.format(self.mssql_query))
        hook.run(self.mssql_query)
        logging.info('Query execution complete.')


class MsSqlPlugin(AirflowPlugin):
    name = 'ms_sql_plugin'

    operators = [
        MsSQLQueryOperator
    ]
