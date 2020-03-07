import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook


class SnowflakeQueryOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 snowflake_conn_id,
                 snowflake_query,
                 s3_conn_id=None,
                 s3_bucket=None,
                 s3_key=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.snowflake_query = snowflake_query
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        logging.info('Snowflake Query Operator Starting')
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        if isinstance(self.s3_conn_id, str):
            self.execute_results_to_s3(hook=hook)
        else:
            self.execute_no_results(hook=hook)
        logging.info('Snowflake Query Operator Complete')

    def execute_results_to_s3(self, hook):
        logging.info('Executing query : {}'.format(self.snowflake_query))
        results = hook.get_pandas_df(self.snowflake_query)
        path = 's3://{}/{}'.format(self.s3_bucket, self.s3_key)
        logging.info('Writing file to : {}'.format(path))
        results.to_csv(path, index=False)
        logging.info('File written to : {}'.format(path))

    def execute_no_results(self, hook):
        logging.info('Executing query : {}'.format(self.snowflake_query))
        hook.run(self.snowflake_query)
        logging.info('Query execution complete.')