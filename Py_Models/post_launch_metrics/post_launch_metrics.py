"""
Post-launch ETL that generates subscriber normalized interim table and final viewership KPI table
"""
import argparse
import datetime
import logging
import pathlib
import pandas as pd
import time

from airflow.models import Variable
from common import snowflake_utils
from typing import Dict, List

SNOWFLAKE_ACCOUNT_NAME: str = Variable.get('SNOWFLAKE_ACCOUNT_NAME')  # 'hbomax.us-east-1'
QUERY_SUBSCRIBER_TABLE: str = 'total_sub_base_table.sql'
CURRENT_PATH: str = pathlib.Path(__file__).parent.absolute()
QUERY_FUNNEL_METRICS: str = 'title_retail_funnel_metrics_update'
# [ndays] since first offered
DAY_LIST: List[int] = [
	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 28
]
PLATFORM_LIST: List[str] = ['hboMax', 'hboNow']
DAY_LATENCY: int = 0  # started counting after [day_latency] days
# Source of viewership, either heartbeat or now_uer_stream
STREAM_TABLE: Dict[str, str] = {
	'hboMax': "'max_prod.viewership.max_user_stream_heartbeat_view'",
	'hboNow': "'max_prod.viewership.now_user_stream'"
}
TARGET_DATE: str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
# The end date of the viewership data
END_DATE: Dict[str, str] = {
	'hboNow': "'2020-05-27'",
	'hboMax': f"'{TARGET_DATE}'"
}
# indicating if a title_name - platform_name - days_since_first_offered combination exists
# in the target table
EXIST_IND_VAL: int = 0

logger = logging.getLogger()

def execute_query(
	query: str,
	database :str,
	schema: str,
	warehouse: str,
	role: str,
	snowflake_env: str
) -> pd.DataFrame:
	"""
	Execute a query on snowflake

	:param query: query to be executed
	:param database: name of the database
	:param schema: name of the schema
	:param warehouse: name of the warehouse
	:param role: name of the role
	:param snowflake_env: environment used in Snowflake
	"""
	connection = snowflake_utils.connect(
		  SNOWFLAKE_ACCOUNT_NAME,
		  database,
		  schema,
		  warehouse,
		  role,
		  snowflake_env,
		  None
	)

	cursor = connection.cursor()
	cursor.execute(query)
	df = pd.DataFrame(cursor.fetchall(), columns = [desc[0] for desc in cursor.description])

	return df

def load_query(filename: str, **kwargs) -> str:
	"""
	Load a query from disk and fill templates parameters

	:param filename: name of the file that contains the query
	"""
	with open(filename, 'r') as f:
		query = f.read()

	query = query.format(**kwargs)
	return query

def update_subscriber_table(
	database: str,
	schema: str,
	warehouse: str,
	role: str,
	snowflake_env: str
) -> pd.DataFrame:
	"""
	Update the denominator table for content funnel metrics

	:param database: name of the database
	:param schema: name of the schema
	:param warehouse: name of the warehouse
	:param role: name of the role
	:param snowflake_env: environment used in Snowflake
	"""
	logger.info(f'Loading query {QUERY_SUBSCRIBER_TABLE}')

	query_subscriber_table = load_query(
		f'{CURRENT_PATH}/{QUERY_SUBSCRIBER_TABLE}',
		database=database,
		schema=schema
	)

	df_subscriber_table = execute_query(
		query=query_subscriber_table,
		database=database,
		schema=schema,
		warehouse=warehouse,
		role=role,
		snowflake_env=snowflake_env
	)
	logger.info(f'Query returned shape: {df_subscriber_table.shape}')

	return df_subscriber_table

def update_funnel_metrics_table(
	database: str,
	schema: str,
	warehouse: str,
	role: str,
	snowflake_env: str
) -> pd.DataFrame:
	"""
	Update the numerator table for content funnel metrics

	:param database: name of the database
	:param schema: name of the schema
	:param warehouse: name of the warehouse
	:param role: name of the role
	:param snowflake_env: environment used in Snowflake
	"""
	# Create latest funnel metrics
	logger.info(f'Loading query {QUERY_FUNNEL_METRICS}')

	df_funnel_metrics = pd.DataFrame()

	for nday in DAY_LIST:
		for platform in PLATFORM_LIST:

			logger.info(f'Getting data for nth day: {nday} on {platform}')

			query_funnel_metrics = load_query(
				f'{CURRENT_PATH}/{QUERY_FUNNEL_METRICS}',
				database=database,
				schema=schema,
				nday=nday,
				day_latency=DAY_LATENCY,
				stream_table=STREAM_TABLE[platform],
				end_date=END_DATE[platform],
				exist_ind_val=EXIST_IND_VAL
			)

			start_time = time.time()

			_df_funnel_metrics = execute_query(
				query=query_funnel_metrics,
				database=database,
				schema=schema,
				warehouse=warehouse,
				role=role,
				snowflake_env=snowflake_env
			)

			end_time = time.time()
			logger.info(f'Time taken {end_time - start_time} seconds')

			df_funnel_metrics = pd.concat([df_funnel_metrics, _df_funnel_metrics], axis=0)

	return df_funnel_metrics

if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument('--SNOWFLAKE_ENV', required=True)
	parser.add_argument('--WAREHOUSE', required=True)
	parser.add_argument('--ROLE', required=True)
	parser.add_argument('--DATABASE', required=True)
	parser.add_argument('--SCHEMA', required=True)
	args = parser.parse_args()

	logger.info('Environment defined:')
	logger.info(f'snowflake env: {args.SNOWFLAKE_ENV}')
	logger.info(f'warehouse: {args.WAREHOUSE}')
	logger.info(f'role: {args.ROLE}')
	logger.info(f'database: {args.DATABASE}')
	logger.info(f'schema: {args.SCHEMA}')

	logger.info('Updating subcriber table')
	df_subscriber_table = update_subscriber_table(
		database=args.DATABASE,
		schema=args.SCHEMA,
		warehouse=args.WAREHOUSE,
		role=args.ROLE,
		snowflake_env=args.SNOWFLAKE_ENV
	)

	logger.info('Updating metrics table')
	df_funnel_metrics = update_funnel_metrics_table(
		database=args.DATABASE,
		schema=args.SCHEMA,
		warehouse=args.WAREHOUSE,
		role=args.ROLE,
		snowflake_env=args.SNOWFLAKE_ENV
	)

	logger.info('Finished table updates')
