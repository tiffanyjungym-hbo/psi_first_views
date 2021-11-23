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
QUERY_FUNNEL_METRICS: str = 'title_retail_funnel_metrics_update.sql'
QUERY_FUNNEL_METRICS_PPRELAUNCH: str = 'title_retail_funnel_metrics_update_prelaunch.sql'
QUERY_FUNNEL_METRICS_LAST_DATE: str = 'title_retail_funnel_metrics_last_date.sql'

## [ndays] since first offered
DAY_LIST: List[int] = [
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28
]

# Calculating for different platforms
PLATFORM_LIST: List[str] = ['hboMax', 'hboNow']
DAY_LATENCY: int = 0  # started counting after [day_latency] days
TARGET_DATE: str = (datetime.datetime.today() - datetime.timedelta(days=DAY_LATENCY)).strftime('%Y-%m-%d')

# Source of viewership, either heartbeat or now_user_stream
VIEWERSHIP_TABLE: Dict[str, str] = {
	'hboMax': "'max_prod.viewership.max_user_stream_heartbeat_view'",
	'hboNow': "'max_prod.viewership.now_user_stream'"
}

# The end date of the viewership data
END_DATE: Dict[str, str] = {
	'hboNow': "'2020-05-27'",
	'hboMax': f"'{TARGET_DATE}'"
}

# indicating if a title_name - platform_name - days_since_first_offered combination exists
# in the target table
EXIST_IND_VAL: int = 0

logger = logging.getLogger()

def to_s3(filename, output_bucket, content):
		client = boto3.client('s3')
		client.put_object(Bucket=output_bucket, Key=filename, Body=content)

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
			# uf the run date is before than last update date, then stop
			query_last_date = load_query(
				f'{CURRENT_PATH}/{QUERY_FUNNEL_METRICS_LAST_DATE}',
				database=database,
				schema=schema,
				nday=nday,
				viewership_table=VIEWERSHIP_TABLE[platform]
			)

			last_date = execute_query(
				query=query_last_date,
				database=database,
				schema=schema,
				warehouse=warehouse,
				role=role,
				snowflake_env=snowflake_env
			)

			last_date = last_date.iloc[0, 0]

			logger.info(f'Last date for nth day: {nday} on {platform} is {last_date}, and end date is {END_DATE[platform]}')

			# if the run date is later than the last update date
			if ((f"'{last_date}'" >= END_DATE[platform])  &  (last_date!=None)):
				logger.info(f'Last date after/equal to end date, so skipping nth day: {nday} on {platform}')
			else:
				logger.info(f'Getting data for nth day: {nday} on {platform}')

				start_time = time.time()

				if nday == 0:
					query_funnel_metrics = load_query(
						f'{CURRENT_PATH}/{QUERY_FUNNEL_METRICS_PPRELAUNCH}',
						database=database,
						schema=schema,
						viewership_table=VIEWERSHIP_TABLE[platform],
						end_date=END_DATE[platform],
						exist_ind_val=EXIST_IND_VAL
					)

				else:
					query_funnel_metrics = load_query(
						f'{CURRENT_PATH}/{QUERY_FUNNEL_METRICS}',
						database=database,
						schema=schema,
						nday=nday,
						day_latency=DAY_LATENCY,
						viewership_table=VIEWERSHIP_TABLE[platform],
						end_date=END_DATE[platform],
						exist_ind_val=EXIST_IND_VAL
					)

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

	logger.info('Updating metrics table')
	df_funnel_metrics = update_funnel_metrics_table(
		database=args.DATABASE,
		schema=args.SCHEMA,
		warehouse=args.WAREHOUSE,
		role=args.ROLE,
		snowflake_env=args.SNOWFLAKE_ENV
	)
