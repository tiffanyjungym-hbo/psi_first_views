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

# Set env variables
SNOWFLAKE_ACCOUNT_NAME: str = Variable.get('SNOWFLAKE_ACCOUNT_NAME')  # 'hbomax.us-east-1'
CURRENT_PATH: str = pathlib.Path(__file__).parent.absolute()
QUERY_TRAILER_METRICS: str = 'title_funnel_metrics_retail_trailer_update.sql'
QUERY_TRAILER_METRICS_D28: str = 'title_funnel_metrics_retail_trailer_update_d28.sql'
QUERY_TRAILER_METRICS_TIMESTAMP: str =  'title_funnel_metrics_retail_trailer_timestamp.sql'
QUERY_TRAILER_METRICS_TIMESTAMP_D28: str = 'title_funnel_metrics_retail_trailer_timestamp_d28.sql'
QUERY_FUNNEL_METRICS_TRAILER_LAST_DATE: str = 'title_funnel_metrics_retail_trailer_last_date.sql'
QUERY_FUNNEL_METRICS_TRAILER_LAST_DATE_D28: str = 'title_funnel_metrics_retail_trailer_last_date_d28.sql'
TARGET_DATE: str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

## [nday_before] since first offered
DAY_LIST: List[int] = [
	-27, -26, -25, -24, -23, -22, -21, -20, -19, -18, -17, -16, -15, -14, -13, -12, -11
	, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0
]

# Calculating for different platforms
PLATFORM_LIST: List[str] = ['hboMax', 'hboNow']
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

def update_trailer_table(
	database: str,
	schema: str,
	warehouse: str,
	role: str,
	snowflake_env: str
) -> pd.DataFrame:
	"""
	Update the numerator table for trailer metrics

	:param database: name of the database
	:param schema: name of the schema
	:param warehouse: name of the warehouse
	:param role: name of the role
	:param snowflake_env: environment used in Snowflake
	"""
	##### Calculate the trailer percent view from trailer launch date to 28 days before launch #####
	# Create latest funnel metrics
	logger.info(f'Loading query {QUERY_TRAILER_METRICS}')

	df_trailer_metrics = pd.DataFrame()
	df_trailer_metrics_d28 = pd.DataFrame()

	for platform in PLATFORM_LIST:
		# if the run date is before than last update date, then stop
		query_last_date = load_query(
			f'{CURRENT_PATH}/{QUERY_FUNNEL_METRICS_TRAILER_LAST_DATE}',
			database=database,
			schema=schema,
			nday_before=-28,
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

		logger.info(f'Last date for nth day: {-28} on {platform} is {last_date}, and end date is {END_DATE[platform]}')

		# if the run date is later than the last update date
		if ((f"'{last_date}'" >= END_DATE[platform])  &  (last_date!=None)):
			logger.info(f'Last date after/equal to end date, so skipping nth day: {-28} on {platform}')
		else:
			logger.info(f'Getting data on {platform}')
			
			start_time = time.time()

			query_trailer_metrics = load_query(
					f'{CURRENT_PATH}/{QUERY_TRAILER_METRICS}',
					database=database,
					schema=schema,
					viewership_table=VIEWERSHIP_TABLE[platform],
					end_date=END_DATE[platform],
					exist_ind_val=EXIST_IND_VAL,
					nday_before = -28
				)

			_df_trailer_metrics = execute_query(
					query=query_trailer_metrics,
					database=database,
					schema=schema,
					warehouse=warehouse,
					role=role,
					snowflake_env=snowflake_env
				)

			end_time = time.time()
			logger.info(f'Time taken {end_time - start_time} seconds')

			# insert an empty row as a record of last updated timestamp
			if _df_trailer_metrics.shape[0]==0:
				query_timestamp_row = load_query(
					f'{CURRENT_PATH}/{QUERY_TRAILER_METRICS_TIMESTAMP}',
					database=database,
					schema=schema,
					end_date=END_DATE[platform]
				)

				execute_query(
					query=query_timestamp_row,
					database=database,
					schema=schema,
					warehouse=warehouse,
					role=role,
					snowflake_env=snowflake_env
				)
				

			df_trailer_metrics = pd.concat([df_trailer_metrics, _df_trailer_metrics], axis=0)


	##### Calculate the trailer percent view from 28 days before launch to the title launch date#####
	logger.info(f'Loading query {QUERY_TRAILER_METRICS_D28}')

	df_trailer_metrics_d28 = pd.DataFrame()

	for platform in PLATFORM_LIST:
		for nday_before in DAY_LIST: 
			query_last_date = load_query(
				f'{CURRENT_PATH}/{QUERY_FUNNEL_METRICS_TRAILER_LAST_DATE_D28}',
				database=database,
				schema=schema,
				nday_before=nday_before,
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

			logger.info(f'Last date for nth day: {nday_before} on {platform} is {last_date}, and end date is {END_DATE[platform]}')

			# if the run date is later than the last update date, then stop
			if ((f"'{last_date}'" >= END_DATE[platform])  &  (last_date!=None)):
				logger.info(f'Last date after/equal to end date, so skipping nth day: {nday_before} on {platform}')
			else:
				logger.info(f'Getting data on {platform}')

				query_trailer_metrics_d28 = load_query(
						f'{CURRENT_PATH}/{QUERY_TRAILER_METRICS_D28}',
						database=database,
						schema=schema,
						viewership_table=VIEWERSHIP_TABLE[platform],
						end_date=END_DATE[platform],
						exist_ind_val=EXIST_IND_VAL,
						nday_before = nday_before
					)

				start_time = time.time()

				_df_trailer_metrics_d28 = execute_query(
						query=query_trailer_metrics_d28,
						database=database,
						schema=schema,
						warehouse=warehouse,
						role=role,
						snowflake_env=snowflake_env
					)

				end_time = time.time()
				logger.info(f'Time taken {end_time - start_time} seconds')

				# insert an empty row as a record of last updated timestamp
				if _df_trailer_metrics_d28.shape[0]==0:
					query_timestamp_row_d28= load_query(
						f'{CURRENT_PATH}/{QUERY_TRAILER_METRICS_TIMESTAMP_D28}',
						database=database,
						schema=schema,
						end_date=END_DATE[platform]
					)

					execute_query(
						query=query_timestamp_row_d28,
						database=database,
						schema=schema,
						warehouse=warehouse,
						role=role,
						snowflake_env=snowflake_env
					)
				df_trailer_metrics_d28 = pd.concat([df_trailer_metrics_d28, _df_trailer_metrics_d28], axis=0)

	return df_trailer_metrics, df_trailer_metrics_d28

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

	logger.info('Updating trailer table')

	# Run the code to get trailer metric

	df_trailer_metrics, df_trailer_metrics_d28 = update_trailer_table(
		database=args.DATABASE,
		schema=args.SCHEMA,
		warehouse=args.WAREHOUSE,
		role=args.ROLE,
		snowflake_env=args.SNOWFLAKE_ENV
	)

	logger.info('Finished trailer table updates')
