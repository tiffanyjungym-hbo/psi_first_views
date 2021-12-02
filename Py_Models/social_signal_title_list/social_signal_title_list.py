"""
Post-launch ETL that generates subscriber normalized interim table and final viewership KPI table
"""
import argparse
import datetime
import logging
import pathlib
import pandas as pd
import time
from io import StringIO # python3; python2: BytesIO 
import boto3

from airflow.models import Variable
from common import snowflake_utils
from typing import Dict, List

SNOWFLAKE_ACCOUNT_NAME: str = Variable.get('SNOWFLAKE_ACCOUNT_NAME')  # 'hbomax.us-east-1'
CURRENT_PATH: str = pathlib.Path(__file__).parent.absolute()
QUERY_SOCIAL_SIGNAL_TITLES: str = 'social_signal_title_list.sql'

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

def update_social_signal_title_list_table(
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
	logger.info(f'Loading query social signal titles')

	start_time = time.time()

	df_social_signal_titles_query = load_query(
		f'{CURRENT_PATH}/{QUERY_SOCIAL_SIGNAL_TITLES}',
		database=database,
		schema=schema
	)

	df_social_signal_titles = pd.DataFrame()

	df_social_signal_titles = execute_query(
				query=df_social_signal_titles_query,
				database=database,
				schema=schema,
				warehouse=warehouse,
				role=role,
				snowflake_env=snowflake_env
			)
	end_time = time.time()
	logger.info(f'Time taken {end_time - start_time} seconds')

	return df_social_signal_titles

if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument('--SNOWFLAKE_ENV', required=True)
	parser.add_argument('--WAREHOUSE', required=True)
	parser.add_argument('--ROLE', required=True)
	parser.add_argument('--DATABASE', required=True)
	parser.add_argument('--SCHEMA', required=True)
	parser.add_argument('--input_bucket', required=True)
	args = parser.parse_args()

	logger.info('Environment defined:')
	logger.info(f'snowflake env: {args.SNOWFLAKE_ENV}')
	logger.info(f'warehouse: {args.WAREHOUSE}')
	logger.info(f'role: {args.ROLE}')
	logger.info(f'database: {args.DATABASE}')
	logger.info(f'schema: {args.SCHEMA}')
	logger.info(f'input_bucket: {args.input_bucket}')

	df_social_signal_titles = update_social_signal_title_list_table(
		database=args.DATABASE,
		schema=args.SCHEMA,
		warehouse=args.WAREHOUSE,
		role=args.ROLE,
		snowflake_env=args.SNOWFLAKE_ENV
	)

	## save the list of titles to wikipedia page view & google index input table
	logger.info('Writing titles list for wikipedia page view & google index')
	
	# bucket name
	output_bucket = args.input_bucket

	# content
	csv_buffer = StringIO()
	df_social_signal_titles.to_csv(csv_buffer, index = False)
	content = csv_buffer.getvalue()

	# file path
	filename = 'google-search-tracker/input/titles/mei_cheng.csv'

	to_s3(filename, output_bucket, content)
