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

	logger.info('Finished subscriber table updates')
