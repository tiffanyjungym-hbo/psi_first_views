"""
Prediction feature extraction process, all in one
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
import boto3

SNOWFLAKE_ACCOUNT_NAME: str = Variable.get('SNOWFLAKE_ACCOUNT_NAME')  # 'hbomax.us-east-1'
CURRENT_PATH: str = pathlib.Path(__file__).parent.absolute()
STAGE: str = 'hbo-ingest-datascience-content-dev'

# Feature queries
PERCENT_VIEW: str = 'feature_extraction_funnel_metric.sql'
VTP: str = 'feature_extraction_vtp.sql'
MP_CLICK: str = 'feature_extraction_mp_click_pre_launch.sql'
CONTENT_COST: str = 'feature_extraction_content_cost.sql'
METADATA: str = 'feature_extraction_metadata.sql'
TRAILER: str = 'feature_extraction_trailer.sql'

# Input table name
TRAILER_TABLE_NAME: str = 'trailer_view_percent_test'
FUNNEL_METRICS_TABLE_NAME: str = 'funnel_metrics_table'

# Feature query group
# QUERY_LIST = [PERCENT_VIEW, VTP, MP_CLICK, CONTENT_COST, METADATA, TRAILER]
QUERY_LIST = [TRAILER]


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


def run_feature_query_list(
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
	feature_data = pd.DataFrame()

	for QUERY_NAME in QUERY_LIST:	
		logger.info(f'Loading query {QUERY_NAME}')
    		
		_query = load_query(
 				f'{CURRENT_PATH}/{QUERY_NAME}',
 				database=database,
 				schema=schema,
                		stage = STAGE,
 				trailer_table=TRAILER_TABLE_NAME,
 				funnel_metrics_table=FUNNEL_METRICS_TABLE_NAME,
 			)
 
		start_time = time.time()
 
		feature_data = execute_query(
 				query=_query,
 				database=database,
 				schema=schema,
 				warehouse=warehouse,
 				role=role,
 				snowflake_env=snowflake_env
 			)
         
        
 
		end_time = time.time()
		logger.info(f'Time taken {end_time - start_time} seconds')
		feature_data = pd.concat([feature_data, feature_data], axis=1)

	return feature_data

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

	logger.info('Extracting prediction features')
	_feature_data = run_feature_query_list(
		database=args.DATABASE,
		schema=args.SCHEMA,
		warehouse=args.WAREHOUSE,
		role=args.ROLE,
		snowflake_env=args.SNOWFLAKE_ENV
	)

	logger.info('Finished feature extraction')
