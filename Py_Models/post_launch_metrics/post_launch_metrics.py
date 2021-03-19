"""
Post-launch ETL that generates subscriber normalized interim table and final viewership KPI table
"""
import airflow
import argparse
import logging
import pandas as pd
import pathlib

from airflow.models import Variable
from common import snowflake_utils

SNOWFLAKE_ACCOUNT_NAME = Variable.get('SNOWFLAKE_ACCOUNT_NAME')  # 'hbomax.us-east-1'
QUERY_SUBSCRIBER_TABLE = 'total_sub_base_table.sql'
QUERY_METRIC_TABLE = ''
CURRENT_PATH = pathlib.Path(__file__).parent.absolute()

logger = logging.getLogger()

def execute_query(query: str, database :str, schema: str, warehouse: str, role: str, snowflake_env: str) -> pd.DataFrame:
	"""
	Execute a query on snowflake
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
	"""
	with open(filename, 'r') as f:
		query = f.read()
	
	query = query.format(**kwargs)
	return query

parser = argparse.ArgumentParser()
parser.add_argument('--SNOWFLAKE_ENV', required=True)
parser.add_argument('--WAREHOUSE', required=True)
parser.add_argument('--ROLE', required=True)
parser.add_argument('--DATABASE', required=True)
parser.add_argument('--SCHEMA', required=True)
args = parser.parse_args()

logger.info(f'Loading query {QUERY_SUBSCRIBER_TABLE}')
query_subscriber_table = load_query(f'{CURRENT_PATH}/{QUERY_SUBSCRIBER_TABLE}', database=args.DATABASE, schema=args.SCHEMA)

logger.info(f'{query_subscriber_table[:100]}')

logger.info('Executing query using environment:')
logger.info(f'snowflake env: {args.SNOWFLAKE_ENV}')
logger.info(f'warehouse: {args.WAREHOUSE}')
logger.info(f'role: {args.ROLE}')
logger.info(f'database: {args.DATABASE}')
logger.info(f'schema: {args.SCHEMA}')

df_subscriber_table = execute_query(
	query=query_subscriber_table,
	database=args.DATABASE,
	schema=args.SCHEMA,
	warehouse=args.WAREHOUSE,
	role=args.ROLE,
	snowflake_env=args.SNOWFLAKE_ENV
)
logger.info(f'Query returned shape: {df_subscriber_table.shape}')
