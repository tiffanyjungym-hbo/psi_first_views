import airflow
import argparse
import pandas as pd

from airflow.models import Variable
from common import snowflake_utils

SNOWFLAKE_ACCOUNT_NAME = Variable.get('SNOWFLAKE_ACCOUNT_NAME')  # 'hbomax.us-east-1'

def execute_query(query: str, database :str, schema: str, warehouse: str, snowflake_env: str) -> pd.DataFrame:
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
	
parser = argparse.ArgumentParser()
parser.add_argument('--SNOWFLAKE_ENV', required=True)
parser.add_argument('--WAREHOUSE', required=True)
parser.add_argument('--ROLE', required=True)
parser.add_argument('--DATABASE', required=True)
parser.add_argument('--SCHEMA', required=True)
args = parser.parse_args()

query = "select * from MAX_PROD.WORKSPACE.AIRFLOW_CONN_TEST limit 10;"

df = execute_query(
	query=query,
	database=args.DATABASE,
	schema=args.SCHEMA,
	warehouse=args.WAREHOUSE,
	snowflake_env=args.SNOWFLAKE_ENV
)

print(f'database: {args.db_name}')
print(f'schema: {args.workspace}')
print(df.head(10))
