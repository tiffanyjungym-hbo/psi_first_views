import airflow
import argparse
import pandas as pd

from airflow.models import Variable
from lib import sample
from common import snowflake_utils

SNOWFLAKE_ACCOUNT_NAME = Variable.get('SNOWFLAKE_ACCOUNT_NAME')  # 'hbomax.us-east-1'

def execute_query(query: str, database, schema) -> pd.DataFrame:
	"""
	Execute a query on snowflake
	"""
	connection = snowflake_utils.connect(
		  SNOWFLAKE_ACCOUNT_NAME,
		  database,
		  schema,
		  "MAX_DATASCIENCE_PROD",
		  "MAX_ETL_PROD",
		  'sf_max_prod',
		  None
	)

	cursor = connection.cursor()					
	cursor.execute(query)	
	df = pd.DataFrame(cursor.fetchall(), columns = [desc[0] for desc in cursor.description])	

	return df
	
parser = argparse.ArgumentParser()
parser.add_argument('--db_name', required=True)
parser.add_argument('--workspace', required=True)
args = parser.parse_args()

query = "select * from MAX_PROD.WORKSPACE.AIRFLOW_CONN_TEST limit 10;"
df = execute_query(query, database=args.db_name, schema=args.workspace)

print(f'database: {args.db_name}')
print(f'schema: {args.workspace}')
print(df.head(10))
