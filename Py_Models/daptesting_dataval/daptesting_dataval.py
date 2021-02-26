import airflow
import argparse
import pandas as pd
from lib import sample
import common.snowflake_utils as su
from airflow.models import DAG, Variable

SNOWFLAKE_ACCOUNT_NAME = Variable.get('SNOWFLAKE_ACCOUNT_NAME')

parser = argparse.ArgumentParser()
parser.add_argument('--db_name', required=True)
parser.add_argument('--workspace', required=True)
args = parser.parse_args()

conn = su.connect(  SNOWFLAKE_ACCOUNT_NAME                     #'hbomax.us-east-1'
					, args.db_name
					, args.workspace
					, "MAX_DATASCIENCE_PROD"
					, "MAX_ETL_PROD"
					, 'sf_max_prod'
					, None
					)

sf_cur = conn.cursor()					
sf_cur.execute("select * from MAX_PROD.WORKSPACE.AIRFLOW_CONN_TEST limit 10;")	
df_data = pd.DataFrame(sf_cur.fetchall(), columns = [ desc[0] for desc in sf_cur.description ])	
print(df_data.head(10))	
print("Iam from Sample.",sample.temp)
