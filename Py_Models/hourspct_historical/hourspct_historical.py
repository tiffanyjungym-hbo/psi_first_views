"""
Post-launch ETL that generates historical %hours watched for seasons/movies/specials and uploads to tabletable
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
QUERY_HOURS_PCT_WINDOW: str = 'hourspct_windows.sql'
QUERY_HOURS_PCT_UPDATE: str = 'hourspct_historical_update.sql'
TARGET_DATE: str = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

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

def update_hours_pct_table(
    database: str,
    schema: str,
    warehouse: str,
    role: str,
    snowflake_env: str,
    window_days: int
    ):
    """
    Update the historical table for percent of hours viewed

    :param database: name of the database
    :param schema: name of the schema
    :param warehouse: name of the warehouse
    :param role: name of the role
    :param snowflake_env: environment used in Snowflake
    """
    # Get unrecorded windows
    logger.info(f'Getting unscored windows {QUERY_HOURS_PCT_WINDOW}')
    
    query_windows = load_query(f'{CURRENT_PATH}/{QUERY_HOURS_PCT_WINDOW}'
                               ,run_date=TARGET_DATE
                               ,window_days=window_days
                               ,database=database
                               ,schema=schema
                              )
    
    start_time = time.time()
    df_windows = execute_query(query=query_windows
                              ,database=database
                              ,schema=schema
                              ,warehouse=warehouse
                              ,role=role
                              ,showflake_env=snowflake_env
                              )
    df_windows.columns = ['catalog_match_id','match_title','hbo_offer_date','window_end']
    
    df_dates = df_windows.loc[:,['hbo_offer_date', 'window_end']].copy()
    df_dates = df_dates.drop_duplicates()
    df_dates = df_dates.set_index('hbo_offer_date')
    df_dates = df_dates.reindex(df_windows.hbo_offer_date.value_counts().index[::-1])
    df_dates = df_dates.reset_index()
    df_dates.columns = ['hbo_offer_date', 'window_end']
    
    for row in df_dates.iterrows():
        logger.info('Getting scores for {}'.format(row[1].hbo_offer_date.strftime('%Y-%m-%d')))
        update_query = load_query(f'{CURRENT_PATH}/{QUERY_HOURS_PCT_UPDATE}'
                                  ,window_start=row[1].hbo_offer_date.strftime('%Y-%m-%d')
                                  ,window_end=row[1].window_end.strftime('%Y-%m-%d')
                                  ,window_days=window_days
                                  ,database=database
                                  ,schema=schema
                                 )
        df_result = execute_query(query=update_query
                                  ,database=database
                                  ,schema=schema
                                  ,warehouse=warehouse
                                  ,role=role
                                  ,snowflake_env=snowflake_env
                                 )

    end_time = time.time()
    logger.info(f'Time taken {end_time - start_time} seconds')
    
    
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

    
    for window in [7,14,21,28,364]:
        update_hours_pct_table(
            database=args.DATABASE,
            schema=args.SCHEMA,
            warehouse=args.WAREHOUSE,
            role=args.ROLE,
            snowflake_env=args.SNOWFLAKE_ENV,
            window_days=window
        )
    
    

    logger.info('Finished trailer table updates')
