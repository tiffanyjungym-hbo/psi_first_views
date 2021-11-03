"""
Post-launch ETL that generates historical %hours watched for seasons/movies/specials and uploads to tabletable
"""
import argparse
import datetime
import logging
import pathlib
import pandas as pd
import time
import os
import sys
from datetime import timedelta

from airflow.models import Variable
from common import snowflake_utils
from typing import Dict, List

# Set env variables
SNOWFLAKE_ACCOUNT_NAME: str = Variable.get('SNOWFLAKE_ACCOUNT_NAME')  # 'hbomax.us-east-1'
CURRENT_PATH: str = pathlib.Path(__file__).parent.absolute()
# QUERY_HOURS_PCT_WINDOW: str = 'hourspct_windows.sql'
# QUERY_HOURS_PCT_UPDATE: str = 'hourspct_historical_update.sql'
# TARGET_DATE: str = (datetime.datetime.today()).strftime('%Y-%m-%d')
QUERY_ACTIVES_BASE_UPDATE: str = 'actives_total_base.sql'

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

def update_actives_base_table(
    database: str,
    schema: str,
    warehouse: str,
    role: str,
    snowflake_env: str,
    ):
    """
    Update the Actives base table: actives_base_first_view
    :param database: name of the database
    :param schema: name of the schema
    :param warehouse: name of the warehouse
    :param role: name of the role
    :param snowflake_env: environment used in Snowflake
    """
    # Get unrecorded windows

    start_time = time.time()
    query_windows = '''SELECT max(start_date) as max_date
                       FROM {database}.{schema}.actives_base_first_view'''\
                    .format(database=database
                               ,schema=schema)

    start_date = execute_query(query=query_windows
                              ,database=database
                              ,schema=schema
                              ,warehouse=warehouse
                              ,role=role
                              ,snowflake_env=snowflake_env
                              )
    logger(start_date)

    max_date = pd.to_datetime(start_date.max_date.values[0]) - timedelta(days=28)
    logger ('curret_date: ' + str(start_date.max_date[0]))
    logger ('start_date: ' + str(max_date))

    query_delete_dates = '''DELETE FROM {database}.{database}.actives_base_first_view
                            WHERE start_date >= '{max_date}' '''\
                            .format(database=database
                           ,schema=schema
                           ,max_date=max_date
                              )
    logger('delete unfinished date: {}'.fomat(query_delete_dates))
#     execute_query(query=query_delete_dates
#                               ,database=database
#                               ,schema=schema
#                               ,warehouse=warehouse
#                               ,role=role
#                               ,snowflake_env=snowflake_env
#                               )
    end_date = pd.to_datetime("now")

    t=max_date

    while (t.strftime('%Y-%m-%d')>=max_date.strftime('%Y-%m-%d') \
       and t.strftime('%Y-%m-%d')<end_date.strftime('%Y-%m-%d')):
        logger.info('update actives base for date: {}'.format(t.strftime('%Y-%m-%d')))
        query_count = '''SELECT count(*) as ct FROM {}.{}.actives_base_first_view
                                    WHERE start_date = '{date}'
                      '''.format(date=t.strftime('%Y-%m-%d'),
                                 database=database,schema=schema)

        ct = execute_query(query=query_count
                              ,database=database
                              ,schema=schema
                              ,warehouse=warehouse
                              ,role=role
                              ,snowflake_env=snowflake_env
                              )

        if ct.ct[0] == 0:
            query_update = load_query(f'{CURRENT_PATH}/{QUERY_ACTIVES_BASE_UPDATE}'
                                       ,date=t.strftime('%Y-%m-%d')
                                       ,database=database
                                       ,schema=schema
                                      )
            logger.info('update actives base: {}'.format(query_update))
            execute_query(query=query_update
                              ,database=database
                              ,schema=schema
                              ,warehouse=warehouse
                              ,role=role
                              ,snowflake_env=snowflake_env
                              )
        else:
            pass
        t=t+ timedelta(days=1)

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


    update_actives_base_table(
        database=args.DATABASE,
        schema=args.SCHEMA,
        warehouse=args.WAREHOUSE,
        role=args.ROLE,
        snowflake_env=args.SNOWFLAKE_ENV,
    )



    logger.info('Finished Actives Base table updates')
