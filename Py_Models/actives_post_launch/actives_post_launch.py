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
import io
import sys
import boto3
from datetime import timedelta

from airflow.models import Variable
from common import snowflake_utils
from typing import Dict, List

# Set env variables
SNOWFLAKE_ACCOUNT_NAME: str = Variable.get('SNOWFLAKE_ACCOUNT_NAME')  # 'hbomax.us-east-1'
CURRENT_PATH: str = pathlib.Path(__file__).parent.absolute()
QUERY_ACTIVES_BASE_UPDATE: str = 'actives_total_base.sql'
QUERY_ACTIVES_TITLE_QUERY = 'title_actives.sql'

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
    df.columns= df.columns.str.lower()
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

    max_date = pd.to_datetime(start_date.max_date.values[0]) - timedelta(days=38)
    logger.info('curret_date: ' + str(start_date.max_date[0]))
    logger.info('start_date: ' + str(max_date))

    query_delete_dates = '''DELETE FROM {database}.{schema}.actives_base_first_view
                            WHERE start_date >= '{max_date}' '''\
                            .format(database=database
                           ,schema=schema
                           ,max_date=max_date
                              )
    logger.info('delete unfinished date: {}'.format(query_delete_dates))
    execute_query(query=query_delete_dates
                              ,database=database
                              ,schema=schema
                              ,warehouse=warehouse
                              ,role=role
                              ,snowflake_env=snowflake_env
                              )
    end_date = pd.to_datetime("now")

    t=max_date

    while (t.strftime('%Y-%m-%d')>=max_date.strftime('%Y-%m-%d') \
       and t.strftime('%Y-%m-%d')<end_date.strftime('%Y-%m-%d')):
        logger.info('update actives base for date: {}'.format(t.strftime('%Y-%m-%d')))
        query_count = '''SELECT count(*) as ct FROM {database}.{schema}.actives_base_first_view
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


def update_pct_active_table(
    database: str,
    schema: str,
    warehouse: str,
    role: str,
    snowflake_env: str,
    ):
    # Get unrecorded windows

    start_time = time.time()
    daily_total_views_query = '''
                            select *, datediff(day, start_date, end_date)+1 as days_after_launch
                            from {database}.{schema}.actives_base_first_view
                            where days_after_launch<= 28
                            order by start_date
                            '''.format(database=database
                               ,schema=schema)

    daily_total_views = execute_query(query=daily_total_views_query
                              ,database=database
                              ,schema=schema
                              ,warehouse=warehouse
                              ,role=role
                              ,snowflake_env=snowflake_env
                              )


    title_actives_query = load_query(f'{CURRENT_PATH}/{QUERY_ACTIVES_TITLE_QUERY}')
    title_actives = execute_query(query=title_actives_query
                          ,database='max_prod'
                          ,schema=schema
                          ,warehouse=warehouse
                          ,role=role
                          ,snowflake_env=snowflake_env
                          )

    title_actives.drop_duplicates(inplace = True)
    title_actives['available_date'] = title_actives['first_release_date'].astype(str).str[0:10:1]

    pct_actives = pd.merge(title_actives[['match_id', 'title', 'title_id', 'days_on_hbo_max', 'available_date', 'cumulative_viewing_subs']],
                  daily_total_views[['start_date', 'end_date', 'cumulative_viewing_subs_denom', 'days_after_launch']],
                  left_on = ['available_date', 'days_on_hbo_max'], right_on = ['start_date', 'days_after_launch'],
                  how = 'inner')
    pct_actives['pct_actives'] = pct_actives['cumulative_viewing_subs']/pct_actives['cumulative_viewing_subs_denom']*100
    pct_actives['real_date'] = (pd.to_datetime(pct_actives['available_date']) +
                                pd.to_timedelta(pct_actives['days_on_hbo_max'], unit='D'))
    pct_actives = pct_actives[['match_id', 'title', 'days_on_hbo_max', 'pct_actives']]

    # Write to S3
    table_name = 'pct_actives_metric_values'
    output_bucket = "hbo-outbound-datascience-content-dev"
    input_bucket = 'hbo-ingest-datascience-content-dev'
    filename ='pct_actives_prediction/' + table_name + '.csv'

    csv_buffer = io.StringIO()
    pct_actives.to_csv(csv_buffer, index = False)
    content = csv_buffer.getvalue()
    client = boto3.client('s3')
    client.put_object(Bucket=output_bucket, Key=filename, Body=content)
    client.put_object(Bucket=input_bucket, Key=filename, Body=content)
    print ('Write to S3 finished')


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

    update_pct_active_table(
    database=args.DATABASE,
    schema=args.SCHEMA,
    warehouse=args.WAREHOUSE,
    role=args.ROLE,
    snowflake_env=args.SNOWFLAKE_ENV,
    )




