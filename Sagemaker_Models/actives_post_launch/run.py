import pandas as pd
import numpy as np
import itertools as it
import os
import io
import logging

import boto3
import sys
import json

from lib.model import ModelMain

# configs
from lib.config import percent_data_process_info
from lib.config import prelaunch_process_info
from lib.config import metadata_process_info
from lib.config import default_params_dict as params_dict
from lib.config import model_name_list
from lib.config import params_tuning_dict

input_bucket = 'hbo-ingest-datascience-content-dev'

logger = logging.getLogger()
logger.info(f'Loading inputs')
data_list =[]

s3 = boto3.resource('s3')
bucket = s3.Bucket(input_bucket)
# Iterates through all the objects, doing the pagination for you. Each obj
# is an ObjectSummary, so it doesn't contain the body. You'll need to call
# get to get the whole body.
for obj in bucket.objects.filter(Prefix='input_percent_view'):
    key = obj.key
    logger.info('Loading csv file {}'.format(key))
    body = obj.get()['Body']
    var_name = key.split('.')[0].split('/')[1]
    print('Reading {0} features'.format(var_name))
    exec("{0}=pd.read_csv(body, na_values = [r'\\\\N'])".format(var_name))
    exec("{0}.columns = {0}.columns.str.lower()".format(var_name))
    
    # exclude the full null columns
    exec("{0} = {0}.loc[:,{0}.isnull().sum()!={0}.shape[0]]".format(var_name))

    # exclude the old Mortal Kombat movie because the trailer percent view 
    # matching matches the trailer of the new movie to the old movie
    # exclude Tom & Jerry due to unresolvable data issue
    exec("{0} = {0}.loc[{0}['match_id_platform'].\
        isin(['1-GYGQBcwsaCIW2XgEAAAAL', '0-GYGQBcwsaCIW2XgEAAAAL', '1-GYEb9QwLgFF9_ZwEAAAA7', '0-GYEb9QwLgFF9_ZwEAAAA7'])==False,:]\
        .reset_index(drop = True)".format(var_name))
    
    # append the feature df
    exec("data_list.append({0})".format(var_name))
    
    
bucket = s3.Bucket(input_bucket)
for obj in bucket.objects.filter(Prefix='pct_actives_prediction/pct_actives'):
    key = obj.key
    logger.info('Loading csv file {}'.format(key))
    body = obj.get()['Body']
    var_name = key.split('.')[0].split('/')[1]
    print('Reading {0} features'.format(var_name))
    exec("{0}=pd.read_csv(body, na_values = [r'\\\\N'])".format(var_name))
    exec("{0}.columns = {0}.columns.str.lower()".format(var_name))
    
    # exclude the full null columns
    exec("{0} = {0}.loc[:,{0}.isnull().sum()!={0}.shape[0]]".format(var_name))

    # exclude the old Mortal Kombat movie because the trailer percent view 
    # matching matches the trailer of the new movie to the old movie
    # exclude Tom & Jerry due to unresolvable data issue
    exec("{0} = {0}.loc[{0}['match_id'].\
        isin(['1-GYGQBcwsaCIW2XgEAAAAL', '0-GYGQBcwsaCIW2XgEAAAAL', '1-GYEb9QwLgFF9_ZwEAAAA7', '0-GYEb9QwLgFF9_ZwEAAAA7'])==False,:]\
        .reset_index(drop = True)".format(var_name))
    
    # append the feature df
    exec("data_list.append({0})".format(var_name))
    
active_data = data_list[-1][['match_id_platform', 'days_after_launch', 'pct_actives']]
active_data['pct_actives_values'] = active_data.groupby(['match_id_platform', 'days_after_launch'])['pct_actives'].transform('mean')
active_data = active_data[['match_id_platform', 'days_after_launch', 'pct_actives_values']]
active_data = active_data[(active_data['match_id_platform'].notnull())
                         &(active_data['days_after_launch'].notnull())]
active_data.drop_duplicates(inplace = True)

active_data = active_data.pivot(index='match_id_platform', columns='days_after_launch', values=['pct_actives_values']).reset_index()
active_data.columns = ['match_id_platform_actives'] + ['percent_actives_day00' + str(i) for i in range(1, 29)]

funnel_metric_feature = data_list[0]
active_data = pd.merge(funnel_metric_feature[['match_id_platform']],
               active_data, left_on = 'match_id_platform', right_on = 'match_id_platform_actives', how = 'left')
active_data.drop(['match_id_platform_actives'], axis =1, inplace = True)
data_list.pop(-1)
data_list.append(active_data)

# start a object
logger.info('Setting up the prediction model')
percentile_used = 0.8
back_consideration_date = 180
nfold = np.floor(back_consideration_date/30)
cv_func = ModelMain(data_list, metadata_process_info['label_columns'], metadata_process_info['num_columns'])

'''
Get the prediction tarjectory over length of data
'''

percent_data_process_info['exact_X_pred'] = False
output_flag = False
new_title_output = pd.DataFrame()
existing_title_output = pd.DataFrame()
back_consideration_date = 180

for day in range(-10,-6):
    # renew the percent_data_process_info data very time
    from lib.config import percent_data_process_info
    from lib.config import prelaunch_process_info
    from lib.config import metadata_process_info

    # determine prelaunch or postlaunch
    if day < 1:
        input_process_info = dict(prelaunch_process_info)
        percent_data_process_info['target_log_transformation'] = False
        percent_data_process_info['log_ratio_transformation'] = False
        input_percentile_used = percentile_used
        model_name = 'lr'
        model_name_list = [model_name]
    elif day<14:
        input_process_info = dict(metadata_process_info)
        percent_data_process_info['target_log_transformation'] = True
        percent_data_process_info['log_ratio_transformation'] = True
        input_percentile_used = percentile_used
        model_name = 'lgb'
        model_name_list = [model_name]
    else:
        input_process_info = dict(metadata_process_info)
        percent_data_process_info['target_log_transformation'] = False
        percent_data_process_info['log_ratio_transformation'] = False
        input_percentile_used = percentile_used
        model_name = 'lr'
        model_name_list = [model_name]

    # just to make the values in the dict back to the initial values
    percent_data_process_info = dict(percent_data_process_info)
    percent_data_process_info['max_num_day'] = day
    
    # get x and y
    logger.info('Get X and y for day {}'.format(day))
    cv_func.get_X_y(percent_data_process_info, 
                     input_process_info, 
                     day001_popularity_threshold = input_percentile_used)
                     
    if cv_func.pred_empty_flag == True:
        print('no title needs to be predicted at day {}'.format(day))
        continue

    # tune parameter
    if model_name not in  ['lr', 'enet']:
        logger.info('Tune parameter for day {}'.format(day))
#         print('Tune parameter for day {}'.format(day))
        cv_func.parameter_tuning(model_name, 
                            params_tuning_dict, 
                            percent_data_process_info,
                            nfold = nfold,
                            back_consideration_date = back_consideration_date)
        
        params_dict = cv_func.min_smape_param['min_smape_original']
        param_stats = cv_func.parameter_tuning_stats
        logger.info('SMAPE for all titles {}'.format(param_stats['min_smape_all']))
        logger.info('SMAPE for the originals {}'.format(param_stats['min_smape_original']))
#         print('SMAPE for all titles {}'.format(param_stats['min_smape_all']))
#         print('SMAPE for the originals {}'.format(param_stats['min_smape_original']))
    
    else:
        logger.info('Do cross prediction for day {}'.format(day))
        print('Do cross prediction for day {}'.format(day))
        cv_func.cross_prediction(
                         model_name_list, 
                         params_dict, 
                         percent_data_process_info, 
                         nfold = nfold, 
                         back_consideration_date = back_consideration_date)
        
        logger.info('SMAPE for all titles {}'.format(cv_func.output['smape_' + model_name].mean()))
        logger.info('SMAPE for the originals {}'.format(cv_func.output.loc[cv_func.output['program_type']==1,'smape_' + model_name].mean()))
#         print('SMAPE for all titles {}'.format(cv_func.output['smape_' + model_name].mean()))
#         print('SMAPE for the originals {}'.format(cv_func.output.loc[cv_func.output['program_type']==1,'smape_' + model_name].mean()))
    
    # make prediction
    logger.info('Making prediction for day {}'.format(day))
    print('Making prediction for day {}'.format(day))
    cv_func.predict_new_titles(model_name_list, 
                               params_dict, 
                               percent_data_process_info)
    
    # process the output
    cur_new_title_output = cv_func.new_title_output
    pred_column = cur_new_title_output.columns[cur_new_title_output.columns.str.contains(model_name)][0]
    cur_new_title_output['pred_day'] = day
    cur_new_title_output = cur_new_title_output.rename(columns = {pred_column:'percent_view_pred'})
    
    # process the existing titles
    cur_existing_title_output = cv_func.output
    pred_column = cur_existing_title_output.columns[cur_existing_title_output.columns.str.contains(model_name)][0]
    cur_existing_title_output['pred_day'] = day
    cur_existing_title_output = cur_existing_title_output.rename(columns = {pred_column:'percent_view_pred'})
    cur_existing_title_output = cur_existing_title_output.rename(columns = {'smape_lgb':'smape'
                                                                    ,'smape_lr':'smape'
                                                                    ,'smape_enet':'smape'
                                                                    ,'mae_lgb':'mae'
                                                                    ,'mae_lr':'mae'
                                                                    ,'mae_enet':'mae'
                                                                    })
    
    if output_flag:
        new_title_output = pd.concat([new_title_output,cur_new_title_output], axis = 0)
        existing_title_output = pd.concat([existing_title_output, cur_existing_title_output], axis = 0)
    else:
        new_title_output = cur_new_title_output
        existing_title_output = cur_existing_title_output
        output_flag = True
          
# final formatting

if new_title_output.shape[0]>0:    
    new_title_output = new_title_output.drop(columns = ['target']).sort_values(['title_name','pred_day'])
    new_title_output = new_title_output[['title_name'
                                        ,'match_id'
                                        ,'match_id_platform'
                                        ,'platform_name'
                                        ,'program_type'
                                        ,'pred_day'
                                        ,'percent_view_pred']]

if existing_title_output.shape[0]>0:           
    existing_title_output = existing_title_output.sort_values(['match_id_platform','pred_day'])
    existing_title_output['platform_name'] = existing_title_output['match_id_platform'].apply(lambda x: x[0])
    existing_title_output = existing_title_output[['title_name'
                                                ,'match_id'
                                                ,'match_id_platform'
                                                ,'platform_name'
                                                ,'program_type'
                                                ,'target'
                                                ,'pred_day'
                                                ,'percent_view_pred'
                                                ,'smape'
                                                ,'mae'
                                                ,'fold']]