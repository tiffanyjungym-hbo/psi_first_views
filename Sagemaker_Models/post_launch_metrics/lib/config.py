"""
"""

model_name_list = ['lgb']

percent_data_process_info ={
    'target_log_transformation':True,
    'log_ratio_transformation': True, # in effect only if target_log_transformation = True
    'target_sigmoid_transformation':False, # in effect only if log_ratio_transformation = True
    'raw_log_feature':True,
    'cumulative_media_cost':True,
    'last_season_percents': False,
    'max_num_day': 14,
    'total_num_day_data':21,
    'exact_X_pred': False,
    'growth_trend_num':0,
    'linear_pred_as_feature':False,
    'all_none_zero':False,
    'percentile_used':0.8,
    'nfold':6,
    'prelaucn_monotonic_features':[
          'wiki_d28_selected'
        , 'trailer_metric_d28_selected'
    ]
}

prelaunch_process_info = {
    'target_col':'day028_percent_viewed',
    'day_column_keywords':
        [],
    'keywords':
        # flags are usually included in the tags, so excluded here
        [
             'tag'
            ,'licensor'
            ,'dayofweek_earliest_date'
        ],
    'other_col':
        [
            'content_category'
          , 'platform_name'
          , 'program_type'
          , 'title_age_approx'
          , 'trailer_metric_d28_selected'
          , 'trailer_metric_d28_cumday_selected'
          , 'trailer_metric_before28_cumday'
          , 'trailer_metric_before28'
          , 'wiki_befored28_total'
          , 'wiki_d28_selected'
          , 'wiki_view_total'
                 ],
    'prelaunch_spec_process':[
         'trailer_metric_d28'
         ,'wiki_d28'
                ],
    'label_columns':['content_category', 'program_type'],
    'num_columns':[  
                     'title_age_approx'
                   , 'trailer_metric_d28_selected'
                   , 'trailer_metric_d28_cumday_selected'
                   , 'trailer_metric_before28_cumday'
                   , 'trailer_metric_before28'
                   , 'wiki_befored28_total'
                   , 'wiki_d28_selected'
                   , 'wiki_view_total'
                   ],
    'main_signal_feature':[
                     'wiki_d28_selected'
                          ],
    'logged_features':[ # control by logged target or not
             'wiki_befored28_total'
            ,'wiki_d28_selected'
            ,'wiki_view_total'
        ],
    
    'logged_features_keyword':[

    ]
}

metadata_process_info = {
    'target_col':'day028_percent_viewed',
    'day_column_keywords':
        ['percent_viewed', 'vtp', 'sub_count','mc'],
    'keywords':
        # flags are usually included in the tags, so excluded here
        ['tag','licensor','title_age', 'dayofweek_earliest_date'],
    'other_col':
        ['single_episode_ind'
         , 'at_release_year'
         , 'content_category'
         , 'platform_name'
         , 'program_type'
         , 'prod_release_year'
         , 'in_sequantial_releasing_period' 
         , 'ln_total_media_cost_pre_launch'
         , 'total_hours'
         , 'season_number_adj'
         , 'retail_trailer_view_metric'
         , 'cumulative_day_num'
         , 'total_trailer_num'
         , 'wiki_prelaunch_total'
                 ],
    'label_columns':['content_category', 'program_type'],
    'num_columns':['prod_release_year'
                   , 'total_hours'
                   , 'title_age_approx'
                   , 'ln_total_media_cost_pre_launch'
                   , 'season_number_adj'
                   , 'retail_trailer_view_metric'
                   , 'cumulative_day_num'
                   , 'total_trailer_num'
                   , 'wiki_prelaunch_total'
                   ],
    'logged_features':[ # control by logged target or not
            'wiki_prelaunch_total'
        ],
    
    'logged_features_keyword':[

    ]
}

default_params_dict = {
    'lgb':
        {'objective': 'mae',
           'num_boost_round': 120,
           'feature_fraction': 1.0,
           'metric': 'mae',
           'max_depth': 10,
           'min_data_in_leaf': 5,
           'learning_rate': 0.05,
           'verbose': -1},
    
    'enet':
        {'alpha': 1e-5,
         'l1_ratio': 0.5}
}
    
params_tuning_dict = {
    'lgb': {
        'objective': ['mae'],
        'num_boost_round':[120,240],
        'feature_fraction': [0.8, 1.0],
        'metric': ['mae'],
        'max_depth':[7, 14],
        'min_data_in_leaf': [5],
        'learning_rate': [0.01, 0.1],
        'verbose':[-1]}
}