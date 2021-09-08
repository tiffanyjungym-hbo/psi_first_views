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
    'linear_pred_as_feature':False
}

prelaunch_process_info = {
    'target_col':'day028_percent_viewed',
    'day_column_keywords':
        ['trailer_metric_before'],
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
         , 'total_hours'
         , 'content_cost'
         , 'season_number_adj'
         , 'trailer_metric_before_selected'
         , 'trailer_metric_before_cumday_selected'
         , 'production_budget_imdb'
                 ],
    'prelaunch_spec_process':['trailer_metric_before'],
    'label_columns':['content_category', 'program_type'],
    'num_columns':['prod_release_year'
                   , 'total_hours'
                   , 'title_age_approx'
                   , 'season_number_adj'
                   , 'trailer_metric_before_selected'
                   , 'trailer_metric_before_cumday_selected'
                   , 'production_budget_imdb'
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
         , 'content_cost'
         , 'season_number_adj'
         , 'retail_trailer_view_metric'
         , 'cumulative_day_num'
         , 'total_trailer_num'
         , 'avg_trail_metric_per_day'
         , 'production_budget_imdb'
                 ],
    'label_columns':['content_category', 'program_type'],
    'num_columns':['prod_release_year'
                   , 'total_hours'
                   , 'title_age_approx'
                   , 'ln_total_media_cost_pre_launch'
                   , 'content_cost'
                   , 'season_number_adj'
                   , 'retail_trailer_view_metric'
                   , 'cumulative_day_num'
                   , 'total_trailer_num'
                   , 'avg_trail_metric_per_day'
                   , 'production_budget_imdb'
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
        {'alpha': 1e-6,
         'l1_ratio': 0.2}
}
    
params_tuning_dict = {
    'lgb': {
        'objective': ['mae'],
        'num_boost_round':[120,240],
        'feature_fraction': [1.0],
        'metric': ['mae'],
        'max_depth':[7, 14],
        'min_data_in_leaf': [5],
        'learning_rate': [0.01, 0.1],
        'verbose':[-1]}
}
