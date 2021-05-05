"""
"""

model_name_list = ['lgb']

percent_data_process_info ={
    'target_log_transformation':True,
    'log_ratio_transformation': True, # in effect only if target_log_transformation = True
    'target_sigmoid_transformation':False, # in effect only if log_ratio_transformation = True
    'raw_log_feature':True,
    'last_season_percents': False,
    'max_num_day': 14,
    'total_num_day_data':21,
    'exact_X_pred': False,
    'growth_trend_num':0,
    'linear_pred_as_feature':False
}

metadata_process_info = {
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
         , 'ln_total_click_from_marketing_spend'
         , 'total_hours'
         , 'retail_trailer_view_metric'
         , 'content_cost'
                 ],
    'label_columns':['content_category', 'program_type'],
    'num_columns':['prod_release_year'
                   , 'retail_trailer_view_metric'
                   , 'total_hours'
                   , 'title_age_approx'
                   , 'ln_total_click_from_marketing_spend'
                   , 'content_cost'
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
    
params_tunning_dict = {
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
