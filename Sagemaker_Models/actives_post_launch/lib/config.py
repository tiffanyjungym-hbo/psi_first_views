"""
"""

model_name_list = ['lgb']

percent_data_process_info ={
    'target_log_transformation':True,
    'log_ratio_transformation': True,
    'raw_log_feature':True,
    'cumulative_media_cost':True,
    'last_season_percents': False,
    'max_num_day': 14,
    'total_num_day_data':21,
    'exact_X_pred': False,
    'growth_trend_num':0,
    'linear_pred_as_feature':False,
    'all_none_zero': False,
    'percentile_used':0.8,
    'nfold':6,
    'prelaucn_monotonic_features':[
             'trailer_metric_d28_selected'
           , 'wiki_view_total'
           , 'wiki_befored28_total'
    ]
}

prelaunch_process_info = {
    'target_col':'day028_percent_actives',
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
          # 'single_episode_ind'
          #, 'at_release_year'
            'content_category'
          , 'platform_name'
          , 'program_type'
#           , 'popcorn_ind'
          #, 'prod_release_year'
          #, 'in_sequantial_releasing_period' 
          #, 'total_hours'
          #, 'content_cost'
          #, 'season_number_adj'
          , 'title_age_approx'
          , 'trailer_metric_d28_selected'
          , 'trailer_metric_d28_cumday_selected'
          , 'trailer_metric_before28_cumday'
          , 'trailer_metric_before28'
          , 'wiki_befored28_total'
          , 'wiki_d28_selected'
          , 'wiki_view_total'
          #, 'total_production_budget_imdb'
                 ],
    'prelaunch_spec_process':[
         'trailer_metric_d28'
         ,'wiki_d28'
         #,'trailer_metric_before'
                ],
    'label_columns':['content_category', 'program_type'],
    'num_columns':[
                   #  'total_hours'
                    'title_age_approx'
                   #,'content_cost'
                   #, 'season_number_adj'
                   , 'trailer_metric_d28_selected'
                   , 'trailer_metric_d28_cumday_selected'
                   , 'trailer_metric_before28_cumday'
                   , 'trailer_metric_before28'
                   , 'wiki_befored28_total'
                   , 'wiki_d28_selected'
                   , 'wiki_view_total'
                   #, 'total_production_budget_imdb'
                   ],
    'main_signal_feature':[
                    #  'trailer_metric_d28_cumday_selected'
                    #, 'trailer_metric_d28_selected' 
                    #, 'day_wiki_view_before28'
                      'wiki_d28_selected'
                          ],
    'logged_features_keywords':[
             'wiki_befored28_total'
            ,'wiki_d28_selected'
        ]
}

metadata_process_info = {
    'target_col':'day028_percent_actives',
    'day_column_keywords':
        ['percent_viewed'
         , 'vtp'
         , 'sub_count'
         , 'mc'
         ,'percent_actives'
         #, 'wiki_view' # blocked for now, before wikipedia page view were regularly updated
        ],
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
#          , 'popcorn_ind'
#         , 'content_cost'
         , 'season_number_adj'
         , 'retail_trailer_view_metric'
         , 'cumulative_day_num'
         , 'total_trailer_num'
#          , 'wiki_prelaunch_total'
#         , 'trailer_metric_before28_cumday'
#         , 'trailer_metric_before28'
#         , 'avg_trail_metric_per_day'
#         , 'total_production_budget_imdb'
                 ],
    'label_columns':['content_category', 'program_type'],
    'num_columns':['prod_release_year'
                   , 'total_hours'
                   , 'title_age_approx'
                   , 'ln_total_media_cost_pre_launch'
#                   , 'content_cost'
                   , 'season_number_adj'
                   , 'retail_trailer_view_metric'
                   , 'cumulative_day_num'
                   , 'total_trailer_num'
#                    , 'wiki_prelaunch_total'
#                   , 'trailer_metric_before28_cumday'
#                   , 'trailer_metric_before28'
#                   , 'avg_trail_metric_per_day'
#                   , 'total_production_budget_imdb'
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
