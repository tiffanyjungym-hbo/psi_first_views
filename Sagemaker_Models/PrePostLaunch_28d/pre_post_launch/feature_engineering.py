"""
"""
import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist
from scipy.spatial.distance import squareform
from pre_post_launch.data_preprocessing import DataPreprocessing
from scipy.spatial.distance import cdist 
pd.options.mode.chained_assignment = None

class FeatureEngineering(DataPreprocessing):
    def __init__(self, data_list,
                     label_columns,
                     num_columns,
                     target_col = 'day028_percent_viewed'
                ):
        DataPreprocessing.__init__(self, data_list, label_columns, target_col)
        self.base_columns = self.base.columns
        self.num_columns = num_columns
        self.X_flag = False
        self.y_flag = False
    
    def get_X_y(self, 
                percent_data_process_info,
                metadata_process_info,
                original_only = False,
                day001_popularity_threshold = -1,
                select_label_threshold = 0.05
               ):
        
        # step 0: copy base data
        self.base_copy = self.base.copy()
        self.target_copy = self.target.copy()
        self.target_copy['match_id'] = self.base_copy['match_id']
        self.day_column_list = []
        
        # step 1.1: original filter
        self.filter_non_originals(original_only)
        
        # step 1.2: popularity filter
        self.filter_popularity(percent_data_process_info, day001_popularity_threshold)
        
        # step 1.3: clean past growth trend records
        self.clean_past_growth_trend_records()
        
        # step 2: process percent data
        self.percent_columns_and_target_process(percent_data_process_info)
        
        # step 3: process metadata
        self.select_metadata_columns(metadata_process_info, select_label_threshold)
        
        # step 4: set y
        self.extract_X_y(percent_data_process_info)
        
        # step 5: set data type 
        self.set_column_dtype()
        
        # step 7: calculate growth trend
        self.calculate_growth_trend_projection(percent_data_process_info)
        
        # step 8: separate_base_and_pred
        self.separate_base_and_pred()
        
        # step 9: get clean X_pred based on 'exact_X' params
        self.clean_X_and_y_pred(percent_data_process_info)
              
        # step 10: turn on the X, y flags
        self.X_flag = True
        self.y_flag = True
        
        print('X and y are ready based on the input params')
        
    def filter_non_originals(self, original_only):
        if original_only:
            print('keeps the originals only')
            self.target_copy = self.target_copy.loc[self.base_copy['program_type']==1,:]
            self.base_copy  = self.base_copy.loc[self.base_copy['program_type']==1,:]
            print('only {} titles considered'.format(self.base_copy.shape[0]))
            
    def filter_popularity(self, percent_data_process_info, day001_popularity_threshold):
        if ((day001_popularity_threshold > 0) & (percent_data_process_info['max_num_day']>=1)):
            threshold = self.base_copy['day001_percent_viewed'].quantile(q=day001_popularity_threshold)
            
            print('keeps the titles above {} percentile day1 viewed over all titles only'.format(np.round(day001_popularity_threshold*100, 0)))
            self.target_copy = self.target_copy.loc[self.base_copy['day001_percent_viewed']>=threshold,:]
            self.base_copy  = self.base_copy.loc[self.base_copy['day001_percent_viewed']>=threshold,:]
            print('only {} titles considered'.format(self.base_copy.shape[0]))
            
    def clean_past_growth_trend_records(self):
        if self.X_flag:
            # drop existing growth trend columns if any
            cleaned_columns = [col for col in self.num_columns if 'growth_trend' in col]
            self.num_columns = [col for col in self.num_columns if col not in cleaned_columns]
            self.selected_columns = [col for col in self.selected_columns if col not in cleaned_columns]
            
            
    def percent_columns_and_target_process(self, percent_data_process_info):
        # create/empty the selected_column
        self.selected_columns = []
        
        # step 1a: a simple percent feature list if max_num_day <= 1
        if percent_data_process_info['max_num_day'] == 0:
            percent_data_process_info['log_ratio_transformation'] = False
            print('the number of days is not large enough to use log ratio transformation')
            
        else:
            # step 1b: process percent data if max_num_day>1
            self._select_percent_and_sub_columns_and_target(percent_data_process_info)
        
        
    def _select_percent_and_sub_columns_and_target(self, percent_data_process_info):   
        day_column_list = []
        # if the target is log ratio, then include the log ratio train features
        if percent_data_process_info['target_log_transformation']:          
            # log ratio
            if percent_data_process_info['log_ratio_transformation']:
                for keyword in self.day_column_keywords:
                    day_column_list.append('log_day001_'+ keyword)
                day_column_list.extend(self.base_columns[((self.base_columns.str.contains('log_ratio')==True)
                                                         #& (self.base_columns.str.contains('vtp')==False)
                                                         )])
            # log
            else:
                day_column_list.extend(self.base_columns[((self.base_columns.str.contains('log')==True) 
                       & (self.base_columns.str.contains('log_ratio')==False)
                       #& (self.base_columns.str.contains('vtp')==False)
                       )])
        # raw
        else:
            day_column_list.extend(self.base_columns[((self.base_columns.str.contains('log')==False)&
                                                      ((self.base_columns.str.contains('percent')==True)))])   
        
        # include raw forcely based on the config input
        if percent_data_process_info['raw_log_feature']:
             day_column_list.extend(self.base_columns[((self.base_columns.str.contains('log')==True) 
                       & (self.base_columns.str.contains('log_ratio')==False)
                       #& (self.base_columns.str.contains('vtp')==False)
                       )])
             day_column_list = list(set(day_column_list))
        
        # includes last season values or not
        if percent_data_process_info['last_season_percents'] == False:
            day_column_list = [col for col in day_column_list if ('last_season' in col) == False]
        
        # filtered the day num then insert into the final list     
        day_column_list = [e for e in day_column_list 
                               if int(e[e.find('day')+3:e.find('day')+6]) 
                               <= percent_data_process_info['max_num_day']]
        
        self.day_column_list = day_column_list
        self.selected_columns.extend(self.day_column_list)
        self.day_column_list_no_last_season = [col for col in self.day_column_list if 'last_season' not in col]       
        
    def select_metadata_columns(self, metadata_process_info, select_label_threshold):
        # attach all columns in the other_col 
        if 'other_col' in metadata_process_info:
            self.selected_columns.extend(metadata_process_info['other_col'])
        
        # use the keywords to find the related columns
        for keyword in metadata_process_info['keywords']:
            selected_key_columns = self.base_columns[self.base_columns.str.contains(keyword)]
            # filtered the base_copy columns with a more strict filter
            selected_key_columns = selected_key_columns[self.base_copy[selected_key_columns].sum()/\
                                 self.base_copy.shape[0] >= select_label_threshold]
            self.selected_columns.extend(self.base_columns[self.base_columns.str.contains(keyword)])
    
    def set_column_dtype(self):
        self.categorical_features = [col for col in self.X.columns if \
                                     ((col not in self.day_column_list) & \
                                      (col not in self.num_columns) & \
                                      (col != 'earliest_offered_timestamp'))]
        
        # set dtypes for light gbm
        self.X[self.categorical_features] = self.X[self.categorical_features].astype('category')
        self.X[self.num_columns] = self.X[self.num_columns].astype('float')
            
    def extract_X_y(self, percent_data_process_info):
        # step 3: get X (y was obtained in the step 2) 
        self.X = self.base_copy[self.selected_columns]
        
        # if the target is log ratio, then include the log ratio train features
        if percent_data_process_info['target_sigmoid_transformation']:
            self.y = self.target_copy['sigmoid_target']    
        elif percent_data_process_info['target_log_transformation']:       
            # log ratio
            if percent_data_process_info['log_ratio_transformation']:
                self.y = self.target_copy['log_ratio_target']
            # log
            else:
                self.y = self.target_copy['log_target']
        # raw
        else:
            self.y = self.target_copy['target']
            
    def separate_base_and_pred(self):
        
        # seperate X_base, X_pred, and y_base
        self.X_base = self.X[self.y!=-100]
        self.y_base = self.y[self.y!=-100]
        self.X_pred = self.X[self.y==-100]
        self.y_pred = self.y[self.y==-100]
        
        # get y_base, y_pred aligned with X_base and X_pred
        self.y_base = self.y_base[self.X_base.index]
        self.y_pred = self.y_pred[self.X_pred.index]
        
        # get timestamp for fold spliting purpose
        self.title_offered_ts = pd.to_datetime(self.base_copy.loc[self.y!=-100, 'earliest_offered_timestamp'])
    
    def clean_X_and_y_pred(self, percent_data_process_info):
        # get final X_pred with exact number of days
        # (find the one with the days of data equal to the max_num_day parameter)
        if percent_data_process_info['exact_X_pred']:
            percent_list_no_target = [val for val in self.percent_list if val!= self.target_col]
            percent_values = self.base_copy[percent_list_no_target]
            percent_not_null = percent_values.apply(\
                                lambda x: [int(loc[loc.find('day')+3:loc.find('day')+6])\
                                for val, loc in zip(x, percent_values.columns) if val!=-100], 
                                    axis= 1)
                
            self.last_percent_day = percent_not_null[self.X_pred.index].\
                apply(lambda x: percent_data_process_info['total_num_day_data']\
                      if x == [] else max(x))
            
            self.y_pred = self.y_pred[self.last_percent_day\
                                      [self.last_percent_day== \
                                       percent_data_process_info['max_num_day']].index]
                
            self.X_pred = self.X_pred.loc[self.last_percent_day\
                                      [self.last_percent_day== \
                                       percent_data_process_info['max_num_day']].index,:]
        # or get the X_pred with days of data more than or equal to max_num_day
        else:
            self.y_pred = self.y_pred[(self.X_pred==-100).sum(axis=1)==0]
            self.X_pred = self.X_pred.loc[(self.X_pred==-100).sum(axis=1)==0,:]
        
        # get y_pred align with X_pred again
        self.y_pred = self.y_pred[self.X_pred.index]
        
    def calculate_growth_trend_projection(self, percent_data_process_info):        
        self.max_order = percent_data_process_info['growth_trend_num']
        
        if ((self.max_order > 0) & (percent_data_process_info['max_num_day']>1)):
            print('using growth trend projection as a feature')
            X, y = self._grow_trend_projection_init()
            
            self._get_unique_time_list(y)            
            
            trend_base = X.loc[y!=-100, ((X.columns.str.contains('viewed') & (X.columns.str.contains('day001')==False)))]
            
            # for each timestamp after 1 year before Max launch
            for timestamp in self.timestamp_unique_list:
                # projected slice & base slice
                trend_projected_slice = X.loc[self.timestamp_list==timestamp, ((X.columns.str.contains('viewed'))& (X.columns.str.contains('day001')==False))]
                trend_base_slice = trend_base.loc[self.base_timestamp_list< timestamp, :]
                
                self.trend_projected_slice = trend_projected_slice
                self.trend_base_slice = trend_base_slice                
                self._trend_iter_init()
                
                projected_values = self._calculate_projection(y, 
                                               trend_projected_slice, 
                                               trend_base_slice)
                
                self.output = pd.concat([self.output, projected_values])
                
            self._final_merge_steps()
            
    def _final_merge_steps(self):
        self.final_output = self.final_output.merge(self.output, left_index = True, right_index = True, how = 'left')
        self.final_output = self.final_output.fillna(-2)
        self.X = self.X.merge(self.final_output, 
                              left_index = True, 
                              right_index = True, 
                              how = 'left')
        
        self.num_columns.extend(self.col_list)
        self.selected_columns.extend(self.col_list)
        
                
    def _calculate_projection(self, y,
                              trend_projected_slice, 
                              trend_base_slice):
        
        for i in range(0, self.max_order):
            if self.dist_mat_flag == False:
                dist_mat = cdist(trend_projected_slice, trend_base_slice)
                dist_mat = pd.DataFrame(dist_mat, index = trend_projected_slice.index, columns = trend_base_slice.index)
                self.dist_mat_flag = True
    
            ind_ = dist_mat.idxmin(axis=1)
            
            for x_ind, y_ind in zip(trend_projected_slice.index, ind_):
                dist_mat.loc[x_ind, y_ind] = np.inf
            
            self.project_list[self.col_list[i]].extend(y[ind_].values)
        
        return pd.DataFrame(self.project_list, 
                 index = trend_projected_slice.index, 
                 columns = ['growth_trend_' + str(order + 1) + '_order' for order in range(0, self.max_order)])
            
    def _trend_iter_init(self):
        self.project_list = {}
        for col in self.col_list:
            self.project_list[col] = []  
        
        self.dist_mat_flag = False
        
    
    def _grow_trend_projection_init(self):
        # set initial values
        self.output = pd.DataFrame({})
        self.final_output = pd.DataFrame({}, index = self.y.index)
        self.col_list = ['growth_trend_' + str(order + 1) + '_order' 
                        for order in range(0, self.max_order)]
        
        return self.X, self.y
       
    def _get_unique_time_list(self, y):
        self.timestamp_list = pd.to_datetime(self.base_copy['earliest_offered_timestamp'])
        self.base_timestamp_list = self.timestamp_list.loc[y!=-100] + pd.Timedelta(days= 28)
        # started to get trend projection a year prior to max start date
        start_timestamp = pd.to_datetime('2020-05-27') - pd.Timedelta(days= 365)
        self.timestamp_unique_list = pd.to_datetime(np.sort(self.timestamp_list[self.timestamp_list> start_timestamp].unique()))

            
            
        
           
        
        
        
        
        
 