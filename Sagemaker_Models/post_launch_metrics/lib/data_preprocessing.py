"""
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import numpy as np
import pandas as pd
from scipy.special import logit
from sklearn.preprocessing import LabelEncoder 
from lib.config import metadata_process_info

class DataPreprocessing():
    def __init__(self, data_list,
                     label_columns,
                     target_col = metadata_process_info['target_col'],
                     day_column_keywords = metadata_process_info['day_column_keywords'],
                     hard_label_threshold = 0.01):
        # init params
        self.data_list = data_list
        self.target_col = target_col
        self.label_columns = label_columns
        self.hard_label_threshold = hard_label_threshold
        self.day_column_keywords = day_column_keywords
        
        # executing steps
        self.combined_data()
        self.set_unobserved_and_zero_values()
        self.percent_list_and_value_cleaning()
        self.seperate_basic_target()
        self.clean_and_generate_tags_based_on_genres()
        self.clean_and_generate_licensors()
        self.log_and_log_ratio_cal()
        self.sigmoid_target_cal()
        self.categorical_feature_label_encoding()
        self.get_list_all_percent()
        # binarize the day of week feature for linear regression to use
        self.dayofweek_dummies()
        
        print('Final title size: {}, All title size: {}'.format(self.base.shape[0], self.df.shape[0]))
    
    def combined_data(self):
        
        # clean the columns nad make them lowercases
        for cnt in range(len(self.data_list)):
            self.data_list[cnt].columns = self.data_list[cnt].columns.str.lower().str.replace("'", "")
        
        df = self.data_list[0]
        
        for data in self.data_list[1:]:
            df = df.merge(data, how = 'inner', on = 'match_id_platform')          
        
        # for some reasons the null problem cant be solve in SQL
        # so use python to solve

        self.df = df
        self.df.columns = self.df.columns.str.lower()
        
    def _get_day_list(self,keyword):
        columns = self.df.columns
        keyword_columns = columns[columns.str.contains(keyword)]
        day_list = pd.Series(keyword_columns).apply(lambda x: x[x.find('day')+3:x.find('day')+6])
        return day_list, keyword_columns
        
    def set_unobserved_and_zero_values(self):
        for keyword in self.day_column_keywords:
            day_list, keyword_columns = self._get_day_list(keyword)
            for day_ in day_list:
                self.df.loc[self.df['min_days_since_offered'] < int(day_), 'day' + day_ + '_' + keyword] = -100
        
            self.df[keyword_columns] = self.df[keyword_columns].fillna(0)
        
    
    def percent_list_and_value_cleaning(self):
        # find percent related columns
        self.percent_list = self.df.columns[((self.df.columns.str.contains('percent')==True) 
                                    # last season values could be zeros
                                   & (self.df.columns.str.contains('last_season')==False)
                                   & (self.df.columns.str.contains('viewed_through')==False))]
        # append vtps
        self.percent_list = self.percent_list.append(\
                                    self.df.columns[self.df.columns.str.contains('vtp')==True])
        
        # consider titles with percent view values the first days
        self.df_pop = self.df.copy()
        
        for col in self.percent_list:
            # set the training set values
            self.df_pop.loc[((self.df_pop[col] <= 1e-10) & 
                             (self.df_pop['min_days_since_offered']>=\
                              int(col[col.find('day')+3:col.find('day')+6]))), col] = 1e-10
            
        self.df_pop = self.df_pop.reset_index(drop = True)
        
    def seperate_basic_target(self):
        # did not add release month and year here, because the data are not sufficient
        self.base = self.df_pop[[col for col in self.df_pop.columns if col != self.target_col]]
        self.target = pd.DataFrame({'target':self.df_pop[self.target_col]})
        
        #self.base.index = self.base['match_id_platform']
        #self.target.index = self.base['match_id_platform']
        
    def clean_and_generate_tags_based_on_genres(self):
        # combine descriptive and wm enterprise genres
        tags = self.base['descriptive_genre_desc_agg'] + '|' + self.base['wm_enterprise_genres_agg'] + '|' + \
                    self.base['navigation_genre_desc_agg']
        tags = tags.fillna('others')
        
        # clean the genres
        tags = tags.str.replace('&|/|:', '|')
        tags = tags.str.replace('originals','original')
        tags = tags.str.replace('historical','history')
        tags = tags.str.replace('romantic comedy','romance|comedy')
        tags = tags.str.replace('sports','sport')
        tags = tags.str.replace('documentaries','documentary')
        tags = tags.str.split('|')
        tags = tags.apply(lambda x: set([e.strip() for e in x if len(e.strip())>0]))
        
        # generate one-hot tags df from the genres
        tags = tags.str.join('|').str.get_dummies()
        
        # only takes the tags that appear in more than 5% of the titles
        tags_count = tags.sum()
        tags_index = tags_count[tags_count>=tags.shape[0]*self.hard_label_threshold].index
        
        # exclude the others tags, they were there to fulfill the format requirement of the pandas function
        tags_index = [x for x in tags_index if x!='others']
        
        # final form
        tags = tags.loc[:,tags_index]
        tags.columns = 'tag_' + tags.columns
        self.base = self.base.drop(columns = ['descriptive_genre_desc_agg', 
                                              'wm_enterprise_genres_agg', 
                                              'navigation_genre_desc_agg'])
        self.base = pd.concat([self.base, tags], axis = 1)
        
    def clean_and_generate_licensors(self):
        licensor = self.base['licensor_agg'].fillna('no data')

        # remove company type
        licensor = licensor.str.replace('inc.','')
        licensor = licensor.str.replace('inc','')
        licensor = licensor.str.replace('llc','')
        licensor = licensor.str.replace('lllp','')
        licensor = licensor.str.replace('llp','')
        licensor = licensor.str.replace('ltd.','')
        licensor = licensor.str.replace('lp','')

        # separate some of the combined brand names
        licensor = licensor.str.replace('/',',')

        # clean and combine big company names
        ## warner media & wm bros
        licensor = licensor.str.replace('warner media direct','warnermedia direct')
        licensor = licensor.str.replace('warner bros. worldwide television distribution ', 'warner bros. international') 
        licensor = licensor.str.replace('warner brothers international tv distribution', 'warner bros. international') 
        licensor = licensor.str.replace('warner bros. international television distribution', 'warner bros. international') 
        licensor = licensor.str.replace('wb studio enterprises','warner bros.') 
        licensor = licensor.str.replace('warner bros. domestic - classic film library (tcm - us)','warner bros.') 
        licensor = licensor.str.replace('warner horizon television','warner bros.') 
        licensor = licensor.str.replace('warner bros. domestic television distribution','warner bros.') 

        ## cartoon network
        licensor = licensor.str.replace('cartoon network productions|cartoon network studios','the cartoon network') 

        ## turner
        licensor = licensor.str.replace('turner entertainment networks|tbs|turner classic movies','turner system')

        ## universal studio
        licensor = licensor.str.replace('universal city studios productions|universal studios licensing|niversal studios pay television','universal studios')

        ## hbo
        licensor = licensor.str.replace('hbo europe s.r.o.|hbo pacific partners','hbo international')
        licensor = licensor.str.replace('hbo ole acqusitions l.l.c.|hbo independent productions','hbo')
    
        ## lionsgate
        licensor = licensor.str.replace('lions gate films|lions gate television|lionsgate','lion gate')

        licensor = licensor.str.split(',')
        licensor = licensor.apply(lambda x: set([e.strip() for e in x if len(e.strip())>2]))
        
        # get one-hot licensor df 
        licensor = licensor.str.join('|').str.get_dummies()
        
        # only takes the licensor that more than 5% of the titles contains
        licensor_count = licensor.sum()
        licensor_index = licensor_count[licensor_count>=licensor.shape[0]*self.hard_label_threshold].index
        
        ## exclude the 'no data' licensors
        licensor_index = [x for x in licensor_index if x!='no data']
        licensor = licensor.loc[:,licensor_index]
        licensor.columns = 'licensor_' + licensor.columns
        licensor.columns = licensor.columns.str.replace(' ', '_')
        
        # concat the df
        self.base = self.base.drop(columns = ['licensor_agg'])
        self.base = pd.concat([self.base,licensor], axis = 1)

    def log_and_log_ratio_cal(self):
        # generate the log ratio and log version of the percent features and target
        self.log_percent_list = [col for col in self.percent_list if col not in [self.target_col]]

        # generate the log ratio and log version of the sub count features
        self.sub_count_list = self.base.columns[self.base.columns.str.contains('sub_count')].values.tolist()
        self.log_percent_list.extend(self.sub_count_list) 

        # add last season percents
        self.log_percent_list.extend(self.base.columns[((self.base.columns.str.contains('percent')==True) 
                                   & (self.base.columns.str.contains('last_season')==True))])
        
        # log process
        for col in self.log_percent_list:
            # log
            self.base['log_' + col] = -100
            self.base.loc[self.base[col]>0,
                           'log_' + col] = np.log(self.base.loc[self.base[col]>0, col])
            
            # log ratio
            for keyword in self.day_column_keywords:
                if ((keyword in col) & (col!= 'day001_' + keyword) & ('mc' not in col)):
                    self.base['log_ratio_' + col] = -100
                    self.base.loc[self.base[col]>0,
                           'log_ratio_' + col] = np.log(self.base.loc[self.base[col]>0, col]/
                                                         self.base.loc[self.base[col]>0, 'day001_' + keyword])               
        
        # process target
        target_greater_than_zero = (self.target['target']>0)
        self.target['log_ratio_target'] = -100
        self.target['log_target'] = -100
        self.target.loc[target_greater_than_zero, 'log_ratio_target'] =\
            np.log(self.target.loc[target_greater_than_zero,'target']/self.base.loc[target_greater_than_zero, 'day001_percent_viewed'])
        self.target.loc[target_greater_than_zero, 'log_target']       =\
            np.log(self.target.loc[target_greater_than_zero,'target'])

    def sigmoid_target_cal(self):
        target_greater_than_zero = (self.target['target']>0)
        self.target['sigmoid_target'] = -100
        self.target.loc[target_greater_than_zero, 'sigmoid_target'] =\
            logit(self.target.loc[target_greater_than_zero,'target'])

    def categorical_feature_label_encoding(self):
        le = LabelEncoder()
        label_columns = [col for col in self.label_columns if col in self.base.columns]
        self.label_codes = {}

        for col in label_columns:
            self.base[col] = le.fit_transform(self.base[col])
            self.label_codes[col] = le.classes_
            
    def get_list_all_percent(self):
        self.all_percent_list = self.base.columns[self.base.columns.str.contains('percent')]       
        
    def dayofweek_dummies(self):
        dow_dummies = pd.get_dummies(self.base['dayofweek_earliest_date'])
        dow_dummies.columns = 'dayofweek_earliest_date_' + dow_dummies.columns.astype('str')
        
        self.base = self.base.drop(columns = ['dayofweek_earliest_date'])
        self.base = self.base.merge(dow_dummies, left_index = True, right_index = True)
        
        
        
    
    