"""
"""
import lightgbm as lgb
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
#from numpy.random import default_rng
from sklearn.linear_model import ElasticNet
from sklearn.linear_model import LinearRegression as lr
import itertools as it
from scipy.special import expit

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick


from lib.feature_engineering import FeatureEngineering

class ModelMain(FeatureEngineering):
    def __init__(self, data_list,
                     label_columns,
                     num_columns,
                     target_col = 'day028_percent_viewed'
                ):
        FeatureEngineering.__init__(self, data_list, label_columns, num_columns, target_col)
        # set parameter_tuned option
        self.parameter_tuned = False
        self.importance_flag = False
        self.optimized_params = None

    def cross_prediction(self, 
                         model_name_list, 
                         params_dict, 
                         percent_data_process_info, 
                         nfold = 6):        
        # reset the flag
        self.performance_bootstrapped_flag = False
    
        
        # step 1: do time split
        self.timesplit(nfold)
        
        # step 2: initialize the params
        self._cross_prediction_init()
        
        # step 3: cross prediction
        fold_n = len(self.train_fold_index) 
        for fold in range(0, fold_n):
            # train and test
            train_index = self.train_fold_index[fold]
            test_index  = self.test_fold_index[fold]
            x_train, x_test = self.X_base.loc[train_index,:], self.X_base.loc[test_index,:]
            y_train, y_test = self.y_base[train_index], self.y_base[test_index]
            match_id_test = self.target_copy['match_id'][test_index]
            
            # loop over the selected models
            for model_name in model_name_list:
                # train 
                model = self.model_train(model_name, 
                        params_dict, 
                        x_train, y_train, 
                        percent_data_process_info)
                self.trained_model[model_name] = model
                
                # predict 
                y_predict = self.model_predict(model_name, x_test, percent_data_process_info)
                
                # output transformation
                y_predict = self.output_transformation(y_predict, x_test, percent_data_process_info)
                
                # recording
                self.output.loc[test_index, 'cross_predict_' + model_name] = y_predict
                
            y_test = self.output_transformation(y_test, x_test, percent_data_process_info)

            # record the target, fold, and match_id 
            self.output.loc[test_index, 'target'] = y_test   
            self.output.loc[test_index, 'fold'] = fold
            self.output.loc[test_index, 'match_id'] = match_id_test
        
        # processing output and calculate evaluation metrics
        self._output_and_metrics_processing(model_name_list)
        
        self.output_flag = True
        
    def model_train(self, model_name, params_dict, 
                    x_train, y_train, percent_data_process_info):
        
        if model_name == 'lgb':
            model = self._lgb_model_train(params_dict, x_train, y_train)
        elif model_name =='lr':
            model = self._lr_model_train(x_train, y_train, percent_data_process_info)
        elif model_name =='enet':
            model = self._enet_model_train(params_dict, x_train, y_train, percent_data_process_info)
        else:
            print('model name not valid')
            
        return model
        
    def model_predict(self, model_name, x_test, percent_data_process_info):
        if ((percent_data_process_info['max_num_day']==0) & (model_name in ['lr', 'enet'])):
            y_predict = [np.nan]*x_test.shape[0]
            print('no data for model {}'.format(model_name))
        elif model_name in ['lr','enet','lgb']:
            if model_name in ['lr','enet']:
                columns_used = list(self.day_column_list_no_last_season)
                columns_used.extend(x_test.columns[x_test.columns.str.contains\
                                            ('dayofweek_earliest_date')])
                x_test  = x_test[columns_used]
            
            y_predict = self.trained_model[model_name].predict(x_test)
        
        else:
            return 0
            print('the model name is not valid')
            
        if model_name in ['enet']:
            y_predict = y_predict.flatten()
                       
        return y_predict
        
        
    def _lgb_model_train(self, params_dict, x_train, y_train):
        # initilization
        params = params_dict['lgb']
        
        # set training data
        train_data = lgb.Dataset(x_train, label=y_train)
        #The test data should not be the predicting fold, should be the 
        #   lastest days in the trian fold
        #test_data = lgb.Dataset(x_test, label=y_test, categorical_feature=self.categorical_features)
        
        # lightgbm model
        model = lgb.train(params,
                               train_data,
                               verbose_eval = False
                         )
        
        # feature importance recording
        if self.importance_flag:
            self.importance += model.feature_importance() 
        else:
            self.importance = model.feature_importance() 
            self.importance_flag = True
            
        return model
        
    def _enet_model_train(self, params_dict, x_train, y_train, percent_data_process_info):
        # initialization
        params = params_dict['enet']
        # benchmark prediction
        if percent_data_process_info['max_num_day']>0:
            model = ElasticNet(
                        alpha = params_dict['enet']['alpha'],
                        l1_ratio = params_dict['enet']['l1_ratio'],
                        max_iter = 10000, 
                        normalize = False).fit(x_train[self.day_column_list_no_last_season].values, 
                                             y_train.values.reshape(-1,1))
        else:
            model = None
            print('no data for elastic net model')                                   
        return model
        
    def _lr_model_train(self, x_train, y_train, percent_data_process_info):
        # benchmark prediction
        columns_used = list(self.day_column_list_no_last_season)
        columns_used.extend(x_train.columns[x_train.columns.str.contains\
                                            ('dayofweek_earliest_date')])
        
        if percent_data_process_info['max_num_day']>0:
            model = lr().fit(x_train[columns_used].values, y_train.values.reshape(-1,1))

        else:
            model = None
                                               
        return model
    
    def _output_and_metrics_processing(self, model_name_list):
        # drop the for-training-only data rows (since they have no rolling cross predicted values)
        self.output = self.output.loc[self.output.drop(columns = ['program_type']).dropna(how = 'all').index,:]
        
        # calculate evaluation metrics
        for model_name in model_name_list:
            self.output['smape_' + model_name] = abs(self.output['cross_predict_' + model_name] - self.output['target'])/\
                                    ((self.output['cross_predict_' + model_name] + self.output['target'])/2)
            self.output['mae_' + model_name] = abs(self.output['cross_predict_' + model_name] - self.output['target'])
        
    def timesplit(self, nfold = 10):
        # The current setting considers all the dates after and excluding the Max launch date
        # the alternative is to include the titles released at the Max launch date
        test_started_time = self.title_offered_ts[self.X_base['platform_name'] == 1].min()
        test_started_time = test_started_time + pd.Timedelta(days = 1)
        test_folds_ind = self.title_offered_ts[self.title_offered_ts>= test_started_time]
        
        # find the date boundaries of the folds
        fold_bound = test_folds_ind.quantile(np.linspace(0,1,nfold+1)).reset_index(drop=True)        
        
        # generate train and test index        
        self.train_fold_index, self.test_fold_index = self._generate_test_and_train_index(fold_bound, self.title_offered_ts)
                
    def _generate_test_and_train_index(self, fold_bound, title_offered_ts):
        # generate folds
        test_fold = []
        train_fold = []

        for cnt in range(len(fold_bound)-1):
            test_lower_bound = fold_bound[cnt]
            # to exclude the upperbound boundary
            test_upper_bound = fold_bound[cnt + 1] - pd.Timedelta(seconds = 1)

            # working on test
            test_ind = title_offered_ts[title_offered_ts.between(test_lower_bound, test_upper_bound)].index
            if len(test_ind)>0:
                test_fold.append(test_ind)

                # working on train
                train_upper_bound = title_offered_ts[test_ind].min() - pd.Timedelta(days = 28)
                train_ind = title_offered_ts[title_offered_ts < train_upper_bound].index
                train_fold.append(train_ind)
                
            else:
                print('Warning: one test fold has no elements, did not include in the test_fold output')
                
        return train_fold, test_fold  
    
    def _cross_prediction_init(self):
        # set output_format
        self.output = pd.DataFrame(self.y_base).copy()
        self.output['program_type'] = self.X_base['program_type']
        self.output.columns = ['target', 'program_type']
        self.output['target'] = np.nan
        
        self.trained_model = {}
        
        self.importance = None
        self.importance_flag = False
        self.output_flag = False
    
        
    def _param_tunning_flag_init(self, model_name):
        # initialize parameter tunning flags
        self.parameter_tunning_stats = {}
        self.parameter_tunning_stats[model_name] = {}
        self.parameter_tunning_stats[model_name]['min_smape_original_flag'] = False
        self.parameter_tunning_stats[model_name]['min_smape_all_flag'] = False     

        
    def _output_transformation(self, y_predict, y_test, 
                               y_predict_benchmark, y_predict_enet, 
                               x_test, percent_data_process_info):
    # result processing

        if percent_data_process_info['target_sigmoid_transformation']:
            y_predict = expit(y_predict)
            y_test = expit(y_test)
            y_predict_benchmark = expit(y_predict_benchmark)
            y_predict_enet = expit(y_predict_enet)

        elif percent_data_process_info['target_log_transformation']:
            y_predict = np.exp(y_predict)
            y_test = np.exp(y_test)
            y_predict_benchmark = np.exp(y_predict_benchmark)
            y_predict_enet = np.exp(y_predict_enet)

            if percent_data_process_info['log_ratio_transformation']:
                y_predict = y_predict*np.exp(x_test['log_day001_percent_viewed'])
                y_test = y_test*np.exp(x_test['log_day001_percent_viewed'])
                y_predict_benchmark = y_predict_benchmark*np.exp(x_test['log_day001_percent_viewed'])
                y_predict_enet = y_predict_enet*np.exp(x_test['log_day001_percent_viewed'])
                
        return y_predict, y_test, y_predict_benchmark, y_predict_enet
    
    def predict_new_titles(self, 
                         model_name_list, 
                         params_dict, 
                         percent_data_process_info):  
        
        # step 1: initialize the params
        self._predict_new_title_init()
        
        # step 2: train then predict
        for model_name in model_name_list:
            # train 
            model = self.model_train(model_name, 
                    params_dict, 
                    self.X_base, self.y_base, 
                    percent_data_process_info)
            
            self.trained_model[model_name] = model
            
            # predict 
            y_predict = self.model_predict(model_name, self.X_pred, percent_data_process_info)
            
            # output transformation
            y_predict = self.output_transformation(y_predict, self.X_pred, percent_data_process_info)
            
            # recording
            self.new_title_output['day_' + str(percent_data_process_info['max_num_day']) + '_' + model_name] = y_predict

        
    
    def _predict_new_title_init(self):
        # set output_format
        self.new_title_output = pd.DataFrame(self.y_pred).copy()
        self.new_title_output['program_type'] = self.X_pred['program_type']
        self.new_title_output.columns = ['target', 'program_type']
        self.new_title_output['target'] = np.nan
        self.new_title_output = self.base_copy[['title_name','match_id']].\
            merge(self.new_title_output, 
                  left_index = True, 
                  right_index = True)
        
        self.trained_model = {}
        
        self.importance = None
        self.importance_flag = False
        self.output_flag = False
    
    def output_transformation(self, y, x_test, percent_data_process_info):
    # result processing
        if percent_data_process_info['target_log_transformation']:
            y = np.exp(y)

            if percent_data_process_info['log_ratio_transformation']:
                y = y*np.exp(x_test['log_day001_percent_viewed'])
                
        return y
    
    def print_performance(self):
        if self.output_flag:
            print('Overall: lgb smape is {}, benchmark smape is {}'.format(self.output['smape_lgb'].mean(), self.output['smape_benchmark'].mean()))
            print('Original: lgb smape is {}, benchmark smape is {}'.format(self.output.loc[self.output['program_type']==1, 'smape_lgb'].mean(), 
                                                                            self.output.loc[self.output['program_type']==1, 'smape_benchmark'].mean()))
        else:
            print('run the cross prediction first')
            
    def feature_importance(self, importance, columns, n_features = 20, filename = 'feature_importance'):
        fig = plt.figure(figsize = (12,6)) 
    # just show one model's result, but should be representative enough
        importance_percent = importance/importance.sum()*100
        importance_percent = pd.DataFrame({'importance':importance_percent, 'feature':columns})
        importance_percent = importance_percent.sort_values('importance', ascending = False).head(n_features)

        plot = importance_percent.plot.bar(x = 'feature', y= 'importance', 
                                           rot = 90, figsize = (8,4))
        
        plt.xlabel('Feature names', fontsize=16)
        plt.ylabel('Frequency [%]', fontsize=16)
        plot.get_figure().savefig('{}.png'.format(filename))

    '''
    # unenable bootstrap functions for now, due to numpy version problem: if it is too old, it does not have default_rng
    # if it is too new, it conflicts with the newest pandas version available

    def bootstrap_confidence_inv(self, output, model_name_list, total_iter = 5000, original_only = False):  
        output_copy = output.reset_index(drop = True).copy()

        # create fin_dict
        fin_dict = {}
        for model_name in model_name_list:
            fin_dict['mean_dist_smape_' + model_name] = []
        
        # consider originals only
        if original_only:
            output_copy = output_copy.loc[output_copy['program_type']==1,:].reset_index(drop = True)
        
    
        for cnt in range(total_iter):
            rng = default_rng()
            numbers = rng.choice(output_copy.shape[0], size=output_copy.shape[0], replace=True)
            
            for model_name in model_name_list: 
                fin_dict['mean_dist_smape_' + model_name].append(output_copy.loc[numbers, 'smape_' + model_name].mean())

        self.performance_bootstrapped_flag = True
        
        return pd.DataFrame(fin_dict)
  
    def bootstrap_p_value_lgb_benchmark(self, compare_pair = ['smape_lgb', 'smape_benchmark'], total_iter = 5000):
        if self.performance_bootstrapped_flag == False:
            self.bootstrap_confidence_inv()
        
        rng = default_rng()
        total_bootstrapped_size = self.performance_bootstrapped.shape[0]
        numbers1 = rng.choice(total_bootstrapped_size, total_bootstrapped_size, replace=True)
        numbers2 = rng.choice(total_bootstrapped_size, total_bootstrapped_size, replace=True)
        smape_sample_1 = self.performance_bootstrapped.loc[numbers1, 'mean_dist_' + compare_pair[0]].reset_index(drop = True)
        smape_sample_2 = self.performance_bootstrapped.loc[numbers2, 'mean_dist_' + compare_pair[1]].reset_index(drop = True)
        pcount = sum(smape_sample_1>=smape_sample_2)*1.0
        self.p_value_lgb_benchmark = pcount/total_iter
        print('probability that {} >= {}: {}'.format('mean_dist_' + compare_pair[0], 
                                                   'mean_dist_' + compare_pair[1], 
                                                   self.p_value_lgb_benchmark))
        
    def bootstrap_p_value_two_samples(self, sample1, sample2, total_iter = 5000):
        return 0
    '''

    def parameter_tunning(self, model_name, 
                          params_tunning_dict, 
                          percent_data_process_info,
                          nfold = 6):
        if model_name!= 'lr':
            self._param_tunning_flag_init(model_name)
            
            model_name_list = [model_name]
            combinations, allNames = self._parameter_tunning_init(params_tunning_dict, model_name)
            
            ct= 1
            for com in combinations:
                params_dict_input = self._set_params_dict(model_name, allNames, com)
                print('parameter combination {}'.format(ct)) 
                ct+=1
                self.cross_prediction(model_name_list, params_dict_input, 
                                           percent_data_process_info, 
                                           nfold)
                
                smape_mean = self.output['smape_lgb'].mean()
                smape_original_mean = self.output.loc[self.output['program_type']==1, 'smape_lgb'].mean()
                
                self._output_updating([smape_mean, smape_original_mean], params_dict_input)
        else:
            print('lr model does not need tunning')
    
    def _output_updating(self, smape_list, params_dict_input):
        smape_type = [['min_smape_all', 'maximum_smape_all'], 
                                        ['min_smape_original', 'maximum_smape_original']]
        smape_flag = ['min_smape_all_flag','min_smape_original_flag']
        for smape, min_max_name, flag in zip(smape_list, smape_type, smape_flag):            
            if smape < self.parameter_tunning_stats[min_max_name[0]]:
                self.parameter_tunning_stats[min_max_name[0]]  = smape
                self.min_smape_param[min_max_name[0]] = params_dict_input
                self.parameter_tunning_stats[flag] = True
                
            if smape > self.parameter_tunning_stats[min_max_name[1]]:
                self.parameter_tunning_stats[min_max_name[1]]  = smape
                

    def _parameter_tunning_init(self, params_tunning_dict, model_name):
        self.parameter_tunning_stats = {}
        self.parameter_tunning_stats['min_smape_all'] = np.inf
        self.parameter_tunning_stats['maximum_smape_all'] = -np.inf
        self.parameter_tunning_stats['min_smape_original'] = np.inf
        self.parameter_tunning_stats['maximum_smape_original'] = -np.inf
        self.parameter_tunning_stats['min_smape_all_flag'] = False
        self.parameter_tunning_stats['min_smape_original_flag'] = False
        self.min_smape_param = {}
        
        return self._gen_combinations(params_tunning_dict, model_name)
        
    
    def _gen_combinations(self, params_tunning_dict, model_name):
        params_dict = params_tunning_dict[model_name]
        allNames = params_dict.keys()
        combinations = list(it.product(*(params_dict[Name] for Name in allNames)))
        return combinations, allNames

    def _set_params_dict(self, model_name, allNames, com):
        params_dict = {}
        params_dict[model_name] = {}
        for key, cnt in zip(allNames, range(len(allNames))):
            params_dict[model_name][key] = com[cnt]
            
        return params_dict
        
        
    
    
    