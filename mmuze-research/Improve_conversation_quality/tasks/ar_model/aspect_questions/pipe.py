#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 13:21:54 2019

@author: amirdavidoff
"""




import pickle


# aspec uspolo model
model = pickle.load(open("/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/aspect_questions/uspolo/model.pickle","rb"))

features = model.get_booster().feature_names

dfp_raw = pd.read_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/all_questions/production_pipeline/df_raw.csv',index_col=0)  

dfp = pd.read_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/all_questions/uspolo/prepare_data_res/df.csv',index_col=0)


cat_dist_pd_after_replacement = pd.read_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/all_questions/uspolo/prepare_data_res/cat_dist_pd_after_replacement.csv',index_col=0,keep_default_na=False)




nlu_cols = ['nlu_intents_list',
     'nlu_subvertical',
     'nlu_positive_aspects',
     'nlu_positive_product_type']

cat_col = ['nbr_response_code','last_nbr_code'] + ["last_{}".format(c) for c in nlu_cols] +['customer_type','platform','os','app','device'] 
num_col = ['question_rank','time_from_start','sum_answer','num_quick_replies']+ ['hour','day_of_week']


x_raw = [ 'nbr_response_code',
 'last_nbr_code',
 'question_rank',
 'time_from_start',
 'sum_answer',
 'num_quick_replies',
 'hour',
 'day_of_week',
 'last_nlu_intents_list',
 'last_nlu_subvertical',
 'last_nlu_positive_aspects',
 'last_nlu_positive_product_type',
 'last_nlu_positive_brands',
 'last_nlu_negative_aspects',
 'last_nlu_negative_product_type',
 'api_msg_user_id',
 'customer_type',
 'platform',
 'device',
 'os',
 'app']


cat_col_replaced = [c+"_replaced" for c in cat_col]

cat_dist_pd_after_replacement = cat_dist_pd_after_replacement[cat_col_replaced]

import time
import json



'''
ustomer type replace NULL with NULLL

'''

cat_dist_pd_after_replacement["customer_type_replaced"] = cat_dist_pd_after_replacement["customer_type_replaced"].replace(["NULL"],["NULLL"])


possible_qs = ["color_question","fabric_question","fit_question",'neckline_question','sleeve style_question','style_question','shopping_open_question_no_gender']

example = json.loads(dfp_raw.iloc[3000,:].to_json())
#example["last_nlu_positive_product_type"] = "A"
example["options"] = possible_qs





aspects = ['nbr_response_code_replaced_color_question',
           'nbr_response_code_replaced_fabric_question',
           'nbr_response_code_replaced_fit_question',
           'nbr_response_code_replaced_neckline_question',
           'nbr_response_code_replaced_sleeve style_question',
           'nbr_response_code_replaced_style_question',
           ]







'''

what this class needs

    1. what to return when empty, or when error ? what is the default ?
    2. 

'''


possible_qs = ["color_question",
               "fabric_question",
               "fit_question",
               'neckline_question',
               'sleeve style_question',
               'style_question',
               'shopping_open_question_no_gender']


## check differences:

sender_id = []
ans = []
what_was_q = []
what_model_said = []
for i in range(dfp_raw.shape[0]):
    
    
    example = json.loads(dfp_raw.iloc[i,:].to_json())
    example["options"] = possible_qs

    
    if example["nbr_response_code"] in possible_qs:
        print(i)
        
        sender_id.append(example["sender_id"])
        ans.append(example["is_answered"])
        what_was_q.append(example["nbr_response_code"])
        what_model_said.append(pipe.predict(example)[0])
        
        
resd = pd.DataFrame(np.column_stack([ans,what_was_q,what_model_said]),columns = ['ans','what_was_q','what_model_said'])
        
    


pipe = ar_pipe(model, cat_dist_pd_after_replacement, x_raw, cat_col, num_col, nlu_cols)

s = time.time()

res = pipe.predict(example)
print("q to ask : {}".format(res[0]))
print("all preds : {}".format(res[1]))
e = time.time()

print(e-s)



class ar_pipe():
    
    
    def __init__(self, model, cat_dist_pd_after_replacement, x_raw, cat_col, num_col, nlu_cols):
        
        self.model = model
        self.cat_dist_pd_after_replacement = cat_dist_pd_after_replacement
        self.x_raw = x_raw
        self.cat_col = cat_col
        self.num_col = num_col
        self.nlu_cols = nlu_cols
        self.features = model.get_booster().feature_names


    def predict(self, json_example):
        
        ''' check keys '''
        
        
        diff = list(set(self.x_raw) -set(json_example.keys()))
        if len(diff)!=0:
            
            raise Exception('json_example dosent have the keys {}'.format(str(diff)))
             
        
        
        ''' check types for num'''
        for c in self.num_col:
            
            # None could be for sum answer first row
            if json_example[c] is not None:
                    
                    json_example[c] = float(json_example[c])
                    
            else:
                json_example[c] = np.nan
    
        
        
        ''' replace with other, dummy for cat, and predict'''
        list_of_dicts = []
        
        
        for i in range(len(json_example["options"])):
            
            rmv = ('options')
            temp = {x: json_example[x] for x in json_example.keys() if x not in rmv}
        
            temp["nbr_response_code"] = json_example["options"][i]
        
        
        
            for key in self.cat_col:
                
                if str(temp[key]).replace("'","") not in set(self.cat_dist_pd_after_replacement[key+"_replaced"]):
                    
                    temp[key] = "other"
                
                temp[key+"_replaced"] = str(temp[key]) # makes sure we are dealing with str         
            list_of_dicts.append(temp)
                
        
        # list of dicts to pd df
        examplep = pd.DataFrame(list_of_dicts)
        
        # gets dummies for what we have
        examplep = pd.concat([examplep[self.num_col],pd.get_dummies(examplep[[c+"_replaced" for c in self.cat_col]])],axis=1)
        
        # adds missing columns and fills them with 0, setting the order as well.
        examplep = examplep.reindex(columns=self.features, fill_value=0)
        
        
        preds = self.model.predict_proba(examplep[self.features])[:,1]
        
        
        return example["options"][np.argmax(preds)], preds





