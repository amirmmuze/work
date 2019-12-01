#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 14:40:14 2019

@author: amirdavidoff
"""
#%%
''' jnd example '''
cols = ['jnd_retailer','nlu_positive_product_type','lag_type','nlu_intents_list','nlu_positive_aspects','nlu_text','nlu_date','is_answered','nbr_date','nbr_ack_text','nbr_possible_answers','nbr_response_code','time_from_start','question_rank','sum_answer','jnd_sender_id','jnd_ts']
delete = jnd2.where(jnd2.jnd_sender_id=='1l5aq45x9y').select(cols).toPandas()
#000f20b8le

#%%
#jnd = sqlContext.read.parquet(jnd_path)  #'/Users/amirdavidoff/Desktop/data/enriched_data/jnd'

''' jnd example '''
cols = ['jnd_retailer','nlu_positive_product_type','nlu_intents_list','nlu_positive_aspects','nlu_text','nlu_date','is_answered','nbr_date','nbr_ack_text','nbr_possible_answers','nbr_response_code','time_from_start','question_rank','sum_answer','jnd_sender_id','jnd_ts']
delete1 = jnd.where(jnd.jnd_sender_id=='dkkk7gp5pxi').select(cols).toPandas()
#000f20b8le

#%%
''' jnd example '''

jnd = sqlContext.read.parquet(jnd_path)  #'/Users/amirdavidoff/Desktop/data/enriched_data/jnd'

cols = ['jnd_retailer','nlu_positive_product_type','nlu_intents_list','nlu_positive_aspects','nlu_text','nlu_date','is_answered','nbr_date','nbr_ack_text','nbr_possible_answers','nbr_response_code','time_from_start','question_rank','sum_answer','jnd_sender_id','jnd_ts']


window = Window.partitionBy("jnd_sender_id").orderBy(["jnd_ts"])




jnd = jnd.withColumn("qs",F.collect_list(F.lag(('nbr_response_code')).over(window)).over(window))


def pad(ls):
    
    if len(ls)>=3:
        return ls[-3:]
    else:
        [0 for i in range(3-len(ls))]+ls
        

pad_udf = F.udf(pad)

delete1 = jnd.where(jnd.jnd_sender_id.isin(ids)).select(cols+['qs']).toPandas()


#%%
jnd = sqlContext.read.parquet(jnd_path)  #'/Users/amirdavidoff/Desktop/data/enriched_data/jnd'
cols = ['jnd_retailer',
        'nlu_text',
        'nlu_date','is_answered','nbr_date',
        'nbr_ack_text','nbr_possible_answers','nbr_response_code','time_from_start','question_rank','sum_answer','jnd_sender_id','jnd_ts']
delete = jnd.where(jnd.jnd_retailer=='962').select(cols).toPandas()
delete = delete.sort_values(['jnd_sender_id','jnd_ts'])




#%%