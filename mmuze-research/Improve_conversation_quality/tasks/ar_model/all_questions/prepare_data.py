#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 10 13:56:35 2019

@author: amirdavidoff



model data preperation


inputs:
    enriched jnd df
    enriched conversation summaries
    api msgs

    
outputs:
    df
    cat_dist_pd
    cat_distinct_pd
    cat_dist_pd_after_replacement

spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/all_questions/prepare_data.py --retailer-id 429 --jnd-source /Users/amirdavidoff/Desktop/data/enriched_data/jnd --conv-summ-source /Users/amirdavidoff/Desktop/data/enriched_data/conv_summ_proc.tsv --api-msgs-source /Users/amirdavidoff/Desktop/data/yaron_sql_dumps/api_messages.csv --dest /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/all_questions/op


TODO:
    
    1. there are empty values in os app device that go to other along the way instead of empty. check resulted csv's

"""



import matplotlib.pyplot as plt
import pandas as pd 
import numpy as np
from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})


from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame, SparkSession
import argparse

import os 
from pathlib import Path
home = str(Path.home())

import sys 

sys.path.append('{}/mmuze-research'.format(os.getenv("HOME")))


print(sys.path)


print(os.getcwd())

from Improve_conversation_quality.preprocessing.functions.generic_imports import *


from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})



def prepare_data(jnd_path, msg_api_path, conv_summ_path, retailer, dest):
    
    '''
    function that prepares the data for the following model script train_model.py.
    
    jnd_path is the path to the nbr_enriched nlu_enriched joined table. 
    msg_api_path is the path to the api_msgs table.
    conv_summ_path is the path to the conversation sumamries enriched table
    
    retailer is the retailer to filter by.
    
    dest is the path to write the results.
    
    returns: df, cat_dist_pd, cat_distinct_pd, cat_dist_pd_after_replacement.
    
    '''
    
    jnd = sqlContext.read.parquet(jnd_path) #'/Users/amirdavidoff/Desktop/data/enriched_data/jnd'
    

    
    ''' yes no '''
    
    qs =jnd.select(['nbr_response_code','nbr_possible_answers']).dropDuplicates(['nbr_response_code']).toPandas()
    
    yes_no = qs[qs["nbr_possible_answers"].astype(str)=="['yes', 'no']"]["nbr_response_code"].tolist()
     
    
    
    nlu_cols = ['nlu_intents_list',
     'nlu_subvertical',
     'nlu_positive_aspects',
     'nlu_positive_product_type',
     'nlu_positive_brands',
     'nlu_negative_aspects',
     'nlu_negative_product_type',
     'nlu_gender',
     'nlu_age_group']
    
    cols = ['jnd_retailer','jnd_sender_id','jnd_ts','is_answered','nbr_response_code','last_nbr_code','question_rank','time_from_start','sum_answer','num_quick_replies','hour','day_of_week']
    cols = cols + ["last_{}".format(c) for c in nlu_cols]
    
    jnd = jnd.fillna( { 'is_answered':0 } )
    
    df = jnd.where((jnd.nbr_response_code.isNotNull()) & (~jnd.nbr_response_code.isin(yes_no))).select(cols)
    
    
    
    ''' add ga data '''
    conv = sqlContext.read.options(header=True,sep='\t').csv(conv_summ_path) #'/Users/amirdavidoff/Desktop/data/enriched_data/conv_summ_proc.tsv'
    conv_cols = ['retailer_id','conversation_id','customer_type','platform','device','os','app']
    conv = conv.select(conv_cols)
    #71849
    # filer july 
    conv = conv.filter((F.to_date("created_at") >= F.lit('2019-07-01')))
    #71849
    
    ''' api msg's '''
    api_msg = sqlContext.read.options(header=True).csv(msg_api_path)# '/Users/amirdavidoff/Desktop/data/yaron_sql_dumps/api_messages.csv'
    #2186424
    
    
    api_msg = api_msg.dropDuplicates(['conversation_id','user_id'])
    #697511
    
    for c in api_msg.columns:
        
        api_msg = api_msg.withColumnRenamed(c,"api_msg_{}".format(c))
    
    
    ''' join conv and apit msg'''
    conv_api_msg = conv.join(api_msg,conv.conversation_id==api_msg.api_msg_conversation_id,how='left')
    #conv_api_msg.count()
    #71849
    
    
    #conv_api_msgp = conv_api_msg.toPandas()
    
    cols_api_msgs = ['api_msg_user_id','customer_type','platform','device','os','app']
    
    
    conv_api_msg = conv_api_msg.select(cols_api_msgs).dropDuplicates(['api_msg_user_id'])
    
    df = df.join(conv_api_msg,df.jnd_sender_id==conv_api_msg.api_msg_user_id,how="left")
    
    
    ''' filter by retailer id '''
    df = df.where(df.jnd_retailer==retailer)
    
    df.toPandas().to_csv(dest+'/df_raw.csv')
    
    ''' examine cat cols distribution '''
    
    cat_col = ['nbr_response_code','last_nbr_code'] + ["last_{}".format(c) for c in nlu_cols] +['customer_type','platform','os','app','device'] # ,'device'
    num_col = ['question_rank','time_from_start','sum_answer','num_quick_replies']+ ['hour','day_of_week']
    
    for c in num_col:
        
        df = df.withColumn(c,F.col(c).cast(DoubleType()))
    
    for c in cat_col:
        
        df = df.withColumn(c,F.col(c).cast(StringType()))
    
    schema = StructType([StructField("row_number", IntegerType())])
    cat_dist = spark.createDataFrame([[i] for i in range(1,41)],schema=schema)       
        
    for c in cat_col:
        
        temp = df.groupBy(c).count().withColumn('percent_'+c,F.col('count')/df.count())
        window = Window.orderBy(temp["percent_"+c].desc())
        temp = temp.withColumn("row_number",F.row_number().over(window))
    #    temp = temp.where(F.col('percent_'+c)>=0.005)
        temp = temp.where(F.col('row_number')<=40)    
        cat_dist = cat_dist.join(temp,on="row_number",how="left")
    
    
    cat_dist_pd = cat_dist.toPandas()
    
    cat_dist_pd.to_csv(dest+"/cat_dist_pd.csv")
    #cat_dist_pd.sort_values(by='rank')
    
    # insights : remove positive brand and both negatives
    
    rmv = ['last_nlu_positive_brands','last_nlu_negative_aspects','last_nlu_negative_product_type']
    cat_col = [c for c in cat_col if c not in rmv]
    
    
    ''' number of distinct '''
    num_of_distincts=[]
    for c in cat_col:
       num_of_distincts.append(df.agg(F.countDistinct(c)).collect()[0][0])
    
    
    cat_distinct_pd = pd.DataFrame([num_of_distincts],columns=cat_col,index=['#distincts'])
                                   
    cat_distinct_pd.to_csv(dest+"/cat_distinct_pd.csv")
                   
                     
    '''  other  '''
    
    import time
    s = time.time()                               
    for feature in cat_col:
        
        values_to_keep = cat_dist_pd[cat_dist_pd["percent_"+feature]>=0.005][feature].tolist()
        
        df = df.withColumn(feature+"_replaced",F.when(F.col(feature).isin(values_to_keep),F.col(feature)).otherwise(F.lit("other")))
                                   
    ''' new dist '''
    
    schema = StructType([StructField("row_number", IntegerType())])
    cat_dist_after_replacement = spark.createDataFrame([[i] for i in range(1,41)],schema=schema)       
        
    for c in [i+"_replaced" for i in cat_col]:
        
        temp = df.groupBy(c).count().withColumn('percent_'+c,F.col('count')/df.count())
        window = Window.orderBy(temp["percent_"+c].desc())
        temp = temp.withColumn("row_number",F.row_number().over(window))
        #temp.where(temp.rank<=20).show()
        cat_dist_after_replacement = cat_dist_after_replacement.join(temp,on="row_number",how='left')
    
    
    cat_dist_pd_after_replacement = cat_dist_after_replacement.toPandas()
    
    cat_dist_pd_after_replacement.to_csv(dest+"/cat_dist_pd_after_replacement.csv")
    
    
    cat_cols_replaced = [i+"_replaced" for i in cat_col]
    
    
    
    
    y_col = ["is_answered"]
    x_col = cat_cols_replaced + num_col
    
    dfp = df.select(['jnd_sender_id']+y_col+x_col).toPandas()
    
    dfp.dtypes
    
    dfp = pd.concat([dfp.drop(cat_cols_replaced, axis=1), pd.get_dummies(dfp[cat_cols_replaced])], axis=1)
    
    x_col_dummied = [c for c in dfp.columns if c !=y_col[0]]
    
    import re
    regex = re.compile(r"\[|\]|<", re.IGNORECASE)
    
    dfp.columns = [regex.sub("_", col) if any(x in str(col) for x in set(('[', ']', '<'))) else col for col in dfp.columns.values]
    
    x_col_dummied = [c for c in dfp.columns if c !=y_col[0]]
    
    x_col_dummied.remove('jnd_sender_id')
    
    dfp = dfp[dfp["time_from_start"]<=500]
    
    
    dfp.to_csv(dest+'/df.csv')
    
    
    
    




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='prepares df for ar modeling ')
    parser.add_argument('--retailer-id', help='retailer id to filter by',type=str)

    parser.add_argument('--jnd-source', help='jnd source path')
    parser.add_argument('--conv-summ-source', help='conversation summaries tsv df source path')
    parser.add_argument('--api-msgs-source', help='api msgs tsv df source path')
    
    parser.add_argument('--dest', help='destination to write')
    


    args = parser.parse_args()

    conf = (
        SparkConf()
            .setAppName("Spark Submit:" + __file__)
            .set("spark.sql.session.timeZone", "UTC")
           # .set("spark.executor.memory", "6g")
           # .set("spark.driver.memory", "6g")
           # .set("spark.yarn.executor.memoryOverhead", "2g")
    )

    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    

    import time
    
    s=time.time()
    prepare_data(args.jnd_source, args.api_msgs_source, args.conv_summ_source, args.retailer_id, args.dest)
    e=time.time()
    
    print("took: {}".format(e-s))
    
    
    
    
    # spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/all_questions/prepare_data.py --retailer-id 962 --jnd-source /Users/amirdavidoff/Desktop/data/enriched_data/jnd --conv-summ-source /Users/amirdavidoff/Desktop/data/enriched_data/conv_summ_proc.csv --api-msgs-source /Users/amirdavidoff/Desktop/data/yaron_sql_dumps/api_messages.csv --dest /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/all_questions/prepare_data_res
    