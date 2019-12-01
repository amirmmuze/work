#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  6 19:08:39 2019

@author: amirdavidoff


this scripts accepts a path to conversation_sumeries table (tsv format), adds new field, saves summary plots and new df to destination

the plots are groupby by retailer id


example execution:
    
    spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/enrich_conv_summ.py --conv-source /Users/amirdavidoff/Desktop/data/yaron_sql_dumps/conversation_summaries.csv --dest-df /Users/amirdavidoff/Desktop/data/enriched_data/conv_summ_proc.tsv
    
    
plots that it writes:
    
    1. plot transaction amount
    2. plot retailer id distribution
    3. activity by time
    4. plot binaries (is clicked, is add to cart, is convertion, etc)
    5. num of msg's 
    
    
adds the following fields:
    
        products_found', 'product_clicked', 'add_to_cart', 'search_exit',
       'ga_product_clicked', 'ga_add_to_cart', 'ga_converted', 'bad_quality',
       'platform', 'device', 'os', 'app', 'year', 'month', 'year_month',
       'created_at_dt', 'day', 'hour',num_of_messages_int
    

"""




import matplotlib.pyplot as plt
from user_agents import parse


from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame, SparkSession
import argparse
import numpy as np

import pandas as pd





def enrich_conv(convp,path):
    
    '''
    adds the following fields to df:
        
        'products_found', 'product_clicked', 'add_to_cart', 'search_exit',
       'ga_product_clicked', 'ga_add_to_cart', 'ga_converted', 'bad_quality',
       'platform', 'device', 'os', 'app', 'year', 'month', 'year_month',
       'created_at_dt', 'day', 'hour'
        
    '''
    
    
     # filtering by organization, can do it by percentage or hard coded
    retailer_counts = convp["retailer_id"].value_counts().to_frame('count')
    retailer_counts["percent"] = retailer_counts["count"] / np.sum(retailer_counts["count"])
    
    #percentage
    retailer_counts = retailer_counts[retailer_counts["percent"]>=0.05]
    
    #hard coded
    retailer_counts = retailer_counts[retailer_counts.index.isin(['962','429'])]
    orgs = retailer_counts.index.tolist()
    
    #filter
    convp = convp[convp["retailer_id"].isin(orgs)]
    
    
    # extract fields from string json's
    import json
    convp["mmuze_status_dict"]  = convp["mmuze_status"].apply(json.loads) 
    convp["channel_dict"]  = convp["channel"].apply(json.loads) 
    
    #convp["intents_dict"]  = convp["intents"].apply(json.loads) 
    # adds 100 columns
    
    
    
    convp = pd.concat([convp.drop(['mmuze_status_dict'], axis=1), convp['mmuze_status_dict'].apply(pd.Series)], axis=1)
    #convp = pd.concat([convp.drop(['intents_dict'], axis=1), convp['intents_dict'].apply(pd.Series)], axis=1)
    
    convp = pd.concat([convp.drop(['channel_dict'], axis=1), convp['channel_dict'].apply(pd.Series)], axis=1)

    
    
    # cast     
    convp[['transaction_value']] = convp[['transaction_value']].replace(["NULL"],['0']).astype(float)
    
    
    convp["year"] = convp['created_at'].apply(lambda s : s[0:4])
    convp["month"] = convp['created_at'].apply(lambda s : s[5:7])
    convp["year_month"] = convp["year"]+"_" + convp["month"]
    
    
    convp["created_at_dt"] = pd.to_datetime(convp['created_at'], errors='coerce')
    convp["day"]  =convp["created_at_dt"].dt.weekday_name
    convp["hour"]  =convp["created_at_dt"].dt.hour
    
    convp["num_of_messages_int"] = np.where(convp["num_of_messages"]=="NULL",0,convp["num_of_messages"])
    convp["num_of_messages_int"]=convp["num_of_messages_int"].fillna(0).astype(int)
    
    
    #binary and casting
    convp["is_get_ga"] = np.where(convp["ga_session_id"]=="NULL",0,1)
    convp["is_returning_customer"] = np.where(convp["customer_type"]=="Returning Visitor",1,0)
    convp["is_new_visitor"] = np.where(convp["customer_type"]=="New Visitor",1,0)
    
    
    #% for there i want number of unique plus freq of most freq
    binary_cols = ['products_found','product_clicked', 'add_to_cart', 'search_exit',
       'ga_product_clicked', 'ga_add_to_cart', 'ga_converted', 'bad_quality']
    
    for c in binary_cols:
        convp[c] = convp[c].fillna(0)
        convp[c] = convp[c].astype(int)
    
    convp.to_csv(path,sep='\t')
    
    

    
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description='conversation_summaries visualization')
    parser.add_argument('--conv-source', help='convtsv source')
    parser.add_argument('--dest-df', help='destination to write df to')


    args = parser.parse_args()

    conf = (
        SparkConf()
            .setAppName("Spark Submit:" + __file__)
        #.set("spark.executor.memory", "1g")
    )

    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    
    
    conv = sqlContext.read.options(header=True,quote = '"',escape='"').csv(args.conv_source) #,quote = '"',escape='"'
   
    conv = conv.filter(F.to_date("created_at") >= F.lit("2019-07-01"))
    conv.count()
    
    
    convp = conv.toPandas()
    
    enrich_conv(convp,args.dest_df)
    
   