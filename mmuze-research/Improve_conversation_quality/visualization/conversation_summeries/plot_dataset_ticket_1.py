#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  6 19:08:39 2019

@author: amirdavidoff


this scripts accepts a path to conversation_sumeries table (tsv format), adds new field, saves summary plots and new df to destination

the plots are groupby by retailer id


example execution:
    
    spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/visualization/conversation_summeries/plot_dataset_ticket_1.py --conv-source /Users/amirdavidoff/Desktop/data/my_sql_export/conversation_summaries.txt --dest-viz /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/visualization/conversation_summeries --dest-df /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/conv_summ_proc.csv
    
    
plots that it writes:
    
    1. plot transaction amount
    2. plot retailer id distribution
    3. activity by time
    4. plot binaries (is clicked, is add to cart, is convertion, etc)
    5. num of msg's 
    
    
adds the following fields:
    
       'product_clicked', 'add_to_cart', 'search_exit',
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




def binary_ci(ag):
    ''' confidence interval for binary feature'''
    
    #print(ag)
    
    mean = np.mean(ag)
    n = len(ag)
    
    return 1.96 * np.sqrt(mean*(1-mean)/n)




def preprocess(convp,path):
    
    '''
    adds the following fields to df:
        
        'product_clicked', 'add_to_cart', 'search_exit',
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
    binary_cols = ['product_clicked', 'add_to_cart', 'search_exit',
       'ga_product_clicked', 'ga_add_to_cart', 'ga_converted', 'bad_quality']
    
    for c in binary_cols:
        convp[c] = convp[c].fillna(0)
        convp[c] = convp[c].astype(int)
    
    convp.to_csv(path,sep='\t')
    
    
    return convp,retailer_counts


def describe_sd(convp,retailer_counts,path):
    
    
    ''' saves plots to path '''
    
    
    orgs = retailer_counts.index.tolist()

    
    
    
    ''' plot transaction amount'''
    #for c in num_cols:
        
     
    #       if c=='transaction_value':
                
    c = 'transaction_value'
    
    grp = convp[convp[c]!=0].groupby('retailer_id').agg({c:["mean","std"]})    
    grp.columns = ["mean","std"]
    
    fig = plt.figure(figsize=(7,7))
    plt.bar(grp.index,grp['mean'],yerr=grp['std'])
    plt.ylabel('average transaction amount',fontsize=12)
    plt.xlabel('retailer_id',fontsize=12)
    plt.title('average transaction amount \nper retailer',fontsize=12)
    plt.savefig('{}/transaction_amount.png'.format(path))

    
    
    ''' plot retailer id distribution '''

    fig = plt.figure(figsize=(7,7))
    
    plt.bar(retailer_counts.index,retailer_counts["count"], lw=2, color='cornflowerblue',label='count')
    
    plt.xlabel('retailer_id',fontsize=12)
    plt.ylabel('conversations coun',fontsize=12)
    plt.title('amount of conversations per retailer \n done after july 19')
    
    plt.text(0.5,50000,"* retailer_id's with less then \n5% freq were dropped \nsuch as 953 288 ")
    #plt.show()
    
    
    plt.savefig('{}/retailer_id_dist.png'.format(path))




    ''' activity by time '''
    # add year_month
    

    
    fig = plt.figure(figsize=(7,7))
    for org in orgs:
        
        
        subset = convp[convp["retailer_id"]==org]
        grp = subset.groupby("year_month").conversation_id.nunique()
        
        
        plt.plot(grp.index,grp,label=org+" conv count: {}".format(subset.shape[0]))
        plt.legend()
        plt.title('number of unique converation ids \nper retailer id',fontsize=12)
        plt.xlabel('year_month',fontsize=12)
        plt.ylabel('count',fontsize=12)
        
    plt.savefig('{}/activity_by_time.png'.format(path))
    
    
    
    for c in ["day","hour"]:
        
        
        grp = convp.groupby(['retailer_id',c],as_index=False).size()
        grp = (grp / grp.groupby(level=0).sum()).reset_index()
        grp.columns = ["retailer_id",c, "percent"]
        
        
        
        
        
        fig = plt.figure(figsize=(7,7))
        grp.pivot(c,"retailer_id", "percent").plot(kind='bar',label=c)
        
        plt.ylabel('{} usage distribution'.format(c),fontsize=12)
        plt.xlabel(c,fontsize=12)
        plt.title('usage percentage by {} \nper retailer'.format(c),fontsize=12)
        plt.savefig('{}/activity_by_{}.png'.format(path,c))
    
    
    
    ''' plot binary '''
    
    binary_cols = ['products_found', 'product_clicked', 'add_to_cart', 'search_exit',
       'ga_product_clicked', 'ga_add_to_cart', 'ga_converted', 'bad_quality']
    
    binary = ['is_get_ga','is_new_visitor','is_returning_customer'] + binary_cols
    
    
    for c in binary:
        
        grp= convp.groupby('retailer_id').agg({c:['mean',binary_ci]})
        grp.columns = ["mean","std"]
        
        
        
        fig = plt.figure(figsize=(7,7))
        plt.bar(grp.index,grp["mean"],yerr=grp["std"],label=c)
        plt.ylabel('{} ratio'.format(c),fontsize=12)
        plt.xlabel('retailer_id',fontsize=12)
        plt.title('percentage of {} \nper retailer'.format(c),fontsize=12)
    
    
    ''' num of msg's '''
    fig = plt.figure(figsize=(7,7))

    for org in orgs:
        
        plt.hist(convp[convp["retailer_id"]==org]["num_of_messages_int"],bins=400, density=True,alpha=0.6,label=org)
        plt.xlim(0,20)
        
    plt.title('num of messages per conversation',fontsize=12)    
    plt.xlabel('num of messages',fontsize=12)
    plt.ylabel('density',fontsize=12)
    plt.savefig('{}/num_of_msg.png'.format(path))

    
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description='conversation_summaries visualization')
    parser.add_argument('--conv-source', help='convtsv source')
    parser.add_argument('--dest-viz', help='destination to write plots to')
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
    
    
    conv = sqlContext.read.options(header=True,sep='\t').csv(args.conv_source) # '/Users/amirdavidoff/Desktop/data/my_sql_export/conversation_summaries.txt'
    #109049
   # conv = sqlContext.read.options(header=True,sep='\t').csv('/Users/amirdavidoff/Desktop/data/my_sql_export/conversation_summaries.txt')
    
    conv = conv.filter(F.to_date("created_at") >= F.lit("2019-07-01"))
    conv.count()
    
    
    convp = conv.toPandas()
    
    convp,retailer_counts = preprocess(convp,args.dest_df)
    
    describe_sd(convp,retailer_counts,args.dest_viz)
    
    
    #describe_sd(convp,'/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/visualization/conversation_summeries')
