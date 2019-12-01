#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 14:18:59 2019

@author: amirdavidoff



visualize nbr enriched data

execution example:
    
    spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/visualization/nbr/nbr_enriched_viz.py --nbr-source /Users/amirdavidoff/Desktop/data/enriched_data/nbr --dest /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/visualization/nbr


saves the following plots:
    
    1. hist of conversation time in seconds.
    2. number of nbr responses per conversation
    3. hist of time diff between each response
    4. bar plot of response code
    5. activity by time

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

from random import randint







def plt_nbr(nbr_enriched,path,orgs = ['429','962','901']):
    
    
    '''
    saves several simple plots to path.
    
    '''
    
    
    
    ''' plot hist of conversation tiem in seconds  '''
    
    fig = plt.figure(figsize=(7,7))

    
    for i,org in enumerate(orgs):
        
    
        subset = nbr_enriched.where(nbr_enriched.retailer_id==org)
        
        grp = subset.groupBy(['sender_id','conv']).agg((F.max('timestamp') - F.min('timestamp')).alias('conv_time')).toPandas()
        
        
        if org == '429':
            plt.hist(grp['conv_time'],bins=2700,label=str('org {} total mean {}').format(org,round(np.mean(grp['conv_time']),3)),density=True,alpha=0.6)
            
        else:
            plt.hist(grp['conv_time'],bins=1700,label=str('org {} total mean {}').format(org,round(np.mean(grp['conv_time']),3)),density=True,alpha=0.6)
        
        
        plt.xlim(0,200)
        
    plt.xlabel('nbr conv time in seconds')
    plt.ylabel('percent of conversations')
    plt.title('nbr conversation time hist per org')
    plt.legend()
    #plt.show()    
    plt.savefig('{}/nbr_conv_time.png'.format(path))

    
    
    fig = plt.figure(figsize=(7,7))

    
    '''  plot number of nbr responses per conversation'''
    
    for i,org in enumerate(orgs):
        
        subset = nbr_enriched.where(nbr_enriched.retailer_id==org)
    
        grp = subset.groupBy(['sender_id','conv']).count().toPandas()
        
        plt.hist(grp['count'],bins=100,label=str('org {} total mean {}').format(org,round(np.mean(grp['count']),3)),density=True,alpha=0.6)
            
        plt.xlim(0,30)
        
    plt.xlabel('nbr responses in conversation')
    plt.ylabel('percent of conversations')
    plt.title('nbr conversation length hist per org')
    plt.legend()
    #plt.show()
    plt.savefig('{}/nbr_number_of_responses.png'.format(path))

    
    ''' plot hist of time diff between each response '''
    
    
    fig = plt.figure(figsize=(7,7))

    
    for org in orgs:
        
        subset = nbr_enriched.where((nbr_enriched.time_diff<21600)&(nbr_enriched.time_diff.isNotNull())&(nbr_enriched.retailer_id==org))
    
        grp = subset.groupBy(['sender_id','conv']).agg(F.mean(F.col('time_diff')).alias('mean_time_diff_in_conv')).toPandas()
        
        if org =='429':
            plt.hist(grp['mean_time_diff_in_conv'],bins=2700,label=str('org {} total mean {}').format(org,round(np.mean(grp['mean_time_diff_in_conv']),3)),density=True,alpha=0.6)
        else:
            plt.hist(grp['mean_time_diff_in_conv'],bins=1700,label=str('org {} total mean {}').format(org,round(np.mean(grp['mean_time_diff_in_conv']),3)),density=True,alpha=0.6)
        plt.xlim(0,100)
        
        
        
    plt.xlabel('nbr avg time diff in seconds')
    plt.ylabel('percent of conversations')
    plt.title('nbr avg time diff between responses in seconds ')
    plt.legend()
    #plt.show()
    plt.savefig('{}/nbr_avg_time_diff.png'.format(path))

    
    
    
    ''' plot bar plot of response code '''    
    
    
    temp = nbr_enriched.where(nbr_enriched.retailer_id.isin(orgs))
    grp = temp.groupBy(['response_code']).count().toPandas().sort_values('count')
    
    
    grp = grp[grp["count"]>100]
    
    grp = grp.reset_index()
    
       
    
    fig = plt.figure(figsize=(35,15))
    plt.barh(grp.index.tolist(),grp['count'],label=org)
    plt.yticks(grp.index.tolist(),grp['response_code'],fontsize=18)
    
    plt.title('nbr response code frequency',fontsize=30)
    #plt.show()
    plt.savefig('{}/nbr_response_code.png'.format(path))

    
    ''' plot activity by time'''
    
    for c in ["year_month","day_of_week_str","hour"]:
        
        fig = plt.figure(figsize=(10,10))  
    
        for org in orgs:
            
            subset = nbr_enriched.where(nbr_enriched.retailer_id==org)
    
    
          
            grp = subset.groupBy([c]).agg(F.countDistinct('sender_id','conv').alias('count'))
            grp = grp.orderBy(c)
            grpd = grp.toPandas()
            grpd["percentage"] = grpd["count"]/np.sum(grpd["count"])
        
            
            
            
            plt.plot(grpd[c], grpd["percentage"], lw=2,label=org) 
     
        
        
        title = 'percentage of unique conversations by {} (utc)'.format(c)
    
        plt.legend(loc="upper right")
        plt.xlabel(c,fontsize=15)
        plt.ylabel('percentage',fontsize=15)
        plt.title(title,fontsize=15)
        #plt.show()
        plt.savefig('{}/nbr_activity_by_{}.png'.format(path,c))






if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='nbr visualization')
    parser.add_argument('--nbr-source', help='nbr enriched  source')
    parser.add_argument('--dest', help='destination to write plots to')


    args = parser.parse_args()

    conf = (
        SparkConf()
            .setAppName("Spark Submit:" + __file__)
        #.set("spark.executor.memory", "1g")
    )

    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    
 
    nbr_enriched = sqlContext.read.parquet(args.nbr_source)
   
    
    plt_nbr(nbr_enriched,args.dest)




    #path = '/Users/amirdavidoff/Desktop/data/enriched_data/nbr'

#nbr_enriched = sqlContext.read.parquet(path)

#plt_nbr(nbr_enriched,'/Users/amirdavidoff/Desktop/data/viz/nbr')