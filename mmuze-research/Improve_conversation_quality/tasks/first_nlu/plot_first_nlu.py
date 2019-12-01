#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 12:03:54 2019

@author: amirdavidoff


plots first nlu, not including metyping, bar plots for each retialer.


example execution:
    
    spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/first_nlu/plot_first_nlu.py --date 2019-10-03 --nlu-source /Users/amirdavidoff/Desktop/data/enriched_data/nlu --dest /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/first_nlu




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




def plot_first_nlu(path,date,dest):
    
    '''
    
    plots first nlu, not including metyping, bar plots for each retialer.
    
    '''
    
    window = Window.partitionBy("sender_id").orderBy(["timestamp"])
    
    window_no_order = Window.partitionBy("sender_id")
    
    nlu = sqlContext.read.parquet(path) #'/Users/amirdavidoff/Desktop/data/enriched_data/nlu'
    
    
    nlu = nlu.filter((F.to_date("date") >= F.lit(date)))

    
    
    #sample = nlu.limit(100).toPandas()
    
    
    cols = ['retailer_id','sender_id','timestamp','text','intents_list','subvertical','positive_aspects',
     'positive_brands',
     'positive_product_type',
     'positive_models',
     'negative_aspects',
     'negative_brands',
     'negative_product_type',
     'negative_models',
     'occasion_tags',
     'occasion_tags_len',
     'product_search_result_len',
     'is_sku']
    
    
    nlu = nlu.select(cols)
    
    
    nlu = nlu.withColumn("is_me_typing",F.when(F.lead("text").over(window).contains(F.col('text')),1).otherwise(0))
    
    
    
    nlu = nlu.withColumn("rank",F.rank().over(window))
    
    nlu = nlu.withColumn("max_rank",F.sum("rank").over(window_no_order))
    
    #      0gitluc5pw 
    
    
    nlu = nlu.where(nlu.is_me_typing==0)
    
    
    nlu = nlu.withColumn("rank_after",F.rank().over(window))
    
    nlu = nlu.withColumn("max_rank_after",F.sum("rank_after").over(window_no_order))
    
    nlu = nlu.where(nlu.max_rank_after==1)
    
    
    
    nlup = nlu.toPandas()
    
    cols_to_examine = ['intents_list','subvertical','positive_aspects',
     'positive_brands',
     'positive_product_type',
     'positive_models',
     'negative_aspects',
     'negative_brands',
     'negative_product_type',
     'negative_models',
     'occasion_tags',
     'occasion_tags_len',
     'product_search_result_len',
     'is_sku']
    
    
    
    for c in cols_to_examine:
    
            grp = nlu.groupBy(["retailer_id",c]).agg(
                            
                            F.count(c).alias('count'),
               
                            ).toPandas()
    
    
            
            # adding column ratio that represents the share of count per source (first / last nlue)        
            grp = grp.assign(ratio=grp['count'].div(grp.groupby('retailer_id')['count'].transform('sum')))
            
    
            # values below this ratio would not show in the bar plots.
            MIN_RATIO = 0.005
          
            ''' plot  '''
            
            grp = grp[grp["ratio"]>=MIN_RATIO]
            
            
            fig = plt.figure(figsize=(12,12))
        
            for src in grp["retailer_id"].unique().tolist():
        
                
            
                temp = grp[grp["retailer_id"]==src]
                temp = temp.sort_values("ratio",ascending=True)
                
                plt.barh(temp[c].astype(str).tolist(),temp['ratio'],label=src+"_first_nlu",alpha=0.5)
                
                
                
            plt.title("first nlu (not including me-typing)\n"+c+"_nlu \n(only shwoing values ratioed >{}%)".format(MIN_RATIO*100))
            plt.legend()

            plt.savefig(dest + '/first_nlu_bar_plots/{}.png'.format(c))
            #plt.show(block=False)    
            #plt.close(fig)
            #plt.show()
            
            # FOR SOME REASON SOME PLOTS ONL SAVE FOR ONE RETAILER WHEN RUNNING SCRIPT CHECK THAT OUT LATER !
            
            
            


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='plot fist nlu bar')

    parser.add_argument('--nlu-source', help='nlu tsv df source path')
    parser.add_argument('--date', help='date to filter by (considers events that are after this date)',type=str)
    parser.add_argument('--dest', help='destination to write plots to')
    


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
    plot_first_nlu(args.nlu_source,args.date,args.dest)
    e=time.time()
    
    print("took: {}".format(e-s))