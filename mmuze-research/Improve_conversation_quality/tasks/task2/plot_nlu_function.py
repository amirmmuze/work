#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  7 14:20:24 2019

@author: amirdavidoff


joins nlu enriched with api msgs and conv summaries enriched.


plots nlu. bar plots and comparison between first and last nlu information obtained.


example execution:
    
    spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task2/plot_nlu_function.py --retailer-id 429 --date 2019-07-01 --nlu-source /Users/amirdavidoff/Desktop/data/enriched_data/nlu --conv-summ-source /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/conv_summ_proc.csv --api-msgs-source /Users/amirdavidoff/Desktop/data/yaron_sql_dumps/api_messages.csv --dest /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task2/function_testing

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



def binary_ci_pd(ag):
    ''' confidence interval for binary feature for pandas'''
    
    #print(ag)
    
 #   mean = np.mean(ag)
  #  n = len(ag)
  
    mean = ag[0]
    n = ag[1]
    
    return 1.96 * np.sqrt(mean*(1-mean)/n)


def plot_nlu(retailer,date,nlu_enr_path,conv_summ_path,api_msg_path,dest_path):
    
    '''
    
    joins nlu with api msgs and conv summaries.


    plots nlu. bar plots and comparison between first and last nlu information obtained.
    
    retailer,date are strings used to filter the data
    
    
    '''
    
    
    
    ''' nlu enriched'''
    nlu_enr =  sqlContext.read.parquet(nlu_enr_path) #'/Users/amirdavidoff/Desktop/data/enriched_data/nlu'
 #   nlu_enr.count()
    #835833
    
    nlu_enr = nlu_enr.filter((F.to_date("date") >= F.lit(date))&(nlu_enr.retailer_id==retailer))
 #   nlu_enr.count()
    # 176957
    
    
    
    # count number of ga that have morethen one sender id (only 1 convs !)
    
  #  my_id_that_have_one_sender_id = nlu_enr.groupBy('my_id').agg(F.countDistinct('sender_id').alias('count')).where(F.col('count')==1)
  
  #  my_id_that_have_one_sender_id.count()
    #31751
    
    
    # colomn rename
  #  my_id_that_have_one_sender_id = my_id_that_have_one_sender_id.withColumnRenamed('my_id','my_id2')
    
    #this filters
   # nlu_enr = nlu_enr.join(my_id_that_have_one_sender_id,nlu_enr.my_id==my_id_that_have_one_sender_id.my_id2)
 #   nlu_enr.count()
    # 73622
    
    
    # add rank
    window = Window.partitionBy("sender_id").orderBy(["timestamp"])
    window_no_order = Window.partitionBy("sender_id")
    
    nlu_enr = nlu_enr.withColumn('rank',F.rank().over(window))
    
    
    # subsetting
    cols_to_take_as_nlu = ['intents_list','is_sku','age_group','gender','subvertical','positive_aspects','positive_product_type','positive_brands',
                           'negative_aspects','negative_brands','negative_product_type','product_search_result_len']
    
    
    nlu_enr2 = nlu_enr.select(['timestamp','date','sender_id','rank']+cols_to_take_as_nlu)
    
    nlu_enr2 = nlu_enr2.withColumn('max_rank',F.max('rank').over(window_no_order))
    
    
    ''' conv summary'''
    # read conversation sumeries tsv
    conv = sqlContext.read.options(header=True,sep='\t').csv(conv_summ_path)#'/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/conv_summ_proc.csv' ,sep='\t'
    #109049
    
    # filer july and 429
    conv = conv.filter((F.to_date("created_at") >= F.lit(date))&(F.col('retailer_id')==retailer))
  #  conv.count()
    # 57361
    
  #  conv.dropDuplicates(['conversation_id']).count()
    #57631
    
    
    
    ''' api msg's '''
    api_msg = sqlContext.read.options(header=True).csv(api_msg_path)#'/Users/amirdavidoff/Desktop/data/my_sql_export/api_messages2.txt' ,sep='\t'
    
  #  api_msg.count()
    #2131712
    api_msg = api_msg.where(F.col('retailer_id')==retailer)
    # 537163
    
    api_msg = api_msg.dropDuplicates(['conversation_id','user_id'])
    
    
    for c in api_msg.columns:
        
        api_msg = api_msg.withColumnRenamed(c,"api_msg_{}".format(c))
    
    
    
    ''' join conv and apit msg'''
    conv_api_msg = conv.join(api_msg,conv.conversation_id==api_msg.api_msg_conversation_id,how='left')
  #  conv_api_msg.count()
    # 57361
    
    
    
    cols = ['retailer_id','api_msg_retailer_id','api_msg_conversation_id','conversation_id',
            
     'customer_type',
     'num_of_messages_int',
     'api_msg_user_id',
     'api_msg_mid','ga_converted','ga_add_to_cart','ga_product_clicked','add_to_cart','conversation_media']
    
    # SWITCH THE STR COLS TO BINARY
    
    conv_api_msg = conv_api_msg.select(cols)
    
   # conv_api_msgp = conv_api_msg.toPandas()
    
    
    ''' join previosu with nlu - first nlu '''
    
    final = nlu_enr2.where(nlu_enr2.rank==1).join(conv_api_msg,conv_api_msg.api_msg_user_id==nlu_enr2.sender_id)
    
  #  final.dropDuplicates().count()
    # 30488
  #  finalp = final.dropDuplicates().toPandas()
    
    
    # WHEN BACK > ADD INTENTS LIST, RUN ALL, SUSBET COLUMNS TO NEEDED, RUN NALYSIS 
    
    # when back, can i catch and then lean all the duplicate short msg's ?
    
    ''' join previosu with nlu - last nlu '''
    
    last = nlu_enr2.where(nlu_enr2.rank==nlu_enr2.max_rank).join(conv_api_msg,conv_api_msg.api_msg_user_id==nlu_enr2.sender_id)
    
  #  last.dropDuplicates().count()
    # 30962
  #  lastp = last.dropDuplicates().toPandas()
    
    
    
    
    
    # INSTEAD OF SPLITTING AND THEN JOINING FIRST AND LAST DO SOMETHING LIKE THIS
    #temp = nlu_enr2.where(nlu_enr2.max_rank>1)
    ##52170
    #jnd = temp.join(conv_api_msg,conv_api_msg.api_msg_user_id==temp.sender_id)
    ##62741
    #
    #jnd = jnd.where((jnd.rank==1 )|( jnd.rank==jnd.max_rank))
    ##25800
    
    
    
    
    
    cols_to_examine = ['intents_list',
     'is_sku',
     'age_group',
     'gender',
     'subvertical',
     'positive_aspects',
     'positive_product_type',
     'positive_brands',
     'negative_aspects',
     'negative_brands',
     'negative_product_type',
     'product_search_result_len',
     'retailer_id','customer_type','num_of_messages_int','conversation_media']
    
    
    
    
    cast_list = F.udf(lambda x : [int(i) for i in x],ArrayType(IntegerType()))
    
    
    spark_df={}
    spark_df["first_nlu"] = final.dropDuplicates(['timestamp','sender_id'])
    spark_df["last_nlu"] = last.dropDuplicates(['timestamp','sender_id'])
    
    
    ''' plot rate (add to cart ,conversion) per value'''
    
    target_columns = ['add_to_cart','ga_add_to_cart','ga_converted','ga_product_clicked']
    agg_fields = []
    agg_fields.extend([F.mean(F.col(col).cast(IntegerType())).alias((col + "_rate")) for col in target_columns])
                    
    
    for path in spark_df:
        
        
        try:
            os.mkdir(dest_path+'/{}_rates/'.format(path))
        except:
            pass
    
        temp = spark_df[path]
        
        df_dic={}
        
        for c in cols_to_examine:
            
            df_dic[c] = temp.groupBy(c).agg(
                    
                    
                                        
                    F.count(c).alias('count'),
                    *agg_fields
                    
                    
                    ).toPandas()
            
            
            for i in target_columns:

                df_dic[c][i+"_ci"] = df_dic[c][[i+"_rate","count"]].apply(binary_ci_pd,axis=1)

            df_dic[c]["ratio"] = df_dic[c]["count"] / np.sum(df_dic[c]["count"])

            
        MIN_RATIO = 0.03
        
        ''' plot the different rate's for each values'''
        
        for k in target_columns:
            
            for key in df_dic:
            
                df = df_dic[key]
            
                df = df[df["ratio"] >= MIN_RATIO]
            
                df = df.sort_values(k+"_rate",ascending=True)
                
            
                fig = plt.figure(figsize=(6,6))
            
            
                plt.errorbar([i for i in range(len(df[k+"_rate"]))], df[k+"_rate"].values, yerr=df[k+"_ci"].values, fmt='o')
                plt.xticks([i for i in range(len(df[k+"_rate"]))], df[key],rotation=30)
        
                plt.title(path+" "+key+" "+k)
                plt.ylabel(k+'_rate')
                plt.xlabel(key)
                plt.savefig(dest_path+'/{}_rates/{}_{}.png'.format(path,k,key))  #'/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task2
                plt.show(block=False)
                plt.close(fig)
        
        # when back think how to compare first nlu to last nlu here. and 1050 clock msg's
        # compare and add ro word report with insights.
    
        
    ''' plot bar plots '''
    
    #takes only convs that have more then 1 msg
    
    final = final.withColumn("source",F.lit("first"))
    final = final.where(final.max_rank>1)
    #12663
    
    
    last = last.withColumn("source",F.lit("last"))
    last = last.where(last.max_rank > 1)
    #13137
    
    
    # there are conv's where the "user" presses new conv, that initialize a new conv id, but same user\sender id
    mrg = final.dropDuplicates(['timestamp','sender_id']).union(last.dropDuplicates(['timestamp','sender_id']))
    
    
    
    
    cols_to_examine = ['intents_list',
     'is_sku',
     'age_group',
     'gender',
     'subvertical',
     'positive_aspects',
     'positive_product_type',
     'positive_brands',
     'negative_aspects',
     'negative_brands',
     'negative_product_type',
     'product_search_result_len',
     'retailer_id','customer_type','num_of_messages_int','conversation_media']
            
    try:
        os.mkdir(dest_path+'/combined_bar_plots')
    except:
        pass
    
    for c in cols_to_examine:
        
        grp = mrg.groupBy(["source",c]).agg(
                        
                        F.count(c).alias('count'),
           
                        ).toPandas()
        
        # adding column ratio that represents the share of count per source (first / last nlue)        
        grp = grp.assign(ratio=grp['count'].div(grp.groupby('source')['count'].transform('sum')))
        

        # values below this ratio would not show in the bar plots.
        MIN_RATIO = 0.01
      
        ''' plot  '''
        
        grp = grp[grp["ratio"]>=MIN_RATIO]
        
        
        fig = plt.figure(figsize=(12,12))
    
        for src in ['first','last']:
    
            
        
            temp = grp[grp["source"]==src]
            temp = temp.sort_values("ratio",ascending=True)
            
            plt.barh(temp[c].astype(str).tolist(),temp['ratio'],label=src+"_nlu",alpha=0.5)
            plt.title(c+"_nlu \n(only shwoing values ratioed >{}%)".format(MIN_RATIO*100))
            plt.legend()
        
        plt.savefig(dest_path + '/combined_bar_plots/{}.png'.format(c))
        plt.show(block=False)    
        plt.close(fig)
    
    
    





if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='joins nlu - api_msgs - conv_summaries. \nplot nlu plots. conversion rates and bar plots. currently works per retailer id. ')
    parser.add_argument('--retailer-id', help='retailer id to filter by',type=str)
    parser.add_argument('--date', help='date to filter by (considers events that are after this date)',type=str)

    parser.add_argument('--nlu-source', help='nlu tsv df source path')
    parser.add_argument('--conv-summ-source', help='conversation summaries tsv df source path')
    parser.add_argument('--api-msgs-source', help='api msgs tsv df source path')
    
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
    plot_nlu(args.retailer_id,args.date,args.nlu_source,args.conv_summ_source,args.api_msgs_source,args.dest)
    e=time.time()
    
    print("took: {}".format(e-s))
    









