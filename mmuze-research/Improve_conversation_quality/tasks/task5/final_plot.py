#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 12:24:04 2019

@author: amirdavidoff



calculate and plot naswer rate

exampel execution:
    
    spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task5/final_plot.py --jnd-source /Users/amirdavidoff/Desktop/data/enriched_data/jnd --sdk-source /Users/amirdavidoff/Desktop/data/yaron_sql_dumps/sdk_reports.csv --dest /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task5/final_plots2


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


def collect_values(ls):
    
    try:
        
        return [c[1] for c in ls]
    
    except:
        
        None

collect_values_udf = F.udf(collect_values,ArrayType(StringType()))


def binary_ci_pd(ag):
    ''' confidence interval for binary feature for pandas'''
    
    #print(ag)
    
 #   mean = np.mean(ag)
  #  n = len(ag)
  
    mean = ag[0]
    n = ag[1]
    
    return 1.96 * np.sqrt(mean*(1-mean)/n)

def binary_ci(ag):
    ''' confidence interval for binary feature'''
    
    #print(ag)
    
    mean = np.mean(ag)
    n = len(ag)
    
    return 1.96 * np.sqrt(mean*(1-mean)/n)


def check_isin(lead_nlu_text,
                   lead_nlu_text2,
                   possible_values,
                   question_code,
                   lag_positive_aspects,
                   lead_positive_aspects,
                   lag_subvertical,
                   lead_subvertical,
                   lag_pos_product_type,
                   lead_pos_product_type,
                   lead_pos_product_type2):
    
        ''' custom function that checks whether a q was answered'''
        
        try:
            
            # check if response value is in quick replies
            if (lead_nlu_text in possible_values) or (lead_nlu_text2 in possible_values):
                return 1
            
            if (question_code=='color_question') and ('color' not in lag_positive_aspects) and ('color' in lead_positive_aspects):
                return 1
            
            if (question_code=='subvertical_selection' or question_code=='subvertical_selection_second') and (lag_subvertical is None) and (lead_subvertical is not None):
                return 1
            
            if (question_code=='product_type_selection') and (lag_pos_product_type is None) and ((lead_pos_product_type is not None) or (lead_pos_product_type2 is not None)):
                return 1
    
            else:
                return 0
        
        except:
            
            None
    
check_isin_udf = F.udf(check_isin,IntegerType())


def calculate_and_plot_ar(jnd_path,sdk_path,dest):


    jnd = sqlContext.read.parquet(jnd_path)  #'/Users/amirdavidoff/Desktop/data/enriched_data/jnd'

    window = Window.partitionBy("jnd_sender_id").orderBy(["jnd_ts"])

    
    ''' plot response code by answer '''
    
    rates = jnd.select(['nbr_response_code','is_answered','jnd_retailer']).toPandas()
    count_codes = rates.groupby(['nbr_response_code','jnd_retailer'],as_index=False).agg(["count"]).reset_index()
    count_codes.columns = ['code','retailer','count']
    
    count_codes.to_csv(dest+'/count_codes.csv')
    
    rates = rates.groupby(['nbr_response_code','jnd_retailer'],as_index=False).agg({"is_answered":["count","mean",binary_ci]})
    #rates["codes"] = rates.index
    rates.columns = ['codes','retailer','is_answered_count','is_answered_mean','is_answered_ci']
    rates = rates.sort_values('is_answered_mean',ascending=True).reset_index(drop=True)
    rates = rates[rates["is_answered_count"]>=50]
    rates["codes"] = np.where(rates["codes"]=="","empty",rates["codes"])
    
    
    fig = plt.figure(figsize=(10,10))
    
    for retailer in rates["retailer"].unique().tolist():
        
        temp = rates[rates["retailer"]==retailer]
        plt.errorbar(temp["is_answered_mean"], temp["codes"], xerr=temp['is_answered_ci'].values, fmt='o',label=retailer,alpha=0.7)
        
        
        
    plt.xlabel('answer rate')
    plt.ylabel('nbr response code')
    plt.title('nbr codes answer rate')
    plt.legend()
    plt.savefig('{}/nbr_code_ctr2.png'.format(dest))  # /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task5
    plt.show(block=False)
    plt.close(fig)    
    
    
    
    
    
    ''' sdk '''
    
    
    #sdk_path = '/Users/amirdavidoff/Desktop/data/yaron_sql_dumps/sdk_reports.csv'
    
    
    spark_sdk  = sqlContext.read.options(header=True).csv(sdk_path) #,sep='\t'
    spark_sdk = spark_sdk.withColumn('date', F.to_date(F.from_unixtime(F.col('timestamp')/F.lit(1000.0))))
    
    spark_sdk = spark_sdk.withColumn('date_time', F.from_unixtime(F.col('timestamp')/F.lit(1000.0)))
    
    spark_sdk = spark_sdk.withColumn("timestamp_int",spark_sdk.timestamp.cast(IntegerType()))
    
    spark_sdk = spark_sdk.where( (F.to_date('date')>= F.lit("2019-07-01")) )
    
    
    sdk_window = Window.partitionBy("user_id").orderBy(["date_time"])
    sdk_window_no_order = Window.partitionBy("user_id")
    
    #sdk = sqlContext.read.parquet('/Users/amirdavidoff/Desktop/data/enriched_data/sdk')
    
    
    
    
    #sdk.count()
    
    #sdk.groupBy('action').count().show(100)
    
    actions = ['click','add to cart']
    
    spark_sdk = spark_sdk.where(spark_sdk.action.isin(actions))
    #sdk.count()
    
    sdk_cols = ['retailer_id','user_id','timestamp','date','date_time','action','value']
    spark_sdk = spark_sdk.select(sdk_cols)
    
    spark_sdk = spark_sdk.withColumn('rank',F.rank().over(sdk_window))
    spark_sdk = spark_sdk.withColumn('max_rank',F.max('rank').over(sdk_window_no_order))
    
    spark_sdk = spark_sdk.withColumn('dates',F.collect_list(F.col('date_time').cast(StringType())).over(sdk_window))
    spark_sdk = spark_sdk.withColumn('actions',F.collect_list('action').over(sdk_window))
    
    spark_sdk2 = spark_sdk.where(spark_sdk.rank==spark_sdk.max_rank)
    
    #sdkp2 = spark_sdk2.toPandas()
    
    
    
    
    
    ''' join jnd and sdk '''
    jnd2 = jnd.join(spark_sdk2.select(['user_id','dates','actions','retailer_id']),spark_sdk.user_id==jnd.jnd_sender_id,how="left")
    #jnd2.count()
    
    
    
    
    def len_clicks(ls):
        
        try:
            return len([c for c in ls if c=='click'])
        except:
            return None
        
    len_clicks_udf = F.udf(len_clicks,IntegerType())
    
    def len_adds(ls):
        
        try:
            return len([c for c in ls if c=='add to cart'])
        except:
            return None
        
    len_adds_udf = F.udf(len_adds,IntegerType())
    
    jnd2 = jnd2.withColumn('clicks',len_clicks_udf(F.col('actions')))
    jnd2 = jnd2.withColumn('adds',len_adds_udf(F.col('actions')))
    
    
    
    ''' add nbr count '''
    jnd_window = Window.partitionBy("jnd_sender_id")
    jnd2 = jnd2.withColumn("nbr_count",F.sum(F.when(F.col('nbr_date').isNotNull(),1).otherwise(0)).over(jnd_window))
    
    
    #jnd2.where(jnd2.nbr_count<=3).select('jnd_sender_id').dropDuplicates().show(100)
    
    #jnd2.where(jnd2.nlu_positive_product_type=='shirt').select('jnd_sender_id').dropDuplicates().show(100)
    
    
    
    
    ''' grp convs '''
    convs_grp = jnd2.groupBy('jnd_sender_id').agg(F.count('nbr_date').alias('nbr_count'),
                             F.sum('is_answered').alias('sum_is_answered'),
                             F.first('clicks').alias('clicks'),
                             F.first('adds').alias('adds'),
                             F.first('jnd_retailer').alias('retailer')).toPandas()
    
    
    convs_grp[['clicks','adds','sum_is_answered']] = convs_grp[['clicks','adds','sum_is_answered']].fillna(0)
    
    
    convs_grp["answers_cut"] = pd.cut(convs_grp.sum_is_answered, [0,1,2,np.Inf], include_lowest=True,right=False)
    convs_grp["answers_cut"].value_counts()
    
    
    
    
    ''' plot answer on click and add '''
    grp = convs_grp.groupby(['answers_cut','retailer'],as_index=False).agg({"clicks":["count","mean",binary_ci],"adds":["count","mean",binary_ci]})
    grp.columns = ['cut','retailer','click_count','click_rate','click_ci','add_to_cart_count','add_to_cart_rate','add_to_cart_ci']
    
    
    
    
    for retailer in ['429']: #convs_grp['retailer'].unique().tolist()
    
        for c in ['click','add_to_cart']:
        
            temp = grp[grp["retailer"]==retailer]
            
            plt.errorbar([i for i in range(len(temp[c+"_rate"]))], temp[c+"_rate"].values, yerr=temp[c+"_ci"].values, fmt='o',label=c+"_"+retailer)
            
            plt.xticks([i for i in range(len(temp[c+"_rate"]))], temp["cut"])
            plt.legend()
               
            
            plt.title("user answer count effect on click / add to cart")
            plt.ylabel('rate')
            plt.xlabel("answer count")
    plt.savefig('{}/answer_click.png'.format(dest)) #/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task5
    plt.show(block=False)
    plt.close(fig)        
    
    
    ''' plot questions for each type'''
    window_type2 = Window.partitionBy(["jnd_sender_id",'lag_type']).orderBy(["jnd_ts"])

    jnd2 = jnd2.withColumn('lag_type',F.lag(F.col('nlu_positive_product_type')).over(window))#F.lit('_'),F.col('code_rank')))
    
    #jnd2 = jnd2.withColumn('lag_type2',F.lag(F.col('nlu_positive_product_type'),2).over(window))#F.lit('_'),F.col('code_rank')))
    
    
    #jnd2 = jnd2.withColumn('lag_type3',F.when(F.col('lag_type').isNull(),F.col('lag_type2')).otherwise(F.col('lag_type')))#F.lit('_'),F.col('code_rank')))
    
    
    #jnd2 = jnd2.withColumn('lag_type_filled',F.when(F.col('nlu_date').isNull(),F.last('nlu_positive_product_type',True).over(Window.partitionBy('jnd_sender_id').orderBy('jnd_ts').rowsBetween(-sys.maxsize, 0))))
    
    #jnd2 = jnd2.withColumn('type_q_rank',F.row_number().over(window_type2))
    
    jnd2 = jnd2.withColumn('type_q_rank2',F.sum(F.when(F.col('nbr_response_code').isNotNull(),1).otherwise(0)).over(window_type2))
    
    jnd2 = jnd2.withColumn('q_rank',F.concat(F.col('type_q_rank2'),F.lit('_'),F.col('nbr_response_code')))
    
    
    
    jnd2 = jnd2.withColumn('lag_nlu_date',F.lag('nlu_date').over(window))
    
    
    cols = ['nlu_positive_product_type','lag_type','nlu_text','nlu_date','is_answered','nbr_date','nbr_response_code','type_q_rank2','q_rank','jnd_sender_id','jnd_ts']
    temp = jnd2.where((jnd2.nbr_response_code.isNotNull()) & (jnd2.lag_type.isNotNull() ) ).select(cols)
    #temp = temp.withColumn('type_q_rank',F.dense_rank().over(window_type2))
    #delete = temp.where(temp.jnd_sender_id=='4rbvkcyxbrg').toPandas()
    
    
    grp = temp.groupBy(['lag_type','q_rank']).agg(
                    
                
                F.count('is_answered').alias('count'),
                F.mean('is_answered').alias('answer_rate')
                
            
            ).toPandas()
    
    
    grp["ci"] = grp[["answer_rate","count"]].apply(binary_ci_pd,axis=1)
    
    grp = grp[grp["count"]>=100]
    
    grp = grp.sort_values(["lag_type","count"],ascending=False)
    
    
    ''' plot types bar plot '''

    
    
    for typ in grp["lag_type"].unique().tolist():
    
        fig = plt.figure(figsize=(10,10))    
    
        temp = grp[grp["lag_type"]==typ]
        temp= temp.sort_values('q_rank',ascending=False)
        
        #temp = temp.sort_values("answer_rate")
        plt.errorbar(temp["answer_rate"], temp["q_rank"], xerr=temp['ci'].values, fmt='o',label=typ,alpha=0.7)
            
        plt.xlabel('answer rate')
        plt.ylabel('order_question')
        plt.title('answer rate per question and order for type {}'.format(typ))
        plt.legend()
       # plt.show()
    
        plt.savefig('{}/{}.png'.format(dest,typ))
        plt.show(block=False)    
        plt.close(fig)
    
    
    
    
    
    
    ''' original plots -  there is a bug check it sometime  bug came after filling answered na with 0  12/11/19'''
    
#    window_type2 = Window.partitionBy(["jnd_sender_id",'lag_type']).orderBy(["jnd_ts"])
#
#    
#    jnd2 = jnd2.withColumn('lag_type',F.lag(F.col('nlu_positive_product_type')).over(window))#F.lit('_'),F.col('code_rank')))
#    jnd2 = jnd2.withColumn('type_q_rank2',F.sum(F.when(F.col('nbr_response_code').isNotNull(),1).otherwise(0)).over(window_type2))
#    
#    jnd2 = jnd2.withColumn('q_rank',F.concat(F.col('type_q_rank2'),F.lit('_'),F.col('nbr_response_code')))
#            
#        
#    
#    original_questions_dic = {}
#    
#    original_questions_dic["shirt"] = ['1_sleeve style_question','2_color_question']
#    original_questions_dic["jeans"] = ['1_fit_question','2_color_question']
#    original_questions_dic["pants"] = ['1_fit_question','2_color_question']
#    original_questions_dic["shorts"] = ['1_fit_question','2_color_question']
#    
#    
#    types = ["shirt",'jeans','pants','shorts']
#    
#    
#    jnd2 = jnd2.withColumn('qs',F.collect_list('q_rank').over(window))
#    
#    for typ in types:
#        
#            
#        jnd2 = jnd2.withColumn('check',
#                (F.array_contains(jnd2.qs, original_questions_dic[typ][0])) &
#                ( F.array_contains(jnd2.qs,  original_questions_dic[typ][1])))
#        #jnd2.where(jnd2.check==True).select('jnd_sender_id').dropDuplicates().show()
#        
#        conv_ids = jnd2.where(jnd2.check==True).select('jnd_sender_id').dropDuplicates()#.rdd.flatMap(lambda x : x).collect()
#        conv_ids = conv_ids.withColumnRenamed("jnd_sender_id","jnd_sender_id2")
#        jnd2 = jnd2.withColumn('q_rank2',F.concat(F.col('type_q_rank2'),F.lit('_'),F.col('nbr_response_code')))
#        
#        
#        temp2 = jnd2.join(conv_ids,jnd2.jnd_sender_id==conv_ids.jnd_sender_id2)
#        
#        cols = ['jnd_retailer','nlu_positive_product_type','lag_type','nlu_text','nlu_date','is_answered','nbr_date','nbr_response_code','type_q_rank2','q_rank','jnd_sender_id','jnd_ts']
#
#        temp = temp2.where((temp2.nbr_response_code.isNotNull()) & (temp2.lag_type.isNotNull() ) ).select(cols)
#    
#        
#        temp2 = temp2.where(temp2.lag_type==typ)
#        
#        rows = temp2.count()
#        num_convs = temp2.agg(F.countDistinct('jnd_sender_id')).collect()[0][0]
#        
#        num_conv_by_retailer = temp2.groupBy('jnd_retailer').agg(F.countDistinct('jnd_sender_id')).toPandas()
#        num_conv_by_retailer.columns = ['retailer','num_conv']
#        
#        
#        ls = []
#        for i in range(0,num_conv_by_retailer.shape[0]):
#            ls.append("for {} there are {} convs".format(num_conv_by_retailer.iloc[i,0],num_conv_by_retailer.iloc[i,1]))
#            
#        ls = '\n'.join(ls)
#        
#        
#        grp2 = temp2.groupBy(['lag_type','q_rank2']).agg(
#                        
#                    
#                    F.count('is_answered').alias('count'),
#                    F.mean('is_answered').alias('answer_rate')
#                    
#                
#                ).toPandas()
#    
#    
#        
#        grp2["ci"] = grp2[["answer_rate","count"]].apply(binary_ci_pd,axis=1)
#        grp2 = grp2[grp2["count"]>=30]
#        grp2 = grp2.sort_values(["q_rank2"],ascending=False)
#        
#        grp2 = grp2[grp2["lag_type"].notnull()]
#        
#        fig = plt.figure(figsize=(5,5))    
#        
#        
#        #temp = temp.sort_values("answer_rate")
#        plt.errorbar(grp2["answer_rate"], grp2["q_rank2"], xerr=grp2['ci'].values, fmt='o')
#            
#        plt.xlabel('answer rate')
#        plt.ylabel('order_question')
#        plt.title('original order answer rate for type {} \n #rows : {}\n #convs: {}\n{}'.format(typ,rows,num_convs,ls))
#                    
#        plt.savefig('{}/original_{}.png'.format(dest,typ))
#        plt.show(block=False)
#        plt.close(fig)        
    
  
    
    




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='calculates answerd questions and visualize them ')

    parser.add_argument('--jnd-source', help='nlu nbr jnd parquet source')
    parser.add_argument('--sdk-source', help='sdk csv source')
    
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
    

    
    calculate_and_plot_ar(args.jnd_source,args.sdk_source,args.dest)
    
    