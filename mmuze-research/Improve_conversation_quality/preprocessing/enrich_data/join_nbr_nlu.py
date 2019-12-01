#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 12:24:10 2019

@author: amirdavidoff



jn conv's nlu and nbr

execute with:
    
     spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/join_nbr_nlu.py --nbr-source /Users/amirdavidoff/Desktop/data/enriched_data/nbr --nlu-source /Users/amirdavidoff/Desktop/data/enriched_data/nlu --dest /Users/amirdavidoff/Desktop/data/enriched_data/jnd



TODO:
    
    1. add gender and asnwer to 002lvdjsz2f
    
    
    
fucked conversations:

    02vovn97or




"""

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame, SparkSession
import argparse
import numpy as np
from user_agents import parse


    
def collect_values(ls):
    
    try:
        
        return [c[1] for c in ls]
    
    except:
        
        None


def join_function(path_nbr,path_nlu):
    
    '''
    returns joined spark df 
    
    '''
    
        
    #nlu
    
   # path = '/Users/amirdavidoff/Desktop/data/enriched_data/nlu'
    spark_nlu = sqlContext.read.parquet(path_nlu)
    
    
    spark_nlu = spark_nlu.filter((F.to_date("date") >= F.lit("2019-07-01")))
    
    
    #nbr
    
   # path = '/Users/amirdavidoff/Desktop/data/enriched_data/nbr'
    spark_nbr = sqlContext.read.parquet(path_nbr)
    
    
    spark_nbr = spark_nbr.filter((F.to_date("date") >= F.lit("2019-07-01")))
    
    
    spark_nbr = spark_nbr.withColumn('source',F.lit('nbr'))
    spark_nlu = spark_nlu.withColumn('source',F.lit('nlu'))
    
    #changed column names
        
    for c in spark_nbr.columns:
    
        spark_nbr = spark_nbr.withColumnRenamed(c,"nbr_{}".format(c))
    
    for c in spark_nlu.columns:
    
        spark_nlu = spark_nlu.withColumnRenamed(c,"nlu_{}".format(c))
            
            
    
    nbr_cols = ['nbr_sender_id','nbr_retailer_id','nbr_timestamp',
                'nbr_ts_plus_response',
                'nbr_conv',
                'nbr_ack_text',
                'nbr_response_code',
                'nbr_possible_values',
                'nbr_source','nbr_date']
    
    nlu_cols = ['nlu_sender_id',
                'nlu_retailer_id',
                'nlu_gender','nlu_age_group','nlu_text',
                'nlu_timestamp',
                'nlu_intents_list',
                'nlu_subvertical',
                'nlu_positive_aspects',
                'nlu_positive_product_type',
                'nlu_positive_brands',
                'nlu_negative_aspects',
                'nlu_negative_product_type',
                'nlu_conv',
                'nlu_source','nlu_date']
    
    
    spark_nbr2 = spark_nbr.select(nbr_cols)
    spark_nlu2= spark_nlu.select(nlu_cols)
    
    
    
    jnd = spark_nlu2.join(spark_nbr2,spark_nbr2.nbr_source==spark_nlu2.nlu_source,how='full_outer')
                         
                         
    
    #jnd.count()
    


    collect_values_udf = F.udf(collect_values,ArrayType(StringType()))
    
    jnd = jnd.withColumn('nbr_possible_answers',collect_values_udf(F.col('nbr_possible_values')))
    
    
    jnd = jnd.withColumn('jnd_sender_id',F.when(F.col('nlu_sender_id').isNull(),F.col('nbr_sender_id')).otherwise(F.col('nlu_sender_id')))
    jnd = jnd.withColumn('jnd_ts',F.when(F.col('nbr_ts_plus_response').isNull(),F.col('nlu_timestamp')).otherwise(F.col('nbr_ts_plus_response')))
    jnd = jnd.withColumn('jnd_retailer',F.when(F.col('nlu_retailer_id').isNull(),F.col('nbr_retailer_id')).otherwise(F.col('nlu_retailer_id')))
    
    
    
    # function that marks q's as answered
    ''' could make this function beter with first\last (that are not none's) instead of taking lad and lag2'''
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
    
    
    
    
    
    window = Window.partitionBy("jnd_sender_id").orderBy(["jnd_ts"])
    
    
    jnd = jnd.withColumn('is_answered',check_isin_udf(F.lead('nlu_text').over(window),
                                                      F.lead('nlu_text',2).over(window),
                                                      F.col('nbr_possible_answers'),
                                                      F.col('nbr_response_code'),
                                                      F.lag('nlu_positive_aspects').over(window),
                                                      F.lead('nlu_positive_aspects').over(window),
                                                      F.lag('nlu_subvertical').over(window),
                                                      F.lead('nlu_subvertical').over(window),
                                                      F.lag('nlu_positive_product_type').over(window),
                                                      F.lead('nlu_positive_product_type').over(window),
                                                      F.lead('nlu_positive_product_type',2).over(window)))
    
    
    jnd = jnd.fillna( { 'is_answered':0 } )

    # fix ids  l22y83vocf, 00fma5y5xgf
    
    
    
    ''' data set features '''
    ''' DONT FORGET THAT YOUVE ADDED RESPONSE TIME TO TS MIGHT BE A HUGE BIAS '''
    
    
    jnd = jnd.withColumn('question_rank',F.sum(F.when(F.col('nbr_response_code').isNotNull(),1).otherwise(0)).over(window))
    
    jnd = jnd.withColumn('time_from_start',F.col('jnd_ts')-F.min('jnd_ts').over(window))
    
    jnd = jnd.withColumn('sum_answer',F.sum(F.lag('is_answered').over(window)).over(window))
    
    
    jnd = jnd.withColumn('num_quick_replies',F.size('nbr_possible_answers'))
    
    jnd = jnd.withColumn('hour',F.hour('nbr_date'))
    
    
    jnd = jnd.withColumn('day_of_week',F.date_format('nbr_date','u'))
    
    jnd = jnd.withColumn("last_nbr_code", F.last(F.lag("nbr_response_code").over(window), True).over(window))
    
    
    nlu_cols = ['nlu_intents_list','nlu_age_group','nlu_gender',
     'nlu_subvertical',
     'nlu_positive_aspects',
     'nlu_positive_product_type',
     'nlu_positive_brands',
     'nlu_negative_aspects',
     'nlu_negative_product_type']
    
    for c in nlu_cols:
        
        jnd = jnd.withColumn("last_{}".format(c), F.last(c, True).over(window))

    return jnd




#delete = jnd.where(jnd.jnd_sender_id=='qwz8nfr42i').toPandas()




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='join all tables nbr, nlu')
    parser.add_argument('--nlu-source', help='nlu tsv df source path')
    parser.add_argument('--nbr-source', help='nbr tsv df source path')
    #parser.add_argument('--sdk-source', help='sdk tsv df source path')
    parser.add_argument('--dest', help='destination to write parquet')


    args = parser.parse_args()

    conf = (
        SparkConf()
            .setAppName("Spark Submit:" + __file__)
            .set("spark.sql.session.timeZone", "UTC")
    )


    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    

    jnd = join_function(args.nbr_source,args.nlu_source)


    jnd.write.mode('overwrite').parquet(args.dest)  #'/Users/amirdavidoff/Desktop/data/enriched_data/jnd'

