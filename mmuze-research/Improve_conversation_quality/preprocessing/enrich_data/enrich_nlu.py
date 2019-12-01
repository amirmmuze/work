#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 11:43:08 2019

@author: amirdavidoff




This scripts purpose is to enrich nlu data.


Script expects tsv input and writes in parquet format (both cuold be altered in the future)



example execution line:
    
    
    spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/enrich_nlu.py --nlu-source /Users/amirdavidoff/Desktop/data/yaron_sql_dumps/nlu_api_jobs.csv --dest /Users/amirdavidoff/Desktop/data/enriched_data/nlu
    


adds the following fields:
    
    'date' : date and time from the unix timestamp.
    'year' : int year.
    'month' : int month.
    'day_of_week' : int day of week.
    'hour' : in hour.
    'day_of_week_str' : Str day of week.
    'year_month' : Str "year" "_" + "month".
    'age_group' : Str fetched from json_response.
    'gender' : Str fetched from json_response.
    'latest' : boolean  , fetched from json_response.
    'intents_list' : list of str , fetched from json_response.
    'subvertical' : str , fetched from json_response.
    'positive_aspects' : list of str , fetched from json_response.
    'positive_brands' : list of str , fetched from json_response.
    'positive_product_type' : str , fetched from json_response.
    'positive_models' : str , fetched from json_response.
    'negative_aspects' : list of str , fetched from json_response.
    'negative_brands' : Struct type containing few fields , fetched from json_response.
    'negative_product_type' : list of str , fetched from json_response.
    'negative_models' : list of str , fetched from json_response.
    'occasion_tags' : list of str , fetched from json_response.
    'occasion_tags_len' : int len of occasions.
    'product_search_result_len' : int 
    'is_sku' : boolean is sku search
    'is_sku0' : list of str of skus
    'my_id' : str , gi if exist si otherwise.
    'time_diff' : int difference between this event compared to previous one in seconds
    'is_start_session' : int binary, after 6 hours fro mprevious engagement a new session starts.
    'conv' : incremental conv id per my_id by is_session_starts
    
    
TODO : add fields that indicate change like change in product vertical.


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



def my_id(gd,si):
    
    '''
    helper function.
    
    if google id equals 'text' or 'NULL' return sender_id. 
    else return google id.
    '''
    
    try:
        if len(str(gd))<=5:
            
            return si
        
        else:   
            return gd
    except:
        return si
    
my_id_udf = F.udf(my_id,StringType())


def arr_len(s):
    '''
    helper function.
    
    return length of array using try escept.
    '''

    try:
        
        return len(s)
    
    except:
        
        return np.nan


arr_len_udf = F.udf(arr_len,IntegerType())



def is_sku(s):
    
    '''
    helper function.
    
    returns True is sku's inut type is a list, otherwise returns np.nan.
    '''

    try:
        
        return type(s)==list
    
    except:
        
        return np.nan


is_sku_udf = F.udf(is_sku,BooleanType())


    
def enrich_nlu(path):
    
    '''
    main function.
    
    param "path" of type String is the path to nlu tsv.
    
    returns spark df with added columns
    
    
    '''
    
    
    #reading data
    spark_nlu  = sqlContext.read.options(header=True,quote = '"',escape='"').csv(path)  #,sep='\t'
    
    
    ''' adding date year month day of week hour '''
    spark_nlu = spark_nlu.withColumn('date', F.from_unixtime(F.col('timestamp')))
    spark_nlu = spark_nlu.withColumn("year",F.year(F.col('date')))
    spark_nlu = spark_nlu.withColumn("month",F.month(F.col('date')))
    spark_nlu = spark_nlu.withColumn("day_of_week",F.dayofweek(F.col('date')))
    spark_nlu = spark_nlu.withColumn("hour",F.hour(F.col('date')))
    
    
    spark_nlu = spark_nlu.withColumn("day_of_week_str",F.date_format("date", 'E'))
    spark_nlu = spark_nlu.withColumn("year_month",F.concat(F.col('year'),F.lit('_'),F.col('month')))


   # filter data
   # spark_nlu = spark_nlu.where(F.col('year')>= 2019)
   # spark_nlu = spark_nlu.where(F.col('json_response').isNotNull())
   # spark_nlu = spark_nlu.where(F.col('retailer_id').isin(['441','962',]))


    ''' extracting json_response '''
    
    json_response_schema = spark.read.json(spark_nlu.rdd.map(lambda row: row.json_response)).schema
    
    
    
    spark_nlu = spark_nlu.withColumn('json_response_dic',F.from_json(F.col('json_response'),json_response_schema))
    spark_nlu = spark_nlu.withColumn('age_group',F.col('json_response_dic.consumer_details.age_group'))
    spark_nlu = spark_nlu.withColumn('gender',F.col('json_response_dic.consumer_details.gender'))
    spark_nlu = spark_nlu.withColumn('latest',F.col('json_response_dic.consumer_details.latest'))
    spark_nlu = spark_nlu.withColumn('intents_list',F.col('json_response_dic.intents.intent'))
    
    spark_nlu = spark_nlu.withColumn('subvertical',F.col('json_response_dic.subvertical'))
    spark_nlu = spark_nlu.withColumn('positive_aspects',F.col('json_response_dic.positive.aspects.aspect'))
    spark_nlu = spark_nlu.withColumn('positive_brands',F.col('json_response_dic.positive.brands'))
    spark_nlu = spark_nlu.withColumn('positive_product_type',F.col('json_response_dic.positive.product_type.parent_type'))
    spark_nlu = spark_nlu.withColumn('positive_models',F.col('json_response_dic.positive.models'))
    
    spark_nlu = spark_nlu.withColumn('negative_aspects',F.col('json_response_dic.negative.aspects.aspect'))
    spark_nlu = spark_nlu.withColumn('negative_brands',F.col('json_response_dic.negative.brands'))
    spark_nlu = spark_nlu.withColumn('negative_product_type',F.col('json_response_dic.negative.product_types.parent_type'))
    spark_nlu = spark_nlu.withColumn('negative_models',F.col('json_response_dic.negative.models'))
    
    spark_nlu = spark_nlu.withColumn('occasion_tags',F.col('json_response_dic.occasion_tags.occasions'))
    spark_nlu = spark_nlu.withColumn('occasion_tags_len',arr_len_udf(F.col('occasion_tags')))
    spark_nlu = spark_nlu.withColumn('product_search_result_len',arr_len_udf(F.col('json_response_dic.product_search_result')))

    spark_nlu = spark_nlu.withColumn('is_sku',is_sku_udf(F.col('json_response_dic.skus')))
    spark_nlu = spark_nlu.withColumn('is_sku0',F.col('json_response_dic.skus'))
    
    
    ''' my id '''  
    # my id is either a valid google id or sender id
    spark_nlu = spark_nlu.withColumn('my_id',F.when(F.col('ga_session_id').isNull(),F.col('sender_id')).otherwise(my_id_udf(F.col('ga_session_id'),F.col('sender_id'))))

    
    
    
    ''' create conv '''
    # sets a conversation id for each "my_id" based on the 6 horus rule.

    window = Window.partitionBy("my_id").orderBy(["timestamp"])
    
    spark_nlu = spark_nlu.withColumn("timestamp",spark_nlu.timestamp.cast(IntegerType()))
    spark_nlu = spark_nlu.withColumn("time_diff", F.col('timestamp') - F.lag(F.col("timestamp")).over(window))
    
    spark_nlu = spark_nlu.withColumn('is_start_session', (F.isnull(F.col("time_diff")) | (F.col("time_diff") > 21600)).cast(IntegerType()))
    spark_nlu = spark_nlu.withColumn("conv", F.sum(F.col('is_start_session')).over(window))
    
    rmv = ['json_response_dic']
    
    spark_nlu = spark_nlu.select([c for c in spark_nlu.columns if c not in rmv])
    
    return spark_nlu







if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='read nlu tsv, enrich data, write in parquet format.')
    parser.add_argument('--nlu-source', help='nlu tsv df source path')
    parser.add_argument('--dest', help='destination to write parquet')


    args = parser.parse_args()

    conf = (
        SparkConf()
            .setAppName("Spark Submit:" + __file__)
        #.set("spark.executor.memory", "1g")
    )

    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    
 
    spark_nlu = enrich_nlu(args.nlu_source)
  
     
   # spark_nlu = enrich_nlu('/Users/amirdavidoff/Desktop/data/my_sql_export/nlu_dump_test.csv')
   
    spark_nlu.write.mode("overwrite").parquet(args.dest)
    
    #spark_nlu_read = sqlContext.read.parquet('/Users/amirdavidoff/Desktop/data/enriched_data/nlu')
    
