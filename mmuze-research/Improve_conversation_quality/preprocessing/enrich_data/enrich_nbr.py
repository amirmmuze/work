#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 17:49:48 2019

@author: amirdavidoff



This scripts purpose is to enrich nbr data.


Script expects tsv input and writes in parquet format (both cuold be altered in the future)



example execution line:
    
    spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/enrich_nbr.py --nbr-source /Users/amirdavidoff/Desktop/data/yaron_sql_dumps/next_response_api_jobs.csv --dest /Users/amirdavidoff/Desktop/data/enriched_data/nbr

    


adds the following fields:

    'date' : date and time from unix.
    'ack_text' : str , bot text response. fetched from 'json_response'.
    'response_code' : str , bot response code fetched from 'json_response'.
    'possible_values' : struct type, quick replies.
    'ts_plus_response' : int , timestamp plus response time.

    'time_diff' : int difference between this event compared to previous one in seconds
    'is_start_session' : int binary, after 6 hours from previous engagement a new session starts.
    'conv' : incremental conv id per sender_id by is_session_starts


TODO : 


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


#path = '/Users/amirdavidoff/Desktop/data/my_sql_export/nbr.csv'

  
def enrich_nbr(path):
    '''
    param path : str path to tsv
    
    returns enriched nbr spark df.
    
    '''
    
    spark_nbr = sqlContext.read.options(header=True,quote = '"',escape='"').csv(path) #,sep='\t'

    spark_nbr = spark_nbr.withColumn('date', F.from_unixtime(F.col('timestamp')))
    
    
    spark_nbr = spark_nbr.withColumn("year",F.year(F.col('date')))
    spark_nbr = spark_nbr.withColumn("month",F.month(F.col('date')))
    spark_nbr = spark_nbr.withColumn("day_of_week",F.dayofweek(F.col('date')))
    spark_nbr = spark_nbr.withColumn("hour",F.hour(F.col('date')))
    
    
    spark_nbr = spark_nbr.withColumn("day_of_week_str",F.date_format("date", 'E'))
    spark_nbr = spark_nbr.withColumn("year_month",F.concat(F.col('year'),F.lit('_'),F.col('month')))
    
    
    json_response_schema = spark.read.json(spark_nbr.rdd.map(lambda row: row.json_response)).schema
    
    spark_nbr = spark_nbr.withColumn('json_response_dic',F.from_json(F.col('json_response'),json_response_schema))
    spark_nbr = spark_nbr.withColumn('ack_text',F.col('json_response_dic.ack_text'))
    spark_nbr = spark_nbr.withColumn('text',F.col('json_response_dic.next_response.text'))
    
    spark_nbr = spark_nbr.withColumn('response_code',F.col('json_response_dic.next_response.data.response_code'))
    spark_nbr = spark_nbr.withColumn('possible_values',F.col('json_response_dic.next_response.possible_values'))





    def collect_values(ls):
        
        try:
            
            return [c[1] for c in ls]
        
        except:
            
            None
    
    collect_values_udf = F.udf(collect_values,ArrayType(StringType()))
    
    spark_nbr = spark_nbr.withColumn('possible_answers',collect_values_udf(F.col('possible_values')))



    spark_nbr = spark_nbr.withColumn('ts_plus_response',F.col('timestamp')+F.col('next_response_time'))
    
    
    ''' create conv '''
    # sets a conversation id for each "sender_id" based on the 6 horus rule.

    window = Window.partitionBy("sender_id").orderBy(["timestamp"])
    
    spark_nbr = spark_nbr.withColumn("timestamp",spark_nbr.timestamp.cast(IntegerType()))
    spark_nbr = spark_nbr.withColumn("time_diff", F.col('timestamp') - F.lag(F.col("timestamp")).over(window))
    
    spark_nbr = spark_nbr.withColumn('is_start_session', (F.isnull(F.col("time_diff")) | (F.col("time_diff") > 21600)).cast(IntegerType()))
    spark_nbr = spark_nbr.withColumn("conv", F.sum(F.col('is_start_session')).over(window))
    
    
    rmv = ['json_response_dic']
    
    spark_nbr = spark_nbr.select([c for c in spark_nbr.columns if c not in rmv])
    

    return spark_nbr







if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='read nbr tsv, enrich data, write in parquet format.')
    parser.add_argument('--nbr-source', help='nbr tsv df source path')
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
    
 
    spark_nbr = enrich_nbr(args.nbr_source)
  
     
   # spark_nbr = enrich_nlu('/Users/amirdavidoff/Desktop/data/my_sql_export/nlu_dump_test.csv')
   
    spark_nbr.write.mode("overwrite").parquet(args.dest)
