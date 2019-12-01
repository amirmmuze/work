#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 16:41:56 2019

@author: amirdavidoff



This scripts purpose is to enrich sdk data.


Script expects tsv input and writes in parquet format (both cuold be altered in the future)



example execution line:
    
    spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/enrich_sdk.py --sdk-source /Users/amirdavidoff/Desktop/data/yaron_sql_dumps/sdk_reports.csv --dest /Users/amirdavidoff/Desktop/data/enriched_data/sdk


adds the following fields:
    
    'date' : date and time from the unix timestamp.
    'browser' : str , browser fetched form user agent.
    'os' : str , os fetched form user agent.
    'device' : str , device fetched form user agent.
    'time_diff' : int difference between this event compared to previous one in seconds.
    'is_start_session' : int binary, after 6 hours fro mprevious engagement a new session starts.
    'conv' : incremental conv id per my_id by is_session_starts
    
    
    
    
TODO : fetch google id from "value"


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



def browser(ua):
    
    '''
    helper function.
    
    input is string user agent
    
    output is string browser.
    
    '''

    try:
        parsed_ua = parse(ua)      
    
        return parsed_ua.browser.family

    except:
        return 'None'
    

browser_udf = F.udf(browser,StringType())


def os(ua):
    
    '''
    helper function.
    
    input is string user agent
    
    output is string os.
    
    '''

    try:
        parsed_ua = parse(ua)      
    
        return parsed_ua.os.family

    except:
        return 'None'
    

os_udf = F.udf(os,StringType())


def device(ua):
    
    '''
    helper function.
    
    input is string user agent
    
    output is string device.
    
    '''

    try:
        parsed_ua = parse(ua)      

        return parsed_ua.device.family
    except:
        return 'None'
    

device_udf = F.udf(device,StringType())


#path = '/Users/amirdavidoff/Desktop/data/my_sql_export/sdk_dump_test.csv'

def enrich_sdk(path):
    
    '''
    
    param "path" of type String is the path to sdk tsv.
    
    returns spark df with added columns
    
    
    '''

    
    #read data
    spark_sdk  = sqlContext.read.options(header=True,quote = '"',escape='"').csv(path) #,sep='\t'
    
    # add date, unix is in mili sec so devide by 1000
    spark_sdk = spark_sdk.withColumn('date', F.from_unixtime(F.col('timestamp')/F.lit(1000.0)))
    
    
    #filter columns
    #rmv = ['evennt_type','total_time', 'server_time', 'ip', 'created_at',
    #       'updated_at']
    
    #spark_sdk = spark_sdk.select([c for c in spark_sdk.columns if c not in rmv])
    
    #filter None's
    
    #spark_sdk = spark_sdk.where(spark_sdk.retailer_id.isNotNull())
    
    # add user agent fields
    #spark_sdk = spark_sdk.withColumn("browser",browser_udf(F.col('user_agent')))
    #spark_sdk = spark_sdk.withColumn("os",os_udf(F.col('user_agent')))
    #spark_sdk = spark_sdk.withColumn("device",device_udf(F.col('user_agent')))
    
    
    ''' create conv '''
    # sets a conversation id for each "user_id" based on the 6 horus rule.

    spark_sdk = spark_sdk.withColumn("timestamp_int",spark_sdk.timestamp.cast(IntegerType()))
    
    window = Window.partitionBy("user_id").orderBy(["timestamp_int"])
    
    
    spark_sdk = spark_sdk.withColumn("time_diff", F.col('timestamp_int') - F.lag(F.col("timestamp_int")).over(window))
    
    spark_sdk = spark_sdk.withColumn('is_start_session', (F.isnull(F.col("time_diff")) | (F.col("time_diff") > 21600)).cast(IntegerType()))
    spark_sdk = spark_sdk.withColumn("conv", F.sum(F.col('is_start_session')).over(window))
    
    
    return spark_sdk




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='read sdk tsv, enrich data, write in parquet format.')
    parser.add_argument('--sdk-source', help='sdk tsv df source path')
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
    
 
    spark_sdk = enrich_sdk(args.sdk_source)
  
     
   # spark_sdk = enrich_sdk('/Users/amirdavidoff/Desktop/data/my_sql_export/nlu_dump_test.csv')
   
    spark_sdk.write.mode("overwrite").parquet(args.dest)
