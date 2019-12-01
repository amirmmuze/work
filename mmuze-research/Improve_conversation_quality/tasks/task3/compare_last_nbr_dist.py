#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 10:09:41 2019

@author: amirdavidoff
"""


import findspark
findspark.init('/Users/amirdavidoff/server/spark-2.4.4-bin-hadoop2.7')
import os
os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/jdk1.8.0_221.jdk/Contents/Home/'


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame, SparkSession,Row
from pyspark.sql import functions as F

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType, DoubleType
from pyspark.sql import DataFrameStatFunctions as statFunc
from pyspark.sql.window import Window

from pyspark.sql.types import *


SUBMIT_ARGS = "--packages mysql:mysql-connector-java:5.1.48 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
#sc._conf.getAll()


conf =SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
#spark.conf.set("spark.sql.session.timeZone", "America/New_York")
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.executor.memory", "6g")
spark.conf.set("spark.driver.memory", "6g")
#spark.conf.set("spark.shuffle.memoryFraction","0.2")

spark.conf.set("spark.yarn.executor.memoryOverhead","2g")

sqlContext = SQLContext(sc)

import matplotlib.pyplot as plt

from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})



''' nlu enriched'''
nlu_enr  = sqlContext.read.parquet('/Users/amirdavidoff/Desktop/data/enriched_data/nlu')
nlu_enr.count()
#835833

nlu_enr = nlu_enr.filter((F.to_date("date") >= F.lit("2019-07-01"))&(nlu_enr.retailer_id=='429'))
nlu_enr.count()
#176957

#sample
sample_nlu = nlu_enr.where(nlu_enr.sender_id=='dh2be36w85g').toPandas()


''' nbr enriched'''
nbr_enr = nbr_enriched = sqlContext.read.parquet('/Users/amirdavidoff/Desktop/data/enriched_data/nbr')
nbr_enr.count()
#295239

nbr_enr = nbr_enr.filter((F.to_date("date") >= F.lit("2019-07-01"))&(nbr_enr.retailer_id=='429'))
nbr_enr.dropDuplicates().count()
#144204


window = Window.partitionBy("sender_id").orderBy(["timestamp"])
window_no_order = Window.partitionBy("sender_id")

nbr_enr = nbr_enr.withColumn('rank',F.rank().over(window))
nbr_enr = nbr_enr.withColumn('max_rank',F.max('rank').over(window_no_order))


cols = ['retailer_id','timestamp','sender_id','response_code','rank','max_rank']

nbr_enr2 = nbr_enr.select(cols)



''' conv summary'''
# read conversation sumeries tsv
conv = sqlContext.read.options(header=True,sep='\t').csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/conv_summ_proc.csv')
#109049

# filer july and 429
conv = conv.filter((F.to_date("created_at") >= F.lit("2019-07-01"))&(F.col('retailer_id')=='429'))
conv.count()
# 57361

conv.dropDuplicates(['conversation_id']).count()



''' api msg's '''
api_msg = sqlContext.read.options(header=True,sep='\t').csv('/Users/amirdavidoff/Desktop/data/my_sql_export/api_messages2.txt')

api_msg.count()
#2131712
api_msg = api_msg.where(F.col('retailer_id')=='429')
# 537163

api_msg = api_msg.dropDuplicates(['conversation_id','user_id'])


for c in api_msg.columns:
    
    api_msg = api_msg.withColumnRenamed(c,"api_msg_{}".format(c))



''' join conv and apit msg'''
conv_api_msg = conv.join(api_msg,conv.conversation_id==api_msg.api_msg_conversation_id,how='left')
conv_api_msg.count()
# 57361

cols = ['retailer_id','api_msg_retailer_id','api_msg_conversation_id','conversation_id',
        
 'customer_type',
 'num_of_messages_int',
 'api_msg_user_id',
 'api_msg_mid','ga_converted','ga_add_to_cart','ga_product_clicked','add_to_cart','conversation_media']

# SWITCH THE STR COLS TO BINARY

conv_api_msg = conv_api_msg.select(cols)

conv_api_msgp = conv_api_msg.toPandas()




''' last nbr'''

last_nbr = nbr_enr2.where(nbr_enr2.rank==nbr_enr2.max_rank).join(conv_api_msg,conv_api_msg.api_msg_user_id==nbr_enr2.sender_id)

last_nbr.count()
#53233
last_nbr.dropDuplicates().count()
# 51595
last_nbrpp = last_nbr.dropDuplicates().toPandas()


''' plot bar plot '''
for c in ['ga_add_to_cart','ga_converted']:
    fig = plt.figure(figsize=(12,12))
    for i in [0,1]:
        
        grp = last_nbrpp[last_nbrpp[c].astype(int)==i].groupby('response_code',as_index=False).size().reset_index()
        grp.columns = ['response_code','count']
        grp = grp.sort_values('count',ascending=True)
        
        grp['percent'] = grp['count'] / np.sum(grp['count'])
            
        plt.barh(grp['response_code'].astype(str).tolist(),grp['percent'],label=i,alpha=0.5)
        plt.legend()
        plt.title('last nbr response code by {}'.format(c))
    plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task3/last_nbr/last_nbr_{}.png'.format(c))
    plt.show()


''' one nbr before last  '''


nbr_enr2 = nbr_enr2.withColumn('max_minus_one',F.col('max_rank') - F.lit(1))

one_before_last_nbr = nbr_enr2.where(nbr_enr2.rank==nbr_enr2.max_minus_one).join(conv_api_msg,conv_api_msg.api_msg_user_id==nbr_enr2.sender_id)

one_before_last_nbr.count()
#24252
one_before_last_nbr.dropDuplicates().count()
# 24252


one_before = one_before_last_nbr.dropDuplicates().toPandas()



''' plot bar plot '''

for c in ['ga_add_to_cart','ga_converted']:

    fig = plt.figure(figsize=(12,12))
    for i in [0,1]:
        
        grp = one_before[one_before[c].astype(int)==i].groupby('response_code',as_index=False).size().reset_index()
        grp.columns = ['response_code','count']
        grp = grp.sort_values('count',ascending=True)
        
        grp['percent'] = grp['count'] / np.sum(grp['count'])
            
        plt.barh(grp['response_code'].astype(str).tolist(),grp['percent'],label=i,alpha=0.5)
        plt.legend()
        plt.title('one before last nbr response code by {}'.format(c))
    plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task3/one_before_last_nbr/one_before_{}.png'.format(c))
    plt.show()

