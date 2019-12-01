#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  6 16:09:40 2019

@author: amirdavidoff
"""


#%%
import matplotlib.pyplot as plt
import pandas as pd 
import numpy as np
from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})


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


#SUBMIT_ARGS = "--packages mysql:mysql-connector-java:5.1.48 pyspark-shell"
#os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
#sc._conf.getAll()
#%%

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
#%%

def binary_ci(ag):
    
    #print(ag)
    
    mean = np.mean(ag)
    n = len(ag)
    
    return 1.96 * np.sqrt(mean*(1-mean)/n)

#%%


conv = sqlContext.read.options(header=True,sep='\t').csv('/Users/amirdavidoff/Desktop/data/my_sql_export/conversation_summaries.txt')
#109049


conv = conv.filter(F.to_date("created_at") >= F.lit("2019-07-01"))
conv.count()
#73014

#%%


convp = conv.toPandas()

#%%

retailer_counts = convp["retailer_id"].value_counts().to_frame('count')

retailer_counts["percent"] = retailer_counts["count"] / np.sum(retailer_counts["count"])

retailer_counts = retailer_counts[retailer_counts["percent"]>=0.05]


retailer_counts = retailer_counts[retailer_counts.index.isin(['962','429'])]

orgs = retailer_counts.index.tolist()

convp = convp[convp["retailer_id"].isin(orgs)]

#%%

''' retailer id distribution '''

fig = plt.figure(figsize=(7,7))

plt.bar(retailer_counts.index,retailer_counts["count"], lw=2, color='cornflowerblue',label='count')

plt.xlabel('retailer_id',fontsize=12)
plt.ylabel('conversations coun',fontsize=12)
plt.title('amount of conversations per retailer \n done after july 19')

plt.text(1,50000,"* retailer_id's with less then \n5% freq were dropped \nsuch as 953 288 ")
plt.show()





#%%
''' activity by time '''
# add year_month

convp["year"] = convp['created_at'].apply(lambda s : s[0:4])
convp["month"] = convp['created_at'].apply(lambda s : s[5:7])
convp["year_month"] = convp["year"]+"_" + convp["month"]


fig = plt.figure(figsize=(7,7))
for org in orgs:
    
    
    subset = convp[convp["retailer_id"]==org]
    grp = subset.groupby("year_month").conversation_id.nunique()
    
    
    plt.plot(grp.index,grp,label=org+" conv count: {}".format(subset.shape[0]))
    plt.legend()
    plt.title('number of unique converation ids \nper retailer id',fontsize=12)
    plt.xlabel('year_month',fontsize=12)
    plt.ylabel('count',fontsize=12)
plt.show()


#%%
import json
convp["mmuze_status_dict"]  = convp["mmuze_status"].apply(json.loads) 
convp["channel_dict"]  = convp["channel"].apply(json.loads) 

#convp["intents_dict"]  = convp["intents"].apply(json.loads) 
# adds 100 columns



convp = pd.concat([convp.drop(['mmuze_status_dict'], axis=1), convp['mmuze_status_dict'].apply(pd.Series)], axis=1)
#convp = pd.concat([convp.drop(['intents_dict'], axis=1), convp['intents_dict'].apply(pd.Series)], axis=1)

convp = pd.concat([convp.drop(['channel_dict'], axis=1), convp['channel_dict'].apply(pd.Series)], axis=1)

#%%
''' occasions and dress codes > too close to zero'''

#convp["occasions_list"]  = convp["occasions"].apply(lambda x: x[1:-1].split(',')) 
#convp["dress_codes_list"]  = convp["dress_codes"].apply(lambda x: x[1:-1].split(',')) 
#
#
#from sklearn.preprocessing import MultiLabelBinarizer
#
#
#mlb = MultiLabelBinarizer()
#convp = convp.join(pd.DataFrame(mlb.fit_transform(convp.pop('occasions_list')),
#                          columns=np.array(["occasion_" + c.replace('"','') for c in list(mlb.classes_)]),
#                          index=convp.index))
#
#mlb = MultiLabelBinarizer()
#convp = convp.join(pd.DataFrame(mlb.fit_transform(convp.pop('dress_codes_list')),
#                          columns=np.array(["dress_code_" + c.replace('"','') for c in list(mlb.classes_)]),
#                          index=convp.index))

#%%

fill_na_zero_cols = ['transaction_value']

convp[['transaction_value']] = convp[['transaction_value']].replace(["NULL"],['0']).astype(float)

#%%

# for these i want avg per retailer id
num_cols = ['transaction_value'] + [c for c in convp if "occasion_"  in c] + [c for c in convp if "dress_code_"  in c]
#%% 


''' transaction amount'''
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
plt.show()
        
# plot dist of occasion and dress code which is close t o0 now      
#        else:
            
#            grp = convp.groupby('retailer_id').agg({c:"mean"})
#        
#            fig = plt.figure(figsize=(7,7))
#            grp.plot(kind='bar')
#            plt.ylabel('average transaction amount',fontsize=12)
#            plt.xlabel('retailer_id',fontsize=12)
#            plt.title('averagetransaction amount \nper retailer',fontsize=12)
#            plt.show()
    

#%%

''' conversation media 99% text'''
cat_cols = ['ga_session_id','customer_type',
       'conversation_media']


binary_cols = ['products_found', 'product_clicked', 'add_to_cart', 'search_exit',
       'ga_product_clicked', 'ga_add_to_cart', 'ga_converted', 'bad_quality']

#%%
#convp[cols] = convp[cols].fillna(0)

for c in cols:
    convp[c] = convp[c].fillna(0)
    convp[c] = convp[c].astype(int)
    


#%%
''' binary '''
convp["is_get_ga"] = np.where(convp["ga_session_id"]=="NULL",0,1)
convp["is_returning_customer"] = np.where(convp["customer_type"]=="Returning Visitor",1,0)
convp["is_new_visitor"] = np.where(convp["customer_type"]=="New Visitor",1,0)


#% for there i want number of unique plus freq of most freq

binary = ['is_get_ga','is_new_visitor','is_returning_customer'] + binary_cols


for c in binary:
    
    grp= convp.groupby('retailer_id').agg({c:['mean',binary_ci]})
    grp.columns = ["mean","std"]
    
    
    
    fig = plt.figure(figsize=(7,7))
    plt.bar(grp.index,grp["mean"],yerr=grp["std"],label=c)
    plt.ylabel('{} ratio'.format(c),fontsize=12)
    plt.xlabel('retailer_id',fontsize=12)
    plt.title('percentage of {} \nper retailer'.format(c),fontsize=12)
    plt.show()




#%%
    
    
    ''' requeirs cleaning'''
    cols = ['platform', 'device', 'os', 'app']
    
    for c in cols:
    
    
        subset = convp[cols+["retailer_id"]]
        
        grp = subset.groupby(['retailer_id',c],as_index=False).size().reset_index()
        
        grp.columns = [c, "retailer_id", "0"]
        
        
        fig = plt.figure(figsize=(7,7))
        grp.pivot(c, "retailer_id", "0").plot(kind='bar',label=c)
    
        plt.ylabel('{} distribution'.format(c),fontsize=12)
        plt.xlabel('retailer_id',fontsize=12)
        plt.title('percentage of {} \nper retailer'.format(c),fontsize=12)
        plt.show()
        
        
        #%%
        
        
        ''' activity day and hour'''




convp["created_at_dt"] = pd.to_datetime(convp['created_at'], errors='coerce')
convp["day"]  =convp["created_at_dt"].dt.weekday_name
convp["hour"]  =convp["created_at_dt"].dt.hour


for c in ["day","hour"]:
    
    
    grp = convp.groupby(['retailer_id',c],as_index=False).size()
    grp = (grp / grp.groupby(level=0).sum()).reset_index()
    grp.columns = ["retailer_id",c, "percent"]
    
    
    
    
    
    fig = plt.figure(figsize=(7,7))
    grp.pivot(c,"retailer_id", "percent").plot(kind='bar',label=c)
    
    plt.ylabel('{} usage distribution'.format(c),fontsize=12)
    plt.xlabel(c,fontsize=12)
    plt.title('usage percentage by {} \nper retailer'.format(c),fontsize=12)
    plt.show()
    
    
    #%%
    
    '''num of msgs per retailer '''
    convp["num_of_messages_int"] = np.where(convp["num_of_messages"]=="NULL",0,convp["num_of_messages"])
    convp["num_of_messages_int"]=convp["num_of_messages_int"].fillna(0).astype(int)
    fig = plt.figure(figsize=(7,7))

    for org in orgs:
        
        plt.hist(convp[convp["retailer_id"]==org]["num_of_messages_int"],bins=400, density=True,alpha=0.6,label=org)
        plt.xlim(0,20)
        
    plt.title('num of messages per conversation',fontsize=12)    
    plt.xlabel('num of messages',fontsize=12)
    plt.ylabel('density',fontsize=12)
    plt.show()