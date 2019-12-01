#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  7 14:20:24 2019

@author: amirdavidoff


"""

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
# 176957



# count number of ga that have morethen one sender id (only 1 convs !)

my_id_that_have_one_sender_id = nlu_enr.groupBy('my_id').agg(F.countDistinct('sender_id').alias('count')).where(F.col('count')==1)
my_id_that_have_one_sender_id.count()
#31751


# colomn rename
my_id_that_have_one_sender_id = my_id_that_have_one_sender_id.withColumnRenamed('my_id','my_id2')


nlu_enr = nlu_enr.join(my_id_that_have_one_sender_id,nlu_enr.my_id==my_id_that_have_one_sender_id.my_id2)
nlu_enr.count()
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
conv = sqlContext.read.options(header=True,sep='\t').csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/conv_summ_proc.csv')
#109049

# filer july and 429
conv = conv.filter((F.to_date("created_at") >= F.lit("2019-07-01"))&(F.col('retailer_id')=='429'))
conv.count()
# 57361

conv.dropDuplicates(['conversation_id']).count()
#57631



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


''' join previosu with nlu - first nlu '''

final = nlu_enr2.where(nlu_enr2.rank==1).join(conv_api_msg,conv_api_msg.api_msg_user_id==nlu_enr2.sender_id)

final.dropDuplicates().count()
# 30488
finalp = final.dropDuplicates().toPandas()


# WHEN BACK > ADD INTENTS LIST, RUN ALL, SUSBET COLUMNS TO NEEDED, RUN NALYSIS 

# when back, can i catch and then lean all the duplicate short msg's ?

''' join previosu with nlu - last nlu '''

last = nlu_enr2.where(nlu_enr2.rank==nlu_enr2.max_rank).join(conv_api_msg,conv_api_msg.api_msg_user_id==nlu_enr2.sender_id)

last.dropDuplicates().count()
# 30962
lastp = last.dropDuplicates().toPandas()





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


from generic_imports import *

binary_ci_udf = F.udf(binary_ci,DoubleType())


cast_list = F.udf(lambda x : [int(i) for i in x],ArrayType(IntegerType()))


spark_df={}
spark_df["first_nlu"] = final.dropDuplicates(['timestamp','sender_id'])
spark_df["last_nlu"] = last.dropDuplicates(['timestamp','sender_id'])


''' plot rate (add to cart ,conversion) per value'''

for path in spark_df:

    temp = spark_df[path]
    
    df_dic={}
    
    for c in cols_to_examine:
        
        df_dic[c] = temp.groupBy(c).agg(
                
                F.count(c).alias('count'),
                cast_list(F.collect_list('add_to_cart')).alias('add_to_cart'),
                
                cast_list(F.collect_list('ga_add_to_cart')).alias('ga_add_to_cart'),
    
                cast_list(F.collect_list('ga_converted')).alias('ga_converted'),
    
                ).toPandas()
        
        df_dic[c]["ratio"] = df_dic[c]["count"] / np.sum(df_dic[c]["count"])
        df_dic[c]["add_to_cart_rate"] =  df_dic[c]["add_to_cart"].apply(np.mean)
        df_dic[c]["ga_add_to_cart_rate"] =  df_dic[c]["ga_add_to_cart"].apply(np.mean)
        df_dic[c]["ga_converted_rate"] =  df_dic[c]["ga_converted"].apply(np.mean)
        
        df_dic[c]["add_to_cart_ci"] =  df_dic[c]["add_to_cart"].apply(binary_ci)
        df_dic[c]["ga_add_to_cart_ci"] =  df_dic[c]["ga_add_to_cart"].apply(binary_ci)
        df_dic[c]["ga_converted_ci"] =  df_dic[c]["ga_converted"].apply(binary_ci)
            
        
      
        
    MIN_RATIO = 0.01
    
    ''' plot the different rate's for each values'''
    
    for c in ['add_to_cart','ga_add_to_cart','ga_converted']:
        
        for key in df_dic:
        
            df = df_dic[key]
        
            df = df[df["ratio"] >= MIN_RATIO]
        
            df = df.sort_values(c+"_rate",ascending=True)
            
        
            plt.errorbar([i for i in range(len(df[c+"_rate"]))], df[c+"_rate"].values, yerr=df[c+"_ci"].values, fmt='o')
            
           
            plt.xticks([i for i in range(len(df[c+"_rate"]))], df[key],rotation=30)
    
            plt.title(path+" "+key+" "+c)
            plt.ylabel(c+'_rate')
            plt.xlabel(key)
            plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task2/{}/rates/{}_{}.png'.format(path,c,key))
            plt.show()
    
    # when back think how to compare first nlu to last nlu here. and 1050 clock msg's
    # compare and add ro word report with insights.

    
''' plot bar plots '''

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
        

for c in cols_to_examine:
    
    grp = mrg.groupBy(["source",c]).agg(
                    
                    F.count(c).alias('count'),
                    cast_list(F.collect_list('add_to_cart')).alias('add_to_cart'),
                    
                    cast_list(F.collect_list('ga_add_to_cart')).alias('ga_add_to_cart'),
        
                    cast_list(F.collect_list('ga_converted')).alias('ga_converted'),
        
                    ).toPandas()
    
    # adding column ratio that represents the share of count per source (first / last nlue)        
    grp = grp.assign(ratio=grp['count'].div(grp.groupby('source')['count'].transform('sum')))
    
    grp["add_to_cart_rate"] =  grp["add_to_cart"].apply(np.mean)
    grp["ga_add_to_cart_rate"] =  grp["ga_add_to_cart"].apply(np.mean)
    grp["ga_converted_rate"] =  grp["ga_converted"].apply(np.mean)
    
    grp["add_to_cart_ci"] =  grp["add_to_cart"].apply(binary_ci)
    grp["ga_add_to_cart_ci"] =  grp["ga_add_to_cart"].apply(binary_ci)
    grp["ga_converted_ci"] =  grp["ga_converted"].apply(binary_ci)
        
        
    MIN_RATIO = 0.01
  
    ''' plot  '''
    
    grp = grp[grp["ratio"]>=MIN_RATIO]
    
    
    fig = plt.figure(figsize=(12,12))

    for src in ['first','last']:

        
    
        temp = grp[grp["source"]==src]
        temp = temp.sort_values("ratio",ascending=True)
        
        plt.barh(temp[c].astype(str).tolist(),temp['ratio'],label=src+"_nlu",alpha=0.5)
        plt.title(c+"_nlu \n(only shwoing values ratioed >1%)")
        plt.legend()
    
    plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task2/combined_bar_plots/{}.png'.format(c))
    plt.show()    
    
    
    



## maybe word cloud ? :
#def visualise_word_map():
#    words=" "
#    for msg in dataset["comment"]:
#    msg = str(msg).lower()
#        words = words+msg+" "
#    wordcloud = WordCloud(width=3000, height=2500, background_color='white').generate(words)
#    fig_size = plt.rcParams["figure.figsize"]
#    fig_size[0] = 14
#    fig_size[1] = 7
#    plt.show(wordcloud)
#    plt.axis("off")

























''' modeling '''

'''

actions out of this
   1. age group binary on adult`
   2. gender binary on male
   3. intents list : bidding > ['Shop Now', 'neutral'],['Not Understood', 'neutral']
                       ['neutral', 'Shop Now'], ['New Search', 'neutral'], other
   4. is_sku to binary
   5. negative_aspects,product_type, positive brands remove 
   6. positive aspects binary [] or not 
   
'''


finalp["age_binary"] = np.where(finalp["age_group"]=='adult',0,1)
finalp["gender_binary"] = np.where(finalp["gender"]=='male',0,1)

finalp["sku_binary"] = finalp['is_sku'].astype(str).replace({"True":1,"False":0})

finalp["positive_aspects_binary"] = np.where(finalp['positive_aspects'].astype(str)=='[]',0,1)

finalp["positive_positive_product_type"] = np.where(finalp['positive_product_type'].astype(str)=='None',0,1)


intents_ls = ["['Shop Now', 'neutral']","['Not Understood', 'neutral']",
                       "['neutral', 'Shop Now']", "['New Search', 'neutral']"]


def fix_intents(ls):
    
    try:
        
        return str([str(s) for s in ls])
                    
    except:
        
        return 'none'


finalp["intents_bind"] = finalp['intents_list'].apply(fix_intents)

finalp.loc[~finalp.intents_bind.isin(intents_ls), 'intents_bind'] = "other"





finalp["add_to_cart"].value_counts(normalize=True)
# ended up with only 2% of add to carts


x_col = ['age_binary',
         'gender_binary','sku_binary',
         'positive_aspects_binary','intents_bind','product_search_result_len','positive_positive_product_type']

y_col = ['add_to_cart']


df = finalp[y_col+x_col]

df['add_to_cart'] = df['add_to_cart'].astype(int)


df2 = pd.get_dummies(df, columns=['intents_bind'])


''' modeling '''

import xgboost as xgb
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import  train_test_split
import re
from generic_imports import *
import shap




regex = re.compile(r"\[|\]|<", re.IGNORECASE)
df2.columns = [regex.sub("_", col) if any(x in str(col) for x in set(('[', ']', '<'))) else col for col in df2.columns.values]


y_col = 'add_to_cart'
x_cols = [c for c in df2.columns.tolist() if c!=y_col]



#chisq test

chisq_res = chisq_to_entire_df(df2,'add_to_cart')
chisq_res.to_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tests/task2/chisq/chisq.csv')



#split 50%  
X_train, X_test, y_train, y_test = train_test_split(df2[x_cols], df2[y_col],
                                                        test_size=0.5, random_state=42,
                                                        stratify=df2[y_col])
#train
bst, dtrain, dtest = train_weighted_xgb(X_train, X_test, y_train, y_test)

#predict
preds_test = bst.predict(dtest)

# auc 67


#confusion matrix
#from sklearn.metrics import confusion_matrix
#confusion_matrix(y_test,np.where(preds_test>=np.mean(preds_test),1,0))

# roc auc
plot_roc_aucs([('xgb',y_test,preds_test)],True,'/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tests/task2/roc/roc_auc.png' )
    


# xgb importance       
importance_types = ['gain']
for f in importance_types :
    importance(bst,f,True,'/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tests/task2/xgb_imp/{}.png'.format(f))
    plt.title(f)
    

#shap    
shap_values = shap.TreeExplainer(bst).shap_values(X_test)

shap.summary_plot(shap_values, X_test,max_display=30,show=False)
plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tests/task2/shap/summary_shap.png')


shap.dependence_plot("positive_aspects_binary", shap_values, X_test,show=False)
plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tests/task2/shap/positive_aspects_binary.png')

shap.dependence_plot("product_search_result_len", shap_values, X_test,show=False)
plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tests/task2/shap/product_search_result_len.png')


