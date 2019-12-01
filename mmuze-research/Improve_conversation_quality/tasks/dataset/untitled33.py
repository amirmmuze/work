#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 10:10:47 2019

@author: amirdavidoff


main dataset



main todo's:
    
    1. understand hwo is the data in conv summ is created and can it be replicated in production?
    3. upgrade is asnwer with first and last, and check on mili second

"""

from generic_imports import *

from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

jnd = sqlContext.read.parquet('/Users/amirdavidoff/Desktop/data/enriched_data/jnd')

#jnd.count()

#jnd.select('nlu_retailer_id').distinct().show()

#jnd = jnd.withColumn('jnd_retailer',F.when(F.col('nlu_retailer_id').isNull(),F.col('nbr_retailer_id')).otherwise(F.col('nlu_retailer_id')))
#
#
#
#def collect_values(ls):
#    
#    try:
#        
#        return [c[1] for c in ls]
#    
#    except:
#        
#        None
#
#collect_values_udf = F.udf(collect_values,ArrayType(StringType()))
#
#jnd = jnd.withColumn('nbr_possible_answers',collect_values_udf(F.col('nbr_possible_values')))
#
#
#
#def check_isin(lead_nlu_text,
#               lead_nlu_text2,
#               possible_values,
#               question_code,
#               lag_positive_aspects,
#               lead_positive_aspects,
#               lag_subvertical,
#               lead_subvertical,
#               lag_pos_product_type,
#               lead_pos_product_type,
#               lead_pos_product_type2):
#    
#    try:
#        
#        # check if response value is in quick replies
#        if (lead_nlu_text in possible_values) or (lead_nlu_text2 in possible_values):
#            return 1
#        
#        if (question_code=='color_question') and ('color' not in lag_positive_aspects) and ('color' in lead_positive_aspects):
#            return 1
#        
#        if (question_code=='subvertical_selection' or question_code=='subvertical_selection_second') and (lag_subvertical is None) and (lead_subvertical is not None):
#            return 1
#        
#        if (question_code=='product_type_selection') and (lag_pos_product_type is None) and ((lead_pos_product_type is not None) or (lead_pos_product_type2 is not None)):
#            return 1
#
#        else:
#            return 0
#    
#    except:
#        
#        None
#
#check_isin_udf = F.udf(check_isin,IntegerType())
#
#
#
#
#
#window = Window.partitionBy("jnd_sender_id").orderBy(["jnd_ts"])
#
#
#jnd = jnd.withColumn('is_answered',check_isin_udf(F.lead('nlu_text').over(window),
#                                                  F.lead('nlu_text',2).over(window),
#                                                  F.col('nbr_possible_answers'),
#                                                  F.col('nbr_response_code'),
#                                                  F.lag('nlu_positive_aspects').over(window),
#                                                  F.lead('nlu_positive_aspects').over(window),
#                                                  F.lag('nlu_subvertical').over(window),
#                                                  F.lead('nlu_subvertical').over(window),
#                                                  F.lag('nlu_positive_product_type').over(window),
#                                                  F.lead('nlu_positive_product_type').over(window),
#                                                  F.lead('nlu_positive_product_type',2).over(window)))
#
#
#
## fix ids  l22y83vocf, 00fma5y5xgf
#
#
#
#''' data set features '''
#''' DONT FORGET THAT YOUVE ADDED RESPONSE TIME TO TS MIGHT BE A HUGE BIAS '''
#
#
#jnd = jnd.withColumn('question_rank',F.sum(F.when(F.col('nbr_response_code').isNotNull(),1).otherwise(0)).over(window))
#
#jnd = jnd.withColumn('time_from_start',F.col('jnd_ts')-F.min('jnd_ts').over(window))
#
#jnd = jnd.withColumn('sum_answer',F.sum(F.lag('is_answered').over(window)).over(window))
#
#
#jnd = jnd.withColumn('num_quick_replies',F.size('nbr_possible_answers'))
#
#jnd = jnd.withColumn('hour',F.hour('nbr_date'))
#
#
#jnd = jnd.withColumn('day_of_week',F.date_format('nbr_date','u'))
#
#jnd = jnd.withColumn("last_nbr_code", F.last(F.lag("nbr_response_code").over(window), True).over(window))
#
#
#nlu_cols = ['nlu_intents_list',
# 'nlu_subvertical',
# 'nlu_positive_aspects',
# 'nlu_positive_product_type',
# 'nlu_positive_brands',
# 'nlu_negative_aspects',
# 'nlu_negative_product_type']
#
#for c in nlu_cols:
#    
#    jnd = jnd.withColumn("last_{}".format(c), F.last(c, True).over(window))

# fix ids  l22y83vocf, 00fma5y5xgf

#%%
    
''' jnd example '''
cols = ['jnd_retailer','nlu_positive_product_type','nlu_intents_list','nlu_positive_aspects','nlu_text','nlu_date','is_answered','nbr_date','nbr_ack_text','nbr_possible_answers','nbr_response_code','time_from_start','question_rank','sum_answer','jnd_sender_id','jnd_ts']
delete = jnd2.where(jnd2.jnd_sender_id=='ctv7gsd6u4').select(cols).toPandas()
#000f20b8le

#%%

''' yes no '''

qs =jnd.select(['nbr_response_code','nbr_possible_answers']).dropDuplicates(['nbr_response_code']).toPandas()



yes_no = qs[qs["nbr_possible_answers"].astype(str)=="['yes', 'no']"]["nbr_response_code"].tolist()
 
#%%


cols = ['jnd_retailer','jnd_sender_id','jnd_ts','is_answered','nbr_response_code','last_nbr_code','question_rank','time_from_start','sum_answer','num_quick_replies','hour','day_of_week']
cols = cols + ["last_{}".format(c) for c in nlu_cols]

jnd = jnd.fillna( { 'is_answered':0 } )

df = jnd.where((jnd.nbr_response_code.isNotNull()) & (~jnd.nbr_response_code.isin(yes_no))).select(cols)


#%%
''' add ga data '''
conv = sqlContext.read.options(header=True,sep='\t').csv('/Users/amirdavidoff/Desktop/data/enriched_data/conv_summ_proc.csv')
conv_cols = ['retailer_id','conversation_id','customer_type','platform','device','os','app']
conv = conv.select(conv_cols)
#71849
# filer july 
conv = conv.filter((F.to_date("created_at") >= F.lit('2019-07-01')))
#71849

''' api msg's '''
api_msg = sqlContext.read.options(header=True).csv('/Users/amirdavidoff/Desktop/data/yaron_sql_dumps/api_messages.csv')# ,sep='\t'
#2186424


api_msg = api_msg.dropDuplicates(['conversation_id','user_id'])
#697511

for c in api_msg.columns:
    
    api_msg = api_msg.withColumnRenamed(c,"api_msg_{}".format(c))


''' join conv and apit msg'''
conv_api_msg = conv.join(api_msg,conv.conversation_id==api_msg.api_msg_conversation_id,how='left')
#conv_api_msg.count()
#71849


#conv_api_msgp = conv_api_msg.toPandas()

cols_api_msgs = ['api_msg_user_id','customer_type','platform','device','os','app']


conv_api_msg = conv_api_msg.select(cols_api_msgs).dropDuplicates(['api_msg_user_id'])

df = df.join(conv_api_msg,df.jnd_sender_id==conv_api_msg.api_msg_user_id,how="left")



#dfp2 = jnd.where(jnd.nbr_response_code.isNotNull()).select(cols).toPandas()

# TODO:
#%%

''' heatmap '''
#from pyspark.mllib.stat import Statistics
#
#num_col = ['question_rank','time_from_start','sum_answer','num_quick_replies']
#
#
#features = df.select(num_col).rdd.map(lambda row: row[0:])
#
#pear_corr_mat=Statistics.corr(features, method="pearson")
#pear_corr_df = pd.DataFrame(pear_corr_mat)
#pear_corr_df.index, pear_corr_df.columns = num_col, num_col
#
#import seaborn as sns
#
#fig = plt.figure(figsize=(5,5))
#sns.heatmap(pear_corr_df, annot=True)


plt.hist(dfp2["sum_answer"],range=(0, 10),bins=30)
plt.title("sum_answer")
plt.show()


from bubble_plot.bubble_plot import bubble_plot




bubble_plot(dfp2,x='hour',y='day_of_week',z_boolean='is_answered',figsize=(30, 5))

#codes = (dfp2["nbr_response_code"].value_counts(normalize=True)>0.1)
#codes = codes[codes==True]
#
#temp = dfp2[dfp2["nbr_response_code"].isin(codes.index.tolist())]
#bubble_plot(dfp2,x='nbr_response_code',y='jnd_retailer',z_boolean='is_answered',figsize=(30, 5),fontsize=5)

#bubble_plot(dfp2,'time_from_start','is_answered', normalization_by_all=False)

#bubble_plot(data,’age’,’target’, normalization_by_all=False)

#%%
''' only uspolo '''
df = df.where(df.jnd_retailer=='962')


#%%



#%%

''' examine cat cols distribution '''

cat_col = ['nbr_response_code','last_nbr_code','jnd_retailer'] + ["last_{}".format(c) for c in nlu_cols] #+['customer_type','platform','os','app','device'] # ,'device'
num_col = ['question_rank','time_from_start','sum_answer','num_quick_replies']+ ['hour','day_of_week']

for c in num_col:
    
    df = df.withColumn(c,F.col(c).cast(DoubleType()))

for c in cat_col:
    
    df = df.withColumn(c,F.col(c).cast(StringType()))

schema = StructType([StructField("row_number", IntegerType())])
cat_dist = spark.createDataFrame([[i] for i in range(1,41)],schema=schema)       
    
for c in cat_col:
    
    temp = df.groupBy(c).count().withColumn('percent_'+c,F.col('count')/df.count())
    window = Window.orderBy(temp["percent_"+c].desc())
    temp = temp.withColumn("row_number",F.row_number().over(window))
#    temp = temp.where(F.col('percent_'+c)>=0.005)
    temp = temp.where(F.col('row_number')<=40)    
    cat_dist = cat_dist.join(temp,on="row_number",how="left")


cat_dist_pd = cat_dist.toPandas()
#cat_dist_pd.sort_values(by='rank')

# insights : remove positive brand and both negatives

rmv = ['last_nlu_positive_brands','last_nlu_negative_aspects','last_nlu_negative_product_type']
cat_col = [c for c in cat_col if c not in rmv]


''' number of distinct '''
num_of_distincts=[]
for c in cat_col:
   num_of_distincts.append(df.agg(F.countDistinct(c)).collect()[0][0])


cat_distinct_pd = pd.DataFrame([num_of_distincts],columns=cat_col,index=['#distincts'])
                               
# insights : check the field values for bondeling         

             #%%                  
'''  ohter '''
import time
s = time.time()                               
for feature in cat_col:
    
    values_to_keep = cat_dist_pd[cat_dist_pd["percent_"+feature]>=0.005][feature].tolist()
    
    df = df.withColumn(feature+"_replaced",F.when(F.col(feature).isin(values_to_keep),F.col(feature)).otherwise(F.lit("other")))
                               
''' new dist '''
schema = StructType([StructField("row_number", IntegerType())])
cat_dist_after_replacement = spark.createDataFrame([[i] for i in range(1,41)],schema=schema)       
    
for c in [i+"_replaced" for i in cat_col]:
    
    temp = df.groupBy(c).count().withColumn('percent_'+c,F.col('count')/df.count())
    window = Window.orderBy(temp["percent_"+c].desc())
    temp = temp.withColumn("row_number",F.row_number().over(window))
    #temp.where(temp.rank<=20).show()
    cat_dist_after_replacement = cat_dist_after_replacement.join(temp,on="row_number",how='left')


cat_dist_pd_after_replacement = cat_dist_after_replacement.toPandas()


cat_cols_replaced = [i+"_replaced" for i in cat_col]


e = time.time()
print("took : {}".format(e-s))

#%%
''' dummy '''

y_col = ["is_answered"]
x_col = cat_cols_replaced + num_col

dfp = df.select(['jnd_sender_id']+y_col+x_col).toPandas()

dfp.dtypes

dfp = pd.concat([dfp.drop(cat_cols_replaced, axis=1), pd.get_dummies(dfp[cat_cols_replaced])], axis=1)

x_col_dummied = [c for c in dfp.columns if c !=y_col[0]]

import re
regex = re.compile(r"\[|\]|<", re.IGNORECASE)

dfp.columns = [regex.sub("_", col) if any(x in str(col) for x in set(('[', ']', '<'))) else col for col in dfp.columns.values]

x_col_dummied = [c for c in dfp.columns if c !=y_col[0]]

x_col_dummied.remove('jnd_sender_id')

dfp = dfp[dfp["time_from_start"]<=500]


#dfp.to_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/df.csv')

# dfp = pd.read_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/df.csv',index_col=0)

#%%

''' bubble '''

bubble_plot(dfp,x='nbr_response_code_replaced_shopping_open_question_no_gender',y='last_nlu_intents_list_replaced__Not Understood, neutral_',z_boolean='is_answered',figsize=(5, 5),fontsize=10)
plt.title('nbr no gender - last not understood')


bubble_plot(dfp,x='nbr_response_code_replaced_offer_other_products_for_type',y='last_nlu_intents_list_replaced__Not Understood, neutral_',z_boolean='is_answered',figsize=(5, 5),fontsize=10)
plt.title('nbr offer other - last not understood')

bubble_plot(dfp,x='last_nlu_intents_list_replaced_other',y='nbr_response_code_replaced_style_question',z_boolean='is_answered',figsize=(5, 5),fontsize=10)
plt.title('last intents other - style')


bubble_plot(dfp2,x='hour',y='day_of_week',z_boolean='is_answered',figsize=(30, 5))




#bubble_plot(dfp,x='sum_answer',y='last_nbr_code_replaced_shopping_open_question_no_gender',z_boolean='is_answered',figsize=(5, 5),fontsize=10)


#%%
''' model '''




from generic_imports import *



X_train, X_test, y_train, y_test = train_test_split(
    dfp[["jnd_sender_id"]+x_col_dummied], dfp[y_col], test_size=0.20, random_state=42,stratify = dfp[y_col])


import xgboost as xgb
res = train_cv_model(X_train.drop('jnd_sender_id',axis=1),y_train)


#%%
print("mean auc roc of 2 fold test sets : {}".format(np.mean(res["test_auc"])))

model = res["model"]

test_pred = model.predict_proba(X_test.drop('jnd_sender_id',axis=1))[:,1]




print("auc roc on help out data : {}".format(roc_auc_score(y_test,test_pred)))
#0.76

#%%

# check and change sum asnwer, check importnace




summary_report(y_test,test_pred)   
    
#summary_report(y_test,test_pred,True,'/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/uspolo/imp')
#%%

importance(model,'gain')
#importance(model,'gain',True,'/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/uspolo/imp')

#%%
''' feature selection '''

import operator

imp_dic =  model.get_booster().get_score(importance_type='gain')

imp_dic = sorted(imp_dic.items(), key=operator.itemgetter(1), reverse=True)


top_features = [c[0] for c in imp_dic[:20]]



#%%
res = train_cv_model(X_train[top_features],y_train)
#%%

np.mean(res["test_auc"])

model = res["model"]

test_pred = model.predict_proba(X_test[top_features])[:,1]

roc_auc_score(y_test,test_pred)


#%%

''' shap '''

import shap


shap_values = shap.TreeExplainer(model).shap_values(X_test.drop("jnd_sender_id",axis=1))
#%%
shap.summary_plot(shap_values, X_test.drop("jnd_sender_id",axis=1),plot_size=(20,10),max_display=30,show=False)
#plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/uspolo/shap/summary_shap.png')

#%%
''' shap top features against all '''


import os

for c in top_features:
    
    shap.dependence_plot(c, shap_values, X_test.drop("jnd_sender_id",axis=1),show=False)
    plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/uspolo/shap/{}_shap.png'.format(c))
#%%
top_3 = [c[0] for c in imp_dic[:3]]
for c in top_3:
    
    try:
        os.mkdir('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/uspolo/shap/{}'.format(c))
    except:
        pass
    
    for r in top_features:
        if r!=c:
            

            shap.dependence_plot(c, shap_values, X_test.drop("jnd_sender_id",axis=1),interaction_index=r,show=False)
            plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/uspolo/shap/{}/{}_{}'.format(c,c,r))


#%%
shap.dependence_plot("sum_answer", shap_values, X_test.drop("jnd_sender_id",axis=1),interaction_index='nbr_response_code_replaced_customer_care_url')

#%%

shap.dependence_plot("time_from_start", shap_values, X_test.drop("jnd_sender_id",axis=1),interaction_index='last_nlu_intents_list_replaced__New Search, neutral_')

#%%
shap.dependence_plot("hour", shap_values, X_test.drop("jnd_sender_id",axis=1),interaction_index='nbr_response_code_replaced_customer_care_url')

#%%

''' examine application '''


example = X_test.iloc[60,:]

optional_questions = [c for c in example.index if "nbr_response_code" in c]

sender_id = example["jnd_sender_id"]

original_rank = example["question_rank"]

probs = []

original_question = example[optional_questions]==1
original_question = original_question[original_question==True].index[0]

for c in optional_questions:
    
    try:
        
  
        
        example[optional_questions] = 0
        example[c] = 1
        
        delete = example.to_frame().T.drop("jnd_sender_id",axis=1)
        delete= delete.apply(pd.to_numeric)
        
        probs.append(model.predict_proba(delete,validate_features=False)[:,1][0])    
    except:
        import pdb
        pdb.set_trace()
    
    

    
temp = pd.DataFrame([optional_questions,probs]).T
temp.columns =["questions","probs"]
temp = temp.sort_values("probs")
temp = temp.reset_index(drop=True)

    

fig = plt.figure(figsize=(10,10))
plt.scatter(temp["probs"],temp["questions"])
plt.scatter(temp[temp["questions"]==original_question]["probs"],temp[temp["questions"]==original_question]["questions"],color="red")
plt.title("orig q is marked by red.\nsender_id = {} ; q_rank = {} ".format(sender_id,original_rank))


#%%

cols = ['jnd_retailer','nlu_positive_product_type','nlu_intents_list','nlu_positive_aspects','nlu_text','nlu_date','is_answered','nbr_date','nbr_ack_text','nbr_possible_answers','nbr_response_code','time_from_start','question_rank','sum_answer','jnd_sender_id','jnd_ts']
delete = jnd.where(jnd.jnd_sender_id==sender_id).select(cols).toPandas()

# check these with efrat : fsjkshdffc, y1lvn8efa1, l22y83vocf, 348oj2lrqm, tzv4sqotg5
#example2 = X_test[X_test["jnd_sender_id"]=='fsjkshdffc']


#%%

q_dic = jnd.select(['nbr_response_code','nbr_possible_answers']).dropDuplicates(['nbr_response_code']).toPandas()


#%%

dfp[dfp["nbr_response_code_replaced_consumer_mismatch_one_group"]==1]

#%%

from sklearn.metrics import confusion_matrix

confusion_matrix(y_test, np.where(test_pred<=np.median(test_pred),0,1))

#%%

from sklearn.metrics import classification_report

print(classification_report(y_test, np.where(test_pred<=np.mean(test_pred),0,1)))