#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 17:00:36 2019

@author: amirdavidoff
"""

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




jnd = sqlContext.read.parquet('/Users/amirdavidoff/Desktop/data/enriched_data/jnd')

jnd.count()

jnd.select('nlu_retailer_id').distinct().show()

jnd = jnd.withColumn('jnd_retailer',F.when(F.col('nlu_retailer_id').isNull(),F.col('nbr_retailer_id')).otherwise(F.col('nlu_retailer_id')))


from generic_imports import *




def collect_values(ls):
    
    try:
        
        return [c[1] for c in ls]
    
    except:
        
        None

collect_values_udf = F.udf(collect_values,ArrayType(StringType()))

jnd = jnd.withColumn('nbr_possible_answers',collect_values_udf(F.col('nbr_possible_values')))





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




''' plot response code by answer '''

rates = jnd.select(['nbr_response_code','is_answered','jnd_retailer']).toPandas()
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
plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task5/nbr_code_ctr2.png')
plt.show()






''' sdk '''
sdk_path = '/Users/amirdavidoff/Desktop/data/my_sql_export/sdk_dump_test.csv'


sdk_path = '/Users/amirdavidoff/Desktop/data/yaron_sql_dumps/sdk_reports.csv'


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

sdk_cols = ['retailer_id','user_id','timestamp','date','date_time','action','value','created_at']
spark_sdk = spark_sdk.select(sdk_cols)

spark_sdk = spark_sdk.withColumn('rank',F.rank().over(sdk_window))
spark_sdk = spark_sdk.withColumn('max_rank',F.max('rank').over(sdk_window_no_order))

spark_sdk = spark_sdk.withColumn('created_at_dates',F.collect_list(F.col('created_at').cast(StringType())).over(sdk_window))
spark_sdk = spark_sdk.withColumn('actions',F.collect_list('action').over(sdk_window))

spark_sdk = spark_sdk.withColumn('first_click',F.when((F.sum(F.when(F.col('action')==F.lit('click'),1)).over(sdk_window))==1,1))


spark_sdk = spark_sdk.withColumn('first_click_date',F.when(F.col('first_click')==1,F.col('created_at')))


spark_sdk = spark_sdk.withColumn('first_click_date_filled',F.first('first_click_date',ignorenulls=True).over(sdk_window_no_order))


#delete = spark_sdk.where(spark_sdk.user_id=='0p7b5jtiawf').toPandas()


spark_sdk2 = spark_sdk.where(spark_sdk.rank==spark_sdk.max_rank)

#sdkp2 = spark_sdk2.toPandas()

sdk question:
    
    0p7b5jtiawf
    1. ts no good
    2. hwo add to cart before click 
    



''' join jnd and sdk '''
jnd2 = jnd.join(spark_sdk2.select(['user_id','created_at_dates','actions','retailer_id','first_click_date_filled']),spark_sdk.user_id==jnd.jnd_sender_id,how="left")
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


for retailer in convs_grp['retailer'].unique().tolist():
    
    for c in ['click','add_to_cart']:
    
        temp = grp[grp["retailer"]==retailer]
        
        plt.errorbar([i for i in range(len(temp[c+"_rate"]))], temp[c+"_rate"].values, yerr=temp[c+"_ci"].values, fmt='o',label=c+"_"+retailer)
        
        plt.xticks([i for i in range(len(temp[c+"_rate"]))], temp["cut"])
        plt.legend()
           
        
        plt.title("user answer count effect on click / add to cart")
        plt.ylabel('rate')
        plt.xlabel("answer count")
#plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task5/answer_click.png')
plt.show()


'''example '''

cols = ['qs','jnd_retailer','nlu_positive_product_type','nlu_intents_list','nlu_positive_aspects','lag_type','lag_type2','lag_type3','lag_type_filled','nlu_text','nlu_date','lag_nlu_date','is_answered','nbr_date','q_rank','nbr_ack_text','nbr_possible_answers','nbr_response_code','type_q_rank','type_q_rank2','jnd_sender_id','jnd_ts']
delete = jnd2.where(jnd2.jnd_sender_id=='0ro7an9h4w').select(cols).toPandas()




cc
1.  frustration
2.  not understood
3.  sub1 > sub2 > cc jox0pehf00i

npt
1. cassidy recognize as phone case 49hhgq65x6k

jnd2.where((jnd2.jnd_retailer=='962')&(jnd2.nbr_response_code=='no_products_for_type')).select('jnd_sender_id').dropDuplicates().show(100)

jnd2.where((jnd2.nbr_response_code=='no_products_for_type')).select('jnd_sender_id').dropDuplicates().show(100)



4rbvkcyxbrg
q3lkdjo0md

dlhm61rkwg

''' q order for each type '''

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

    plt.savefig('/Users/amirdavidoff/Desktop/delete2/{}.png'.format(typ))
    plt.show(block=False)    
    plt.close(fig)




#problems:


# investigate:

''' original order'''






original_questions_dic = {}

original_questions_dic["shirt"] = ['1_sleeve style_question','2_color_question']
original_questions_dic["jeans"] = ['1_fit_question','2_color_question']
original_questions_dic["pants"] = ['1_fit_question','2_color_question']
original_questions_dic["shorts"] = ['1_fit_question','2_color_question']


types = ["shirt",'jeans','pants','shorts']


jnd2 = jnd2.withColumn('qs',F.collect_list('q_rank').over(window))

for typ in types:
    
        
    jnd2 = jnd2.withColumn('check',
            (F.array_contains(jnd2.qs, original_questions_dic[typ][0])) &
            ( F.array_contains(jnd2.qs,  original_questions_dic[typ][1])))
    #jnd2.where(jnd2.check==True).select('jnd_sender_id').dropDuplicates().show()
    
    conv_ids = jnd2.where(jnd2.check==True).select('jnd_sender_id').dropDuplicates()#.rdd.flatMap(lambda x : x).collect()
    conv_ids = conv_ids.withColumnRenamed("jnd_sender_id","jnd_sender_id2")
    jnd2 = jnd2.withColumn('q_rank2',F.concat(F.col('type_q_rank2'),F.lit('_'),F.col('nbr_response_code')))
    
    
    temp2 = jnd2.join(conv_ids,jnd2.jnd_sender_id==conv_ids.jnd_sender_id2)
    
    temp = temp2.where((temp2.nbr_response_code.isNotNull()) & (temp2.lag_type.isNotNull() ) ).select(cols)

    
    temp2 = temp2.where(temp2.lag_type==typ)
    
    grp2 = temp2.groupBy(['lag_type','q_rank2']).agg(
                    
                
                F.count('is_answered').alias('count'),
                F.mean('is_answered').alias('answer_rate')
                
            
            ).toPandas()


    
    grp2["ci"] = grp2[["answer_rate","count"]].apply(binary_ci_pd,axis=1)
    grp2 = grp2[grp2["count"]>=30]
    grp2 = grp2.sort_values(["q_rank2"],ascending=False)
    
    grp2 = grp2[grp2["lag_type"].notnull()]
    
    fig = plt.figure(figsize=(7,7))    
    
    
    #temp = temp.sort_values("answer_rate")
    plt.errorbar(grp2["answer_rate"], grp2["q_rank2"], xerr=grp2['ci'].values, fmt='o')
        
    plt.xlabel('answer rate')
    plt.ylabel('order_question')
    plt.title('original order answer rate for type {}'.format(typ))
    plt.show()

plt.savefig('/Users/amirdavidoff/Desktop/delete/{}.png'.format(typ))
plt.show(block=False)    
plt.close(fig)









'''example '''

delete = jnd2.where(jnd2.jnd_sender_id=='espl42nfi').toPandas()
cols = ['created_at_dates','actions',"nbr_count",'nlu_positive_product_type','lag_type','nlu_subvertical','nlu_positive_aspects','nlu_positive_brands', 'nlu_negative_aspects','nlu_negative_product_type','nlu_text','nlu_date','nbr_ack_text','nbr_possible_answers','is_answered','nbr_date','nbr_response_code','type_q_rank','nbr_possible_values','jnd_sender_id','jnd_ts']
cols = ['check','check_sum','qs','retailer_id','nlu_positive_product_type','nlu_positive_aspects','lag_type','lag_type2','lag_type3','lag_type_filled','nlu_text','nlu_date','is_answered','nbr_date','q_rank','nbr_response_code','type_q_rank','type_q_rank2','jnd_sender_id','jnd_ts']
delete = delete[cols]




jnd2.where(jnd2.q_rank=='sleeve style_question_2').select('jnd_sender_id').dropDuplicates().show()



''' for each type codes dist '''

window_type = Window.partitionBy(["jnd_sender_id",'nlu_positive_product_type']).orderBy(["jnd_ts"])


#&F.lag(F.col('nlu_positive_product_type')).over(window).isNotNull()
#jnd2 = jnd2.withColumn('code_rank',F.sum(F.when(F.col('nbr_response_code').isNotNull(),1).otherwise(0)).over(window_type))

jnd2 = jnd2.withColumn('lead_code',F.lead(F.col('nbr_response_code')).over(window))#F.lit('_'),F.col('code_rank')))

subset = jnd2.select(['nlu_positive_product_type','lead_code','jnd_sender_id']).toPandas()

subset = subset.dropna()

delete = subset.groupby('nlu_positive_product_type')['lead_code'].value_counts()

subset['shift_nbr_response_code'] = subset['nbr_response_code'].shift(-1)

delete = jnd2p[jnd2p['nlu_positive_product_type']!=None].groupby(['nlu_positive_product_type'])['nbr_response_code'].value_counts()





''' for each code ctr by type '''

jnd2 = jnd2.withColumn('lead_result',F.lead(F.col('is_answered')).over(window))#F.lit('_'),F.col('code_rank')))
jnd2 = jnd2.withColumn('lag_type',F.lag(F.col('nlu_positive_product_type')).over(window))#F.lit('_'),F.col('code_rank')))


grp_type = jnd2.select(['lag_type','is_answered','nbr_response_code','jnd_sender_id']).toPandas()
grp_type = grp_type.dropna()

grp_type2 = grp_type.groupby(['lag_type','nbr_response_code'],as_index=False).agg({"is_answered":["count","mean",binary_ci]})
grp_type2.columns = ['type','code','count','ctr','ci']
grp_type2 = grp_type2.sort_values(['type','count'],ascending=False)



''' add time to question '''

jnd2 = jnd2.withColumn('time_to_q',F.when(F.col('nbr_date').isNotNull(),(F.max(F.col('jnd_ts')).over(window)-F.min(F.col('jnd_ts')).over(window))))


delete = jnd2.where(F.col('is_answered').isNotNull()).select(['jnd_sender_id','is_answered','time_to_q']).toPandas()

delete = delete[delete["time_to_q"]>0]

delete2 = jnd2.where(jnd2.jnd_sender_id=='ic3tdgezl8').select(cols+['time_to_q']).toPandas()


delete["answers_cut"] = pd.cut(delete.time_to_q, [0,1,2,3,4,5,6,7,8,9,10,15,20,30,60,90,np.Inf], include_lowest=True,right=False)
delete["answers_cut"].value_counts()

grp = delete.groupby(['answers_cut']).agg({'is_answered':["count","mean",binary_ci]})

grp.columns = ['count','mean','ci']



fig = plt.figure(figsize=(7,7))
plt.errorbar(grp.index.astype(str).tolist(), grp["mean"].values, yerr=grp["ci"].values, fmt='o',label=c)



# WHEN BACK, THINK WHAT MIGHT EFFET ANSWER, QUESTION TIME SINCE STARRT? QUESTION RANK ?

# add lag type

''' research specific response code'''


jnd.where((F.col('nbr_response_code')=='color_question') & (F.col('is_answered')==1)).select('jnd_sender_id').show()


#|  6t2ngepckyi|
#|   qlb4vslaa2|
#|   gxw5c240e4|
#|   dlhm61rkwg|
#|   dlhm61rkwg|
#|   n5my0fw1qq|
#|   j4sdmh91fw



#   & (F.col('is_answered')==0)

jnd.where((F.col('nbr_response_code')=='product_type_selection')& (F.col('is_answered')==1)).select('jnd_sender_id').dropDuplicates().show()


# answered me but not quick replies

# does nlu round ts up or down ? here ps4juwg6d3x, q3lkdjo0md
# asnwered  = 'np0egk6hdn','',''


# first can mark fro mpossible values !! do it




# todo:
# bug to fix oh397wdxkv two problems. one was not realy answered and down below was answered.
# check rates high count low answer






''' sample '''
delete = jnd2.where(jnd2.jnd_sender_id=='1l5aq45x9y').toPandas()
cols = ['created_at_dates','actions',"nbr_count",'nlu_positive_product_type','nlu_subvertical','nlu_positive_aspects','nlu_positive_brands', 'nlu_negative_aspects','nlu_negative_product_type','nlu_text','nlu_date','nbr_ack_text','nbr_possible_answers','is_answered','nbr_date','nbr_response_code','nbr_possible_values','jnd_sender_id','jnd_ts']
delete = delete[cols]


q_dic={}

for ids in ['4rbvkcyxbrg','1l5aq45x9y','3zcod19h0qg','auetez5bre','02b8g1clfm','m3w8d28cu6','0qzsqg4pmwh','7o9y9jjt1v','hwsm2slzmz','w0m9didjj8']:
    
    q_dic[ids] = jnd2.where(jnd2.jnd_sender_id==ids).select(cols).toPandas()




''' last nlu click ratio '''


cols = ['nlu_timestamp','nlu_date','nlu_intents_list',
 'nlu_subvertical',
 'nlu_positive_aspects',
 'nlu_positive_product_type',
 'nlu_positive_brands',
 'nlu_negative_aspects','nlu_negative_product_type','first_click_date_filled','jnd_sender_id','jnd_ts','jnd_retailer']


temp = jnd2.where(jnd2.nlu_timestamp.isNotNull()  ).select(cols) #& jnd2.first_click_date_filled.isNotNull()


'''when back find a way to leave last with no click to compare later '''


window = Window.partitionBy("jnd_sender_id").orderBy('jnd_ts')

window_no_order = Window.partitionBy("jnd_sender_id")


temp = temp.withColumn('sum_nlu_before_click',F.sum(F.when((temp.nlu_timestamp <= F.unix_timestamp(F.col('first_click_date_filled')))
                                                    ,1).otherwise(None)).over(window))
        
        
temp = temp.withColumn('max_nlu_before_click',F.max('sum_nlu_before_click').over(window_no_order))

#delete = temp.where(temp.jnd_sender_id=='018taf0v6a').toPandas()


temp = temp.withColumn('sum_without_click',F.sum(F.when(temp.first_click_date_filled.isNull(),1).otherwise(0)).over(window))
temp = temp.withColumn('max_without_click',F.max('sum_without_click').over(window_no_order))



temp = temp.withColumn('last_click_match',F.when((F.sum(F.when(temp.sum_nlu_before_click==temp.max_nlu_before_click,1).otherwise(0)).over(window))==1,1))


#temp = temp.where(temp.nlu_timestamp <= F.unix_timestamp(F.col('first_click_date_filled')))


last_nlu_click = temp.where((temp.last_click_match==1) |(temp.first_click_date_filled.isNull() & (temp.max_without_click==temp.sum_without_click))).dropDuplicates(['nlu_timestamp','jnd_sender_id'])


#last_nlu_click["click_flag"] = np.where(last_nlu_click["first_click_date_filled"].isnull(),"no_click","click")

last_nlu_click = last_nlu_click.withColumn('click_flag',F.when(F.col('first_click_date_filled').isNull(),F.lit("no_click")).otherwise(F.lit("click")))


cols_to_examine = ['nlu_intents_list',
     'nlu_subvertical',
     'nlu_positive_aspects',
     'nlu_positive_product_type',
     'nlu_positive_brands',
     'nlu_negative_aspects',
     'nlu_negative_product_type',
]

''' plot click no click for nlu'''

for c in cols_to_examine:
    
    grp = last_nlu_click.groupBy(["click_flag",c]).agg(
                        
                        F.count(c).alias('count'),
           
                        ).toPandas()
        
    # adding column ratio that represents the share of count per source (first / last nlue)        
    grp = grp.assign(ratio=grp['count'].div(grp.groupby('click_flag')['count'].transform('sum')))
        
    MIN_RATIO = 0.01

    grp = grp[grp["ratio"]>=MIN_RATIO]

        
    fig = plt.figure(figsize=(12,12))

    for src in ['no_click','click']:
    

        
            temp = grp[grp["click_flag"]==src]
            temp = temp.sort_values("ratio",ascending=True)
            
            plt.barh(temp[c].astype(str).tolist(),temp['ratio'],label=src,alpha=0.5)
            plt.title("last "+c+"\n(only shwoing values ratioed >{}%)".format(MIN_RATIO*100))
            plt.legend()

    plt.savefig('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task7/{}.png'.format(c))
    plt.show()



'''
new q:
    order here for shirt is different: f8kqmesuuw




1. not dash shown ids = (w0m9didjj8,1043672409.1567289302), (m3w8d28cu6,1790097369.1544969389), (auetez5bre,1977010956.1560529556)

agg q's:
    
    1. why sleeve if polo ? 4rbvkcyxbrg
    
     
    
    1. how do i know this clarify has been answered ? 1l5aq45x9y
    
    1. several user responses 3zcod19h0qg
    
    1. why would someone start with new search ? 10vzaztcka auetez5bre 
    
    1. why empty responses with no continuation ? 02b8g1clfm
    1. how come several nbr q's without response ? 5kfb52am61l 4wnzce1tw1 ci6odhcggx 1ixrn8ibyt
    2. some conv's go suddenly to customer care, why?  GET IDS HERE  m3w8d28cu6
    3. why sometimes we dont offer any question like here 0qzsqg4pmwh
    4. timing mili 7o9y9jjt1v oh397wdxkv
    5. same quick replies for product_type_selection and no_products_for_type and product_type_selection_second, hwsm2slzmz
        even thought product_type_selection has higher
    6. two user responses w0m9didjj8
    
    
    
problematic:
    
    zeo04kr948 2fab73rcua d48kxdzxr5
    
    
notes:
    1. some conv's go suddenly to customer care, why?
    2. remove my addition of nbr respons time
    3. q's change with time without answer, why? 5kfb52am61l 4wnzce1tw1

get to joind:
    
    1. num of products
    2. nlu change text

1. check the problematic id and ask vicky.
    
    1.1 answered automatic yes, and should do mili
    
    1.2 yes\no when no product will return yes.

2. add answered mark if lead answer text is in possible values, and check how much answered?

    2.1 wmyyedstuj answered but not quick answeres

    2.2 another indication would be the nlu
    

'''