#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 27 12:11:57 2019

@author: amirdavidoff
"""



api_msg_path = '/Users/amirdavidoff/Desktop/data/my_sql_export/api_messages2.txt'

api_msg = sqlContext.read.options(header=True,sep='\t').csv(api_msg_path)



api2 = sqlContext.read.options(header=True).csv('/Users/amirdavidoff/Desktop/data/yaron_sql_dumps/api_messages2.csv')




cols = ['jnd_sender_id','clicks','nlu_text','is_answered']

delete = jnd2.select(cols).toPandas()


delete = delete[delete['clicks'].isna()]


delete2 = delete.groupby('jnd_sender_id').size()

delete3 = delete.groupby('jnd_sender_id').agg({'is_answered':"sum","jnd_sender_id":"count"})


delete4 = delete3[(delete3["is_answered"]==0)&(delete3["jnd_sender_id"]>=30)]



path = '/Users/amirdavidoff/Desktop/data/yaron_sql_dumps/nlu_api_jobs.csv'
spark_nlu  = sqlContext.read.options(header=True,quote = '"',escape='"').csv(path)

delete2 = spark_nlu.limit(10000).toPandas()


nlu_enr = enrich_nlu(path)



nlu_enr.write.mode("overwrite").parquet('/Users/amirdavidoff/Desktop/data/tests')


delete = sqlContext.read.parquet('/Users/amirdavidoff/Desktop/data/tests')




conv_summ_path = '/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/conv_summ_proc.csv'
conv = sqlContext.read.options(header=True ,sep='\t').csv(conv_summ_path)
conv.columns
conv.count()

conv_summ_path2 = '/Users/amirdavidoff/Desktop/data/yaron_sql_dumps/conversation_summaries.csv'

conv2 = sqlContext.read.options(header=True,quote = '"',escape='"' ).csv(conv_summ_path2)
conv2.columns
conv2.count()



api_msg_path = '/Users/amirdavidoff/Desktop/data/my_sql_export/api_messages2.txt'
api_msg = sqlContext.read.options(header=True,sep='\t').csv(api_msg_path)# 
api_msg.columns


api_msg_path2 = '/Users/amirdavidoff/Desktop/data/yaron_sql_dumps/api_messages.csv'
api_msg2 = sqlContext.read.options(header=True).csv(api_msg_path2)# 
api_msg2.columns




def binary_ci(ag):
    ''' confidence interval for binary feature'''
    
    #print(ag)
    
 #   mean = np.mean(ag)
  #  n = len(ag)
  
    mean = ag[0]
    n = ag[1]
    
    return 1.96 * np.sqrt(mean*(1-mean)/n)


spark_binary_ci_udf = F.udf(spark_binary_ci,DoubleType())



target_columns = ['add_to_cart','ga_add_to_cart','ga_converted','ga_product_clicked']
agg_fields = []
agg_fields.extend([F.mean(F.col(col).cast(IntegerType())).alias((col + "_rate")) for col in target_columns])
        

delete4 = temp.groupBy(c).agg(
                    
                    
                    
                    # THIS PART IS NOT SMART MAKE IT MMORE EFICIENT NOT SAVING THE LISTS
                    
                    F.count(c).alias('count'),
                    *agg_fields
                    
                    
                    ).toPandas()
                    
for i in target_columns:
    delete4[i+"_ci"] = delete4[[i+"_rate","count"]].apply(binary_ci,axis=1)
                    
             
            
            df_dic[c]["ratio"] = df_dic[c]["count"] / np.sum(df_dic[c]["count"])
      
        
        
c= 'ga_product_clicked'

df = df.sort_values(c+"_rate",ascending=False)




fig = plt.figure(figsize=(6,6))


plt.errorbar([i for i in range(len(df[c+"_rate"]))], df[c+"_rate"].values, yerr=df[c+"_ci"].values, fmt='o')
plt.xticks([i for i in range(len(df[c+"_rate"]))], df[df.columns[0]],rotation=30)
        

plt.title(path+" "+key+" "+k)
plt.ylabel(k+'_rate')
plt.xlabel(key)
plt.savefig(dest_path+'/{}_rates/{}_{}.png'.format(path,k,key))  #'/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task2
plt.show(block=False)
plt.close(fig)