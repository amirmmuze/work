#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 10:34:39 2019

@author: amirdavidoff
"""


import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt



nbr_enr = nbr_enriched = sqlContext.read.parquet('/Users/amirdavidoff/Desktop/data/enriched_data/nbr')
nbr_enr.count()
#295239

nbr_enr = nbr_enr.filter((F.to_date("date") >= F.lit("2019-07-01"))&(nbr_enr.retailer_id=='429'))
nbr_enr.dropDuplicates().count()

nbr_enr =nbr_enr.withColumn("replaced_response_code", F.when(F.col('response_code').isNull(),F.lit("no_answer")).otherwise(F.col('response_code')))


window = Window.partitionBy("sender_id").orderBy(["ts_plus_response"])

window_no_order = Window.partitionBy("sender_id")


cols = ['sender_id','timestamp','conv','ts_plus_response','response_code','replaced_response_code']
nbr_enr2 = nbr_enr.select(cols)

nbr_enr2 = nbr_enr2.withColumn('rank',F.rank().over(window))
nbr_enr2 = nbr_enr2.withColumn('max_rank',F.max('rank').over(window_no_order))



def last_two(ls):
    
    try:
        return ls[-2:]
    except:
        return "error"


last_two_udf = F.udf(last_two,ArrayType(StringType()))


nbr_enr2 = nbr_enr2.withColumn('codes_list',last_two_udf(F.collect_list(F.col('replaced_response_code')).over(window)))

#remove first, leaves us only with >1 lists of codes
nbr_enr2 = nbr_enr2.where(nbr_enr2.rank > 1)


nbr_enr2 = nbr_enr2.withColumn('from',nbr_enr2.codes_list[0])
nbr_enr2 = nbr_enr2.withColumn('to',nbr_enr2.codes_list[1])

#%%

delete2 = nbr_enr2.where(nbr_enr2.sender_id=='041i2nuwtm').toPandas()


#%%

''' get conversion rates '''



''' conv summary'''
# read conversation sumeries tsv
conv = sqlContext.read.options(header=True,sep='\t').csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/preprocessing/enrich_data/conv_summ_proc.csv')
#109049

# filer july and 429
conv = conv.filter((F.to_date("created_at") >= F.lit("2019-07-01"))&(F.col('retailer_id')=='429'))
conv.count()
# 57361

conv.dropDuplicates(['conversation_id']).count()


#%%
''' adding vonersion to nbr '''



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




nbr3 = nbr_enr2.join(conv_api_msg.dropDuplicates(['api_msg_user_id']),conv_api_msg.api_msg_user_id==nbr_enr2.sender_id,how='left')

nbr3.count()

#%%

#removing short conv's to lower the weight for subvertical > subvertical_second

nbr3 = nbr3.where(nbr3.max_rank>3)

delete = nbr3.toPandas()

#%%%

grp = nbr3.groupBy(['from','to']).agg(
        
        
                    F.count(F.lit(1)).alias("count"),
                    F.sum('ga_add_to_cart').alias("sum_ga_add_to_cart")
        
        )



grpp = grp.toPandas()



grpp["weight"] = grpp["sum_ga_add_to_cart"] / grpp["count"]




#grpp = grpp.sort_values('ratio',ascending=False)
#grpp = grpp.reset_index(level=0, drop=True)
#grpp["cumsum"] =  grpp['ratio'].cumsum()



grpp = grpp[grpp["count"]>=100]

grpp = grpp.reset_index(level=0, drop=True)


#%%

grpp.to_csv('/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/task4/pairs.csv')


#%%


fig = plt.figure(figsize=(12,12))
ax = fig.gca()



G = nx.DiGraph()

for i in range(0,grpp.shape[0]):
    G.add_edges_from([(grpp['from'][i], grpp['to'][i])],weight=grpp["weight"][i])
    
#edge_labels=dict([((u,v,),d['weight'])
 #                for u,v,d in G.edges(data=True)])


pos = nx.circular_layout(G) #k=0.25
#pos = nx.shell_layout(G)

edges = G.edges()
weights = [G[u][v]['weight'] for u,v in edges]
weights2=[x*5 for x in weights]
d=nx.degree(G)

nx.draw(G,pos, with_labels=False,node_size=[v[1]*10 for v in d],node_color="cyan", width=weights2) #,ax=ax
nx.draw_networkx_labels(G,pos,font_color='r',font_size=14)#,ax=ax

plt.savefig("week3.png")
#plt.show()


#%%
#plt.show()
plt.clf()
plt.cla()