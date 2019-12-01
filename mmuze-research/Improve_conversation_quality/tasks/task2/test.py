#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 11:40:00 2019

@author: amirdavidoff
"""



final = final.withColumn("source",F.lit("first"))
final = final.where(final.max_rank>1)
#12663

last = last.withColumn("source",F.lit("last"))
last = last.where(last.max_rank > 1)
#13137

mrg = final.dropDuplicates(['timestamp','sender_id']).union(last.dropDuplicates(['timestamp','sender_id']))
mrg.count()
# 20840


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
            
    grp["ratio"] = grp["count"] / np.sum(grp["count"])
    
    grp = grp.assign(ratio=grp['count'].div(grp.groupby('source')['count'].transform('sum')))

    
    grp["add_to_cart_rate"] =  grp["add_to_cart"].apply(np.mean)
    grp["ga_add_to_cart_rate"] =  grp["ga_add_to_cart"].apply(np.mean)
    grp["ga_converted_rate"] =  grp["ga_converted"].apply(np.mean)
    
    grp["add_to_cart_ci"] =  grp["add_to_cart"].apply(binary_ci)
    grp["ga_add_to_cart_ci"] =  grp["ga_add_to_cart"].apply(binary_ci)
    grp["ga_converted_ci"] =  grp["ga_converted"].apply(binary_ci)
        
        
    MIN_RATIO = 0.01
  
    ''' plot bar plots '''
    
    grp = grp[grp["ratio"]>=MIN_RATIO]
    
    
    fig = plt.figure(figsize=(12,12))

    for src in ['first','last']:

        
    
        temp = grp[grp["source"]==src]
        temp = temp.sort_values("ratio",ascending=True)
        
        plt.barh(temp[c].astype(str).tolist(),temp['ratio'],label=src+"_nlu",alpha=0.5)
        plt.title(c)
        plt.legend()
    plt.show()
    
    
    # when back check positive_product_type, subvertical, how come all have higher for last
    # check how come negative aspects is empty
    # when back try the same for the rate plot, will be trickeer
    
    
