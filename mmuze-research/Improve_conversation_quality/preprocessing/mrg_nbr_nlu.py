#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  6 11:39:02 2019

@author: amirdavidoff



"""



sdk_enriched = sqlContext.read.parquet('/Users/amirdavidoff/Desktop/data/enriched_data/sdk')

sdk_enriched.count()


sdk_enriched2 = sdk_enriched.where(sdk_enriched.action=='add to cart')

sdk_enrichedp = sdk_enriched2.select(['user_id','conv','timestamp','date','action']).toPandas()





nbr_enrichedp = nbr_enriched.select(['sender_id','conv','date','timestamp']).toPandas()


sdk_enrichedp.columns = ['sdk_'+c for c in sdk_enrichedp.columns]

nbr_enrichedp.columns = ['nbr_'+c for c in nbr_enrichedp.columns]



test = nbr_enrichedp.merge(sdk_enrichedp,left_on=['nbr_sender_id','nbr_conv'],
                           right_on=['sdk_user_id','sdk_conv'],suffixes=('_left', '_right'))






sample = nbr_enriched.where(nbr_enriched.sender_id=='czqir37lTft').toPandas()


sdk_sample = sdk_enriched.where(sdk_enriched.user_id=='czqir37lTft').toPandas()