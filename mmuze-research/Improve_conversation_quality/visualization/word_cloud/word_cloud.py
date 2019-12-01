#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 17:21:35 2019

@author: amirdavidoff
"""



nlu_enr = nbr_enriched = sqlContext.read.parquet('/Users/amirdavidoff/Desktop/data/enriched_data/nlu')
nlu_enr.count()
#835833

nlu_enr = nlu_enr.filter((F.to_date("date") >= F.lit("2019-07-01"))&(nlu_enr.retailer_id=='429'))
nlu_enr.count()
#73622



nlu_enr = nlu_enr.dropDuplicates(['sender_id','timestamp'])
nlu_enr.count()
#66039



grp = nlu_enr.select('text').groupBy('text').count().toPandas()

text = nlu_enr.select('text').rdd.flatMap(lambda x : x).collect()


clean = [s for s in text if s!=None]

clean_text = " ".join(i for i in clean)

show_wordcloud(clean_text)