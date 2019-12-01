#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pymongo import MongoClient, ReadPreference
# from sshtunnel import SSHTunnelForwarder
# import pprint
from bson.objectid import ObjectId
# import pyperclip as clip
# import json
import tensorflow as tf
import pandas as pd
import numpy as np
import itertools as it
from functools import partial
# from datasu import auc
# import pixiedust
# clip.copy(json.dumps(m1['msg']))
import sys
# sys.path.append("..")
from random import shuffle
# import nbimporter
# from nbimporter import NotebookLoader
# loader = NotebookLoader("..")


pd.options.mode.chained_assignment = None  # default='warn'


# from angie.service.app_config import AppConfig
from angie.service.managers.msg_similarity_manager import MsgSimilarityManager
from angie.service.managers.genie_msg_manager import GenieMsgManager
from angie.datacontracts.msg_similarity import SimilarityMeasure
from angie.libs.tensorflow_utils import sim_exact


# In[16]:


def get_random_messages(n, rpr_key):
    fltr = {'rpr_key':rpr_key}
    embs_len = db.genie_conversation_messages_embs.find(fltr).count();
    random_ints = np.random.randint(0,embs_len, n)
    msgs_embs = []
    for r_ind in random_ints.tolist():
        rnd_msg_emb = list(db.genie_conversation_messages_embs.find(fltr).skip(r_ind).limit(1))[0];
        msgs_embs.append(rnd_msg_emb)

    msg_ids = map(lambda ms: ms['genie_conversation_message_id'], msgs_embs)
    msgs = gniMsgManager.get_messages_by_ids(msg_ids)

    random_msg_vectors = [{'text': msg.get_text(), 'vector': np.array(msg_emb['msg_vector'])} for msg_emb, msg in zip(msgs_embs, msgs)]   
    
    return random_msg_vectors


# In[17]:


def get_cands_for_query(rpr_key, row):
    
#     import pdb; pdb.set_trace()
    
    K = 10
    query_vector = np.array(row['q_vector'])
    query = row['query']
    if 'genie_conversation_message_id' in row:
        query_mess_id = row['genie_conversation_message_id']
        sims = msgSimManager.get_top_k_similar_msgs_for_msg(query_mess_id, K*100, rpr_key, SimilarityMeasure.ANGDIST, chunk_size=10000)
    else:                
        sims = msgSimManager.get_top_k_similar_msgs_for_text(query, K*100, rpr_key, SimilarityMeasure.ANGDIST, chunk_size=10000)        
    
    sims_ids = list(map(lambda ms: ms.genie_msg_id, sims))
    sim_msgs = gniMsgManager.get_messages_by_ids(sims_ids)        
    sim_msgs_texts = [{'query': query, 'text': msg.get_text().strip(), 'sim_score': sim.similarity_score, 'is_random':False}  for sim, msg in zip(sims, sim_msgs)]
    
    sim_msgs_texts_unique = list({sm['text']:sm for sm in sim_msgs_texts}.values())    
    sim_msgs_texts_K = sorted(filter(lambda sm: sm['text'] != query, sim_msgs_texts_unique), key=lambda sm: sm['sim_score'], reverse=True)[0:K]
    
    rnd_msg_vectors = get_random_messages(K, rpr_key)    
    rnd_vectors= list(map(lambda m_v: m_v['vector'], rnd_msg_vectors))     
    query_rnd_sims = sim_exact([query_vector] ,rnd_vectors)[0].squeeze()
    rnd_msgs_texts = [{'query': query, 'text': msg['text'], 'sim_score': similarity_score, 'is_random':True} for similarity_score, msg in zip(query_rnd_sims, rnd_msg_vectors)]
    
    all_cand_msgs = sim_msgs_texts_K+rnd_msgs_texts    
    shuffle(all_cand_msgs)
    return all_cand_msgs


# In[18]:



path = '/home/ubuntu/STS2'

# encoder_name = 'BERT'
# rpr_key = 'text_bert_1'
# from angie.libs.embed.embedding_model_wrapper_bert import EmbeddingModelWrapper_BERT
# encoder = EmbeddingModelWrapper_BERT()

encoder_name = 'USE'
rpr_key = 'text_use_1'
from angie.libs.embed.embedding_model_wrapper_use import UseEmbeddingModelWrapper
encoder = UseEmbeddingModelWrapper()


# In[19]:


# MONGO_HOST = 'ec2-52-23-187-115.compute-1.amazonaws.com'
# MONGO_DB = "marketpulzz"
# server = SSHTunnelForwarder(
#     MONGO_HOST,
#     ssh_username='ubuntu',
#     ssh_pkey="/Users/sasha/.ssh/mmuze.pem",
# #     ssh_private_key_password="secret",
#     remote_bind_address=('127.0.0.1', 27017),
#     local_bind_address=('127.0.0.1', 63329),
#     set_keepalive = 5,
# )

# server.start()
# client = MongoClient('127.0.0.1', server.local_bind_port) # server.local_bind_port is assigned local port
# db = client[MONGO_DB]


# In[20]:


MONGO_DB = "marketpulzz"
# client = MongoClient('127.0.0.1', 27017)

read_preference = ReadPreference.PRIMARY
mongo_hosts = ['ec2-52-23-187-115.compute-1.amazonaws.com:27017', 'ec2-52-90-96-8.compute-1.amazonaws.com:27017']
client = MongoClient(mongo_hosts, read_preference = read_preference)

db = client[MONGO_DB]


# In[21]:


msgSimManager = MsgSimilarityManager(db)
gniMsgManager = GenieMsgManager(db)


# In[22]:


query_input_path = f'{path}/STS2_queries.xlsx'
output_path = f'{path}/STS2_{encoder_name}-v1.0.xlsx'
df_queries = pd.read_excel(query_input_path)


# In[23]:


df_queries['q_vector'] = encoder.embed(df_queries['query'].tolist()).tolist()


# In[24]:


sts2_rows_lists = df_queries.apply(partial(get_cands_for_query, rpr_key), axis=1)
sts2_rows =list(it.chain(*sts2_rows_lists))


# In[25]:


df_sts2 = pd.DataFrame(sts2_rows)
df_sts2.to_excel(output_path)

print("finished!")
# In[ ]:


# df_sts2


# In[ ]:




