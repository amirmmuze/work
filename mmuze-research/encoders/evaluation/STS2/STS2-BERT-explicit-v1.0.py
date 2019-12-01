#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pymongo import MongoClient, ReadPreference
# from sshtunnel import SSHTunnelForwarder
# import pprint
from bson.objectid import ObjectId
# import pyperclip as clip
# import json
# import tensorflow as tf
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
from angie.datacontracts.msg_similarity import SimilarityMeasure
from angie.libs.tensorflow_utils import sim_exact


# In[16]:
import numpy as np
from angie.libs.singleton import singleton
from angie.libs.mongo_utils import iterate_by_chunks
from angie.datacontracts.msg_similarity import *
from angie.datacontracts.msg import Message
from angie.libs.tensorflow_utils import sim_exact

from angie.datacontracts.http.msg_filter import MsgFilter
from angie.datacontracts.http.msg_representation_response import MsgEmbRepresentationResponse

from abc import ABC, abstractmethod

from typing import List
from bert_serving.client import BertClient
from numpy import ndarray


class MsgEmbRepresentation:
    def __init__(self, genie_msg_id: str, msg_vector: ndarray):
        self.genie_msg_id = genie_msg_id
        self.msg_vector = msg_vector  # .tolist()


class GenieMsgManager(object):

    def __init__(self, db=None):
        self.db = db

    def getMessagesForIds(self, genie_ids: List[str]) -> List[Message]:
        genie_obj_ids = list(map(ObjectId, genie_ids))
        filter = {'_id': {'$in': genie_obj_ids}}
        msgs_cursor = self.db.genie_conversation_messages.find(filter)
        msgs = list(map(lambda m: Message(m), msgs_cursor))
        return msgs

    def getMessageById(self, genie_id: str) -> Message:
        filter = {'_id': ObjectId(genie_id)}
        msg_hash = self.db.genie_conversation_messages.find_one(filter)
        if msg_hash:
            msg = Message(msg_hash)
            return msg
        else:
            return None


@singleton
class EmbeddingModelWrapper_BERT:

    def __init__(self):
        self.bert_client = BertClient()

    def embed(self, texts: List[str]):
        emb = self.bert_client.encode(texts)
        return emb


class BaseMsgRprConvertor(ABC):
    def __init__(self, msg: Message):
        self.msg = msg
        super().__init__()

    @abstractmethod
    def get_rpr(self):
        pass


class MsgRprConvertor_Text_BERT(BaseMsgRprConvertor):

    def get_rpr(self):
        text = self.msg.get_text()
        msg_vector = EmbeddingModelWrapper_BERT().embed([text])[0]
        return msg_vector


class MsgSimilarityManager(object):

    def __init__(self, db=None):
        self.db = db
        self.genie_msg_manager = GenieMsgManager(self.db)

    def getEmbRepresForMessages(self, messages: List[Message], rpr_key: str) -> List[MsgEmbRepresentation]:
        rpr_conv = MsgRprConvertor_Text_BERT
        msg_reprs = list(map(lambda m: MsgEmbRepresentation(m.get_genie_id(), rpr_conv(m).get_rpr()), messages))
        return msg_reprs

    def getEmbRepresForIds(self, genie_ids: list, rpr_key: str) -> List[MsgEmbRepresentation]:
        messages = self.genie_msg_manager.getMessagesForIds(genie_ids)
        msg_reprs = self.getEmbRepresForMessages(messages, rpr_key)
        return msg_reprs

    def getTopKSimilarMsgsForMsg(self, genie_id: str, K: int, rpr_key: str, sim_measure: SimilarityMeasure, messages_filter: MsgFilter = None, chunk_size=10000) -> List[MsgSimilarity]:
        genie_msg = self.genie_msg_manager.getMessageById(genie_id)
        if genie_msg:
            msg_rpr = self.getEmbRepresForMessages([genie_msg], rpr_key)[0]
            k_sims_all = self.getTopKSimilarMsgsForQueryVector(msg_rpr.msg_vector, K + 1, rpr_key, sim_measure, messages_filter=messages_filter, chunk_size=chunk_size)
            k_sims = list(filter(lambda ms: ms.genie_msg_id != genie_id, k_sims_all))
            return k_sims
        else:
            return list()

    def getTopKSimilarMsgsForText(self, query_text: str, K: int, rpr_key: str, sim_measure: SimilarityMeasure, messages_filter: MsgFilter = None, chunk_size=10000) -> List[MsgSimilarity]:
        query_vector = {
                           'text_bert_1': lambda q: self.embedTextsBERT([q])[0]
                       }.get(rpr_key)(query_text),
        k_sims = self.getTopKSimilarMsgsForQueryVector(query_vector[0], K, rpr_key, sim_measure, messages_filter=messages_filter, chunk_size=chunk_size)
        return k_sims

    def getTopKSimilarMsgsForQueryVector(self, query_vector: np.ndarray, K: int, rpr_key: str, sim_measure: SimilarityMeasure, messages_filter: MsgFilter = None, chunk_size=10000) -> List[MsgSimilarity]:

        _chunk_size = max(K, chunk_size)
        msg_embs_iter = iterate_by_chunks(self.db.genie_conversation_messages_embs, chunksize=_chunk_size, start_from=0,
                                          query={'rpr_key': rpr_key}, projection={'msg_vector': 1, 'genie_conversation_message_id': 1})
        top_k_similar = []
        chunk_n = 0
        for msg_embs_cursor in msg_embs_iter:
            chunk_n = chunk_n + 1

            try:
                msgs_embs_docs = list(msg_embs_cursor)
                msg_vectors = list(map(lambda me: me['msg_vector'], msgs_embs_docs))

                sims_res = sim_exact([query_vector], np.array(msg_vectors))

                sim_scores = {
                    SimilarityMeasure.ANGDIST: sims_res[0],
                    SimilarityMeasure.COSSIM: sims_res[1]
                }.get(sim_measure)  # , lambda z: raise_(ValueError(f"{sim_measure} not implemented")))

                msg_sims = [MsgSimilarity(str(msg['genie_conversation_message_id']), float(sim_score[0])) for msg, sim_score in zip(msgs_embs_docs, sim_scores)]

                k_similar = list(sorted(top_k_similar + msg_sims, key=lambda ms: ms.similarity_score, reverse=True))
                top_k_similar = k_similar[:K]

                print(f'chunk #: {chunk_n}, chunk_len: {len(msgs_embs_docs)}')
            except Exception as e:
                print(e)
        return top_k_similar

    # region Patch methods to treat to partially treat imports conflicts of BERT and USE

    def embedTextsBERT(self, texts: List[str]):
        """
        This is patch method to partially treat imports conflicts of BERT and USE
        """
        vector = EmbeddingModelWrapper_BERT().embed(texts)
        return vector

    # endregion


def get_random_messages(n, rpr_key):
    fltr = {'rpr_key':rpr_key}
    embs_len = db.genie_conversation_messages_embs.find(fltr).count();
    random_ints = np.random.randint(0,embs_len, n)
    msgs_embs = []
    for r_ind in random_ints.tolist():
        rnd_msg_emb = list(db.genie_conversation_messages_embs.find(fltr).skip(r_ind).limit(1))[0];
        msgs_embs.append(rnd_msg_emb)

    msg_ids = map(lambda ms: ms['genie_conversation_message_id'], msgs_embs)
    msgs = gniMsgManager.getMessagesForIds(msg_ids)

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
# path = '/Users/sasha/temp/STS2'

encoder_name = 'BERT'
rpr_key = 'text_bert_1'
from angie.libs.embed.embedding_model_wrapper_bert import EmbeddingModelWrapper_BERT
encoder = EmbeddingModelWrapper_BERT()

# encoder_name = 'USE'
# rpr_key = 'text_use_1'
# from angie.libs.embed.embedding_model_wrapper_use import EmbeddingModelWrapper_USE
# encoder = EmbeddingModelWrapper_USE()


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




