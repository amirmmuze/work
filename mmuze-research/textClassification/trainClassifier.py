#wrangles sentences and their intents into sparse vector representation and integer labels
import sys
import csv
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import spacy
import re
import random
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer

nlp = spacy.load("en_core_web_sm")

def read_data_from_csv(file_name):
    X = []
    y = []
    with open(file_name + ".csv", "r") as f:
        reader = csv.reader(f)
        for line in reader:
            y.append(line[0])
            X.append(line[1])
    return X, np.array(y)

train_msgs, y_train = read_data_from_csv(sys.argv[1])
dev_msgs, y_dev = read_data_from_csv(sys.argv[2])
ids, labels = read_data_from_csv(sys.argv[3])
label_to_id_dict = dict(zip(labels, ids))

my_stop_words = ["  ", " ", "the", "a", "an", "of", "to", 
                 "for", "in", "on", "that", "this", "and", 
                 "of", "at", "in", "between", "is", "are",
                "am", "i", "do", "does", "be", "if", "please", "s"
                "there", "any", "have"]

def Tokenize(text):
    return text.split(' ')

#class takes a list of sentences 
class SemanticHasher:
    
    def __init__(self, sentences):
        self.all_sub_tokens = {} #All trigrams in sentences are keys, value is trigram id
        self.num_of_sub_tokens = 0
        self.examples = [] #hashed sentences. Example: ["I pie", "no"] -> ['#I# #pi pie ie#', '#no no#']
        self.stop_words = my_stop_words
        self.subword_hash(sentences)
    
    #This function is private. Not to be used outside of class
    def subword_hash(self, sentences): 
        for raw_text in sentences:
            example = self.hash_sentence(raw_text, init_stage = 1)
            self.examples.append(example)
            
    #this function hashes the sentence after pre-processing it, to be used outside of class
    def hash_sentence(self, sentence, init_stage = 0):
        hashed_sentence = []
        tokens = nlp(sentence)
        for token in tokens:
            if token.text in self.stop_words:
                continue
            word = token.lemma_
            #if word in self.words_to_delete:
                #continue
            hashed_word = '#' + word + '#'
            for index in range(0, len(hashed_word) - 2):
                trigram = hashed_word[index : index + 3]
                hashed_sentence.append(trigram)
                #condition is false when function is called outside of class
                if init_stage and trigram not in self.all_sub_tokens:
                    self.all_sub_tokens[trigram] = self.num_of_sub_tokens  
                    self.num_of_sub_tokens += 1
        return ' '.join(hashed_sentence)

vec = CountVectorizer(tokenizer=Tokenize, lowercase=False)
#Creating train
my_hasher = SemanticHasher(train_msgs)
X_train = vec.fit_transform(my_hasher.examples)

#creating and hashing dev set
hashed_dev_msgs = [my_hasher.hash_sentence(msg) for msg in dev_msgs]
X_dev = vec.transform(hashed_dev_msgs)

#make sure number of samples matches number of labels
assert len(y_train) == X_train.shape[0]

#make sure number of features matches number of trigrams seen in train_data
assert my_hasher.num_of_sub_tokens == X_train.shape[1]
print("Wrangling done")

clf = LogisticRegression()
clf.fit(X_train, y_train)
y_pred_dev = clf.predict(X_dev)
report = classification_report(y_dev, y_pred_dev, target_names=label_to_id_dict.keys(), output_dict=True)
df = pd.DataFrame(report).transpose()
df.to_csv("classificationReport.csv")