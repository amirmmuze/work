#module for pre-processing a text data set
import sys
import csv
import re
import nltk
import spacy
from nltk.corpus import wordnet
from collections import Counter
from sklearn.model_selection import train_test_split
import random
import numpy as np

#This class cleans, augments textual data and creates test, dev, train csv files
class DataPreProcessor:
    
    def __init__(self, nlp = spacy.load("en_core_web_sm")):
        self.nlp = nlp
    
    #Main function of class, takes a list of sentences and a list of their respective labels (text) and returns 
    # the extended sentences list and their respective labels (np array of integers)
    def pre_process(self, data_file_name):
        samples, labels = self.read_data_from_csv(data_file_name)
        samples, labels = self.remove_rare_data(samples, labels)
        samples, labels = self.augment_and_clean_data(samples, labels)
        y, label_to_id = self.create_y(labels)
        self.write_data_files(samples, y)
        return label_to_id
    
    def remove_rare_data(self, samples, labels):
        labels_counter = Counter(labels)
        indexes_to_delete = []
        new_samples, new_labels = [], []
        for index, label in enumerate(labels):
            if labels_counter[label] > 10:
                new_labels.append(labels[index])
                new_samples.append(samples[index])
        return new_samples, new_labels
    
    #This function augments and cleans the text sample
    def augment_and_clean_data(self, samples, labels, label_threshold_to_extend = 100):
        extended_samples = []
        extended_labels = []
        labels_counter = Counter(labels)
        
        for i, sample in enumerate(samples):
            clean_text = self.clean_text(sample)
            extended_samples.append(clean_text)
            extended_labels.append(labels[i])
            if labels_counter[labels[i]] < label_threshold_to_extend: #check if sample is of rare label
                similar_sample = self.clean_text(self.get_similar_msg(clean_text)) 
                if similar_sample != clean_text:
                    extended_samples.append(similar_sample)
                    extended_labels.append(labels[i])
        assert len(extended_samples) == len(extended_labels)
        return extended_samples, extended_labels
    
    
    #Turns list of labels in text to numpy arrays of labels
    def create_y(self, labels):
        num_of_labels = 0
        label_to_id = {}
        y = []
        for label in labels:
            if label not in label_to_id:
                label_to_id[label] = num_of_labels
                num_of_labels+=1
            y.append(label_to_id[label])
        return np.array(y), label_to_id

    #Function to clean text
    def clean_text(self, text):
        text = text.lower()
        text = re.sub(r"what's", "what is", text)
        text = re.sub(r"can't", "can not", text)
        text = re.sub(r"i'm", "i am", text)
        text = re.sub(r"n't", " not", text)
        text = re.sub(r"\'s", "", text)
        text = re.sub(r"\'ve", " have", text)
        text = re.sub(r"\'re", " are", text)
        text = re.sub(r"\'d", " would", text)
        text = re.sub(r"\'ll", " will", text)
        text = re.sub('[^a-zA-Z0-9 ]', '', text)
        text = text.strip(' ')
        return text
        
    #returns a sentence with synonyms for input text
    def get_similar_msg(self, text):
        noun_tags = ["NN", "NNS"]
        verb_pos = ["VERB"]
        similar_msg = []
        tokens = self.nlp(text)
        for i, token in enumerate(tokens):
            if token.tag_ in noun_tags or token.pos_ in verb_pos:
                syn = self.get_synonym(token.text)
                similar_msg.append(syn)
            else:
                similar_msg.append(token.text)
        return ' '.join(similar_msg)
    
    #Returns a synonym for word from WordNet
    def get_synonym(self, word):
        index = random.randint(0,int(len(wordnet.synsets(word))/2))
        try:
            return wordnet.synsets(word)[index].name().split('.')[0]
        except:
            return word

    #splits X, y into train, dev, test and writes to csv files
    def write_data_files(self, X, y):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1, stratify = y)
        X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.3, random_state=1, stratify = y_train)
        self.write_to_csv(X_train, y_train, "train")
        self.write_to_csv(X_val, y_val, "dev")
        self.write_to_csv(X_test, y_test, "test")
        print("Pre-processing done")
            
    def write_to_csv(self, samples, labels, file_name):
        with open(file_name + '.csv', 'a') as csvfile:
            writer = csv.writer(csvfile)
            for sample, label in zip(samples, labels):
                row = [label, " ".join(sample.split())]
                writer.writerow(row)
                
    def read_data_from_csv(self, file_name):
        X = []
        y = []
        with open(file_name + ".csv", "r") as f:
            reader = csv.reader(f)
            for line in reader:
                y.append(line[0])
                X.append(line[1])
        return X, y

my_PP = DataPreProcessor()
data_file_name = sys.argv[1]
label_to_id_dict = my_PP.pre_process(data_file_name)
with open('label_to_id.csv', 'w') as f:
    for key in label_to_id_dict.keys():
        f.write("%s,%s\n"%(key,label_to_id_dict[key]))