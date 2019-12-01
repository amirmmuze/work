#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 15:03:07 2019

@author: amirdavidoff
"""




from pyspark.sql import functions as F
from pyspark.sql import DataFrameStatFunctions as statFunc


import matplotlib.pyplot as plt
import pandas as pd 
import numpy as np
import xgboost as xgb
from sklearn.model_selection import  train_test_split


from sklearn.metrics import precision_recall_curve
from sklearn.calibration import calibration_curve
from random import randint,random
from sklearn.metrics import accuracy_score, roc_auc_score, roc_curve, auc, log_loss
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import StratifiedKFold


import matplotlib.pyplot as plt
import pandas as pd 
import numpy as np
from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})



import argparse

import os 


def importance(model,importance_type='gain',write=False,path=""):
    
    '''
    xgb importance plot
    '''
    
    fig = plt.figure(figsize=(10,10))
    ax = fig.gca()
    xgb.plot_importance(model, max_num_features=30, height=0.8, ax=ax,importance_type=importance_type)
    if write:
       plt.savefig('{}/importance.png'.format(path))
       plt.show(block=False)
       plt.close(fig)
    else:
       plt.show()
       
       

def train_cv_model(X,Y):
    '''
    train cv xgb model and return auc vectors for test and train
    
    '''
    skf = StratifiedKFold(n_splits=3,shuffle=True)


    auc=[]
    auc_train=[]
    
    for train_index, test_index in skf.split(X, Y):
        print(train_index)
        
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        y_train, y_test = Y.iloc[train_index], Y.iloc[test_index]
        
        
        model = xgb.XGBClassifier(objective ='binary:logistic',
                                 colsample_bytree=1,
                                 learning_rate=0.03,
                                 max_depth=6,
                                 subsample=0.8,
                                 n_estimators=500,
                                 base_score=0.22,
                                 seed=2,
                                 )
    
    
        eval_result={}
        eval_set = [(X_train, y_train,'train'),(X_test, y_test,'test')]
        
        model.fit(X_train, y_train,verbose=True, eval_set=eval_set, eval_metric="auc",
                   callbacks=[xgb.callback.record_evaluation(eval_result),xgb.callback.early_stop(15)] 
                         )
            
        
        
        preds_test =  model.predict_proba(X_test)[:,1]
        roc_auc_score(y_test, preds_test)
        
        preds_train =  model.predict_proba(X_train)[:,1]
        roc_auc_score(y_train, preds_train)
        
        auc.append(roc_auc_score(y_test, preds_test) )
        auc_train.append(roc_auc_score(y_train, preds_train) )
    
    
    #print("test mean: {0}  train mean: {1}".format(np.mean(auc),np.mean(auc_train)))
    return {"test_auc":auc,"train_auc":auc_train,"model":model}


def summary_report(y,pred,write=False,path=''):
    '''
    
    this function will provide a performance report for a binary classifier:
    
    param : y list of labels.
    param : pred listof predictions.
    param : write boolean if to save plots.
    param : path string path where to save the plots to.
    
    '''
    
    
    ''' PRECISION RECALL CURVE '''
    prec, rec, thresholds_new = precision_recall_curve(y, pred)
    
    fig = plt.figure(figsize=(7,7))    
    plt.plot(rec, prec, lw=2, color='red',label='auc = {}'.format(auc(rec, prec))) 
    plt.legend(loc="upper right")
    plt.xlabel('recall',fontsize=15)
    plt.ylabel('precision',fontsize=15)
    plt.title('recall precision curve',fontsize=15)
    if write:
        plt.savefig('{}/precision_recall_graph.png'.format(path))
        plt.show(block=False)
        plt.close(fig)
    
    else:
        plt.show()


    ''' CALIBRATION CURVE '''
    fig = plt.figure(figsize=(7,7))    
    fraction_of_positives, mean_predicted_value = calibration_curve(y, pred, n_bins=20)
    plt.plot([0, 1], [0, 1], "k:", label="Perfectly calibrated")

    plt.plot(fraction_of_positives,mean_predicted_value , "s-")
    plt.legend(loc="upper right")
    plt.xlabel('fraction of positive',fontsize=15)
    plt.ylabel('mean predicted value',fontsize=15)
    plt.title('calibration curve',fontsize=15)
    if write:
       plt.savefig('{}/calibration_curve.png'.format(path))
       plt.show(block=False)
       plt.close(fig)
    else:
       plt.show()
   
    
    ''' PRED HIST '''
    fig = plt.figure(figsize=(7,7))    
    plt.hist(pred,bins=50)
    plt.legend(loc="upper right")
    plt.xlabel('prediction',fontsize=15)
    plt.ylabel('count',fontsize=15)
    plt.title('prediction histograhm',fontsize=15)
    if write:
       plt.savefig('{}/prediction_hist.png'.format(path))
       plt.show(block=False)
       plt.close(fig)
    else:
       plt.show()
    
     
    ''' AUC '''
    fig = plt.figure(figsize=(7,7))    
    fpr, tpr, _ = roc_curve(y, pred)
    roc_auc = auc(fpr, tpr)
    
    lw = 2
    plt.plot(fpr, tpr, color='darkorange',
             lw=lw, label='ROC curve (area = %0.2f)' % roc_auc)
    plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic example')
    plt.legend(loc="lower right")
    if write:
       plt.savefig('{}/roc_auc_curve.png'.format(path))
       plt.show(block=False)
       plt.close(fig)
    else:
       plt.show()

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



       
       
       
       
def train_weighted_xgb(X_train, X_test, y_train, y_test):
    
    '''
    trains simlpe xgb classifier
    '''
    
    
  
    
    y_train.value_counts(normalize=True)
    y_test.value_counts(normalize=True)
    
    
    
    param = {"objective" :'binary:logistic',
                             "colsample_bytree":1,
                            "learning_rate":0.03,
                             "max_depth":4,
                             "subsample":1,
                             "seed":42,
                             'eval_metric': 'auc',
                             'base_score': 0.02,
    #                         'alpha':5
                                 }
    num_round=700
    weight=np.where(y_train==1,1,0.5)
    dtrain = xgb.DMatrix(X_train, label=y_train,weight=weight)  #,weight=weight
    dtest = xgb.DMatrix(X_test, label=y_test)
    
    watchlist=[(dtrain,'train'),(dtest,'test')]
    
    bst = xgb.train(param, dtrain,num_round,evals=watchlist,early_stopping_rounds=40)
    
    
    return bst, dtrain, dtest





def plot_roc_aucs(pairs, write=False, path=''):
    
    '''
    auc of roc plot
    
    pairs is a list of tuples like [(name,true,pred),(),()]
    
    '''
    
    
    
    plt.figure(figsize=(10,10))
    for name, true, pred in pairs:
        fpr, tpr, thresholds = roc_curve(true, pred)
        roc_auc = auc(fpr, tpr, reorder=True)

        lw = 2
        plt.plot(fpr, tpr,
                 lw=lw, label='%s ROC curve (area = %0.2f)' % (name, roc_auc))
    plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Click Prediction ROC ({0})'.format("Test Data"))
    plt.legend(loc="lower right")
    
    if write:
        
        plt.savefig(path)
    
    else:
        
        plt.show()
        
        
        
        
        
        
        
        
import pandas
from scipy.stats import chisquare,chi2_contingency



def chisq_to_entire_df(df,y_col):
    
    '''
    performs chi square test to aall columns by y
    
    returns stats
    '''
    
    stats=[]
    ps=[]
    
    x_cols = [c for c in df.columns.tolist() if c != y_col]
    
    for c in x_cols:
        
        stat,p,_,_ = chisq_of_df_cols(df,c,y_col)
        
        stats.append(stat)
        ps.append(p)
        
    res = pd.DataFrame([stats,ps],columns=x_cols,index=["stat","p"]).T
    
    res = res.sort_values('stat',ascending=False)
        
    return res




def chisq_of_df_cols(df, c1, c2):
    groupsizes = df.groupby([c1, c2]).size()
    ctsum = groupsizes.unstack(c1)
    # fillna(0) is necessary to remove any NAs which will cause exceptions
    return(chi2_contingency(ctsum.fillna(0)))
    
    
    
    
    
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
stopwords = set(STOPWORDS)

def show_wordcloud(data, title = None):
    wordcloud = WordCloud(
        background_color='white',
        stopwords=stopwords,
        max_words=200,
        max_font_size=40, 
        scale=3,
        random_state=1 # chosen at random by flipping a coin; it was heads
    ).generate(str(data))

    fig = plt.figure(1, figsize=(16, 16))
    plt.axis('off')
    if title: 
        fig.suptitle(title, fontsize=20)
        fig.subplots_adjust(top=2.3)

    plt.imshow(wordcloud)
    plt.show()