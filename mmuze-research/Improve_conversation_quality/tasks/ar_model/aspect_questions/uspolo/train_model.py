#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 10 14:07:01 2019

@author: amirdavidoff



train xgb classifier

inputs:
    df from prepared_data.py

    
outputs:
    pickled model
    iportance plots
    shap viz


execuation:

    spark-submit /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/aspect_questions/uspolo/train_model.py --df-source /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/aspect_questions/uspolo/df.csv --dest /Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/ar_model/aspect_questions/uspolo

"""

import matplotlib.pyplot as plt
import pandas as pd 
import numpy as np
from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})


from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame, SparkSession
import argparse

import os 
from pathlib import Path
home = str(Path.home())

import sys 

sys.path.append('{}/mmuze-research'.format(os.getenv("HOME")))


print(sys.path)


print(os.getcwd())


os.chdir(os.getenv("HOME"))
print(os.getcwd())

from Improve_conversation_quality.preprocessing.functions.generic_imports import *


from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

import pickle

def train_model(df_path,dest):
    

    '''
    
    function that takes data in and ouputs a pickled model plus evaluation visualizations (importance, curves, shap)
    
    the model is an xgb binary classifier 
    
    df_path is the path to the df produced by the script prepare_data.py
    dest if the path to write everything to.
    
    
    '''

    dirs = ['imp','shap']
    
    for d in dirs:
        
        print(d)
        import os # this solved an error of  - UnboundLocalError: local variable 'os' referenced before assignment - dont know why
        try:
            os.mkdir(dest+'/{}/'.format(d))
        except:
            pass


    dfp = pd.read_csv(df_path,index_col=0)  #'/Users/amirdavidoff/mmuze-research/Improve_conversation_quality/tasks/dataset/df.csv'

    not_x = ['is_answered','jnd_sender_id']
    y_col = ["is_answered"]
    x_col_dummied = [c for c in dfp.columns if c not in not_x]

    
        
    X_train, X_test, y_train, y_test = train_test_split(
        dfp[["jnd_sender_id"]+x_col_dummied], dfp[y_col], test_size=0.20, random_state=42,stratify = dfp[y_col])
    
    
    import xgboost as xgb
    res = train_cv_model(X_train.drop('jnd_sender_id',axis=1),y_train)
    
    
    #
    
    model = res["model"]
    
    pickle.dump(model,open(dest+'/model.pickle',"wb"))
    
    test_pred = model.predict_proba(X_test.drop('jnd_sender_id',axis=1))[:,1]
      
    print("mean auc roc of 3 fold test sets : {}".format(np.mean(res["test_auc"])))

    print("auc roc on help out data : {}".format(roc_auc_score(y_test,test_pred)))




    
    
    summary_report(y_test,test_pred,True,dest+'/imp')
    importance(model,'gain',True,dest+'/imp')
    
    #
    ''' feature selection '''
    
    import operator
    
    imp_dic =  model.get_booster().get_score(importance_type='gain')
    
    imp_dic = sorted(imp_dic.items(), key=operator.itemgetter(1), reverse=True)
    
    top_features = [c[0] for c in imp_dic[:20]]
  
    
    ''' shap '''
    
    import shap
    
    
    shap_values = shap.TreeExplainer(model).shap_values(X_test.drop("jnd_sender_id",axis=1))
    
    shap.summary_plot(shap_values, X_test.drop("jnd_sender_id",axis=1),plot_size=(20,10),max_display=30,show=False)
    plt.savefig(dest+'/shap/summary_shap.png')
    
    
    ''' shap top features against all '''
    
    
    import os
    
    for c in top_features:
        
        shap.dependence_plot(c, shap_values, X_test.drop("jnd_sender_id",axis=1),show=False)
        plt.savefig(dest+'/shap/{}_shap.png'.format(c))
    #
    top_3 = [c[0] for c in imp_dic[:3]]
    for c in top_3:
        
        try:
            os.mkdir(dest+'/shap/{}'.format(c))
        except:
            pass
        
        for r in top_features:
            if r!=c:
                
    
                shap.dependence_plot(c, shap_values, X_test.drop("jnd_sender_id",axis=1),interaction_index=r,show=False)
                plt.savefig(dest+'/shap/{}/{}_{}'.format(c,c,r))






if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='train and evaluate a binary classifier  ')

    parser.add_argument('--df-source', help='df source from prepare_data.py ')  
    parser.add_argument('--dest', help='destination to write to')
    


    args = parser.parse_args()

    conf = (
        SparkConf()
            .setAppName("Spark Submit:" + __file__)
            .set("spark.sql.session.timeZone", "UTC")
           # .set("spark.executor.memory", "6g")
           # .set("spark.driver.memory", "6g")
           # .set("spark.yarn.executor.memoryOverhead", "2g")
    )

    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    

    import time
    
    s=time.time()
    train_model(args.df_source, args.dest)
    e=time.time()
    
    print("took: {}".format(e-s))
    