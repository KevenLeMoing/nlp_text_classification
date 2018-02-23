# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 11:22:23 2016

@author: k_lemo
"""


import pandas as pd
import numpy as np
import time
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
# from sklearn import metrics
# from sklearn import linear_model

from sklearn import cross_validation
from sklearn.cross_validation import KFold
from sklearn.cross_validation import LeaveOneOut

# Classification Random Forest


# Définition de la pipeline
rf_clf = Pipeline([('vect', TfidfVectorizer(analyzer = "word",stop_words=stopWords,max_features = 1000)),
                   ('clf', RandomForestClassifier(n_estimators = 150,n_jobs=-1))])

# Chargement des données d'apprentissage et des données à analyser
data_80 = data.sample(frac=0.8)
data_20 = data.loc[~data.index.isin(data_80.index)]

# Apprentissage
tps1 = time.clock()
rf_clf = rf_clf.fit(data_80["full_text"],data_80["cpc_1"])
tps2 = time.clock() 
print "Training done in ",tps2 - tps1," seconds."
print""

# Prédictions
tps1 = time.clock()
result = rf_clf.predict(data_20["full_text"])
tps2 = time.clock() 
print "Predictions done in ",tps2 - tps1," seconds."
print""

# On sauvegarde le resultat
# output = pd.DataFrame( data={"id":data_20["doc_id"], "cpc_1":result} )
# output.to_csv( "\Users\k_lemo\Documents\Stage\Data_EPO\resultat_RF.csv", index=False, quoting=3)

# On calcule le taux de bonnes prédictions
print 'Match rate: ',np.mean(result == data_20["cpc_1"])*100,"%"
print''
print 'Confusion Matrix:'
metrics.confusion_matrix(data_20["cpc_1"],result)