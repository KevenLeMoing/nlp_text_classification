# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 11:11:40 2016

@author: k_lemo
"""

# DocId CPC Text

import pandas as pd
import numpy as np
import time

import sklearn
from sklearn import *
from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn import (manifold, datasets, decomposition, ensemble, discriminant_analysis, random_projection)

start_time = time.time()
data = pd.read_csv('C:\Users\k_lemo\Documents\Stage\Data\dataset_five.csv')
stopwords = pd.read_csv("\Users\k_lemo\Documents\Stage\stopwords\All.txt", header=None, sep=r"\s+") 
stopWords = [word for word in set(stopwords[0])]

tfIdf = TfidfVectorizer(analyzer = "word", stop_words=stopWords,max_features = 100).fit_transform(data['Text'])
tfIdf=tfIdf.toarray()

tfIdf.to_csv('C:\Users\k_lemo\Documents\Stage\Data\tfidf.csv', sep=',', encoding='utf-8', index=False)



print("--- %s seconds ---" % (time.time() - start_time))



