# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 11:28:40 2016

@author: k_lemo
"""


import pandas as pd
import numpy as np
import time

from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
from sklearn.decomposition import TruncatedSVD

import matplotlib.pyplot as plt
import matplotlib
from matplotlib import *



tsne = TSNE(n_components=2, init='pca',random_state=0).fit_transform(tfIdf)
start_time = time.time()
tsne = TSNE(n_components=2, init='pca',random_state=0)
X1 = tsne.fit_transform(tfIdf.toarray())
print("--- %s seconds ---" % (time.time() - start_time))

for i in range(len(data.index)):
    #plt.plot(clusters[0][i][0], clusters[0][i][1],'.',color=plt.cm.Set1(clusters[1][i] / float(centers)))
    plt.plot(X1[:,0],X1[:,1],'.',color=plt.cm.Set1(data["CPC1_corresp"][i] / float(len(data["CPC1_corresp"]))))

#X1
#X1[:,0]

plt.figure(figsize=(20, 20))
for i in range(len(data.index)):
    #plt.plot(clusters[0][i][0], clusters[0][i][1],'.',color=plt.cm.Set1(clusters[1][i] / float(centers)))
    plt.plot(X1[i][0],X1[i][1],'.',color=plt.cm.Set1(data["CPC1_corresp"][i] / float(len(set(data["CPC1_corresp"])))))
    


X_reduced = TruncatedSVD(n_components=50, random_state=0).fit_transform(tfIdf)
X_embedded = TSNE(n_components=2, perplexity=40, verbose=2).fit_transform(X_reduced)


for i in range(len(data.index)):
    #plt.plot(clusters[0][i][0], clusters[0][i][1],'.',color=plt.cm.Set1(clusters[1][i] / float(centers)))
    plt.plot(X1[:,0],X1[:,1],'.',color=plt.cm.Set1(data["CPC1_corresp"][i] / float(len(data["CPC1_corresp"]))))

#X1
#X1[:,0]

plt.figure(figsize=(20, 20))
for i in range(len(data.index)):
    #plt.plot(clusters[0][i][0], clusters[0][i][1],'.',color=plt.cm.Set1(clusters[1][i] / float(centers)))
    plt.plot(X1[i][0],X1[i][1],'.',color=plt.cm.Set1(data["CPC1_corresp"][i] / float(len(set(data["CPC1_corresp"])))))
    


X_reduced = TruncatedSVD(n_components=50, random_state=0).fit_transform(tfIdf)
X_embedded = TSNE(n_components=2, perplexity=40, verbose=2).fit_transform(X_reduced)