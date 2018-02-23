# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 10:21:58 2016

@author: k_lemo
"""

import csv
#import itertools
import time
import sys
#import pandas as pd


start_time = time.time()
# Implémentation du writter
csvfile_writer = open('C:\Users\k_lemo\Documents\Stage\Data\dataset_five.csv', 'wb')
writer = csv.writer(csvfile_writer)
writer.writerow(('DocId','CPC','Text'))

nError,nEmpty,nLine,nOk = 0,0,0,0
error = False
# stopwords = pd.read_csv("\Users\k_lemo\Documents\Stage\stopwords\All.txt", header=None, sep=r"\s+") 
# stopWords = [word for word in set(stopwords[0])]

csv.field_size_limit(sys.maxint)
with open('C:\Users\k_lemo\Documents\Stage\Data\dataset.csv') as csvfile_reader:
    reader = csv.reader(csvfile_reader)
    for row in reader:   
    #for row in itertools.islice(reader, 100):
        nLine +=1
        try:           
            docid = row[0]
            cpc = row[1]
            text = row[2]
            if cpc in (' B65D',' H01R',' H01H',' C07C',' F16K') :  
                nOk += 1                
                writer.writerow((docid,cpc,text.encode('utf8')))
        except:
            nError += 1 
            
            
csvfile_reader.close()
csvfile_writer.close()

print('Nombre élements : '),nLine
print("")
print('Lignes bien exécutées : '),nOk
print("")
print('Erreurs de TRY : '),nError
print("")
print("--- %s seconds ---" % (time.time() - start_time))