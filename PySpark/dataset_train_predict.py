# -*- coding: utf-8 -*-
"""
Created on Thu Aug 25 10:45:55 2016

@author: k_lemo
"""

import csv
# import itertools
import time
import sys
#import pandas as pd


start_time = time.time()
# Implémentation des writters
csvfile_writerTrain = open('C:\Users\k_lemo\Documents\Stage\Data\dataset_train.csv', 'wb')
writer_train = csv.writer(csvfile_writerTrain)
writer_train.writerow(('DocId','CPC','Text'))

csvfile_writerPredict = open('C:\Users\k_lemo\Documents\Stage\Data\dataset_predict.csv', 'wb')
writer_predict = csv.writer(csvfile_writerPredict)
writer_predict.writerow(('DocId','CPC','Text'))

nError,nEmpty,nLine,nOk = 0,0,0,0
b65d,h01r,h01h,c07c,f16k,p = 0,0,0,0,0,0
error = False
# stopwords = pd.read_csv("\Users\k_lemo\Documents\Stage\stopwords\All.txt", header=None, sep=r"\s+") 
# stopWords = [word for word in set(stopwords[0])]

csv.field_size_limit(sys.maxint)
with open('C:\Users\k_lemo\Documents\Stage\Data\dataset_homogene.csv') as csvfile_reader:
    reader = csv.reader(csvfile_reader)
    for row in reader:   
    #for row in itertools.islice(reader, 100):
        nLine +=1
        try:           
            docid = row[0]
            cpc = row[1]
            text = row[2]
            
            if cpc == ' B65D' and b65d != 8000 :
                writer_train.writerow((docid,cpc,text.encode('utf8')))                                                
                b65d += 1                                       
            
            elif cpc == ' H01R' and h01r != 8000:                  
                writer_train.writerow((docid,cpc,text.encode('utf8')))                                                
                h01r += 1                        
                    
            elif cpc == ' H01H' and h01h != 8000:                
                writer_train.writerow((docid,cpc,text.encode('utf8')))                                                
                h01h += 1                        
            
            elif cpc == ' C07C' and c07c != 8000:           
                writer_train.writerow((docid,cpc,text.encode('utf8')))                                                
                c07c += 1                        
                        
            elif cpc == ' F16K' and f16k != 8000:
                writer_train.writerow((docid,cpc,text.encode('utf8')))                                                
                f16k += 1  
                
            else :
                writer_predict.writerow((docid,cpc,text.encode('utf8'))) 
                p += 1 
                
            nOk += 1            
        except:
            nError += 1 
            
            
csvfile_reader.close()
csvfile_writerTrain.close()
csvfile_writerPredict.close()

print('Nombre élements : '),nLine
print("")
print('Lignes bien exécutées : '),nOk
print("")
print('Erreurs de TRY : '),nError
print("")
print(b65d,h01r,h01h,c07c,f16k,p)
print("")
print("--- %s seconds ---" % (time.time() - start_time))