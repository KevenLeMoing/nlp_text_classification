# -*- coding: utf-8 -*-
"""
Created on Thu Aug 25 09:22:34 2016

@author: k_lemo
"""
import csv
# import itertools
import time
import sys
#import pandas as pd


start_time = time.time()
# Implémentation du writter
csvfile_writer = open('C:\Users\k_lemo\Documents\Stage\Data\dataset_homogene.csv', 'wb')
writer = csv.writer(csvfile_writer)
writer.writerow(('DocId','CPC','Text'))

nError,nEmpty,nLine,nOk = 0,0,0,0
b65d,h01r,h01h,c07c,f16k = 0,0,0,0,0
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
            
            if cpc == ' B65D' and b65d != 10000 :
                writer.writerow((docid,cpc,text.encode('utf8')))                                                
                b65d += 1                                       
            
            if cpc == ' H01R' and h01r != 10000:                  
                writer.writerow((docid,cpc,text.encode('utf8')))                                                
                h01r += 1                        
                    
            if cpc == ' H01H' and h01h != 10000:                
                writer.writerow((docid,cpc,text.encode('utf8')))                                                
                h01h += 1                        
            
            if cpc == ' C07C' and c07c != 10000:           
                writer.writerow((docid,cpc,text.encode('utf8')))                                                
                c07c += 1                        
                        
            if cpc == ' F16K' and f16k != 10000:
                writer.writerow((docid,cpc,text.encode('utf8')))                                                
                f16k += 1       
                 
            nOk += 1            
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
print(b65d,h01r,h01h,c07c,f16k)
print("")
print("--- %s seconds ---" % (time.time() - start_time))