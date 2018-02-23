# -*- coding: utf-8 -*-
"""
Created on Thu Aug 04 09:50:54 2016

@author: k_lemo
"""

import csv
from csv import*
#import itertools
import time
#import sys
import pandas as pd
from pandas import *

start_time = time.time()
metadata = read_csv('C:\Users\k_lemo\Documents\Stage\Data\KIME\METADATA\metadata.csv',error_bad_lines=False, iterator=True, chunksize=10000)
metadata = concat(metadata, ignore_index=True)
titre_cpc = read_csv('C:\Users\k_lemo\Documents\Stage\Data\KIME\XML\importXml1\\titreCpc_parsed_OK.csv')
#error_bad_lines=False, iterator=True, chunksize=10000,header=None)
#data = concat(data, ignore_index=True)

#data = data.ix[1:]
#data.columns = ['DocId','Fulltext']

nError,nEmpty,nLine,nOk = 0,0,0,0
data = DataFrame(np.zeros(0,dtype=[('DocId', 'i4'),('SectionID', 'i4'),('Type', 'a50'),('Title', 'a50'),('Main_CPC', 'a50')]))

for i in range(len(metadata)):
    try:
        nLine +=1 
        docid1 = metadata['DocId'][i]               
        sectionid = metadata['SectionID'][i]
        type_text = metadata['Type'][i]
        
        for j in range(len(titre_cpc)):
            docid2 = titre_cpc['DocId'][j]
            titre = titre_cpc['Title'][j]
            cpc = titre_cpc['Main_CPC'][j]
            
            if docid2 == docid1:
                break                
                data = data.append({'DocId':docid2,'SectionID':sectionid,'Type':type_text,'Title':titre,'Main_CPC':cpc},ignore_index=True)
                nOk += 1
    except:
        nError += 1
    

data.to_csv('C:\Users\k_lemo\Documents\Stage\Data\KIME\jointure_OK.csv', sep=',', encoding='utf-8', index=False)

print('Nombre élements : '),nLine
print("")
print('Lignes bien exécutées : '),nOk
print("")
print('Erreurs de TRY : '),nError
print("")
print("--- %s seconds ---" % (time.time() - start_time))
