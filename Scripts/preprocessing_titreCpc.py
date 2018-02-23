# -*- coding: utf-8 -*-
"""
Created on Wed Aug 03 09:48:16 2016

@author: k_lemo
"""

import csv

import itertools

import re
import time


start_time = time.time()
# Implémentation du writter
csvfile_writer = open('C:\Users\k_lemo\Documents\Stage\Data\KIME\XML\importXml1\\titreCpc_parsed_OK.csv', 'wb')
writer = csv.writer(csvfile_writer)
writer.writerow(('DocId','Title','Main_CPC'))

nError,nEmpty,nLine,nOk = 0,0,0,0
error = False


with open('C:\Users\k_lemo\Documents\Stage\Data\KIME\XML\importXml1\\titreCpc_parsed.csv') as csvfile_reader:
    reader = csv.DictReader(csvfile_reader)
    for row in reader:   
    #for row in itertools.islice(reader, 100):
        nLine +=1
        try:
            docid = row['DocId']            
            titre = row['Title']            
            if titre =='Patent publication information':
                titre = ''            
            cpc = row['Main_CPC']          
            if cpc[2].isalpha():
                cpc = cpc[2]+cpc[4:7]                
            else:
                cpc = cpc[:4]
            writer.writerow((docid,titre,cpc))
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
print("--- %s seconds ---" % (time.time() - start_time))