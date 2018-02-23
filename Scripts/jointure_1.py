# -*- coding: utf-8 -*-
"""
Created on Wed Aug 03 13:45:17 2016

@author: k_lemo
"""

import csv
#import itertools
import time
#import sys


start_time = time.time()
# Implémentation du writter
csvfile_writer = open('C:\Users\k_lemo\Documents\Stage\Data\KIME\jointure_1234.csv', 'wb')
writer = csv.writer(csvfile_writer)
writer.writerow(('DocId','SectionID','Type','Title','Main_CPC'))

nError,nEmpty,nLine,nOk = 0,0,0,0
error = False

#csv.field_size_limit(sys.maxint)
with open('C:\Users\k_lemo\Documents\Stage\Data\KIME\METADATA\metadata.csv') as csvfile_reader1:
    reader1 = csv.DictReader(csvfile_reader1)    
    try:                 
        for row1 in reader1:   
        #for row in itertools.islice(reader, 100):
            nLine +=1            
            docid1 = row1['DocId']                
            sectionid = row1['SectionID']
            type_text = row1['Type']
            
            with open('C:\Users\k_lemo\Documents\Stage\Data\KIME\XML\importXml1\\titreCpc_parsed_OK.csv') as csvfile_reader2:
                reader2 = csv.DictReader(csvfile_reader2)
                for row2 in reader2:        
                #for row1 in itertools.islice(reader1, 100):    
                    docid2 = row2['DocId']
                    titre = row2['Title']
                    cpc = row2['Main_CPC']
                    if docid2 == docid1:                        
                        writer.writerow((docid1,sectionid,type_text,titre,cpc))
                        nOk += 1
                        break
    except:
        nError += 1
                
csvfile_reader1.close()
csvfile_reader2.close()
csvfile_writer.close()

print('Nombre élements : '),nLine
print("")
print('Lignes bien exécutées : '),nOk
print("")
print('Erreurs de TRY : '),nError
print("")
print("--- %s seconds ---" % (time.time() - start_time))