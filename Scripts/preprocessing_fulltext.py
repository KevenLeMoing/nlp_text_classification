# -*- coding: utf-8 -*-
"""
Created on Wed Aug 03 11:00:26 2016

@author: k_lemo
"""

import csv
import itertools
import bs4 as bs
import re
import time
import sys


start_time = time.time()
# Implémentation du writter
csvfile_writer = open('C:\Users\k_lemo\Documents\Stage\Data\KIME\XML\importXml2\New\\fulltext_parsed_part6-9_OK.csv', 'wb')
writer = csv.writer(csvfile_writer)
writer.writerow(('SectionID','Type','Text'))

nError,nEmpty,nLine,nOk = 0,0,0,0
error = False

csv.field_size_limit(sys.maxint)
with open('C:\Users\k_lemo\Documents\Stage\Data\KIME\XML\importXml2\New\Parsed\\fulltext_parsed_part6-9.csv') as csvfile_reader:
    reader = csv.DictReader(csvfile_reader)
    for row in reader:   
    #for row in itertools.islice(reader, 100):
        nLine +=1
        try:
            sectionid = row['SectionID']            
            typeText = row['Type']
            text = row['Text']
            text = bs.BeautifulSoup(text, "lxml").get_text()
            text = re.sub("[^a-zA-Z\xc4\xe4\xc9\xe9\xd6\xf6\xdc\xfc\xdf\xc0\xe0\xc2\xe2\xc6\xe6\xc7\xe7\xc8\xe8\xca\xea\xcb\xeb\xce\xee\xcf\xef\x8c\x9c\xd9\xf9\xdb\xfb]"," ",text)           
            writer.writerow((sectionid,typeText,text.encode('utf8')))
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