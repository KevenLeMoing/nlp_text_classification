# -*- coding: utf-8 -*-
"""
Created on Tue Aug 02 15:12:38 2016

@author: k_lemo
"""


import csv
import sys
import itertools
#Importation de re pour traiter les expressions régulières
import re

from lxml import etree
import xml.etree.ElementTree as ET
from lxml import objectify

import xml.parsers.expat as xmlparser

import re



import time




start_time = time.time()
# Implémentation du writter
csvfile_writer = open('C:\Users\k_lemo\Documents\Stage\Data\KIME\XML\importXml2\New\Parsed\\fulltext_part9_parsed.csv', 'wb')
writer = csv.writer(csvfile_writer)
writer.writerow(('SectionID','Type','Text'))

nError,nEmpty,nLine,nOk = 0,0,0,0
error = False

#csv.field_size_limit(sys.maxint)
with open('C:\Users\k_lemo\Documents\Stage\Data\KIME\XML\importXml2\New\part9.csv') as csvfile_reader:
    reader = csv.reader(csvfile_reader)
    for row in reader:    
    #for row in itertools.islice(reader, 100):
        nLine +=1
        try:
            sectionID = ''.join(row[:1])
            fulltext = ''.join(row[1:])
            root = ET.fromstring(fulltext)
            typeText = root.get('type')    
            text = ''
            for element in root.iter("p"):
                if(element.text != None): text += element.text + ' '
            if text:
                writer.writerow((sectionID.encode('utf8'),typeText.encode('utf8'),text.encode('utf8')))
                print('WORKING : '),fulltext
                nOk += 1
            else:
                nEmpty+=1
                
        except:
            if error == False:
                print("Unexpected error : ", sys.exc_info()[0])
                print("")
                print('NOT WORKING : '),fulltext
                print("")
                error = True
            nError+=1

csvfile_reader.close()
csvfile_writer.close()

print('Nombre élements : '),nLine
print("")
print('Lignes parsées : '),nOk
print("")
print('Textes vides : '),nEmpty
print("")
print('Erreurs de parsing : '),nError
print("")
print("--- %s seconds ---" % (time.time() - start_time))