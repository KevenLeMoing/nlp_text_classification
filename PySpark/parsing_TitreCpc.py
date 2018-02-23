# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 09:17:44 2016

@author: k_lemo
"""

#Ressources
import sys,re,time
import csv
from csv import *
import pandas as pd
from pandas import *
import bs4 as bs
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords
from lxml import etree
import xml.etree.ElementTree as ET
from lxml import objectify
import xml.parsers.expat as xmlparser
import io
from io import StringIO, BytesIO
from ipykernel import kernelapp as app

# Lecture du fichier csv
start_time = time.time()
data = read_csv('.\\Titre_CPC\\Data\\' + sys.argv[1],error_bad_lines=False, iterator=True, chunksize=10000,header=None)
data = concat(data, ignore_index=True)

# Nommage des colonnes 
data = data.ix[1:]
data.columns = ['DocId','Fulltext']

# Création des deux dataframes vides pour stocker les données parsées 
data_title = DataFrame(np.zeros(0,dtype=[('DocId', 'i4'),('Title', 'a50')]))
data_cpc = DataFrame(np.zeros(0,dtype=[('DocId', 'i4'),('Main_CPC', 'a50')]))

# Parsing
n=0
for i in range(len(data)):
    try:
        docid = data['DocId'][i]
        doc = data['Fulltext'][i]
        doc = doc[doc.find("<teiHeader>"):doc.find("</teiHeader>")+12]

        root = ET.fromstring(doc)
        title = root.findall("./fileDesc/titleStmt/title")
        cpc = root.findall(".//ident")

        # Le titre
        for i in range(len(title)):
            ti = title[i].text
            data_title = data_title.append({'DocId':docid, 'Title': ti},ignore_index=True)

        # Les classifications
        list_cpc = []
        for i in range(len(cpc)):
            if len(cpc[i].text)>=4:
                list_cpc.append(cpc[i].text)
        data_cpc = data_cpc.append({'DocId':docid,'Main_CPC':list_cpc[0]},ignore_index=True)
    except: 
        n+=1

# Jointure des données et écriture au format csv
data = data_title.merge(data_cpc, on="DocId")
data.to_csv('.\\Titre_CPC\\Parsed\\'+sys.argv[1], sep=',', encoding='utf-8', index=False)

# FICHIER DE LOG
# Implémentation du writer
file_path = '.\\Titre_CPC\\Log\\'+sys.argv[1]
file = open(file_path, "wb")
writer = csv.writer(file)
# Ecriture de la ligne d'en-tête 
writer.writerow(('Fichier parsé','Parsing time','Number of errors'))
t = time.time() - start_time
writer.writerow((sys.argv[1],t,n))
file.close()









 
