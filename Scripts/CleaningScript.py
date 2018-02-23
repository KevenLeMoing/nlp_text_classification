##T-SEN : http://scikit-learn.org/stable/auto_examples/manifold/plot_lle_digits.html
import csv
from bs4 import BeautifulSoup
import re



fichier = open('dataset.csv', 'rb')
fichiercsv = csv.reader(fichier, delimiter=',')

soup = BeautifulSoup(fichiercsv, 'html.parser')
print(soup.get_text())




def cleanhtml(raw_html)

    cleanr = re.compile('<.*?>')

    cleantext = re.sub(cleanr, '', raw_html)

    return cleantext


from BeautifulSoup import BeautifulSoup

cleantext = BeautifulSoup(raw_html).text


def remove_html_markup(s):
    tag = False
    quote = False
    out = ""

    for c in s:
        if c == '<' and not quote:
            tag = True
        elif c == '>' and not quote:
            tag = False
        elif (c == '"' or c == "'") and tag:
            quote = not quote
        elif not tag:
            out = out + c

    return out


import csv, math

fichier = open('file.csv', 'rb')
fichiercsv = csv.reader(fichier, delimiter=';')

for ligne in fichiercsv:
    if ligne:
        valeur = float(ligne[0])
        print valeur * math.pi

fichier.close()

