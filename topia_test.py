#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  8 03:09:17 2017

@author: souley
"""

from topia.termextract import extract
from topia.termextract import tag
import glob
import csv

def save_terms_csv(filePath, terms):
    with open(filePath, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for term,r1,r2 in terms:
            writer.writerow((term.encode('utf-8'),r1,r2))

def keyterms(text, language='english'):
    # initialize the tagger with the required language
    tagger = tag.Tagger(language)
    tagger.initialize()
 
    # create the extractor with the tagger
    extractor = extract.TermExtractor(tagger=tagger)
    # invoke tagging the text
#    s = nltk.data.load('corpora/operating/td1.txt',format = 'raw')
    extractor.tagger(text)
    # extract all the terms, even the &amp;quot;weak&amp;quot; ones
    extractor.filter = extract.DefaultFilter(singleStrengthMinOccur=1)
    # extract
    return extractor(text)

texts = []
corpus_path = '../data/Competences/data_analytics/data_analytics-DSDA01_predictive_analytics/'
for filename in glob.glob(corpus_path + '/*.txt'):
    text = open(filename, 'r').read().decode('utf-8')
    texts.append(text)

corpus_text ='\n'.join(text for text in texts)
#extractor = extract.TermExtractor()
#print(sorted(extractor(text)))

#print(sorted(keyterms(corpus_text), key=lambda x: x[1] * x[2], reverse=True))

save_terms_csv('../results/dsda01-topia.csv', keyterms(corpus_text))
