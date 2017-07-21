#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  8 14:43:38 2017

@author: souley
"""

from __future__ import absolute_import
from __future__ import print_function
#import six
#__author__ = 'a_medelyan'

import rake
#import operator
#import io

import glob
import csv

def save_terms_csv(filePath, terms):
    with open(filePath, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for term,score in terms:
            writer.writerow((term.encode('utf-8'),score))

corpus_path = '../data/Competences/data_analytics/data_analytics-DSDA01_predictive_analytics/'
texts = []
for filename in glob.glob(corpus_path + '/*.txt'):
    texts.append(open(filename, 'r').read().decode('utf-8'))
#    break
corpus_text = '\n'.join(text for text in texts)

# EXAMPLE ONE - SIMPLE
stoppath = "SmartStoplist.txt"

# 1. initialize RAKE by providing a path to a stopwords file
rake_object = rake.Rake(stoppath, 5, 3, 4)

keywords = rake_object.run(corpus_text)
save_terms_csv('../results/dsda01-RAKE.csv', sorted(keywords, reverse=True, key=lambda itp: itp[1]))
#print(sorted(keywords, reverse=True, key=lambda itp: itp[1]))