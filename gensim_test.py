#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 15:27:22 2017

@author: souley

Testing Gensim for complex text transformations
"""

import logging


from gensim import corpora

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


documents = ["Human machine interface for lab abc computer applications",
          "A survey of user opinion of computer system response time",
          "The EPS user interface management system",
          "System and human system engineering testing of EPS",
          "Relation of user perceived response time to error measurement",
          "The generation of random binary unordered trees",
          "The intersection graph of paths in trees",
          "Graph minors IV Widths of trees and well quasi ordering",
          "Graph minors A survey"]

 # remove common words and tokenize
stoplist = set('for a of the and to in'.split())
texts = [[word for word in document.lower().split() if word not in stoplist]
         for document in documents]

 # remove words that appear only once
from collections import defaultdict
frequency = defaultdict(int)
for text in texts:
    for token in text:
        frequency[token] += 1

texts = [[token for token in text if frequency[token] > 1]
         for text in texts]

from pprint import pprint  # pretty-printer
pprint(texts)
