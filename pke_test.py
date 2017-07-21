#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 10 03:34:27 2017

@author: souley
"""

import pke

# initialize keyphrase extraction model, here TopicRank
#extractor = pke.KPMiner(input_file='../data/Competences/data_analytics/data_analytics-DSDA01_predictive_analytics/DataAnalytics.txt')
#extractor = pke.TfIdf(input_file='../data/CV/cv_1.json')
extractor = pke.Kea(input_file='../data/raw_competence2/DSDA/DSDA01.txt')
# load the content of the document, here document is expected to be in raw
# format (i.e. a simple text file) and preprocessing is carried out using nltk
extractor.read_document(format='raw')

# keyphrase candidate selection, in the case of TopicRank: sequences of nouns
# and adjectives
extractor.candidate_selection()

# candidate weighting, in the case of TopicRank: using a random walk algorithm
extractor.candidate_weighting()

# N-best selection, keyphrases contains the 10 highest scored candidates as
# (keyphrase, score) tuples
keyphrases = extractor.get_n_best(n=10)

print(keyphrases)