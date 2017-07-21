#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  9 16:18:56 2017

@author: souley
"""


import glob 

KAG_BASE_PATH = '../data/Competences/'
KAG_DA_PATH = KAG_BASE_PATH + 'data_analytics/'
COMP_DADS01_PATH = KAG_DA_PATH + 'data_analytics-DSDA01_predictive_analytics/'

def get_kag(kag_dir):
    if kag_dir == 'data_analytics':
        return 'DSDA'
    elif kag_dir == 'data_management_curation':
        return 'DSDM'
    elif kag_dir == 'data_science_engineering':
        return 'DSENG'
    elif kag_dir == 'domain_knowledge':
        return 'DSDK'
    elif kag_dir == 'scientific_research_methods':
        return 'DSRM'
    else:
        return ''

    
    
def preprocess_competences(in_path, out_path):
    import os
    spacy_lang = en_core_web_sm.load()
    for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
        for comp_path in glob.glob(kag_path + '/*'):
                corpus = textacy.corpus.Corpus(spacy_lang)
                texts = []
                for filename in glob.glob(comp_path + '/*.txt'):
                    texts.append(open(filename, 'r').read().decode('utf-8'))
            #        break
                corpus_text = '\n'.join(text for text in texts)
                corpus.add_text(corpus_text)

        _ , cv_file = os.path.split(filename)
    doc = corpus[0]
    doc_idf = corpus.word_doc_freqs(lemmatize=None, weighting='idf', lowercase=True, as_strings=True)
    termList = keyterms.sgrank(doc,ngrams=(1, 2, 3), normalize=u'lower', window_width=1500, n_keyterms=100, idf=doc_idf)  
    save_terms_csv('../results/sgrank/dsda01.csv', termList)
