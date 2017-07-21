#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 10:35:44 2017
A simple script to test various term extraction methods
@author: souley
"""

import textacy
import glob 
import codecs

import en_core_web_sm
from textacy import data, keyterms, preprocess_text, spacy_utils

import csv

KAG_BASE_PATH = '../data/Competences/'
KAG_DA_PATH = KAG_BASE_PATH + 'data_analytics/'
COMP_DADS01_PATH = KAG_DA_PATH + 'data_analytics-DSDA01_predictive_analytics/'
CV_PATH = '../data/CV/'
JOB_PATH = '../data/jobs_json/'


RES_BASE_PATH = '../results/'
RES_COMP_PATH = RES_BASE_PATH + '/sgrank/competences/'
RES_KAG_PATH = RES_BASE_PATH + '/sgrank/KAG/'
RES_CV_PATH = RES_BASE_PATH + 'sgrank/CV/'
RES_JOB_PATH = RES_BASE_PATH + 'sgrank/jobs/'

def read_corpus(corpus_path):
#    spacy_lang = data.load_spacy('en')
    spacy_lang = en_core_web_sm.load()
    corpus = textacy.corpus.Corpus(u'en')
    for filename in glob.glob(corpus_path + '/*.txt'):
        content = open(filename, 'r').read().decode('utf-8')
        spacy_doc = spacy_lang(preprocess_text(content), parse=False)
        corpus.add_doc(spacy_doc)
       
    return corpus

def read_texts(corpus_path):
#    spacy_lang = data.load_spacy('en')
    spacy_lang = en_core_web_sm.load()
#    corpus = textacy.corpus.Corpus(u'en')
    texts = []
    for filename in glob.glob(corpus_path + '/*.txt'):
        print "Keyterms for " + filename
        content = open(filename, 'r').read().decode('utf-8')
        texts.append(content)
#        break # Just for one doc for testing purposes
    text = '\n'.join(content for content in texts)
    return spacy_lang(preprocess_text(text), parse=False)
    
def read_texts2(corpus_path):
    spacy_lang = en_core_web_sm.load()
    corpus = textacy.corpus.Corpus(spacy_lang)
    for filename in glob.glob(corpus_path + '/*.txt'):
        print "Keyterms for " + filename
        content = open(filename, 'r').read().decode('utf-8')
        corpus.add_text(content)
#        corpus.add_text(spacy_lang(preprocess_text(content), parse=False))
        break # Just for one doc for testing purposes
    return corpus

def read_corpus_in_doc(corpus_path):
    spacy_lang = en_core_web_sm.load()
    corpus = textacy.corpus.Corpus(spacy_lang)
    texts = []
    for filename in glob.glob(corpus_path + '/*.txt'):
        content = open(filename, 'r').read().decode('utf-8') # testing preprocess
        clean_text = preprocess_text(content, no_punct=True, no_contractions=True, no_accents=True)
        texts.append(clean_text)
#        texts.append(open(filename, 'r').read().decode('utf-8'))
#        break
    corpus_text = '\n'.join(text for text in texts)
    corpus.add_text(corpus_text)
#        corpus.add_text(spacy_lang(preprocess_text(content), parse=False))
#        break # Just for one doc for testing purposes
    return corpus

def save_terms(tf_dict):
    with codecs.open('corpus_term_freq.txt', 'w', 'utf-8') as vf:
        for key, val in tf_dict.iteritems():
            vf.write(key + ', ' + str(val) + '\n')

def save_terms_csv(filePath, terms):
    with open(filePath, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for term, score in terms:
            writer.writerow((term.encode('utf-8'), score))
   
def read_cv(cv_id):
    import json
    spacy_lang = en_core_web_sm.load()
    corpus = textacy.corpus.Corpus(spacy_lang)
    filename = 'cv_{0}.json'.format(cv_id)
    cv_path = CV_PATH + filename
    content = ''
    with open(cv_path) as cv_file:    
        content = json.load(cv_file)
    corpus_text = content['description']
#    corpus.add_text(preprocess_text(corpus_text, no_urls=True, no_emails=True, 
#                                    no_phone_numbers=True, no_numbers=True, no_currency_symbols=True, 
#                                    no_punct=True, no_contractions=True, no_accents=True))
    corpus.add_text(preprocess_text(corpus_text, no_punct=True, no_contractions=True, no_accents=True))
    return corpus

def read_cv2(cv_path):
    import json
    spacy_lang = en_core_web_sm.load()
    corpus = textacy.corpus.Corpus(spacy_lang)
    content = ''
    with open(cv_path) as cv_file:    
        content = json.load(cv_file)
#    corpus_text = content['description']
    corpus_text = content.get('description', u'')
#    corpus.add_text(preprocess_text(corpus_text, no_urls=True, no_emails=True, 
#                                    no_phone_numbers=True, no_numbers=True, no_currency_symbols=True, 
#                                    no_punct=True, no_contractions=True, no_accents=True))
    corpus.add_text(preprocess_text(corpus_text, no_punct=True, no_contractions=True, no_accents=True))
    return corpus

def preprocess_jobs_or_cvs(in_path, out_path):
    import os
    for filename in glob.glob(in_path + '*.json'):
        _ , cv_file = os.path.split(filename)
        res_file = out_path + '{0}.csv'.format(cv_file[0:cv_file.index('.')])
        if not os.path.isfile(res_file):
            corpus = read_cv2(filename)
            termList = keyterms.sgrank(corpus[0],ngrams=(1, 2), normalize=u'lower', window_width=500, n_keyterms=30, idf=None)         
            save_terms_csv(res_file, termList)
    #        break # Test
        
          
def preprocess_competences(in_path, out_path):
    import os
    spacy_lang = en_core_web_sm.load()
    for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
        _ , kag_name = os.path.split(kag_path)
        for comp_path in glob.glob(kag_path + '/*'):
            corpus = textacy.corpus.Corpus(spacy_lang)
            texts = []
            for filename in glob.glob(comp_path + '/*.txt'):
#                content = open(filename, 'r').read().decode('utf-8') # testing preprocess
#                clean_text = preprocess_text(content, no_punct=True, no_contractions=True, no_accents=True)
#                texts.append(clean_text)
                    texts.append(open(filename, 'r').read().decode('utf-8'))
            corpus_text = '\n'.join(text for text in texts)
            corpus.add_text(corpus_text)
            _ , comp_file = os.path.split(comp_path)
            sindex = len(kag_name) + 1
            eindex = sindex + comp_file[sindex:].index('_') 
            res_file = '{}.csv'.format(comp_file[sindex:eindex]) 
            doc_idf = corpus.word_doc_freqs(lemmatize=None, weighting='idf', lowercase=True, as_strings=True)
            termList = keyterms.sgrank(corpus[0],ngrams=(1, 2, 3), normalize=u'lower', window_width=500, n_keyterms=30, idf=doc_idf)  
            save_terms_csv(out_path + res_file, termList)
#            break
#        break

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


def preprocess_kags(in_path, out_path):
    import os
    spacy_lang = en_core_web_sm.load()
    for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
        _ , kag_name = os.path.split(kag_path)
        corpus = textacy.corpus.Corpus(spacy_lang)
        texts = []
        for comp_path in glob.glob(kag_path + '/*'):
            for filename in glob.glob(comp_path + '/*.txt'):
                texts.append(open(filename, 'r').read().decode('utf-8'))
        corpus_text = '\n'.join(text for text in texts)
        corpus.add_text(corpus_text)
#        _ , comp_file = os.path.split(comp_path)
#        sindex = len(kag_name) + 1
#        eindex = sindex + comp_file[sindex:].index('_') 
        res_file = '{}.csv'.format(get_kag(kag_name)) 
        doc_idf = corpus.word_doc_freqs(lemmatize=None, weighting='idf', lowercase=True, as_strings=True)
        termList = keyterms.sgrank(corpus[0],ngrams=(1, 2, 3), normalize=u'lower', window_width=500, n_keyterms=30, idf=doc_idf)  
        save_terms_csv(out_path + res_file, termList)
#        break

def main():
#    preprocess_jobs_or_cvs(CV_PATH, RES_CV_PATH)
#    preprocess_jobs_or_cvs(JOB_PATH, RES_JOB_PATH)
#    preprocess_competences(KAG_BASE_PATH, RES_COMP_PATH)
    preprocess_kags(KAG_BASE_PATH, RES_KAG_PATH)
    
    
if __name__ == '__main__':
    main()
