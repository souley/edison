#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 17:08:35 2017
Script initially fetched from Internet, credit:https://gist.github.com/clemsos/7692685
@author: souley
"""


#!/usr/bin/env python
# -*- coding: utf-8 -*-


#from gensim import corpora, models, similarities
from time import time

from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer
from gensim import corpora, models, similarities
#import gensim
#from gensim.models.coherencemodel import CoherenceModel
#import pyLDAvis.gensim

import glob
import os

from pathlib import Path

KAG_BASE_PATH = '../data/Competences/'
RES_BASE_PATH = '../results/'
JOB_PATH = '../data/jobs_json/'
NEWJOB_PATH = '../data/newjobs_json/'
CV_PATH = '../data/CV/'
COMP2_BASE_PATH = '../data/raw_competence2/'

tokenizer = RegexpTokenizer(r'\w+')
p_stemmer = PorterStemmer()


def clean(filepath, tjson=None):
    from nltk.corpus import stopwords 
#    from nltk.stem.wordnet import WordNetLemmatizer
    import string
    stop = set(stopwords.words('english'))
    stop.add(sw for sw in ['data','also','can'])
    exclude = set(string.punctuation) 
    import json
    import codecs
    with codecs.open(filepath, 'r', encoding='utf-8') as rfile:
        if not tjson is None:
            content = json.load(rfile)
            content = content.get('description', u'')
        else:
            content = rfile.read()
        
        raw = content.lower()#.decode('utf-8', 'ignore')
        tokens = tokenizer.tokenize(raw)
        
        # remove stop words from tokens
        stopped_tokens = [i for i in tokens if not i in stop]
        
        punc_free = [ch for ch in stopped_tokens if ch not in exclude]
        
        stemmed_tokens = [p_stemmer.stem(i) for i in punc_free]
#        stemmed_tokens = [WordNetLemmatizer().lemmatize(word) for word in stopped_tokens]
        # add tokens to list
        return stemmed_tokens


def read_comp_doc():
    texts = {}
    for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
        _ , kag_name = os.path.split(kag_path)
        for comp_path in glob.glob(kag_path + '/*'):
            _ , comp_file = os.path.split(comp_path)
            sindex = len(kag_name) + 1
            eindex = sindex + comp_file[sindex:].index('_') 
            comp_key = comp_file[sindex:eindex]
            comp_tokens = []
            for filename in glob.glob(comp_path + '/*.txt'):
                comp_tokens = comp_tokens + clean(filename) #tokenize_clean(filename)
            texts[comp_key] = comp_tokens
    return texts

def job_comp_sims():
    texts = read_comp_doc()
    # turn our tokenized documents into a id <-> term dictionary
    dictionary = corpora.Dictionary(texts.values())
        
    # convert tokenized documents into a document-term matrix
    corpus = [dictionary.doc2bow(text) for text in texts.values()]
    
    # Transform Text with TF-IDF
    tfidf = models.TfidfModel(corpus) # step 1 -- initialize a model

    res_path = os.getcwd() + '/../results/tfidf/jobcomp/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "jobcomp.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    job_count = 0
    ### compute similarities for a few jobs from old jobs
    for filename in glob.glob(JOB_PATH + '*.json'):
        if job_count < 5:
            job_text = clean(filename, 'json')
            job_bow = dictionary.doc2bow(job_text)
            job_tfidf = tfidf[job_bow]
            index = similarities.MatrixSimilarity(tfidf[corpus])
            sims = index[job_tfidf]
            comps = texts.keys()
            comp_count = 0
            with open(res_file_path, mode="a") as text_file:
                for doc,sim in enumerate(sims):
                    text_file.write(str(job_count) + "," + str(comp_count) + "," + comps[doc] + "," + str(sim) + "\n")
                    comp_count += 1           
            job_count += 1
    ### compute similarities for new jobs
    for filename in glob.glob(NEWJOB_PATH + '*.json'):
        job_text = clean(filename, 'json')
        job_bow = dictionary.doc2bow(job_text)
        job_tfidf = tfidf[job_bow]
        index = similarities.MatrixSimilarity(tfidf[corpus])
        sims = index[job_tfidf]
        comps = texts.keys()
        comp_count = 0
        with open(res_file_path, mode="a") as text_file:
            for doc,sim in enumerate(sims):
                text_file.write(str(job_count) + "," + str(comp_count) + "," + comps[doc] + "," + str(sim) + "\n")
                comp_count += 1           
        job_count += 1

def cv_comp_sims():
    texts = read_comp_doc()
    # turn our tokenized documents into a id <-> term dictionary
    dictionary = corpora.Dictionary(texts.values())
        
    # convert tokenized documents into a document-term matrix
    corpus = [dictionary.doc2bow(text) for text in texts.values()]
    
    # Transform Text with TF-IDF
    tfidf = models.TfidfModel(corpus) # step 1 -- initialize a model

    ## compute similarities for CVs 
    res_path = os.getcwd() + '/../results/tfidf/cvcomp/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "cvcomp.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    cv_count = 0
    for filename in glob.glob(CV_PATH + '*.json'):
        if cv_count < 6:
            cv_text = clean(filename, 'json')
            cv_bow = dictionary.doc2bow(cv_text)
            cv_tfidf = tfidf[cv_bow]
            index = similarities.MatrixSimilarity(tfidf[corpus])
            sims = index[cv_tfidf]
            comps = texts.keys()
            comp_count = 0
            with open(res_file_path, mode="a") as text_file:
                for doc,sim in enumerate(sims):
                    text_file.write(str(cv_count) + "," + str(comp_count) + "," + comps[doc] + "," + str(sim) + "\n")
                    comp_count += 1           
            cv_count += 1


def read_job_doc():
    texts = {}
    job_count = 0
    for filename in glob.glob(JOB_PATH + '*.json'):
        if job_count < 5:
            _ , job_name = os.path.split(filename)
            texts[job_count] = clean(filename, 'json')
            job_count += 1
    for filename in glob.glob(NEWJOB_PATH + '*.json'):
        _ , job_name = os.path.split(filename)
        texts[job_count] = clean(filename, 'json')
        job_count += 1
    return texts

def job_cv_sims():
    texts = read_job_doc()
    # turn our tokenized documents into a id <-> term dictionary
    dictionary = corpora.Dictionary(texts.values())
        
    # convert tokenized documents into a document-term matrix
    corpus = [dictionary.doc2bow(text) for text in texts.values()]
    
    # Transform Text with TF-IDF
    tfidf = models.TfidfModel(corpus) # step 1 -- initialize a model

    res_path = os.getcwd() + '/../results/tfidf/jobcv/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "jobcv.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    ## compute similarities for CVs 
    cv_count = 0
    for filename in glob.glob(CV_PATH + '*.json'):
        if cv_count < 6:
            cv_text = clean(filename, 'json')
            cv_bow = dictionary.doc2bow(cv_text)
            cv_tfidf = tfidf[cv_bow]
            index = similarities.MatrixSimilarity(tfidf[corpus])
            sims = index[cv_tfidf]
#            jobs = texts.keys()
            comp_count = 0
            with open(res_file_path, mode="a") as text_file:
                for doc,sim in enumerate(sims):
                    text_file.write(str(cv_count) + "," + str(doc) + "," + str(sim) + "\n")
                    comp_count += 1           
            cv_count += 1         



if __name__ == '__main__':
#    job_comp_sims()
#    cv_comp_sims()
    job_cv_sims()
