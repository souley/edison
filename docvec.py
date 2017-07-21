#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun May 21 12:38:21 2017

@author: souley
"""

#from os import listdir
#from os.path import isfile, join
import numpy as np
import pandas as pd

import gensim
import glob
import os

from sklearn.decomposition import TruncatedSVD # For test
from sklearn.manifold import TSNE
import matplotlib.pyplot as plt

KAG_BASE_PATH = '../data/Competences/'
JOB_PATH = '../data/jobs_json/'
NEWJOB_PATH = '../data/newjobs_json/'
CV_PATH = '../data/CV/'

LabeledSentence = gensim.models.doc2vec.LabeledSentence

def clean(filepath, tjson=None):
    from nltk.tokenize import RegexpTokenizer
    from nltk.stem.porter import PorterStemmer
    from nltk.corpus import stopwords 
    import string
    import json
    import codecs

    tokenizer = RegexpTokenizer(r'\w+')
    p_stemmer = PorterStemmer()  
    stop = set(stopwords.words('english'))
    exclude = set(string.punctuation) 
#    lemma = WordNetLemmatizer()
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
        # add tokens to list
        return stemmed_tokens

def build_comp_labsents():
    texts = {}
    for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
        _ , kag_name = os.path.split(kag_path)
        for comp_path in glob.glob(kag_path + '/*'):
            _ , comp_file = os.path.split(comp_path)
            sindex = len(kag_name) + 1
            eindex = sindex + comp_file[sindex:].index('_') 
            comp_key = comp_file[sindex:eindex]
#            docLabels.append(comp_key)
            comp_tokens = []
            for filename in glob.glob(comp_path + '/*.txt'):
                comp_tokens = comp_tokens + clean(filename) #tokenize_clean(filename)
            texts[comp_key] = comp_tokens
    return texts

class LabeledLineSentence(object):
    def __init__(self, doc_list, labels_list):
       self.labels_list = labels_list
       self.doc_list = doc_list
    def __iter__(self):
        for idx, doc in enumerate(self.doc_list):
            yield LabeledSentence(words=doc,tags=[self.labels_list[idx]])
#            yield LabeledSentence(words=doc.read().strip().split(),tags=[self.labels_list[idx]])
            
 
def cosine(d1, d2):
    from math import fabs
    """
    Compute cosine similarity between two docvecs in the trained set, specified by int index or
    string tag. (TODO: Accept vectors of out-of-training-set docs, as if from inference.)

    """
    return fabs(np.dot(gensim.matutils.unitvec(d1), gensim.matutils.unitvec(d2)))

def cosine2(d1, d2):
    """
    Compute cosine similarity between two docvecs in the trained set, specified by int index or
    string tag. (TODO: Accept vectors of out-of-training-set docs, as if from inference.)

    """
    from scipy import spatial
    from math import fabs
    result = spatial.distance.cosine(d1, d2)
    return fabs(1.0-float(result))    

def kl_divergence(d1, d2):
    from scipy.stats import entropy
    """
    Compute Kullback-Leibler divergence between two docvecs in the trained set.
    """
    return entropy(d1, d2)

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
 

def job_comp_sims():
    from pathlib import Path
    texts = build_comp_labsents()
    data = texts.values()
    docLabels = texts.keys()
    it = LabeledLineSentence(data, docLabels)
    
    model = gensim.models.Doc2Vec(size=300, window=10, min_count=5, workers=24,alpha=0.025, min_alpha=0.025) # use fixed learning rate
    model.build_vocab(it)
    for epoch in range(10):
        model.train(it)
        model.alpha -= 0.002 # decrease the learning rate
        model.min_alpha = model.alpha # fix the learning rate, no deca
        model.train(it)
        
    res_path = os.getcwd() + '/../results/d2v/jobcomp/'
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
            job_vector = model.infer_vector(job_text, steps=10)
            comp_count = 0
            with open(res_file_path, mode="a") as text_file:
                for doc in range(0, len(docLabels)):
                    text_file.write(str(job_count) + "," + str(comp_count) + "," + docLabels[doc] + "," + str(float("{0:.3f}".format(cosine2(job_vector,model.docvecs[doc])))) + "\n")
                    comp_count += 1           
            job_count += 1
    ### compute similarities for new jobs
    for filename in glob.glob(NEWJOB_PATH + '*.json'):
        job_text = clean(filename, 'json')
        job_vector = model.infer_vector(job_text)
        comp_count = 0
        with open(res_file_path, mode="a") as text_file:
            for doc in range(0, len(docLabels)):
                text_file.write(str(job_count) + "," + str(comp_count) + "," + docLabels[doc] + "," + str(float("{0:.3f}".format(cosine2(job_vector,model.docvecs[doc])))) + "\n")
                comp_count += 1           
        job_count += 1

def cv_comp_sims():
    from pathlib import Path
    texts = build_comp_labsents()
    data = texts.values()
    docLabels = texts.keys()
    it = LabeledLineSentence(data, docLabels)
    
    model = gensim.models.Doc2Vec(size=300, window=10, min_count=5, workers=24,alpha=0.025, min_alpha=0.025) # use fixed learning rate
    model.build_vocab(it)
    for epoch in range(10):
        model.train(it)
        model.alpha -= 0.002 # decrease the learning rate
        model.min_alpha = model.alpha # fix the learning rate, no deca
        model.train(it)
        
    res_path = os.getcwd() + '/../results/d2v/cvcomp/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "cvcomp.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    cv_count = 0
    ### compute similarities for a few jobs from old jobs
    for filename in glob.glob(CV_PATH + '*.json'):
        if cv_count < 6:
            cv_text = clean(filename, 'json')
            cv_vector = model.infer_vector(cv_text, steps=10)
            comp_count = 0
            with open(res_file_path, mode="a") as text_file:
                for doc in range(0, len(docLabels)):
                    text_file.write(str(cv_count) + "," + str(comp_count) + "," + docLabels[doc] + "," + str(float("{0:.3f}".format(cosine2(cv_vector,model.docvecs[doc])))) + "\n")
                    comp_count += 1           
            cv_count += 1


def build_job_labsents():
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
    from pathlib import Path
    texts = build_job_labsents()
    data = texts.values()
    docLabels = texts.keys()
    it = LabeledLineSentence(data, docLabels)
    
    model = gensim.models.Doc2Vec(size=300, window=10, min_count=5, workers=24,alpha=0.025, min_alpha=0.025) # use fixed learning rate
    model.build_vocab(it)
    for epoch in range(10):
        model.train(it)
        model.alpha -= 0.002 # decrease the learning rate
        model.min_alpha = model.alpha # fix the learning rate, no deca
        model.train(it)
        
    res_path = os.getcwd() + '/../results/d2v/jobcv/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "jobcv.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    cv_count = 0
    ### compute similarities for a few jobs from old jobs
    for filename in glob.glob(CV_PATH + '*.json'):
        if cv_count < 6:
            cv_text = clean(filename, 'json')
            cv_vector = model.infer_vector(cv_text, steps=10)
            job_count = 0
            with open(res_file_path, mode="a") as text_file:
                for doc in range(0, len(docLabels)):
                    text_file.write(str(cv_count) + "," + str(job_count) + "," + str(float("{0:.3f}".format(cosine2(cv_vector,model.docvecs[doc])))) + "\n")
                    job_count += 1           
            cv_count += 1


if __name__ == '__main__':
#    job_comp_sims()
#    cv_comp_sims()
    job_cv_sims()
