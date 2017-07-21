#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 15:27:22 2017

@author: souley

Testing Gensim for complex text transformations
"""

#import logging
import itertools
from gensim import corpora, models

from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS 
from gensim.models.coherencemodel import CoherenceModel

import glob
import os

#logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

KAG_BASE_PATH = '../data/Competences/'
RES_BASE_PATH = '../results/'

#global call_count
call_count = 0
### A tester
def clean(doc):
    from nltk.corpus import stopwords 
    from nltk.stem.wordnet import WordNetLemmatizer
    import string
    stop = set(stopwords.words('english'))
    exclude = set(string.punctuation) 
    lemma = WordNetLemmatizer()
#    global call_count
#    if call_count == 0:
#        print('###exclude = '+ str(exclude))
    stop_free = " ".join([i for i in doc.lower().split() if i not in stop])
    punc_free = ''.join(ch for ch in stop_free if ch not in exclude)
#    if call_count == 0:
#        print('###punc_free = '+ str(punc_free))
#        call_count += 1
    normalized = " ".join(lemma.lemmatize(word.decode('utf-8')) for word in punc_free.split())
#    if call_count == 0:
#        print('###normalized = '+ normalized.encode('utf-8'))
#        call_count += 1
    return normalized

#doc_clean = [clean(doc).split() for doc in doc_complete]  


def tokenize(text):
    return [token for token in simple_preprocess(text) if token not in STOPWORDS]

def iter_dir(kag_dir):
#    for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
#        _ , kag_name = os.path.split(kag_path)
    _ , kag_name = os.path.split(kag_dir)
    tokens = []
#    global call_count
    for comp_path in glob.glob(kag_dir + '/*'):
#            _ , comp_file = os.path.split(comp_path)
#            sindex = len(kag_name) + 1
#            eindex = sindex + comp_file[sindex:].index('_') 
#            title = comp_file[sindex:eindex]
#            tokens = []
        for filename in glob.glob(comp_path + '/*.txt'):
#            _ , title = os.path.split(comp_path)
            with open(filename, 'r') as compfile:
                content = compfile.read()
#            tokens.append(clean(content))
            tokens += tokenize(clean(content))
#            tokens += tokenize(content)
#    if call_count == 0:
#        print('###['+ kag_name + '] tokens = ' + str(tokens).encode('utf-8'))
#        call_count += 1
    yield kag_name, tokens

        
class EDISONCorpus(object):
    def __init__(self, corpus_dir, dictionary, clip_docs=None):
        """
        Parse the first `clip_docs` Wikipedia documents from file `dump_file`.
        Yield each document in turn, as a list of tokens (unicode strings).
        
        """
        self.corpus_dir = corpus_dir
        self.dictionary = dictionary
        self.clip_docs = clip_docs
    
    def __iter__(self):
        self.titles = []
        for title, tokens in itertools.islice(iter_dir(self.corpus_dir), self.clip_docs):
            self.titles.append(title)
            yield self.dictionary.doc2bow(tokens)
    
    def __len__(self):
        return self.clip_docs

def gen_save_corpus():
    for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
        _ , kag_name = os.path.split(kag_path)
        doc_stream = (tokens for _, tokens in iter_dir(kag_path))
        id2word = corpora.Dictionary(doc_stream)
        # create a stream of bag-of-words vectors
        kag_corpus = EDISONCorpus(kag_path, id2word)
        kag_lda = models.LdaModel(kag_corpus, num_topics=5, id2word=id2word, passes=20)
#        kag_topics = kag_lda.show_topics()
        kag_lda.show_topics(5)
#        cm = CoherenceModel(model=kag_lda, texts=doc_stream, dictionary=id2word, coherence='c_v')
#        print goodcm.get_coherence()
        res_file = '{0}.txt'.format(RES_BASE_PATH + kag_name)
        with open(res_file, 'w') as tt_file: 
            for item in kag_lda.show_topics():
                print>>tt_file, item
#            print>>tt_file, 'coherence model=' + cm.get_coherence()
#            tt_file.write(kag_topics)

def use_corpus():
    doc_stream = (tokens for _, tokens in iter_dir(KAG_BASE_PATH))
    id2word = corpora.Dictionary(doc_stream)
    edison_corpus = corpora.MmCorpus(RES_BASE_PATH + 'edison_corpus.mm')
    lda_model = models.LdaModel(edison_corpus, num_topics=10, id2word=id2word, passes=10)
    lda_model.print_topics(-1)

if __name__ == '__main__':
    gen_save_corpus()
#    use_corpus()
