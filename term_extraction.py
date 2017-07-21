#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 10:35:44 2017
A simple script to test various term extraction methods
@author: souley
"""

#import pickle

def read_corpus(corpus_path):
    from nltk.corpus.reader.plaintext import PlaintextCorpusReader
    corpus = PlaintextCorpusReader(corpus_path, ".*\.txt")
    ctext = corpus.raw()
#    with open('corpus.txt', 'w') as cf:
#        cf.write(ctext.encode('utf-8'))
    return ctext
 
def extract_candidate_chunks(text, grammar=r'KT: {(<JJ>* <NN.*>+ <IN>)? <JJ>* <NN.*>+}'):
    import itertools, nltk, string
    
    # exclude candidates that are stop words or entirely punctuation
    punct = set(string.punctuation)
    stop_words = set(nltk.corpus.stopwords.words('english'))
    # tokenize, POS-tag, and chunk using regular expressions
    chunker = nltk.chunk.regexp.RegexpParser(grammar)
    tagged_sents = nltk.pos_tag_sents(nltk.word_tokenize(sent) for sent in nltk.sent_tokenize(text))
    all_chunks = list(itertools.chain.from_iterable(nltk.chunk.tree2conlltags(chunker.parse(tagged_sent))
                                                    for tagged_sent in tagged_sents))
    # join constituent chunk words into a single chunked phrase
    candidates = [' '.join(word for word, pos, chunk in group).lower()
                  for key, group in itertools.groupby(all_chunks, lambda (word,pos,chunk): chunk != 'O') if key]

    return [cand for cand in candidates
            if cand not in stop_words and not all(char in punct for char in cand)]
                          
def extract_candidate_words(text, good_tags=set(['JJ','JJR','JJS','NN','NNP','NNS','NNPS'])):
    import itertools, nltk, string

    # exclude candidates that are stop words or entirely punctuation
    punct = set(string.punctuation)
    stop_words = set(nltk.corpus.stopwords.words('english'))
    # tokenize and POS-tag words
    tagged_words = itertools.chain.from_iterable(nltk.pos_tag_sents(nltk.word_tokenize(sent)
                                                                    for sent in nltk.sent_tokenize(text)))
    # filter on certain POS tags and lowercase all words
    candidates = [word.lower() for word, tag in tagged_words
                  if tag in good_tags and word.lower() not in stop_words
                  and not all(char in punct for char in word)]

    
#    with open('candidates.txt', 'wb') as fp:
#        for item in candidates:
#            print>>fp, item.encode('utf-8')
##        pickle.dump(candidates, fp)
        
    return candidates

def score_keyphrases_by_tfidf(texts, candidates='chunks'):
    import gensim
    
    # extract candidates from each text in texts, either chunks or words
    if candidates == 'chunks':
        boc_texts = [extract_candidate_chunks(text) for text in texts]
    elif candidates == 'words':
        boc_texts = [extract_candidate_words(text) for text in texts]
        
    print '### candidates: ' + str(len(boc_texts))
    with open('candidates.txt', 'wb') as fp:
        for item in boc_texts:
            print>>fp, item.encode('utf-8')
 
    # make gensim dictionary and corpus
    dictionary = gensim.corpora.Dictionary(boc_texts)
    corpus = [dictionary.doc2bow(boc_text) for boc_text in boc_texts]
    # transform corpus with tf*idf model
    tfidf = gensim.models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]
#    print "type tfidf: " + type(corpus_tfidf)
#    print "type dictionary: " + type(dictionary)
    return corpus_tfidf, dictionary

def main():
    corpus_path = '../data/Competences/data_analytics/data_analytics-DSDA01_predictive_analytics/'
    texts = read_corpus(corpus_path)
    ctfidf, cdict = score_keyphrases_by_tfidf(texts)
#    d = {cdict.get(id): value for doc in ctfidf for id, value in doc}
#    print(d)
#    with open('corpus_tfidf.txt', 'w') as mf:
#        mf.write(ctfidf)
#    with open('corpus_vocab.txt', 'w') as vf:
#        vf.write(cdict)

if __name__ == '__main__':
    main()
