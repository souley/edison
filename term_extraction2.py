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
#import matplotlib
#def read_corpus(corpus_path):
#    corpus = textacy.corpus.Corpus(u'en')
#    for filename in glob.glob(corpus_path + '/*.txt'):
#        content = open(filename, 'r').read().decode('utf-8')
##        clean_content = textacy.preprocess.preprocess_text(content, fix_unicode=True, lowercase=True, 
##                                           no_numbers=True, no_currency_symbols=True, no_punct=True)
#        clean_content = textacy.preprocess.preprocess_text(content, lowercase=True, no_punct=True)
#        corpus.add_text(clean_content)

KAG_BASE_PATH = '../data/Competences/'
KAG_DA_PATH = KAG_BASE_PATH + 'data_analytics/'
COMP_DADS01_PATH = KAG_DA_PATH + 'data_analytics-DSDA01_predictive_analytics/'
CV_PATH = '../data/CV/'
JOB_PATH = '../data/jobs_json/'
NEWJOB_PATH = '../data/newjobs_json/'
CATEGORY_BASE_PATH = '../data/Categories/'
COMP_BASE_PATH2 = '../data/raw_competence2/'
JOBMS_PATH = '../data/jobs_erwin/data1apriltest.docx'

RES_BASE_PATH = '../results/'
RES_COMP_PATH = RES_BASE_PATH + 'sgrank/competences/'
RES_KAG_PATH = RES_BASE_PATH + 'sgrank/KAG/'
RES_CV_PATH = RES_BASE_PATH + 'sgrank/CV/'
RES_JOB_PATH = RES_BASE_PATH + 'sgrank/jobs/'
RES_NEWJOB_PATH = RES_BASE_PATH + 'sgrank/newjobs/'
RES_CATEGORY_PATH = RES_BASE_PATH + 'sgrank/categories/'
RES_COMP_PATH2 = RES_BASE_PATH + 'sgrank2/competences/'
RES_JOBREQ_PATH = RES_BASE_PATH + 'sgrank/jobreqs/'
RES_JOBMS_PATH = RES_BASE_PATH + 'sgrank/jobms/'

CV_KW_RATIO = 0.08

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
        print ("Keyterms for " + filename)
        content = open(filename, 'r').read().decode('utf-8')
        texts.append(content)
#        break # Just for one doc for testing purposes
    text = '\n'.join(content for content in texts)
    return spacy_lang(preprocess_text(text), parse=False)
    
def read_texts2(corpus_path):
    spacy_lang = en_core_web_sm.load()
    corpus = textacy.corpus.Corpus(spacy_lang)
    for filename in glob.glob(corpus_path + '/*.txt'):
        print ("Keyterms for " + filename)
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
            
def save_terms_text(filePath, termList):
    with codecs.open(filePath, 'w', 'utf-8') as vf:
        for term in termList:
            chunks = term.split()
            comp_term = chunks[0]
            if len(chunks) > 1:
                comp_term = '_'.join(chunk for chunk in chunks)
            vf.write(comp_term+'\n')    # add + '\n' yo get one keyword per line
    
        


def save_terms_csv(filePath, terms):
    with open(filePath, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for term in terms:
#            print ("term="+str(term))
            writer.writerow([term[0].encode('utf-8'), term[1]])

  
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
    corpus.add_text(corpus_text)
#    corpus.add_text(preprocess_text(corpus_text, no_urls=True, no_emails=True, 
#                                    no_phone_numbers=True, no_numbers=True, no_currency_symbols=True, 
#                                    no_punct=True, no_contractions=True, no_accents=True))
#    corpus.add_text(preprocess_text(corpus_text, no_punct=True, no_contractions=True, no_accents=True))
    return corpus
    
def term_list(termScoreList):
    termList = []
    for term, _ in termScoreList:
        termList.append(term)
    return termList

def preprocess_jobs_or_cvs_combined(in_path, out_path):
    import os
    for filename in glob.glob(in_path + '*.json'):
        _ , cv_file = os.path.split(filename)
        res_file = out_path + '{0}.csv'.format(cv_file[0:cv_file.index('.')])
        if not os.path.isfile(res_file):
            corpus = read_cv2(filename)
            termList1 = term_list(keyterms.textrank(corpus[0], normalize=u'lower', n_keyterms=30))
            termList2 = term_list(keyterms.sgrank(corpus[0], ngrams=(1, 2), normalize=u'lower', window_width=100, n_keyterms=70, idf=None))
            termSet1 = set(termList1)
            termSet2 = set(termList2)
            diffSet = termSet1 - termSet2
            termList = termList2 + list(diffSet)
#            save_terms_csv(res_file, termList)
            save_terms_text(res_file, termList)
        

def preprocess_job_market(in_path, out_path):
    import os
    import json
    import codecs
    job_texts = []
    for filename in glob.glob(in_path + '*.json'):
        try:
            with codecs.open(filename, encoding='utf-8') as job_file:    
                content = json.load(job_file)
                job_texts.append(content.get('description', u''))
        except:
            print("===Exception reading file " + filename)
            continue
    spacy_lang = en_core_web_sm.load()
    corpus = textacy.corpus.Corpus(spacy_lang)
    corpus_text = '\n'.join(text for text in job_texts)
    corpus.add_text(corpus_text)
    
    res_file = out_path + 'job_market.csv'
    if not os.path.isfile(res_file):
        termList1 = term_list(keyterms.textrank(corpus[0], normalize=u'lower', n_keyterms=30))
        termList2 = term_list(keyterms.sgrank(corpus[0], ngrams=(1, 2), normalize=u'lower', window_width=100, n_keyterms=70, idf=None))
        termSet1 = set(termList1)
        termSet2 = set(termList2)
        diffSet = termSet1 - termSet2
        termList = termList2 + list(diffSet)
        save_terms_text(res_file, termList)
          
def preprocess_category(in_path, out_path, category_name):
    import os
    spacy_lang = en_core_web_sm.load()
    print('===GIVEN CATEGORY: ' + category_name)
    for cat_path in glob.glob(in_path + '*'):
        _ , cat_name = os.path.split(cat_path)
        print('===CATEGORY: ' + cat_name)
        if category_name == cat_name:
            print('###Fine, found category directory ...')
#        for comp_path in glob.glob(kag_path + '/*'):
            corpus = textacy.corpus.Corpus(spacy_lang)
            texts = []
            for filename in glob.glob(cat_path + '/*.txt'):
                    texts.append(open(filename, 'r').read().decode('utf-8'))
            corpus_text = '\n'.join(text for text in texts)
            corpus.add_text(corpus_text)
#            _ , comp_file = os.path.split(comp_path)
#            sindex = len(kag_name) + 1
#            eindex = sindex + comp_file[sindex:].index('_') 
#            res_file = '{}.csv'.format(comp_file[sindex:eindex]) 
            res_file = '{}.csv'.format(category_name.lower()) 
            
            termList1 = term_list(keyterms.textrank(corpus[0], normalize=u'lower', n_keyterms=30))
            doc_idf = corpus.word_doc_freqs(lemmatize=None, weighting='idf', lowercase=True, as_strings=True)
            termList2 = term_list(keyterms.sgrank(corpus[0],ngrams=(1, 2, 3), normalize=u'lower', window_width=500, n_keyterms=70, idf=doc_idf)  )
            termSet1 = set(termList1)
            termSet2 = set(termList2)
            diffSet = termSet1 - termSet2
            termList = termList2 + list(diffSet)
            save_terms_text(out_path + res_file, termList)
            break


def preprocess_competences_combined(in_path, out_path):
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
            
            termList1 = term_list(keyterms.textrank(corpus[0], normalize=u'lower', n_keyterms=30))
            doc_idf = corpus.word_doc_freqs(lemmatize=None, weighting='idf', lowercase=True, as_strings=True)
            termList2 = term_list(keyterms.sgrank(corpus[0],ngrams=(1, 2, 3), normalize=u'lower', window_width=500, n_keyterms=70, idf=doc_idf)  )
            termSet1 = set(termList1)
            termSet2 = set(termList2)
            diffSet = termSet1 - termSet2
            termList = termList2 + list(diffSet)
            save_terms_text(out_path + res_file, termList)

#            doc_idf = corpus.word_doc_freqs(lemmatize=None, weighting='idf', lowercase=True, as_strings=True)
#            termList = keyterms.sgrank(corpus[0],ngrams=(1, 2, 3), normalize=u'lower', window_width=500, n_keyterms=30, idf=doc_idf)  
#            save_terms_csv(out_path + res_file, termList)
#            break
#        break


def preprocess_competences(in_path, out_path):
    import os
    spacy_lang = en_core_web_sm.load()
    for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
        _ , kag_name = os.path.split(kag_path)
        for comp_path in glob.glob(kag_path + '/*'):
            corpus = textacy.corpus.Corpus(spacy_lang)
            texts = []
            for filename in glob.glob(comp_path + '/*.txt'):
                    texts.append(open(filename, 'r').read().decode('utf-8'))
            corpus_text = '\n'.join(text for text in texts)
            corpus.add_text(corpus_text)
            _ , comp_file = os.path.split(comp_path)
            sindex = len(kag_name) + 1
            eindex = sindex + comp_file[sindex:].index('_') 
            res_file = '{}.csv'.format(comp_file[sindex:eindex]) 
            
#            termList1 = term_list(keyterms.textrank(corpus[0], normalize=u'lower', n_keyterms=30))
#            doc_idf = corpus.word_doc_freqs(lemmatize=None, weighting='idf', lowercase=True, as_strings=True)
#            termList2 = term_list(keyterms.sgrank(corpus[0],ngrams=(1, 2, 3), normalize=u'lower', window_width=500, n_keyterms=70, idf=doc_idf)  )
#            termSet1 = set(termList1)
#            termSet2 = set(termList2)
#            diffSet = termSet1 - termSet2
#            termList = termList2 + list(diffSet)
#            save_terms_text(out_path + res_file, termList)

#            doc_idf = corpus.word_doc_freqs(lemmatize=None, weighting='idf', lowercase=True, as_strings=True)
            termList = keyterms.sgrank(corpus[0],normalize=u'lower', n_keyterms=100)  
            save_terms_csv(out_path + res_file, termList)
#            break
#        break


def preprocess_jobs_or_cvs(in_path, out_path):
    import os
    job_count = 0 # limit number of jobs processed
    for filename in glob.glob(in_path + '*.json'):
        _ , joc_file = os.path.split(filename)
        res_file = out_path + '{0}.csv'.format(joc_file[0:joc_file.index('.')])
        if not os.path.isfile(res_file):
            corpus = read_cv2(filename)
            termList = keyterms.sgrank(corpus[0],normalize=u'lower', n_keyterms=100)  
            save_terms_csv(res_file, termList)
            job_count += 1
        if job_count >= 100:
            break

def read_job_req(job_path):
    import json
    spacy_lang = en_core_web_sm.load()
    corpus = textacy.corpus.Corpus(spacy_lang)
    content = ''
    with open(job_path) as cv_file:    
        content = json.load(cv_file)
    corpus_text = content.get('skillsDescription', u'')
    if not corpus_text:
        return None
    corpus.add_text(corpus_text)
    return corpus

def preprocess_jobs(in_path, out_path):
    import os
    job_count = 0 # limit number of jobs processed
    for filename in glob.glob(in_path + '*.json'):
        _ , cv_file = os.path.split(filename)
        res_file = out_path + '{0}.csv'.format(cv_file[0:cv_file.index('.')])
        if not os.path.isfile(res_file):
            corpus = read_job_req(filename)
            if not corpus is None:
                termList = keyterms.sgrank(corpus[0],normalize=u'lower', n_keyterms=100)  
                save_terms_csv(res_file, termList)
                job_count += 1
            if job_count >= 100:
                break

def preprocess_competences2(in_path, out_path):
    import os
    spacy_lang = en_core_web_sm.load()
    for kag_path in glob.glob(in_path + '/*'):
        _ , kag_name = os.path.split(kag_path)
        print('===KAG: ' + kag_name)
        for filename in glob.glob(kag_path + '/*.txt'):
            _ , comp_file = os.path.split(filename)
            print('===competence file: ' + comp_file)
            if comp_file.index('.') >= 5:
                print('===preprocessing competence file: ' + comp_file)
                corpus = textacy.corpus.Corpus(spacy_lang)
                corpus.add_text(open(filename, 'r').read().decode('utf-8'))
#                doc_idf = corpus.word_doc_freqs(lemmatize=None, weighting='idf', lowercase=True, as_strings=True)
                termList = term_list(keyterms.sgrank(corpus[0], ngrams=(1, 2, 3), normalize=u'lower', idf=None))
                res_file = '{}.csv'.format(comp_file[:-4]) 
                print('===Writing to: ' + res_file)
                save_terms_text(out_path + res_file, termList)
#                break




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
        termList1 = term_list(keyterms.textrank(corpus[0], normalize=u'lower', n_keyterms=30))
        doc_idf = corpus.word_doc_freqs(lemmatize=None, weighting='idf', lowercase=True, as_strings=True)
        termList2 = term_list(keyterms.sgrank(corpus[0],ngrams=(1, 2, 3), normalize=u'lower', window_width=500, n_keyterms=70, idf=doc_idf)  )
        termSet1 = set(termList1)
        termSet2 = set(termList2)
        diffSet = termSet1 - termSet2
        termList = termList2 + list(diffSet)
#        save_terms_csv(out_path + res_file, termList)
        save_terms_text(out_path + res_file, termList)
#        break

def build_corpus(corpus_path):
    texts = []
    for filename in glob.glob(corpus_path + '/*.txt'):
        texts.append(open(filename, 'r').read().decode('utf-8'))
    corpus_text = '\n'.join(text for text in texts)    
    with codecs.open('linkedin_manual.txt', 'w', 'utf-8') as vf:
        vf.write(corpus_text)

   
def preprocess_ms_jobs(in_path, out_path):
    from docx import Document
    import os
    from googletrans import Translator
    spacy_lang = en_core_web_sm.load()
    _ , res_name = os.path.split(out_path)
    wordDoc = Document(in_path)
    job_count = 0
    for table in wordDoc.tables:
        desc = ''
        req = ''
        for row in table.rows:
            for cell in row.cells:
                if cell.text == "Functieomschrijving":
                    desc = row.cells[1].text
                if cell.text == "Functie-eisen":
                    req = row.cells[1].text
        if desc and req:
            corpus = textacy.corpus.Corpus(spacy_lang)
            corpus_text = '\n'.join(text for text in [desc, req])
            translator = Translator()
            corpus_text_en = translator.translate(corpus_text, dest='en').text
            corpus_text_en = corpus_text_en.encode('ascii', 'ignore')
            corpus_text_en = corpus_text_en.decode('utf-8')
            corpus.add_text(corpus_text_en)
            termList = keyterms.sgrank(corpus[0], ngrams=(1, 2, 3), normalize=u'lower', n_keyterms=100) 
            res_file = out_path + 'job{0}.csv'.format(job_count)
            save_terms_csv(res_file, termList)
            job_count += 1



def main():
#    build_corpus('webcrawling/linkedin_manual')
#    preprocess_jobs_or_cvs(CV_PATH, RES_CV_PATH)
#    preprocess_jobs_or_cvs(JOB_PATH, RES_JOB_PATH)
#    preprocess_jobs(JOB_PATH, RES_JOBREQ_PATH)
    preprocess_ms_jobs(JOBMS_PATH, RES_JOBMS_PATH)
#    preprocess_jobs_or_cvs(NEWJOB_PATH, RES_NEWJOB_PATH)
#    preprocess_competences(KAG_BASE_PATH, RES_COMP_PATH)
#    preprocess_kags(KAG_BASE_PATH, RES_KAG_PATH)
#    preprocess_job_market(JOB_PATH, RES_JOB_PATH)
#    preprocess_category(CATEGORY_BASE_PATH, RES_CATEGORY_PATH, 'Education')
#    preprocess_competences2(COMP_BASE_PATH2,RES_COMP_PATH2)
    
    
if __name__ == '__main__':
    main()
