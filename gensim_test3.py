from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer
from gensim import corpora, models, similarities
#import gensim
from gensim.models.coherencemodel import CoherenceModel
import pyLDAvis.gensim

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

# create English stop words list
en_stop = get_stop_words('en')

en_stop.append(['can', 'may', '1', '2', 's'])
# Create p_stemmer of class PorterStemmer
p_stemmer = PorterStemmer()
    
#import string
#from nltk.stem.wordnet import WordNetLemmatizer

def tokenize_clean(filepath, tjson=None):
    import json
    import codecs
#    print('=Reading file: ' + filepath)
    with codecs.open(filepath, 'r', encoding='utf-8') as rfile:
        if not tjson is None:
            content = json.load(rfile)
            content = content.get('description', u'')
        else:
            content = rfile.read()
        
        raw = content.lower()#.decode('utf-8', 'ignore')
        tokens = tokenizer.tokenize(raw)
        
        # remove stop words from tokens
        stopped_tokens = [i for i in tokens if not i in en_stop]
        
        stemmed_tokens = [p_stemmer.stem(i) for i in stopped_tokens]
#        stemmed_tokens = [WordNetLemmatizer().lemmatize(word) for word in stopped_tokens]
        # add tokens to list
        return stemmed_tokens

def clean(filepath, tjson=None):
    from nltk.corpus import stopwords 
    from nltk.stem.wordnet import WordNetLemmatizer
    import string
    stop = set(stopwords.words('english'))
    stop.add(sw for sw in ['data','also','can'])
    exclude = set(string.punctuation) 
#    lemma = WordNetLemmatizer()
    import json
    import codecs
#    print('=Reading file: ' + filepath)
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
    
    # generate LDA model
    ldamodel = models.ldamodel.LdaModel(corpus, num_topics=40, id2word = dictionary, passes=50)
    #print(ldamodel.print_topics(num_topics=5))
    cm = CoherenceModel(model=ldamodel, texts=texts.values(), dictionary=dictionary, coherence='c_v')
    print(cm.get_coherence())

    res_path = os.getcwd() + '/../results/lda/jobcomp/'
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
            job_text = tokenize_clean(filename, 'json')
            job_bow = dictionary.doc2bow(job_text)
            job_lda = ldamodel[job_bow]
            index = similarities.MatrixSimilarity(ldamodel[corpus])
            sims = index[job_lda]
            comps = texts.keys()
            comp_count = 0
            with open(res_file_path, mode="a") as text_file:
                for doc,sim in enumerate(sims):
                    text_file.write(str(job_count) + "," + str(comp_count) + "," + comps[doc] + "," + str(sim) + "\n")
                    comp_count += 1           
            job_count += 1
    ### compute similarities for new jobs
    for filename in glob.glob(NEWJOB_PATH + '*.json'):
        job_text = tokenize_clean(filename, 'json')
        job_bow = dictionary.doc2bow(job_text)
        job_lda = ldamodel[job_bow]
        index = similarities.MatrixSimilarity(ldamodel[corpus])
        sims = index[job_lda]
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
    
    # generate LDA model
    ldamodel = models.ldamodel.LdaModel(corpus, num_topics=40, id2word = dictionary, passes=50)
    #print(ldamodel.print_topics(num_topics=5))
    cm = CoherenceModel(model=ldamodel, texts=texts.values(), dictionary=dictionary, coherence='c_v')
    print(cm.get_coherence())

    ## compute similarities for CVs 
    res_path = os.getcwd() + '/../results/lda/cvcomp/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "cvcomp.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    cv_count = 0
    for filename in glob.glob(CV_PATH + '*.json'):
        if cv_count < 6:
            cv_text = tokenize_clean(filename, 'json')
            cv_bow = dictionary.doc2bow(cv_text)
            cv_lda = ldamodel[cv_bow]
            index = similarities.MatrixSimilarity(ldamodel[corpus])
            sims = index[cv_lda]
            comps = texts.keys()
            comp_count = 0
            with open(res_file_path, mode="a") as text_file:
                for doc,sim in enumerate(sims):
                    text_file.write(str(cv_count) + "," + str(comp_count) + "," + comps[doc] + "," + str(sim) + "\n")
                    comp_count += 1           
            cv_count += 1
    
#texts = {}
#for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
#    _ , kag_name = os.path.split(kag_path)
#    for comp_path in glob.glob(kag_path + '/*'):
#        _ , comp_file = os.path.split(comp_path)
#        sindex = len(kag_name) + 1
#        eindex = sindex + comp_file[sindex:].index('_') 
#        comp_key = comp_file[sindex:eindex]
#        comp_tokens = []
#        for filename in glob.glob(comp_path + '/*.txt'):
#            comp_tokens = comp_tokens + tokenize_clean(filename)
#        texts[comp_key] = comp_tokens
#
## turn our tokenized documents into a id <-> term dictionary
#dictionary = corpora.Dictionary(texts.values())
#    
## convert tokenized documents into a document-term matrix
#corpus = [dictionary.doc2bow(text) for text in texts.values()]
#
## generate LDA model
#ldamodel = models.ldamodel.LdaModel(corpus, num_topics=40, id2word = dictionary, passes=50)
##print(ldamodel.print_topics(num_topics=5))
#cm = CoherenceModel(model=ldamodel, texts=texts.values(), dictionary=dictionary, coherence='c_v')
#print(cm.get_coherence())

#pyLDAvis.enable_notebook()
#pyLDAvis.gensim.prepare(ldamodel, corpus, dictionary)

### write document-topic assignment to file for checking
#res_file = '{0}.txt'.format(RES_BASE_PATH + 'LDAtopics')
#with open(res_file, 'w') as tt_file: 
#    print>>tt_file, '=Model coherence: ' + str(cm.get_coherence())
#    print>>tt_file, '=topic - word assignments'
#    for item in ldamodel.show_topics(num_topics=40):
#        print>>tt_file, item
#    print>>tt_file, '=document - topic assignments'
#    for comp in texts:
#        comp_bow = dictionary.doc2bow(texts[comp])
#        print>>tt_file, comp
#        print>>tt_file, ldamodel.get_document_topics(comp_bow)

#### read job file
#job_text = tokenize_clean('../data/jobs_json/job_23264289.json', 'json')
##print('###job_text='+str(job_text))
#job_bow = dictionary.doc2bow(job_text)
#job_lda = ldamodel[job_bow]
#index = similarities.MatrixSimilarity(ldamodel[corpus])
#sims = index[job_lda]
##print(list(enumerate(sims)))
#comps = texts.keys()
#for doc,sim in enumerate(sims):
#    print('sim job - ' + comps[doc] + ' = ' + str(sim))



### compute similarities for CVs 
#res_path = os.getcwd() + '/../results/lda/cvcomp/'
#if not os.path.exists(res_path):
#    os.makedirs(res_path)
#res_file_path = res_path + "cvcomp.csv"
#res_file = Path(res_file_path)
#if res_file.is_file():
#    os.remove(res_file_path)
#cv_count = 0
#for filename in glob.glob(CV_PATH + '*.json'):
#    if cv_count < 6:
#        cv_text = tokenize_clean(filename, 'json')
#        cv_bow = dictionary.doc2bow(cv_text)
#        cv_lda = ldamodel[cv_bow]
#        index = similarities.MatrixSimilarity(ldamodel[corpus])
#        sims = index[cv_lda]
#        comps = texts.keys()
#        comp_count = 0
#        with open(res_file_path, mode="a") as text_file:
#            for doc,sim in enumerate(sims):
#                text_file.write(str(cv_count) + "," + str(comp_count) + "," + comps[doc] + "," + str(sim) + "\n")
#                comp_count += 1           
#        cv_count += 1


def read_job_doc():
    texts = {}
    job_count = 0
    for filename in glob.glob(JOB_PATH + '*.json'):
        if job_count < 5:
            _ , job_name = os.path.split(filename)
            texts[job_count] = tokenize_clean(filename, 'json')
            job_count += 1
    for filename in glob.glob(NEWJOB_PATH + '*.json'):
        _ , job_name = os.path.split(filename)
        texts[job_count] = tokenize_clean(filename, 'json')
        job_count += 1
    return texts

def job_cv_sims():
    texts = read_job_doc()
    # turn our tokenized documents into a id <-> term dictionary
    dictionary = corpora.Dictionary(texts.values())
        
    # convert tokenized documents into a document-term matrix
    corpus = [dictionary.doc2bow(text) for text in texts.values()]
    
    # generate LDA model
    ldamodel = models.ldamodel.LdaModel(corpus, num_topics=40, id2word = dictionary, passes=50)
    #print(ldamodel.print_topics(num_topics=5))
    cm = CoherenceModel(model=ldamodel, texts=texts.values(), dictionary=dictionary, coherence='c_v')
    print(cm.get_coherence())

    res_path = os.getcwd() + '/../results/lda/jobcv/'
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
            cv_text = tokenize_clean(filename, 'json')
            cv_bow = dictionary.doc2bow(cv_text)
            cv_lda = ldamodel[cv_bow]
            index = similarities.MatrixSimilarity(ldamodel[corpus])
            sims = index[cv_lda]
#            jobs = texts.keys()
            comp_count = 0
            with open(res_file_path, mode="a") as text_file:
                for doc,sim in enumerate(sims):
                    text_file.write(str(cv_count) + "," + str(doc) + "," + str(sim) + "\n")
                    comp_count += 1           
            cv_count += 1         
            
def read_comp2_doc():
    texts = {}
    for comp_path in glob.glob(COMP2_BASE_PATH + '/*'):
        print('=comp_path: ' + comp_path)
        for filename in glob.glob(comp_path + '/*.txt'):
            _ , comp_file = os.path.split(filename)
            comp_name = comp_file[:-4]
            print('=competence: ' + comp_name)
            if len(comp_name) >= 6:
                texts[comp_name] = clean(filename) #tokenize_clean(filename)
            
    return texts


def visualize_topics():
#    import pyLDAvis   
    
    texts = read_comp_doc()
    # turn our tokenized documents into a id <-> term dictionary
    dictionary = corpora.Dictionary(texts.values())
        
    # convert tokenized documents into a document-term matrix
    corpus = [dictionary.doc2bow(text) for text in texts.values()]
    
    # generate LDA model
    ldamodel = models.ldamodel.LdaModel(corpus, num_topics=20, id2word = dictionary, passes=50)
    #print(ldamodel.print_topics(num_topics=5))
    cm = CoherenceModel(model=ldamodel, texts=texts.values(), dictionary=dictionary, coherence='c_v')
    print(cm.get_coherence())

    print('=Preparing data for visualisation ...')
    vis_data = pyLDAvis.gensim.prepare(ldamodel, corpus, dictionary, mds='mmds')
    print('=Visualizing prepared data ...')
    pyLDAvis.show(vis_data)

if __name__ == '__main__':
    visualize_topics()
#    job_cv_sims()