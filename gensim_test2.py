from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer
from gensim import corpora, models
#import gensim
from gensim.models.coherencemodel import CoherenceModel
import pyLDAvis

import glob
import os

KAG_BASE_PATH = '../data/Competences/'

tokenizer = RegexpTokenizer(r'\w+')

# create English stop words list
en_stop = get_stop_words('en')

en_stop.append(['can', 'may', '1', '2', 's'])
# Create p_stemmer of class PorterStemmer
p_stemmer = PorterStemmer()
    
#import string
from nltk.stem.wordnet import WordNetLemmatizer

texts = []
for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
    _ , kag_name = os.path.split(kag_path)
    for comp_path in glob.glob(kag_path + '/*'):
        for filename in glob.glob(comp_path + '/*.txt'):
            with open(filename, 'r') as compfile:
                content = compfile.read()
            
                raw = content.lower().decode('utf-8')
                tokens = tokenizer.tokenize(raw)
            
                # remove stop words from tokens
                stopped_tokens = [i for i in tokens if not i in en_stop]
                
                stemmed_tokens = [p_stemmer.stem(i) for i in stopped_tokens]
#                stemmed_tokens = [WordNetLemmatizer().lemmatize(word) for word in stopped_tokens]
                # add tokens to list
                texts.append(stemmed_tokens)

# turn our tokenized documents into a id <-> term dictionary
dictionary = corpora.Dictionary(texts)
    
# convert tokenized documents into a document-term matrix
corpus = [dictionary.doc2bow(text) for text in texts]

# generate LDA model
ldamodel = models.ldamodel.LdaModel(corpus, num_topics=5, id2word = dictionary, passes=20)
print(ldamodel.print_topics(num_topics=5))
cm = CoherenceModel(model=ldamodel, texts=texts, dictionary=dictionary, coherence='c_v')
print(cm.get_coherence())
#pyLDAvis.enable_notebook()
print('=Preparing data for visualisation ...')
vis_data = pyLDAvis.gensim.prepare(ldamodel, corpus, dictionary)
print('=Visualizing prepared data ...')
pyLDAvis.show(vis_data)
