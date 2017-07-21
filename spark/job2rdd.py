#Script that processes txt files placed under jobs/ to make them processable by Spark
# -*- coding: utf-8 -*-
import os
import json
from pathlib import Path

### Using keyterms instead of whole job ad
def get_terms(text):
    term_list = ''
    lines = text.split('\n')
    for line in lines:
        tokens = line.split(',')
        if len(tokens) >= 2:
            term_list = term_list + tokens[0] + ','
    return term_list[:-1]

def main():
    dir_path = "RDD/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    jobs_file_path = "./RDD/job_market.jsonl"
    jobs_file = Path(jobs_file_path)
    if jobs_file.is_file():
        os.remove(jobs_file_path)
    path = os.getcwd() + '/../../results/combined/jobs'
    counter = 0
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
#        if filename[-3:] == "csv" and 'market' in filename: ### For job markaet analysis
            try:
                fullpath = path + '/' + filename
                fullstr = Path(fullpath).read_text().strip()
                terms = get_terms(fullstr)
                if len(terms) > 0:
                    jeysan = {}
                    jeysan['jobid'] = counter
                    jeysan['description'] = terms    
                    counter += 1
                    with open(jobs_file_path, mode="a") as text_file:
                        text_file.write(json.dumps(jeysan, ensure_ascii=False) + u"\n")
            except:
                print(fullpath)
                continue

def link_terms(term):
    tokens = term.split(' ')
    if len(tokens) >= 2:
        return '_'.join(token for token in tokens)
    return tokens[0]

def get_linked_terms(text):
    term_list = ''
    lines = text.split('\n')
    for line in lines:
        term,score = line.strip().split(',')
        linked_term = link_terms(term)
        term_list = term_list + linked_term + "\\n',"
    return term_list[:-1]

def nljobs():
    import codecs
    dir_path = "RDD/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    jobs_file_path = "./RDD/nljobs.jsonl"
    jobs_file = Path(jobs_file_path)
    if jobs_file.is_file():
        os.remove(jobs_file_path)
    path = os.getcwd() + '/../../results/sgrank/jobms'
    counter = 0
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            try:
                fullpath = path + '/' + filename
#                fullstr = Path(fullpath).read_text().strip()
                with codecs.open(fullpath, encoding='utf-8') as f:
                    fullstr = '\n'.join(repr(line) for line in f)                 
                terms = get_linked_terms(fullstr)
                if len(terms) > 0:
                    jeysan = {}
                    jeysan['jobid'] = counter
                    jeysan['description'] = terms    
                    counter += 1
                    with open(jobs_file_path, mode="a") as text_file:
                        text_file.write(json.dumps(jeysan, ensure_ascii=False) + u"\n")
            except:
                print(fullpath)
                continue

if __name__ == '__main__':
    nljobs()

