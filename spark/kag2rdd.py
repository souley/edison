#Script that processes txt files placed under Categores/ to make them processable by Spark
#Format for placing new files: Categories/<Category_name>/<skillname>.txt
# -*- coding: utf-8 -*-
import os
import json
from pathlib import Path

import codecs

def main():
    import os
    dir_path = "RDD/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    category_file_path = "./RDD/competences2.jsonl"
    kag_file = Path(category_file_path)
    if kag_file.is_file():
        os.remove(category_file_path)
    path = os.getcwd() + '/../../results/sgrank2/competences'
    counter = 0
    for filename in os.listdir(path):
        _ , comp_file = os.path.split(filename)
        if filename[-3:] == "csv" and comp_file.index('.') >= 6:
            try:
                fullpath = path + '/' + filename
#                print('=step1')
#                fullstr = Path(fullpath).open().read().strip()
#                print('=step1')
#                terms = get_terms(fullstr)
                with codecs.open(fullpath, encoding='utf-8') as f:
                    terms = ','.join(repr(line) for line in f)                 
                jeysan = {}
                jeysan['cpid'] = counter
                jeysan['category'] = filename[:-4]
#                print('=step2')
                jeysan['text'] = terms    
                print("###Writing '" + terms + "' as category: " + filename[:-4] + " with id: " + str(counter))
                counter += 1
#                print('=step3')
                with open(category_file_path, mode="a") as text_file:
                    text_file.write(json.dumps(jeysan, ensure_ascii=False) + u"\n")
            except:
                print(fullpath)
                continue

def main_cat():
    dir_path = "RDD/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    category_file_path = "./RDD/categories.jsonl"
    kag_file = Path(category_file_path)
    if kag_file.is_file():
        os.remove(category_file_path)
    path = os.getcwd() + '/../../results/combined/categories'
    counter = 0
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            try:
                fullpath = path + '/' + filename
                with codecs.open(fullpath, encoding='utf-8') as f:
                    terms = ','.join(repr(line) for line in f)                 
                jeysan = {}
                jeysan['catid'] = counter
                jeysan['category'] = filename[:-4]
                jeysan['text'] = terms    
                counter += 1
                with open(category_file_path, mode="w") as text_file:
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
        term,score = line.split(',')
        linked_term = link_terms(term)
        term_list = term_list + linked_term + "\\n',"
    return term_list[:-1]

def write_comps():
    import codecs
    dir_path = "RDD/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    comps_file_path = "./RDD/competences.jsonl"
    comps_file = Path(comps_file_path)
    if comps_file.is_file():
        os.remove(comps_file_path)
    path = os.getcwd() + '/../../results/sgrank/competences'
    counter = 0
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            try:
                fullpath = path + '/' + filename
#                fullstr = Path(fullpath).read_text().strip()
                with codecs.open(fullpath, encoding='utf-8') as f:
#                    terms = ','.join(repr(link_terms(term) for term, _ in line.split(',') for line in f))
                    fullstr = '\n'.join(repr(line) for line in f) 
#                    print(fullstr)
                terms = get_linked_terms(fullstr)
                if len(terms) > 0:
                    jeysan = {}
                    jeysan['cpid'] = counter
                    jeysan['category'] = filename[:-4]
                    jeysan['text'] = terms    
#                    jeysan['jobid'] = counter
#                    jeysan['description'] = terms    
                    counter += 1
                    with open(comps_file_path, mode="a") as text_file:
                        text_file.write(json.dumps(jeysan, ensure_ascii=False) + u"\n")
            except:
                print(fullpath)
                continue

if __name__ == '__main__':
    write_comps()
