#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
SGrankVectorizer: a vectorizer based of SGrank scores of document key terms
Created on Mon Jul  3 13:52:32 2017

@author: souley
"""

#Script that processes txt files placed under Categores/ to make them processable by Spark
#Format for placing new files: Categories/<Category_name>/<skillname>.txt
# -*- coding: utf-8 -*-
from pathlib import Path

def link_terms(term):
    tokens = term.split(' ')
    if len(tokens) >= 2:
        return '_'.join(token for token in tokens)
    return tokens[0]


def calculate_distance(vec_job, vec_cv):
    from scipy import spatial
    cv = map(float, vec_cv)
    jobs = map(float, vec_job)
    result = spatial.distance.cosine(cv, jobs)
    return float(result)    

def cosine_similarity(vec1, vec2):
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    n_vec = np.array([vec1, vec2])
    n_vec = n_vec.astype(np.float)
    result = cosine_similarity(n_vec)
    return float(result[0][1])    
    
def build_comp_vocabulary(comppath, vocab):
    import os
    comp_dicts = []
    for compfile in os.listdir(comppath):
        if compfile[-3:] == "csv":
            try:
                comp_dict = {}
                fullpath = comppath + '/' + compfile
                with open(fullpath, 'r') as cf: 
                    ccontent = cf.readlines()
                    for line in ccontent:
                        term,score = line.strip().split(',')
                        linked_term = link_terms(term)
                        comp_dict[linked_term] = score
                        vocab.add(linked_term)
                comp_dicts.append(comp_dict)
            except:
                print(fullpath)
                continue
    return comp_dicts


def sgrank_jobcomp():
    import os
    from sets import Set
    res_path = os.getcwd() + '/../results/sgrank-vectorizer/jobcomp/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "job-comp.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    jobpath = os.getcwd() + '/../results/sgrank_0/jobs'
    comppath = os.getcwd() + '/../results/sgrank_0/competences'
    job_count = 0
    for jobfile in os.listdir(jobpath):
        if jobfile[-3:] == "csv" and job_count < 100:
            vocab = Set([])
            job_dict = {}
            with open(jobpath + '/' + jobfile, 'r') as jf: 
                content = jf.readlines()
                for line in content:
                    term,score = line.strip().split(',')
                    linked_term = link_terms(term)
                    job_dict[linked_term] = score
                    vocab.add(linked_term)
            comp_count = 0
#            if job_count == 0:
#            print("===vocabulary1: " + str(len(vocab)))
            comp_dicts = build_comp_vocabulary(comppath, vocab)
            
            job_vec = []
            for term in vocab:
                job_vec.append(job_dict.get(term, 0.0))
            for compfile in os.listdir(comppath):
                if compfile[-3:] == "csv":
                    comp_vec = []
                    for term in vocab:
                        comp_vec.append(comp_dicts[comp_count].get(term, 0.0))
                    with open(res_file_path, mode="a") as text_file:
                        text_file.write(str(job_count) + "," + str(comp_count) + "," + compfile[:-4] + "," + str(cosine_similarity(job_vec, comp_vec)) + "\n")
                    comp_count += 1
            job_count += 1


def sgrank_cvcomp():
    import os
    from sets import Set
    res_path = os.getcwd() + '/../results/sgrank-vectorizer/cvcomp/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "cv-comp.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    cvpath = os.getcwd() + '/../results/sgrank_0/CV'
    comppath = os.getcwd() + '/../results/sgrank_0/competences'
    cv_count = 0
    for cvfile in os.listdir(cvpath):
        if cvfile[-3:] == "csv" and cv_count < 100:
            vocab = Set([])
            cv_dict = {}
            with open(cvpath + '/' + cvfile, 'r') as cf: 
                content = cf.readlines()
                for line in content:
                    term,score = line.strip().split(',')
                    linked_term = link_terms(term)
                    cv_dict[linked_term] = score
                    vocab.add(linked_term)
            comp_count = 0
#            if job_count == 0:
#            print("===vocabulary1: " + str(len(vocab)))
            comp_dicts = build_comp_vocabulary(comppath, vocab)
            
            cv_vec = []
            for term in vocab:
                cv_vec.append(cv_dict.get(term, 0.0))
            for compfile in os.listdir(comppath):
                if compfile[-3:] == "csv":
                    comp_vec = []
                    for term in vocab:
                        comp_vec.append(comp_dicts[comp_count].get(term, 0.0))
                    with open(res_file_path, mode="a") as text_file:
                        text_file.write(str(cv_count) + "," + str(comp_count) + "," + compfile[:-4] + "," + str(cosine_similarity(cv_vec, comp_vec)) + "\n")
                    comp_count += 1
            cv_count += 1

def sgrank_jobcv():
    import os
    from sets import Set
    res_path = os.getcwd() + '/../results/sgrank-vectorizer/jobcv/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "job-cv.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    jobpath = os.getcwd() + '/../results/sgrank_0/jobs'
    cvpath = os.getcwd() + '/../results/sgrank_0/CV'
    job_count = 0
    for jobfile in os.listdir(jobpath):
        if jobfile[-3:] == "csv" and job_count < 100:
            vocab = Set([])
            job_dict = {}
            with open(jobpath + '/' + jobfile, 'r') as jf: 
                content = jf.readlines()
                for line in content:
                    term,score = line.strip().split(',')
                    linked_term = link_terms(term)
                    job_dict[linked_term] = score
                    vocab.add(linked_term)
            cv_count = 0
#            if job_count == 0:
#            print("===vocabulary1: " + str(len(vocab)))
            cv_dicts = build_comp_vocabulary(cvpath, vocab)
            
            job_vec = []
            for term in vocab:
                job_vec.append(job_dict.get(term, 0.0))
            for cvfile in os.listdir(cvpath):
                if cvfile[-3:] == "csv":
                    cv_vec = []
                    for term in vocab:
                        cv_vec.append(cv_dicts[cv_count].get(term, 0.0))
                    with open(res_file_path, mode="a") as text_file:
                        text_file.write(str(job_count) + "," + str(cv_count) + "," + str(cosine_similarity(job_vec, cv_vec)) + "\n")
                    cv_count += 1
            job_count += 1


if __name__ == '__main__':
#    sgrank_jobcomp()
#    sgrank_cvcomp()
    sgrank_jobcv()
