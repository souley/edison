#Script that processes txt files placed under Categores/ to make them processable by Spark
#Format for placing new files: Categories/<Category_name>/<skillname>.txt
# -*- coding: utf-8 -*-
from pathlib import Path

import codecs
import textacy

### Using keyterms instead of whole job ad
def get_terms(filepath):
    term_list = []
    with open(filepath, 'r') as jf: 
        content = jf.readlines()
        for line in content:
            tokens = line.split(',')
            term_list.append(tokens[0])
    return term_list

def jaccard_jobcomp():
    import os
    res_path = os.getcwd() + '/../results/jaccard/jobcomp/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "job-comp.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    jobpath = os.getcwd() + '/../results/sgrank/jobs'
    comppath = os.getcwd() + '/../results/sgrank/competences'
    job_count = 0
    for jobfile in os.listdir(jobpath):
        if jobfile[-3:] == "csv" and job_count < 100:
            _ , job_file = os.path.split(jobfile)
            jobterms = get_terms(jobpath + '/' + jobfile)
#            with open(jobpath + '/' + jobfile, 'r') as jf: 
#                content = jf.readlines()
#                jobterms = [x.strip() for x in content]
            comp_count = 0
            for compfile in os.listdir(comppath):
                _ , comp_file = os.path.split(compfile)
                if compfile[-3:] == "csv":
                    try:
                        fullpath = comppath + '/' + compfile
                        compterms = get_terms(fullpath)
#                        with open(fullpath, 'r') as cf: 
#                            ccontent = cf.readlines()
#                            compterms = [x.strip() for x in ccontent]
                        jc_sim = textacy.similarity.jaccard(jobterms, compterms, fuzzy_match = True)
                        with open(res_file_path, mode="a") as text_file:
                           text_file.write(str(job_count) + "," + str(comp_count) + "," + compfile[:-4] + "," + str(jc_sim) + "\n")
                        comp_count += 1
                    except:
                        print(fullpath)
                        continue
            job_count += 1


def jaccard_newjobcomp():
    import os
    res_path = os.getcwd() + '/../results/jaccard/newjobcomp/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "newjob-comp.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    jobpath = os.getcwd() + '/../results/combined/newjobs'
    comppath = os.getcwd() + '/../results/combined/competences'
    job_count = 0
    for jobfile in os.listdir(jobpath):
        if jobfile[-3:] == "csv":
            _ , job_file = os.path.split(jobfile)
            with open(jobpath + '/' + jobfile, 'r') as jf: 
                content = jf.readlines()
                jobterms = [x.strip() for x in content]
            comp_count = 0
            for compfile in os.listdir(comppath):
                _ , comp_file = os.path.split(compfile)
                if compfile[-3:] == "csv":
                    try:
                        fullpath = comppath + '/' + compfile
                        with open(fullpath, 'r') as cf: 
                            ccontent = cf.readlines()
                            compterms = [x.strip() for x in ccontent]
                        jc_sim = textacy.similarity.jaccard(jobterms, compterms, fuzzy_match = True)
                        with open(res_file_path, mode="a") as text_file:
                           text_file.write(str(job_count) + "," + str(comp_count) + "," + compfile[:-4] + "," + str(jc_sim) + "\n")
                        comp_count += 1
                    except:
                        print(fullpath)
                        continue
            job_count += 1

def jaccard_cvcomp():
    import os
    res_path = os.getcwd() + '/../results/jaccard/cvcomp/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "cv-comp.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    cvpath = os.getcwd() + '/../results/sgrank/CV'
    comppath = os.getcwd() + '/../results/sgrank/competences'
    cv_count = 0
    for cvfile in os.listdir(cvpath):
        if cvfile[-3:] == "csv":
            _ , cv_file = os.path.split(cvfile)
            cvterms = get_terms(cvpath + '/' + cvfile)
#            with open(cvpath + '/' + cvfile, 'r') as jf: 
#                content = jf.readlines()
#                cvterms = [x.strip() for x in content]
            comp_count = 0
            for compfile in os.listdir(comppath):
                _ , comp_file = os.path.split(compfile)
                if compfile[-3:] == "csv":
                    try:
                        fullpath = comppath + '/' + compfile
                        compterms = get_terms(fullpath)
#                        with open(fullpath, 'r') as cf: 
#                            ccontent = cf.readlines()
#                            compterms = [x.strip() for x in ccontent]
                        cc_sim = textacy.similarity.jaccard(cvterms, compterms, fuzzy_match = True)
                        with open(res_file_path, mode="a") as text_file:
                           text_file.write(str(cv_count) + "," + str(comp_count) + "," + compfile[:-4] + "," + str(cc_sim) + "\n")
                        comp_count += 1
                    except:
                        print(fullpath)
                        continue
            cv_count += 1


def jaccard_jobcv():
    import os
    res_path = os.getcwd() + '/../results/jaccard/jobcv/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "job-cv.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    jobpath = os.getcwd() + '/../results/sgrank/jobs'
    cvpath = os.getcwd() + '/../results/sgrank/CV'
    job_count = 0
    for jobfile in os.listdir(jobpath):
        if jobfile[-3:] == "csv":
            _ , job_file = os.path.split(jobfile)
            jobterms = get_terms(jobpath + '/' + jobfile)
#            with open(jobpath + '/' + jobfile, 'r') as jf: 
#                content = jf.readlines()
#                jobterms = [x.strip() for x in content]
            cv_count = 0
            for cvfile in os.listdir(cvpath):
                _ , cv_file = os.path.split(cvfile)
                if cvfile[-3:] == "csv":
                    try:
                        fullpath = cvpath + '/' + cvfile
                        cvterms = get_terms(fullpath)
#                        with open(fullpath, 'r') as cf: 
#                            ccontent = cf.readlines()
#                            cvterms = [x.strip() for x in ccontent]
                        jc_sim = textacy.similarity.jaccard(jobterms, cvterms, fuzzy_match = True)
                        with open(res_file_path, mode="a") as text_file:
                           text_file.write(str(job_count) + "," + str(cv_count) + "," + str(jc_sim) + "\n")
                        cv_count += 1
                    except:
                        print(fullpath)
                        continue
            job_count += 1


def jaccard_newjobcv():
    import os
    res_path = os.getcwd() + '/../results/jaccard/newjobcv/'
    if not os.path.exists(res_path):
        os.makedirs(res_path)
    res_file_path = res_path + "newjob-cv.csv"
    res_file = Path(res_file_path)
    if res_file.is_file():
        os.remove(res_file_path)
    jobpath = os.getcwd() + '/../results/combined/newjobs'
    cvpath = os.getcwd() + '/../results/combined/CV'
    job_count = 0
    for jobfile in os.listdir(jobpath):
        if jobfile[-3:] == "csv":
            _ , job_file = os.path.split(jobfile)
            with open(jobpath + '/' + jobfile, 'r') as jf: 
                content = jf.readlines()
                jobterms = [x.strip() for x in content]
            cv_count = 0
            for cvfile in os.listdir(cvpath):
                _ , cv_file = os.path.split(cvfile)
                if cvfile[-3:] == "csv":
                    try:
                        fullpath = cvpath + '/' + cvfile
                        with open(fullpath, 'r') as cf: 
                            ccontent = cf.readlines()
                            cvterms = [x.strip() for x in ccontent]
                        jc_sim = textacy.similarity.jaccard(jobterms, cvterms, fuzzy_match = True)
                        with open(res_file_path, mode="a") as text_file:
                           text_file.write(str(job_count) + "," + str(cv_count) + "," + str(jc_sim) + "\n")
                        cv_count += 1
                    except:
                        print(fullpath)
                        continue
            job_count += 1

if __name__ == '__main__':
#    jaccard_cvcomp()
    jaccard_jobcv()
#    jaccard_newjobcv()
    
#    jaccard_jobcomp()
