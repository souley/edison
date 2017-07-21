#Script that writes things to a running mongodb
from pymongo import MongoClient
import os
from pathlib import Path
import json
#import re

def cvkag_collection(db):
    collection = db['tfidf_cvkag']
    path = 'results/sgrank-tfidf/cv-kag'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "cvid", "kagid", "category", "distance"
#                    print('===LINE=' + line)
                    cvid, kagid, category, distance = line.strip().split(',')
                    obj = {"cvid": int(cvid), "kagid": int(kagid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

def cvcomp_collection(db):
    collection = db['tfidf_cvcomp']
    path = 'results/sgrank-tfidf/cv-comp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "cvid", "cpid", "category", "distance"
                    cvid, cpid, category, distance = line.strip().split(',')
                    obj = {"cvid": int(cvid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)


def jobkag_collection(db):
    collection = db['tfidf_jobkag']
    path = 'results/sgrank-tfidf/job-kag'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    jobid, kagid, category, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "kagid": int(kagid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)


def jobcomp_collection(db):
    collection = db['tfidf_jobcomp']
    path = 'results/sgrank-tfidf/job-comp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    jobid, cpid, category, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)


def jobcv_collection(db):
    collection = db['tfidf_jobcv']
    path = 'results/sgrank-tfidf/job-cv'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    #"jobid", "cvid", "distance"
                    jobid, cvid, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cvid": int(cvid), "distance": float(distance)}
                    collection.insert_one(obj)


def persist_raw_cvs(db):
    collection = db['raw_cvs']
    q = Path('RDD/raw_cvs.jsonl')
    with q.open() as f:
        for line in f:
            json_obj = json.loads(line.strip())
            try:
                obj = {"cvid": json_obj['cvid'], "description": json_obj["description"]}
                collection.insert_one(obj)
            except:
                continue


def persist_raw_jobs(db):
    collection = db['raw_jobs']
    q = Path('RDD/raw_jobs.jsonl')
    with q.open() as f:
        for line in f:
            json_obj = json.loads(line.strip())
            try:
                obj = {"jobid": json_obj['jobid'], "description": json_obj["description"]}
                collection.insert_one(obj)
            except:
                continue

def persist_competences(db):
    collection = db['competences']
    q = Path('RDD/competences.jsonl')
    with q.open() as f:
        for line in f:
            json_obj = json.loads(line.strip())
            try:
                obj = {"cpid": json_obj['cpid'], "category": json_obj["category"]}
                collection.insert_one(obj)
            except:
                continue    

def jmktcomp_collection(db):
    collection = db['tfidf_jmktcomp']
    path = 'results/sgrank-tfidf/jmkt-comp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    #"jobid", "cvid", "category", "distance"
                    jobid, cpid, category, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cpid": int(cpid), "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

def catcomp_collection(db):
    collection = db['tfidf_catcomp']
    path = 'results/sgrank-tfidf/cat-comp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    #"jobid", "cvid", "category", "distance"
                    catid, cpid, category, distance = line.strip().split(',')
                    obj = {"catid": int(catid), "cpid": int(cpid), "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

   
### Testing new jobs
def newjobcomp_collection(db):
    collection = db['tfidf_newjobcomp']
    path = 'results/sgrank-tfidf/newjob-comp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    jobid, cpid, category, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)


def newjobcv_collection(db):
    collection = db['tfidf_newjobcv']
    path = 'results/sgrank-tfidf/newjob-cv'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    #"jobid", "cvid", "distance"
                    jobid, cvid, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cvid": int(cvid), "distance": float(distance)}
                    collection.insert_one(obj)


def persist_newraw_jobs(db):
    collection = db['newraw_jobs']
    q = Path('RDD/newraw_jobs.jsonl')
    with q.open() as f:
        for line in f:
            json_obj = json.loads(line.strip())
            try:
                obj = {"jobid": json_obj['jobid'], "description": json_obj["description"]}
                collection.insert_one(obj)
            except:
                continue

### Jaccard similarities for jobs vs EDISON
def jacc_jobcomp_collection(db):
    collection = db['jacc_jobcomp']
    path = '../../results/jaccard/jobcomp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    jobid, cpid, category, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

### Jaccard similarities for CVs vs EDISON
def jacc_cvcomp_collection(db):
    collection = db['jacc_cvcomp']
    path = '../../results/jaccard/cvcomp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    cvid, cpid, category, distance = line.strip().split(',')
                    obj = {"cvid": int(cvid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

### Jaccard similarities for (new) jobs vs CVs
def jacc_jobcv_collection(db):
    collection = db['jacc_jobcv']
    path = '../../results/jaccard/jobcv'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "cvid", "distance"
                    jobid, cvid, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cvid": int(cvid), \
                    "distance": float(distance)}
                    collection.insert_one(obj)

### SGRank similarities for jobs vs EDISON
def sgrank_jobcomp_collection(db):
    collection = db['sgrank_jobcomp']
    path = '../../results/sgrank-vectorizer/jobcomp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    jobid, cpid, category, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

### SGRank similarities for CVs vs EDISON
def sgrank_cvcomp_collection(db):
    collection = db['sgrank_cvcomp']
    path = '../../results/sgrank-vectorizer/cvcomp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    cvid, cpid, category, distance = line.strip().split(',')
                    obj = {"cvid": int(cvid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

### SGRank similarities for new jobs vs CVs
def sgrank_jobcv_collection(db):
    collection = db['sgrank_jobcv']
    path = '../../results/sgrank-vectorizer/jobcv'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "cvid", "distance"
                    jobid, cvid, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cvid": int(cvid), \
                    "distance": float(distance)}
                    collection.insert_one(obj)

### LDA similarities for jobs vs EDISON
def lda_jobcomp_collection(db):
    collection = db['lda_jobcomp']
    path = '../../results/lda/jobcomp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    jobid, cpid, category, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

### LDA similarities for CVs vs EDISON
def lda_cvcomp_collection(db):
    collection = db['lda_cvcomp']
    path = '../../results/lda/cvcomp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    cvid, cpid, category, distance = line.strip().split(',')
                    obj = {"cvid": int(cvid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

### LDA similarities for new jobs vs CVs
def lda_jobcv_collection(db):
    collection = db['lda_jobcv']
    path = '../../results/lda/jobcv'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "cvid", "distance"
                    cvid, jobid, distance = line.strip().split(',')
                    obj = {"cvid": int(cvid), "jobid": int(jobid), \
                    "distance": float(distance)}
                    collection.insert_one(obj)

### Doc2Vec similarities for jobs vs EDISON
def d2v_jobcomp_collection(db):
    collection = db['doc2vec_jobcomp']
    path = '../../results/d2v/jobcomp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    jobid, cpid, category, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

### Doc2Vec similarities for CVs vs EDISON
def d2v_cvcomp_collection(db):
    collection = db['doc2vec_cvcomp']
    path = '../../results/d2v/cvcomp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    cvid, cpid, category, distance = line.strip().split(',')
                    obj = {"cvid": int(cvid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

### Doc2Vec similarities for new jobs vs CVs
def d2v_jobcv_collection(db):
    collection = db['doc2vec_jobcv']
    path = '../../results/d2v/jobcv'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "cvid", "distance"
                    cvid, jobid, distance = line.strip().split(',')
                    obj = {"cvid": int(cvid), "jobid": int(jobid), \
                    "distance": float(distance)}
                    collection.insert_one(obj)


def tfidf_cvcomp_collection(db):
    collection = db['tfidf2_cvcomp']
    path = '../../results/tfidf/cvcomp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "cvid", "cpid", "category", "distance"
                    cvid, cpid, category, distance = line.strip().split(',')
                    obj = {"cvid": int(cvid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)


def tfidf_jobcomp_collection(db):
    collection = db['tfidf2_jobcomp']
    path = '../../results/tfidf/jobcomp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    jobid, cpid, category, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)


def tfidf_jobcv_collection(db):
    collection = db['tfidf2_jobcv']
    path = '../../results/tfidf/jobcv'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    #"jobid", "cvid", "distance"
                    jobid, cvid, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cvid": int(cvid), "distance": float(distance)}
                    collection.insert_one(obj)

def persist_nl_jobs(db):
    collection = db['nl_jobs']
    q = Path('RDD/jobms.jsonl')
    with q.open() as f:
        for line in f:
            json_obj = json.loads(line.strip())
            try:
                obj = {"jobid": json_obj['jobid'], "description": json_obj["description"]}
                collection.insert_one(obj)
            except:
                continue

### New jobs FROM Erwin
def tfidf_nljobcomp_collection(db):
    collection = db['tfidf_nljobcomp']
    path = 'results/sgrank-tfidf/nljob-comp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "category", "distance"
                    jobid, cpid, category, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

### New jobs FROM Erwin
def tfidf_nljobcv_collection(db):
    collection = db['tfidf_nljobcv']
    path = 'results/sgrank-tfidf/nljob-cv'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "jobid", "kagid", "distance"
                    jobid, cpid, distance = line.strip().split(',')
                    obj = {"jobid": int(jobid), "cpid": int(cpid), \
                    "distance": float(distance)}
                    collection.insert_one(obj)


def tfidf_cvcomp_collection2(db):
    collection = db['tfidf_cvcomp']
    path = 'results/sgrank-tfidf/cv-comp'
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
            fullpath = path + '/' + filename
            q = Path(fullpath)
            with q.open() as f:
                for line in f:
                    # "cvid", "cpid", "category", "distance"
                    cvid, cpid, category, distance = line.strip().split(',')
                    obj = {"cvid": int(cvid), "cpid": int(cpid), \
                    "category": category, "distance": float(distance)}
                    collection.insert_one(obj)

def main():
    client = MongoClient('localhost', 27017)
    db = client['edison-database']

#    cvkag_collection(db)
#    cvcomp_collection(db)
#    jobkag_collection(db)
#    jobcomp_collection(db)
    
#    jobcv_collection(db)
#    jmktcomp_collection(db)
#    catcomp_collection(db)
    
#    persist_competences(db)
#    persist_raw_cvs(db)
#    persist_raw_jobs(db)

#    persist_newraw_jobs(db)
#    newjobcomp_collection(db)
#    newjobcv_collection(db)

#    jacc_jobcomp_collection(db)
#    jacc_cvcomp_collection(db) 
#    jacc_jobcv_collection(db)

#    sgrank_jobcomp_collection(db)
#    sgrank_cvcomp_collection(db) 
#    sgrank_jobcv_collection(db)

#    lda_jobcomp_collection(db)
#    lda_cvcomp_collection(db) 
#    lda_jobcv_collection(db)

#    d2v_jobcomp_collection(db)
#    d2v_cvcomp_collection(db) 
#    d2v_jobcv_collection(db)

#    tfidf_jobcomp_collection(db)
    tfidf_cvcomp_collection2(db) 
#    tfidf_jobcv_collection(db)
    
    persist_nl_jobs(db)
    tfidf_nljobcomp_collection(db)
    tfidf_nljobcv_collection(db)
    


if __name__ == '__main__':
    main()

