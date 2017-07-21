# Script that processes job files placed under data/jobs_json to make them processable by Spark
# -*- coding: utf-8 -*-
import os
import json
from pathlib import Path

import codecs

def nljob2rdd():
    from docx import Document
    import os
    from googletrans import Translator

    dir_path = "RDD/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    job_file_path = "./RDD/jobms.jsonl"
    job_file = Path(job_file_path)
    if job_file.is_file():
        os.remove(job_file_path)
    path = os.getcwd() + '/../../data/jobs_erwin/data1apriltest.docx'
    
    wordDoc = Document(path)
    counter = 0
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
            text = '\n'.join(text for text in [desc, req])
            translator = Translator()
            text_en = translator.translate(text, dest='en').text
            text_en = text_en.encode('ascii', 'ignore')
            text_en = text_en.decode('utf-8')
            jeysan = {}
            jeysan['jobid'] = counter
            jeysan['description'] = text_en  
            counter += 1
            with open(job_file_path, mode="a") as text_file:
                text_file.write(json.dumps(jeysan, ensure_ascii=False) + u"\n")


def job2rdd():
    dir_path = "RDD/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    cvs_file_path = "./RDD/newraw_jobs.jsonl"
    cvs_file = Path(cvs_file_path)
    if cvs_file.is_file():
        os.remove(cvs_file_path)
    path = os.getcwd() + '/../../data/newjobs_json'
    counter = 0
    for filename in os.listdir(path):
        if filename[-4:] == "json":
            try:
                fullpath = path + '/' + filename
                with codecs.open(fullpath) as cv_file:    
                    content = json.load(cv_file)
                    text = content.get('description', u'')
                jeysan = {}
                jeysan['jobid'] = counter
                jeysan['description'] = text   
                counter += 1
                with open(cvs_file_path, mode="a") as text_file:
                    text_file.write(json.dumps(jeysan, ensure_ascii=False) + u"\n")
                if counter == 100: # voluntarily reduce to 100 jobs for testing
                    break
            except:
                print(fullpath)
                continue

if __name__ == '__main__':
    nljob2rdd()



