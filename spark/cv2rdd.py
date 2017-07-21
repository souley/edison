# Script that processes cv files placed under CVs/ to make them processable by Spark
# -*- coding: utf-8 -*-
import os
import json
from pathlib import Path

import codecs


def main():
    dir_path = "RDD/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    cvs_file_path = "./RDD/newjobs.jsonl"
    cvs_file = Path(cvs_file_path)
    if cvs_file.is_file():
        os.remove(cvs_file_path)
    path = os.getcwd() + '/../../results/combined/newjobs'
    counter = 0
    for filename in os.listdir(path):
        if filename[-3:] == "csv":
#        if filename[-3:] == "csv" and "market" in filename:   ### For job market
            try:
                fullpath = path + '/' + filename
                with codecs.open(fullpath, encoding='utf-8') as f:
                    terms = ','.join(repr(line) for line in f) 
                jeysan = {}
                jeysan['jobid'] = counter
                jeysan['description'] = terms    
                counter += 1
                with open(cvs_file_path, mode="a") as text_file:
                    text_file.write(json.dumps(jeysan, ensure_ascii=False) + u"\n")
            except:
                print(fullpath)
                continue

if __name__ == '__main__':
    main()


