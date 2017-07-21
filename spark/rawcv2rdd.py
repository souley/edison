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
    cvs_file_path = "./RDD/raw_cvs.jsonl"
    cvs_file = Path(cvs_file_path)
    if cvs_file.is_file():
        os.remove(cvs_file_path)
    path = os.getcwd() + '/../../data/CV'
    counter = 0
    for filename in os.listdir(path):
        if filename[-4:] == "json":
            try:
                fullpath = path + '/' + filename
                with codecs.open(fullpath) as cv_file:    
                    content = json.load(cv_file)
                    text = content.get('description', u'')
                jeysan = {}
                jeysan['cvid'] = counter
                jeysan['description'] = text   
                counter += 1
                with open(cvs_file_path, mode="a") as text_file:
                    text_file.write(json.dumps(jeysan, ensure_ascii=False) + u"\n")
            except:
                print(fullpath)
                continue

if __name__ == '__main__':
    main()



