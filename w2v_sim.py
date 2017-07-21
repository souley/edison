#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 11 02:18:46 2017

@author: souley
"""

#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 10:35:44 2017
A simple script to test various term extraction methods
@author: souley
"""

import textacy
import glob 
import codecs

import en_core_web_sm
from textacy import data, keyterms, preprocess_text, spacy_utils

import csv

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.path import Path
from matplotlib.spines import Spine
from matplotlib.projections.polar import PolarAxes
from matplotlib.projections import register_projection

def _radar_factory(num_vars):
    theta = 2*np.pi * np.linspace(0, 1-1./num_vars, num_vars)
    theta += np.pi/2

    def unit_poly_verts(theta):
        x0, y0, r = [0.5] * 3
        verts = [(r*np.cos(t) + x0, r*np.sin(t) + y0) for t in theta]
        return verts

    class RadarAxes(PolarAxes):
        name = 'radar'
        RESOLUTION = 1

        def fill(self, *args, **kwargs):
            closed = kwargs.pop('closed', True)
            return super(RadarAxes, self).fill(closed=closed, *args, **kwargs)

        def plot(self, *args, **kwargs):
            lines = super(RadarAxes, self).plot(*args, **kwargs)
            for line in lines:
                self._close_line(line)

        def _close_line(self, line):
            x, y = line.get_data()
            # FIXME: markers at x[0], y[0] get doubled-up
            if x[0] != x[-1]:
                x = np.concatenate((x, [x[0]]))
                y = np.concatenate((y, [y[0]]))
                line.set_data(x, y)

        def set_varlabels(self, labels):
            self.set_thetagrids(theta * 180/np.pi, labels)

        def _gen_axes_patch(self):
            verts = unit_poly_verts(theta)
            return plt.Polygon(verts, closed=True, edgecolor='k')

        def _gen_axes_spines(self):
            spine_type = 'circle'
            verts = unit_poly_verts(theta)
            verts.append(verts[0])
            path = Path(verts)
            spine = Spine(self, spine_type, path)
            spine.set_transform(self.transAxes)
            return {'polar': spine}

    register_projection(RadarAxes)
    return theta

def radar_graph(labels = [], values = [], optimum = []):
    N = len(labels) 
    theta = _radar_factory(N)
    max_val = max(max(optimum), max(values))
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1, projection='radar')
    ax.plot(theta, values, color='k')
    ax.plot(theta, optimum, color='r')
    ax.set_varlabels(labels)
    #plt.show()
    plt.savefig("w2v_radar.png", dpi=100)

KAG_BASE_PATH = '../data/Competences/'
KAG_DA_PATH = KAG_BASE_PATH + 'data_analytics/'
COMP_DADS01_PATH = KAG_DA_PATH + 'data_analytics-DSDA01_predictive_analytics/'
CV_PATH = '../data/CV/'
JOB_PATH = '../data/jobs_json/'


RES_BASE_PATH = '../results/'
RES_COMP_PATH = RES_BASE_PATH + '/sgrank/competences/'
RES_KAG_PATH = RES_BASE_PATH + '/sgrank/KAG/'
RES_CV_PATH = RES_BASE_PATH + 'sgrank/CV/'
RES_JOB_PATH = RES_BASE_PATH + 'sgrank/jobs/'



def build_kag_docs():
    import os
    docs = []
    for kag_path in glob.glob(KAG_BASE_PATH + '/*'):
        _ , kag_name = os.path.split(kag_path)
        texts = []
        for comp_path in glob.glob(kag_path + '/*'):
            for filename in glob.glob(comp_path + '/*.txt'):
                texts.append(open(filename, 'r').read().decode('utf-8'))
        doc_text = '\n'.join(text for text in texts)
        docs.append(textacy.doc.Doc(doc_text, lang=u'en'))
    return docs

def build_cv_doc():
    import json
    doc_text = ''
    for cv_path in glob.glob(CV_PATH + '/*.json'):
        with open(cv_path) as cv_file:    
            content = json.load(cv_file)
            doc_text = content.get('description', u'')
        break
    return textacy.doc.Doc(doc_text)

def build_job_doc():
    import json
    doc_text = ''
    for job_path in glob.glob(JOB_PATH + '/*.json'):
        with open(job_path) as job_file:    
            content = json.load(job_file)
            doc_text = content.get('description', u'')
        break
    return textacy.doc.Doc(doc_text)

def compute_sims(kag_docs, in_doc):
    sims = []
    for doc in kag_docs:
        sims.append(textacy.similarity.word2vec(doc, in_doc))
    return sims

def main():
    kag_docs = build_kag_docs()
    cv_doc = build_cv_doc()
    job_doc = build_job_doc()
    cv_sims = compute_sims(kag_docs, cv_doc)
    job_sims = compute_sims(kag_docs, job_doc)
    radar_graph(['DSDA','DSDK','DSDM','DSENG','DSRM'], cv_sims, job_sims)


if __name__ == '__main__':
    main()
