#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 10 12:30:18 2017

@author: souley
"""

import textacy
import glob 

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
    plt.savefig("radar.png", dpi=100)

RES_BASE_PATH = '../results/'
RES_COMP_PATH = RES_BASE_PATH + '/sgrank/competences/'
RES_KAG_PATH = RES_BASE_PATH + '/sgrank/KAG/'
RES_CV_PATH = RES_BASE_PATH + 'sgrank/CV/'
RES_JOB_PATH = RES_BASE_PATH + 'sgrank/jobs/'


def get_terms(file_path):
    import os
    terms = {}
    _ , file_name = os.path.split(file_path)
    raw_terms = []
    with open(file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        for term, _ in csvreader:
            raw_terms.append(term)
    terms[file_name[:file_name.index('.')]] = raw_terms
    return terms

def get_job_terms():
    terms = []
    for job_path in glob.glob(RES_JOB_PATH + '/*.csv'):
        terms.append(get_terms(job_path))
        break
    return terms    

def get_cv_terms():
    terms = []
    for cv_path in glob.glob(RES_CV_PATH + '/*.csv'):
        terms.append(get_terms(cv_path))
        break
    return terms    

def get_kag_terms():
    terms = []
    for kag_path in glob.glob(RES_KAG_PATH + '/*.csv'):
        terms.append(get_terms(kag_path))
    return terms    
    

def build_dtm():
    kag_terms = get_kag_terms()
    cv_terms = get_cv_terms()
    job_terms = get_job_terms()
    
    all_terms = []
    for items in kag_terms:
        all_terms = all_terms + items.values()
    for items in cv_terms:
        all_terms = all_terms + items.values()
    for items in job_terms:
        all_terms = all_terms + items.values()
        
#    dtm, i2t = textacy.vsm.doc_term_matrix(all_terms, weighting=u'tfidf')
#    num_docs = dtm.get_shape()[0]
#    cv_sims = []
#    
#    for kn in range(0, num_docs - 2):
#        cv_sims.append(textacy.math_utils.cosine_similarity(dtm.getrow(kn), dtm.getrow(5)))
#    print(cv_sims)
    cv_sims = []
    cv_str = ''.join(term for term in cv_terms[0].values()[0])
    kag_names = []
    for items in kag_terms:
        kag_str = ''.join(term for term in items.values()[0])
        cv_sims.append(textacy.similarity.jaccard(kag_str, cv_str))
        kag_names.append(items.keys()[0])
    print(kag_names)
    
    job_sims = []
    job_str = ''.join(term for term in job_terms[0].values()[0])
    for items in kag_terms:
        kag_str = ''.join(term for term in items.values()[0])
        job_sims.append(textacy.similarity.jaccard(kag_str, job_str))
    
#    plot_radar(kag_names, cv_sims, job_sims)
    radar_graph(kag_names, cv_sims, job_sims)
    

def main():
    build_dtm()
    
if __name__ == '__main__':
    main()
