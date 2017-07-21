#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  8 01:01:44 2017

@author: souley
"""

from textacy import data, keyterms, preprocess_text, spacy_utils

#import spacy
import en_core_web_sm

#spacy_lang = data.load_spacy('en_core_web_sm')
spacy_lang = en_core_web_sm.load()
#spacy_lang = data.load_spacy('en')
#text = """
#Friedman joined the London bureau of United Press International after completing his master's degree. He was dispatched a year later to Beirut, where he lived from June 1979 to May 1981 while covering the Lebanon Civil War. He was hired by The New York Times as a reporter in 1981 and re-dispatched to Beirut at the start of the 1982 Israeli invasion of Lebanon. His coverage of the war, particularly the Sabra and Shatila massacre, won him the Pulitzer Prize for International Reporting (shared with Loren Jenkins of The Washington Post). Alongside David K. Shipler he also won the George Polk Award for foreign reporting.
#In June 1984, Friedman was transferred to Jerusalem, where he served as the New York Times Jerusalem Bureau Chief until February 1988. That year he received a second Pulitzer Prize for International Reporting, which cited his coverage of the First Palestinian Intifada. He wrote a book, From Beirut to Jerusalem, describing his experiences in the Middle East, which won the 1989 U.S. National Book Award for Nonfiction.
#Friedman covered Secretary of State James Baker during the administration of President George H. W. Bush. Following the election of Bill Clinton in 1992, Friedman became the White House correspondent for the New York Times. In 1994, he began to write more about foreign policy and economics, and moved to the op-ed page of The New York Times the following year as a foreign affairs columnist. In 2002, Friedman won the Pulitzer Prize for Commentary for his "clarity of vision, based on extensive reporting, in commenting on the worldwide impact of the terrorist threat."
#In February 2002, Friedman met Saudi Crown Prince Abdullah and encouraged him to make a comprehensive attempt to end the Arab-Israeli conflict by normalizing Arab relations with Israel in exchange for the return of refugees alongside an end to the Israel territorial occupations. Abdullah proposed the Arab Peace Initiative at the Beirut Summit that March, which Friedman has since strongly supported.
#Friedman received the 2004 Overseas Press Club Award for lifetime achievement and was named to the Order of the British Empire by Queen Elizabeth II.
#In May 2011, The New York Times reported that President Barack Obama "has sounded out" Friedman concerning Middle East issues.
#"""

text = """
Effectively use variety of Machine Learning (including supervised, unsupervised, semisupervised learning), Data Mining, Prescriptive and Predictive Analytics techniques for complex data analysis
"""

spacy_doc = spacy_lang(preprocess_text(text), parse=False)
#spacy_doc = spacy_lang(text.decode('utf-8'), parse=False)
observed = [term for term, _ in keyterms.sgrank(spacy_doc)]

print(observed)