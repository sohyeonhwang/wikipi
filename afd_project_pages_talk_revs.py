import sys
import os
import pandas as pd
import numpy as np
import ast
import datefinder
from bs4 import BeautifulSoup
from collections import Counter
import wikifunctions as wf
import datetime

ts = datetime.datetime.now().strftime("%m-%d-%Y_%H-%M-%S")
talk_df_delsort_revs = wf.get_all_page_revisions("Wikipedia talk:WikiProject Deletion sorting")
talk_df_delsort_revs.to_csv('wikipedia_talk_wikiproject_delete_sorting_revs_{}.tsv'.format(ts),sep='\t',header=True, index=False,encoding='utf-8')

talk_df_wir_revs = wf.get_all_page_revisions("Wikipedia_talk:WikiProject_Women")
talk_df_wir_revs.to_csv('wikipedia_talk_wikiproject_women_{}.tsv'.format(ts),sep='\t',header=True, index=False,encoding='utf-8')