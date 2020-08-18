import sys
import pandas as pd
import argparse
import numpy as np
import glob, os, re
import csv
from pathlib import Path
from datetime import datetime
import time
import collections 
from deltas import segment_matcher, text_split
from multiprocessing import Pool, cpu_count

start_time = time.time()

def parse_args():
    parser = argparse.ArgumentParser(description='Create a dataset.')
    parser.add_argument('-i', '--input', help='monthly_diff_[la]wiki_strings to input', required=True, type=str)
    parser.add_argument('--lang', help='Specify which language edition', default='es',type=str)
    parser.add_argument('-o', '--output-directory', help='Output directory', default='./output_strings', type=str)
    parser.add_argument('-ofn', '--output-filename', help='filename for the output file of tsv', default='stringsWrangled', type=str)
    args = parser.parse_args()
    return(args)

def tokenize_prep(regex_string):
    # we want to make Wikipedia:Droit de l'auteur --> Wikipedia_Droit_de_l'auteur
    regex_string = re.sub(r'\d' , '', regex_string)
    regex_string_l = regex_string.split(', ')
    
    temp_l = []
    for s in regex_string_l:
        s = s.strip().replace(' ','_')
        s = s.replace('_WP:',' WP:')
        s = s.replace('_Wp:',' Wp:')
        s = s.replace('_wp:',' wp:')
        s = s.replace('_Wikipedia:',' Wikipedia:')
        s = s.replace('_WIKIPEDIA:',' WIKIPEDIA:')
        s = s.replace('_Wikipédia:',' Wikipédia:')
        s = s.replace('_WIKIPÉDIA:',' Wikipédia:')
        s = s.strip()
        s = s.strip().replace(' ',', ')
        s = s.strip().replace('_,',',')
        temp_l.append(s)
    new_string = ', '.join(temp_l).strip()
    new_string = new_string.replace(':','')
    new_string_l = new_string.split(', ')
    new_string_l = [x.replace(',','').strip() for x in new_string_l]
    new_string = ', '.join(new_string_l).strip()
    return new_string

def reverse_tokenize_prep(regex_string):
    # we want to make Wikipedia_Droit_de_l'auteur --> Wikipedia:Droit de l'auteur
    regex_string_l = regex_string.split(', ')
    temp_l = []
    for s in regex_string_l:
        s = s.strip().replace('_',' ')
        s = re.sub(r'^(WP|Wp|wp)', 'WP:', s)
        s = re.sub(r'^(Wikipedia|WIKIPEDIA)','Wikipedia:',s)
        s = re.sub(r'^(Wikipédia|WIKIPÉDIA)','Wikipédia:',s)
        temp_l.append(s)
    new_string = new_string = ', '.join(temp_l).strip()
    return new_string

def compare_rev_regexes(current, prev, revision_diff_bool, revert):
    # takes in the values from a row
    return current

def diff_find(row):
    r_current = row['regexes']
    r_prev = row['regexes_prev']
    # revision has a difference in regex from last revision of article
    r_bool = row['regexes_diff_bool']
    revert = row['revert']

    r_diff = compare_rev_regexes(r_current,r_prev,r_bool,revert)
    return r_diff

def count_diff(row):
    if row["regexes_diff"] == "":
        diff_count = 0
    else:
        diff_count = len(row["regexes_diff"].split(", "))
    return diff_count

def pd_apply_diff_find(df):
    print("  > Running pd_apply_diff_find: diff find, count diffs")
    df['regexes_diff'] = df.apply(diff_find,axis=1)
    df['regexes_diff_count'] = df.apply(count_diff,axis=1)
    return df

def parallelize_dataframe(df, func, n_cores):
    '''
    Adapted from: https://towardsdatascience.com/make-your-own-super-pandas-using-multiproc-1c04f41944a1
    '''
    print("> Using {} cores, applying {} to the dataframe".format(n_cores, func))
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

if __name__ == "__main__":
    args = parse_args()

    if not os.path.isdir(args.output_directory):
        os.mkdir(args.output_directory)

    # checking args and retrieving inputs
    print("INPUT:\t{}".format(args.input))
    print(" LANG:\t{}".format(args.lang))
    print("O_DIR:\t{}".format(args.output_directory))
    print(" O_FN:\t{}".format(args.output_filename))

    input_path = glob.glob("{}/{}*.tsv".format(args.input, args.lang))

    file_path = input_path[0]
    print(file_path)

    #index is actually YYYY-MM_NAMESPACE
    #regexes_diff is the string of diffs
    dtypes={'index':np.str, 'regexes_diff':np.str}

    # to chunk
    pd_dfs = pd.read_csv(file_path, sep="\t", header=0, dtype=dtypes, chunksize=1000)

    input("There are {} cores. To start data processing, press enter.".format(cpu_count()))
    print("Begin data processing of chunks, each chunk parallelized...")
    print("=========================================================================================")

    # initialize the dict for the data outputs that we want, so that we can build while chunking
    string_out_df = pd.DataFrame()

    track = 1
    # iterate through chunks
    for pd_df in pd_dfs:
        print("\nCHUNK: {}".format(track))
        start_time = time.time()
        
        # get rid of the {{EMPTYBABY}} place holders
        pd_df = pd_df.replace("{{EMPTYBABY}}","")
        pd_df.drop(['core_regexes','core_prev','core_diff_bool','reverteds'], axis=1, inplace=True)
        pd_df['regexes_diff'] = ""
        pd_df['regexes_diff_count'] = 0
        bool_d = {'TRUE': True, 'FALSE': False}
        pd_df['revert'] = pd_df['revert'].map(bool_d)
        pd_df['anon'] = pd_df['anon'].map(bool_d)

        #print(pd_df.columns)
        print(pd_df.shape)

        memory = pd_df.memory_usage().sum()
        print('> Total current memory of dataframe is:', memory,'Bytes.')
        #print(pd_df.memory_usage())

        #processed_df = pd_df.apply(diff_find,axis=1)
        #print("--- Without parallelization, this took: %s seconds ---" % (time.time() - start_time))

        # function should take in pd_df, do the apply(diff_find) and return the result as a df
        partitions = cpu_count()
        processed_df = parallelize_dataframe(pd_df, pd_apply_diff_find, partitions)

        print("> Processing this chunk in parallel took: %s seconds.\n" % (time.time() - start_time))
        #print(processed_df.head(5))

        # Now we create the data we need for our figures
        mid_time = time.time()

        print("\n> Wrangling diff-processed df into updating output data took: %s seconds" % (time.time() - mid_time))
        print("=========================================================================================")
        track += 1

    print("Finished going through chunks!")

    # output.
    #str_out_df = pd.DataFrame.from_dict(mnstr_out_dict,orient='index')
    #str_out_df = str_out_df.reset_index()
    #print(str_out_df.head(5))

    out_filepath = "{}/{}_{}wiki_strings_{}.tsv".format(args.output_directory,args.output_filename,args.lang,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    #str_out_df.to_csv(out_filepath,sep='\t',header=True) 
    print("Find the output here: {}\nDone.\n".format(out_filepath))
