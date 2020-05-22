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
    parser.add_argument('-i', '--input', help='Filtered master_regex.tsv to input', required=True, type=str)
    parser.add_argument('--lang', help='Specify which language edition', default='es',type=str)
    parser.add_argument('-o', '--output-directory', help='Output directory', default='./masterdiff_output', type=str)
    parser.add_argument('-ofn', '--output-filename', help='filename for the output file of tsv', default='monthly_diff', type=str)
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
    # we want to write a function that will find the difference between new rev and old rev

    # NO CHANGE
    #print(revision_diff_bool)
    if revision_diff_bool == 0:
        diffs = '{{EMPTYBABY}}' 

    # Edit is a revert, we don't do anything...
    elif revert == True:
        diffs = '{{EMPTYBABY}}' 

    # THERE WAS SOME CHANGE
    else:
        diffs = []

        current = tokenize_prep(current)
        prev = tokenize_prep(prev)

        # deltas 
        current_t = text_split.tokenize(current)
        prev_t = text_split.tokenize(prev)
        operations = segment_matcher.diff(prev_t,current_t)

        # structures to keep track of delta-changes
        op_names = []
        op_names_noequal = []
        op_changes = []
        op_changes_noequal = []

        # for each delta change in this revision
        for op in operations:
            # e.g. insert:  p="" c = "WPNPOV, WPNPOV" 
            c = "".join(current_t[op.b1:op.b2]).strip()
            p = "".join(prev_t[op.a1:op.a2]).strip()
            #no interest in empties, the equal [] --> [] case

            if p == "," and c == ",":
                continue

            # not empty but need to deal with commas while leaving internal commas in
            if len(c)>1:
                if c[0] ==",":
                    c = c[1:].strip()
                if c[-1]==",":
                    c = c[:-1].strip()
            if len(p)>1:
                if p[0] ==",":
                    p = p[1:].strip()
                if p[-1]==",":
                    p = p[:-1].strip()

            op_changes.append(c)
            op_names.append(op.name)
            if op.name != "equal":
                # if what gets appended is '', we know that a delete has occurred
                op_changes_noequal.append(c)
                op_names_noequal.append(op.name)
        
        #print("Number of delta operations: {}".format(len(op_names)))
        
        # now we are processing cases of diff going through the operations
        # there is just one insert OR delete somewhere
        if len(op_names_noequal) == 1 and op_names_noequal[0] == "insert":
            diffs.append(op_changes_noequal[0])

        elif len(op_names_noequal) == 1 and op_names_noequal[0] == "delete":
            diffs = diffs
        
        # there are just multiple inserts (no deletes)
        elif "delete" not in op_names_noequal and "insert" in op_names_noequal:
            for change in op_changes_noequal:
                diffs.append(change)
        
        # there are just a bunch of deletes (no inserts); continue on 
        elif "insert" not in op_names_noequal and "delete" in op_names_noequal:
            diffs = diffs

        # something more complicated is afoot: inserts AND deletes
        else:
            #comaparing the regexes in current and prev as collections
            intersection = collections.Counter(prev.split(", ")) & collections.Counter(current.split(", "))
            union = collections.Counter(prev.split(", ")) | collections.Counter(current.split(", "))
            opn_counts = collections.Counter(op_names_noequal)

            new_in_current = collections.Counter(current.split(", ")) - collections.Counter(prev.split(", "))
            
            # prev and current are completely different - this involves multiple deletes AND inserts
            # if 'equal' is not in op_names
            if "equal" not in op_names and (intersection == collections.Counter()):
                for new in current.split(", "):
                    diffs.append(new)

            # prev and current have the same contents, but different order; we assume page has been re-arranged and there is nothing to add
            # there is the possibility that the same stuff that got deleted gets added as a new thing, but this seems a little unlikely within one edit and we have to make a design choice here
            elif intersection == union:
                diffs = diffs 

            # there is some overlap in content of policy invocations of prev and current revisions
            # we must figure out the meaningful differences
            else:
                # op_names are the names of each delta op, in order e.g. ['insert','delete','equal']
                # op_names_noequal are only the inserts/deletes e.g. ['insert', 'delete']
                # op_changes are the CURRENT strings for the given segment delta, in order of op_names e.g. ['WPNPOV','','WikipediaRun, Wikipedia Run']
                # opn_counts tell us how many of each operation exist in the delta
                # new_in_current is a collection of the regexes that are new in current (not in prev)

                # one insert, one or multiple deletes
                # we only care about the inserts
                if opn_counts["insert"] == 1 and opn_counts["delete"] >= 1:
                    temp = [op_changes[i] for i in range(0,len(op_names)) if op_names == "insert"]
                    for t in  temp:
                        diffs.append(t)

                # multiple inserts, one or multiple delete
                # we only care about the inserts that didn't exist before
                # we need to make sure that the insert isn't simply something that existed before
                elif opn_counts["insert"] > 1 and opn_counts["delete"] >= 1:
                    #temp = [op_changes[i] for i in range(0,len(op_names)) if op_names == "insert"]
                    #for t in  temp:
                    #    diffs.append(t)

                    #print("new in current: {}".format(new_in_current))

                    temp = []
                    for item in new_in_current:
                        #print("item: {}".format(item))
                        #print(new_in_current[item])
                        for i in range(0,new_in_current[item]):
                            temp.append(item)
                    diffs = diffs + temp

                # cases that I can't think of; just add what exists in the new, but not the old
                else:
                    temp = []
                    for item in new_in_current:
                        for i in range(0,new_in_current[item]):
                            temp.append(item)
                    diffs = diffs + temp

    # make the diff list into a string
    # calculate the counts of the diff now

    if diffs == "{{EMPTYBABY}}":
        diff_string = ""
    else:
        diff_string = reverse_tokenize_prep(", ".join(diffs))

    #print("diff_string: {}".format(diffs))
    assert ("{{EMPTYBABY}}" not in diff_string), "EMPTYBABY placeholder detected!"
    return diff_string

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
    
    pd.options.display.max_rows = None

    dtypes={'articleid':np.int64, 'namespace':np.int16, 'anon':np.str, 'deleted':np.bool, 'revert':np.str, 'reverteds':np.str,'revid':np.int64, 'date_time':np.str, 'year':np.int16, 'month':np.int8, 'regexes':np.str, 'core_regexes':np.str,'regexes_prev':np.str, 'core_prev':np.str, 'regexes_diff_bool':np.int8, 'core_diff_bool':np.int8}
    parse_dates_in = ['date_time']

    # to chunk
    pd_dfs = pd.read_csv(file_path, sep="\t", header=0, dtype=dtypes, parse_dates=parse_dates_in,chunksize=5)#1000000)

    input("There are {} cores. To start data processing, press enter.".format(cpu_count()))
    print("Begin data processing of chunks, each chunk parallelized...")
    print("=========================================================================================")

    # initialize the dict for the data outputs that we want, so that we can build while chunking
    # MONTHLY - NAMESPACE
    mn_out_df = pd.DataFrame()
    mnstr_out_dict = {}

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
        # MONTHLY - NAMESPACE --> YYYY-MM_NAMESPACENUM: {"monthly_regex_revs":##,"monthly_regex_num":##, ...}
        # number of revisions (that have a diff change) that month --> "revid":['count']
        # total number of new regexes for the month --> "regexes_diff_count":['sum']
        mndf = processed_df.groupby(['year','month','namespace']).agg({"regexes_diff_count":['sum'],"revid":['count']})
        mndf.columns = ["monthly_regex_num","monthly_regex_revs"]
        mndf = mndf.reset_index()

        # number of revisions (that have diff change) that month by ANON
        adf = processed_df.loc[processed_df['anon'] == True]
        adf = adf.groupby(['year','month','namespace']).agg({"regexes_diff_count":['sum'],"revid":['count']})
        adf.columns = ["monthly_regex_num_anon","monthly_regex_revs_anon"]
        adf = adf.reset_index()
        #print(adf.head())

        # mndf + adf
        temp = pd.merge(mndf,adf, on=['year','month','namespace'],how='outer')
        temp = temp.fillna(value=0)
        temp['month'] = temp['month'].where(temp['month']>=10, '0'+temp['month'].astype(str))
        temp['YYYY-MM_NS'] = temp['year'].astype(str) + '-' + temp['month'].astype(str) + '_' + temp['namespace'].astype(str)
        temp.drop(['year','month','namespace'], axis=1, inplace=True)
        temp.set_index('YYYY-MM_NS',inplace=True)

        # cumulative diff string for the month --> agg lambda join
        strdf = processed_df.groupby(['year','month','namespace'])['regexes_diff'].agg(lambda col: ', '.join(col))
        strdf.columns = ["monthly_regex_str"]
        strdf = strdf.reset_index()
        strdf['month'] = strdf['month'].where(strdf['month']>=10, '0'+strdf['month'].astype(str))
        strdf['YYYY-MM_NS'] = strdf['year'].astype(str) + '-' + strdf['month'].astype(str) + '_' + strdf['namespace'].astype(str)
        strdf.drop(['year','month','namespace'], axis=1, inplace=True)
        strdf.set_index('YYYY-MM_NS',inplace=True)
        str_dict = strdf.to_dict('index')

        #print("*** mnstr_out_dict before adding this chunk:")
        #print(mnstr_out_dict)
        #print("***  what we're adding in this chunk:")
        #print(str_dict)

        # update mn_out_df (dataframe) and mnstr_out_dict
        # if we're at first chunk, the initialized output dict is just the temp we have
        if track == 1:
            mn_out_df = temp
            mnstr_out_dict = str_dict
        # otherwise we update by adding values of corresponding cells
        else:
            mn_out_df = mn_out_df.add(temp,fill_value=0)
            
            for key in str_dict:
                if key not in mnstr_out_dict:
                    mnstr_out_dict[key] = str_dict[key]
                else:
                    mnstr_out_dict[key]['regexes_diff'] = mnstr_out_dict[key]['regexes_diff'] + ", " + str_dict[key]['regexes_diff']

        #print("***  mn_out_df after adding this chunk:")
        #print(mnstr_out_dict)

        print("\n> Wrangling diff-processed df into updating output data took: %s seconds" % (time.time() - mid_time))
        print("=========================================================================================")
        track += 1

    print("Finished going through chunk!")

    # mn_out_df is a dataframe. we want to output.
    mn_out_df = mn_out_df.reset_index()
    print(mn_out_df.head())

    out_filepath = "{}/{}{}_monthlyCounts.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    mn_out_df.to_csv(out_filepath,sep='\t',header=True) 
    print("Find the output here: {}\nDone.\n".format(out_filepath))

    # mnstr_out_dict is a dict... make into df and output.
    str_out_df = pd.DataFrame.from_dict(mnstr_out_dict,orient='index')
    str_out_df = str_out_df.reset_index()
    print(str_out_df.head())

    out_filepath = "{}/{}{}_monthlyDiffStrings.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    str_out_df.to_csv(out_filepath,sep='\t',header=True) 
    print("Find the output here: {}\nDone.\n".format(out_filepath))
