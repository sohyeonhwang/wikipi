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
    parser.add_argument('-o', '--output-directory', help='Output directory', default='./tsvCrunchOutput', type=str)
    parser.add_argument('-ofn', '--output-filename', help='filename for the output file of tsv', default='testCrunch', type=str)
    args = parser.parse_args()
    return(args)

def findCoreColumns(onlyRegexCols):
    #CORE COLUMNS
    #ENGLISH:
    #SPANISH:	53_WIKIPEDIA:PUNTO_DE_VISTA_NEUTRAL, 69_WIKIPEDIA:WIKIPEDIA_NO_ES_UNA_FUENTE_PRIMARIA, 64_WIKIPEDIA:VERIFICABILIDAD
    #FRENCH: 	26_WIKIPÉDIA:NEUTRALITÉ_DE_POINT_DE_VUE, 19_WIKIPÉDIA:TRAVAUX_INÉDITS, 21_WIKIPÉDIA:VÉRIFIABILITÉ
    #GERMAN:
    #JAPANESE:	14_WIKIPEDIA:中立的な観点,16_WIKIPEDIA:独自研究は載せない, 15_WIKIPEDIA:検証可能性
    
    if "53_WIKIPEDIA:PUNTO_DE_VISTA_NEUTRAL" in onlyRegexCols:
        coreDFColumn = [c for c in onlyRegexCols if (c[:2]==str(53) or c[:2]==str(69) or c[:2]==str(64))]
    elif "26_WIKIPÉDIA:NEUTRALITÉ_DE_POINT_DE_VUE" in onlyRegexCols:
        coreDFColumn = [c for c in onlyRegexCols if (c[:2]==str(26) or c[:2]==str(19) or c[:2]==str(21))]
    else:
        coreDFColumn = [c for c in onlyRegexCols if (c[:2]==str(14) or c[:2]==str(15) or c[:2]==str(16))]

    return coreDFColumn

def tokenize_prep(regex_string):
    # we want to make Wikipedia:Droit de l'auteur --> Wikipedia_Droit_de_l'auteur
    regex_string = regex_string.replace(':','')
    regex_string_l = regex_string.split(', ')
    temp_l = []
    for s in regex_string_l:
        s = s.strip().replace(' ','_')
        temp_l.append(s)
    new_string = new_string = ', '.join(temp_l).strip()
    return new_string

def reverse_tokenize_prep(regex_string):
    # we want to make Wikipedia_Droit_de_l'auteur --> Wikipedia:Droit de l'auteur
    regex_string = regex_string.replace('WP','WP:')
    regex_string = regex_string.replace('Wikipedia','Wikipedia:')
    regex_string = regex_string.replace('Wikipédia','Wikipédia:')
    regex_string_l = regex_string.split(', ')
    temp_l = []
    for s in regex_string_l:
        s = s.strip().replace('_',' ')
        temp_l.append(s)
    new_string = new_string = ', '.join(temp_l).strip()
    return new_string

def compare_rev_regexes(current, prev, revision_diff_bool):
    diff_count = 0
    # we want to write a function that will find the difference between new rev and old rev

    # NO CHANGE
    if revision_diff_bool == 0:
        diffs = '{{EMPTYBABY}}' 
        diff_count = 0

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
        
        print("Number of delta operations: {}".format(len(op_names)))
        
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

                    temp = []
                    for item in new_in_current:
                        for x in range(0,item[1]):
                            temp.append(item[0])
                    diffs = diffs + temp

                # cases that I can't think of; just add what exists in the new, but not the old
                else:
                    temp = []
                    for item in new_in_current:
                        for x in range(0,item[1]):
                            temp.append(item[0])
                    diffs = diffs + temp

    # make the diff list into a string
    # calculate the counts of the diff now
    diff_string = reverse_tokenize_prep(", ".join(diffs))
    diff_count = len(diff_string.split(", "))
    return diff_string, diff_count

def diff_find(row):
    r_current = row['regexes'].replace('{{EMPTYBABY}}','')
    r_prev = row['regexes_prev'].replace('{{EMPTYBABY}}','')
    # revision has a difference in regex from last revision of article
    r_bool = row['regexes_diff_bool']

    c_current = row['core_regexes'].replace('{{EMPTYBABY}}','')
    c_prev = row['core_prev'].replace('{{EMPTYBABY}}','')
    c_bool = row['core_diff_bool']

    r_diff, r_diff_count = compare_rev_regexes(r_current,r_prev,r_bool)
    c_diff, c_diff_count = compare_rev_regexes(c_current,c_prev,c_bool)

    row['regexes_diff'] = r_diff
    row['core_diff'] = c_diff
    row['regexes_diff_count'] = r_diff_count
    row['core_diff_count'] = c_diff_count

def pd_apply_diff_find(df):
    df = df.apply(diff_find,axis=1)
    return df

def parallelize_dataframe(df, func, n_cores=30):
    '''
    Adapted from: https://towardsdatascience.com/make-your-own-super-pandas-using-multiproc-1c04f41944a1
    '''
    print("Using {} cores, applying {} to {}".format(n_cores, func, df))
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.imap(func, df_split))
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

    input_path = glob.glob("{}/*.csv".format(args.input))

    for i in input_path:
        print(i)

    file_path = input_path[0]
    print(file_path)

    pd.options.display.max_columns = None
    pd.options.display.max_rows = None

    pd_df = pd.read_csv(file_path, sep="\t", header=0, )
    print(pd_df.columns)

    print("\n")
    print(pd_df.head(10))

    cores = cpu_count()
    print("There are {} cores; should be 28".format(cores))

    pd_df.info(verbose=True)
    
    # TODO function should take in pd_df, do the apply(diff_find) and return the result
    #processed_df = parallelize_dataframe(pd_df, pd_apply_diff_find, n_cores=4)

    # TODO CHECK THE STATUS OF / Get rid of the {{EMPTYBABY}}
    #processed_df.head(30)
    # PYSPARK: master_regex_one_df = master_regex_one_df.na.fill('{{EMPTYBABY}}')

    # Now that we have, by-revision:
    # articleid, namespace, YYYY_MM, date_time, regexes, regexes_prev, core_regex, core_prev
    ## regexes_diff_bool, core_diff_bool 
        # keep track of # of revision; that have a new regex / 0 for no new regex, 1 for diff
        ## we can sum this for the # of revisions with difference in regex / total number of revisions
    ## regexes_diff, core_diff 
        # keep track of the actual additions per revision (string)
    ## regexes_diff_count, core_diff_count
        # count the number of new policy invocations from core/regexes_diff (per revision)

    #TODO Let's make the MONTHLY SMOOTH files ; groupBy YYYY_MM ...

    # the regexes_diff_bool / regexes_diff_count --> also in the output of tsvMasterMake in the monthly files
    # sum up the regexes_diff_bool --> num_revs_with_regex_diff, core_diff_bool --> num_revs_with_core_diff
    # sum up the regexes_diff_count, core_diff_count --> regexes_diff_count_monthly, core_diff_count_monthly
        # this is the number of new policy invocations in that month
    # f.count(*) --> num_revs --> this can be found in the tsvMasterMakeOutput (monthly_namespace, monthly)

    # TODO concatenate all of the strings of regexes_diff and core_diff that are not empty --> regexes_diff_monthly, core_diff_monthly
    # PYSPARK TODO concat_ws for regexes/core_diff_monthly CONDITIONAL --> WHEN NOT EMPTY

    # ["regexes_diff_bool","core_diff_bool","regexes_diff_count","core_diff_count"]

    #monthly_diff_df = processed_df.groupby(["year","month","namespace"]).agg({"regexes_diff_bool":['sum'], "core_diff_bool":['sum'], "regexes_diff_count":['sum'], "core_diff_count":['sum']})
    #monthly_diff_df.columns = ["num_revs_with_regex_diff","num_revs_with_core_diff","regexes_diff_count_monthly","core_diff_count_monthly"]
    #monthly_diff_df = monthly_diff_df.reset_index()


    # TODO CHECK THE STATUS OF / Get rid of the {{EMPTYBABY}}
    #regex.replace("{{EMPTYBABY ")

    out_filepath = "{}/{}{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    print("Find the output here: {}".format(out_filepath))

    #TODO OUTPUT FILE BUT PANDAS
    #processed_df.to_csv(out_file_path, sep='\t',header=True)
    
