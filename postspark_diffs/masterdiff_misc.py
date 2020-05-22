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

def diff_find(row):
    r_current = row['regexes']
    r_prev = row['regexes_prev']
    # revision has a difference in regex from last revision of article
    r_bool = row['regexes_diff_bool']
    revert = row['revert']

    #c_current = row['core_regexes']
    #c_prev = row['core_prev']
    #c_bool = row['core_diff_bool']
    
    r_diff = compare_rev_regexes(r_current,r_prev,r_bool,revert)
    #c_diff, c_diff_count = compare_rev_regexes(c_current,c_prev,c_bool,revert)

    #row['regexes_diff'] = r_diff
    #row['core_diff'] = c_diff
    #row['regexes_diff_count'] = r_diff_count
    #row['core_diff_count'] = c_diff_count
    return r_diff




    # not chunking
    #pd_df = pd.read_csv(file_path, sep="\t", header=0, dtype=dtypes, parse_dates=parse_dates_in)
    #memory = pd_df.memory_usage().sum()
    #print('Total Current memory is-', memory,'Bytes.')
    #print(pd_df.memory_usage())




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
    # the regexes_diff_bool and regexes_diff_count --> also in the output of tsvMasterMake in the monthly files
    # sum up the regexes_diff_bool --> num_revs_with_regex_diff, core_diff_bool --> num_revs_with_core_diff
    # sum up the regexes_diff_count, core_diff_count --> regexes_diff_count_monthly, core_diff_count_monthly
        # this is the number of new policy invocations in that month
    # f.count(*) --> num_revs --> this can be found in the tsvMasterMakeOutput (monthly_namespace, monthly)

    # TODO concatenate all of the strings of regexes_diff and core_diff that are not empty --> regexes_diff_monthly, core_diff_monthly
    # PYSPARK TODO concat_ws for regexes/core_diff_monthly CONDITIONAL --> WHEN NOT EMPTY

