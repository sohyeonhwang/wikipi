'''
    #just glob it (different from just one: df_1 = df_regex_make(files_l[0]))

    # Compared just one file and regex make of directory
    #print('Checking that the df from path/* is indeed different from one file input...')
    #print("glob/*:{}\none_file:{}".format(master_regex_one_df.count(),df_1.count()))
    #print(master_regex_one_df.describe().show())
    #print(df_1.describe().show())


    #print("Columns of the processed dataframe:\n")
    #for c in master_regex_one_df.columns:
    #    print("\t{}".format(c))    


    ## regexes_diff core_diff keep track of the actual additions (string)
    # current = regexes, prev = regexes_prev
    #diff_regex, diff_regex_count = diff_find(master_regex_one_df.regexes,master_regex_one_df.regexes_prev)
    #diff_core, diff_core_count = diff_find(master_regex_one_df.core_regexes,master_regex_one_df.core_prev)

    #master_regex_one_df = master_regex_one_df.withColumn('regexes_diff',f.when(f.isnull(diff_regex), '{{EMPTYBABY}}').otherwise(diff_regex))
    #master_regex_one_df = master_regex_one_df.withColumn('core_diff', f.when(f.isnull(diff_core), '{{EMPTYBABY}}').otherwise(diff_core))

    ## diff_regex_count diff_core_count counts the number of new policy invocations from core/regexes_diff
    #master_regex_one_df = master_regex_one_df.withColumn('regexes_diff_count')
    #master_regex_one_df = master_regex_one_df.withColumn('core_diff_count')

    #temp_df = master_regex_one_df.select(master_regex_one_df.revid, master_regex_one_df.regexes,master_regex_one_df.regexes_prev,master_regex_one_df.core_regexes,master_regex_one_df.core_prev)
    #temp_df.withColumn('regexes_diff', lit('{{EMPTYBABY}}')).cast(types.StringType())
    #temp_df.withColumn('core_diff', lit('{{EMPTYBABY}}')).cast(types.StringType())
    #temp_df.withColumn('regexes_diff_count', lit(0)).cast(types.LongType())
    #temp_df.withColumn('core_diff_count', lit(0)).cast(types.LongType())
    #temp_df.forEach(diff_find)

    #master_regex_one_df = master_regex_one_df.join(temp_df, on=['revid'], how='outer')



    master_regex_one_df.write.partitionBy("articleid","YYYY_MM").mode("overwrite").csv(out_filepath,sep='\t',header=True)

    # We now read from the partitioned data articleid, YYYY_MM
    partitioned_df = spark.read.option("basePath","{}/".format(out_filepath)).csv("{}/articleid=*/YYYY_MM=*".format(out_filepath))
    print(partitioned_df.rdd.getNumPartitions())
    
    out_filepath = "{}/{}{}.tsv".format(args.output_directory,args.output_filename,datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S"))
    print("Find the output here: {}".format(out_filepath))
'''

        order = []
        #temp = {}
        #op.name equal, delete, insert
        for op in operations:
            order = order.append(op.name)
            # we ignore the starting equals
            if op.name=='equal' and 'delete' not in order and 'insert' not in order:
                continue

            #TODO
            elif op.name=='equal' and ('delete' in order or 'insert' in order):
                continue

            # now that we're past the equal parts, what is different?
            # what was in prev, what is now in current
            if op.name == 'insert':
                # we care about repr(''.join(current_t[op.b1:op.b2]))
                inserted = ''.join(current_t[op.b1:op.b2]).replace("  ", " ").strip()

                # if there's a comma, it means that more than one got inserted
                if ',' in inserted:
                    tmp = inserted.split(',')
                    for t in tmp:
                        diff.append(t)
                    inserted = inserted.replace(',',' ')

                else:
                    diff.append(inserted)

            if op.name == 'delete':
                deleted = ''.join(prev_t[op.a1:op.a2])
                diff_sub.append(deleted)







            # c and p are same length - things were both deleted and added 
            # e.g. 10 20 30 --> 10 40 50; or 10 20 30 --> 10 30 20
            if c_count == p_count:
                # we have the same number of policies being invoked in general
                #if collections.Counter(current) == collections.Counter(prev):
            
            # c > p ... so like more was added than deleted
            # e.g. 10 20 30 40 50 --> 10 30 50 60 70 (10e,20d,30e,40d,50e,60 70a)
            # e.g. 10 20 30 40 50 --> 70 80 90 10 20 30 40 (70 80 90a, 10 20 30 40e, 50d)
            elif c_count > p_count:
                break
            
            # c < p
            # e.g. 10 20 30 40 50 --> 30 40 50 (10 20d, 30 40 50e, 60 70 80a)
            elif p_count < c_count:
                break




    #rp_df = rp_df.groupBy("YYYY_MM","namespace").count().alias("num_revs").sum("regexes_diff_bool").alias("num_revs_with_regex_diff").sum("core_diff_bool").alias("num_revs_with_core_diff").sum("regexes_diff_count").alias("regexes_diff_count_monthly").sum("core_diff_count").alias("core_diff_count_monthly")
    #rp_df = rp_df.groupBy("YYYY_MM","namespace").agg(f.count("*").alias("num_revs"),f.sum("regexes_diff_bool","core_diff_bool","regexes_diff_count","core_diff_count").alias("num_revs_with_regex_diff","num_revs_with_core_diff","regexes_diff_count_monthly","core_diff_count_monthly"))


    '''
    # now have articleid, namespace, YYYY_MM, date_time, regexes, prev_rev_regex, diff
    # 2.2 - monthly smoosh, monthly_df
        # want in monthly_df:
        # YYYY_MM, namespace, regexes_start, regexes_end, not_count_diff(next_month_start-month_start)
        # core_regexes_start, core_regexes_end, not_count_diff(next_month_start - month_start)
        # count(revisions), count(revs_with_diff), count(revs_with_core_diff)
    # forEachPartition:
        # A. cumulatively cat diff, core_diff (# of policy invocations may be different from # rev with core_diff)
        # B. end - start
    #

    # 2.2 TODO ALTERNATIVE BY MONTH DIFF
    # from the partitioned data
    # get first and last of each month for each article
    # so each articleid will have two rows for every month, that means 24/year, which means ~410 per article...

    # new_df --> articleID, YYYY_MM, namespace, regex_start, regex_end, core_start, core_end
    # one row per articleID + YYYY_MM combo
    # in new_df: calculate the diff for each articleid, month (new column diff)
    # diff is a COUNT. for YYYYMM would be YYYYMM+1(regex_start) - YYYYMM(regex_start)
    # first month starts at 0; last month is regex_end - regex_start
    # core_diff follows same logic, just for the core
    print("\nGenerate new column of diff, core_diff")

    # smoosh into year/month (no more articleid)
    # and then we add up all the diffs (groupBy -->YYYY_MM, namespace. so YYYY_MM, diff)
    # so a df that is: one row per YYYY_MM + namespace combo.
    # smooth into just YYYY_MM, diff info (ignore namespace) in R




    # 3 TODO F1. needs the by-rev
    # YYYY_MM, namespace, regex_diff, core_diff, refex_diff_count, core_diff_count
    # from the partitionBy(articleid, YYYY_MM) situation, we want to get:
    # the count of revisions
    # the count of revisions with policy invocation (there is a diff adding), or core policy invocation
    # per month 
    # active editor data is elsewhere.

    # MONTHLY BASIC DATA. of revisions with policy invoked, how many had core policy invocations by-month and cumul
    # count revisions with core policy invocation
    # count revisions

    # 4 TODO F4. YYYY_MM, namespace, regex_diff (not count)
    # we want to have the new policy invocations in a given month, so export that or use the exported file from F1
    # going to have to write a separate script that goes through the regex_diff of a month and checks ILL status


    print("We have now built the columns with the diffs (current, prev).")
    print("--- %s seconds ---" % (time.time() - start_time))


    '''