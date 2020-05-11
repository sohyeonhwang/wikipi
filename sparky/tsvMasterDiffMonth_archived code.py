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