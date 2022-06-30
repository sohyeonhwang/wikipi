# How to run wikiq to process Wikipedia dumps

Let's say you want to run `wikiq` on a bunch of Wikipedia data dump files, perhaps for multiple language editions. Depending on how you are running it, this could be quite time-consuming, so you want to parallelize the processing of dumps as much as possible. 

This assumes that you are using Hyak, and comes from Sohyeon's experience learning to run a big job on mox (klone may have slightly different configurations!). In particular, the main task at hand is to run many calls of `wikiq` on dumps from several language editions, to detect certain regular expressions using the revision pattern arguments of `wikiq`.

## Software

First, get `wikiq`. Instructions are [here](https://wiki.communitydata.science/Wikiq).

## Testing to ensure you will submit good jobs

You should test to make sure that the wikiq call you plan to make actually works and things are set up in the right way. That is, the reason why we usually want to submit jobs with a slurm sbatch script file is because we want to call wikiq many times, once for each dump file, as efficiently as possible. Sometimes, this is for multiple language editions but fundamentally, we are doing the same thing with wikiq to many dump files.

I recommend having a directory set up like so:
```
project_dir
--- /mediawiki_dump_tools # this contains wikiq as ~/mediawiki_dump_tools/wikiq
--- /raw_data #organize each language edition as a subfolder, like so
------ /enwiki # these contain the dump files
------ /frwiki
------ /jawiki
--- /test_data # structure it the same as the actual data you will process, but maybe just one or two dump files per language edition
------ /enwiki 
------ /frwiki
------ /jawiki
--- /output # this is where your wikiq outputs will go
--- /output_test # outputs just for testing
------ /test1 
------ /test2 # ... and so on
```

There will be some other important files and scripts, but we will cover that in a bit. First, to testing.

### Test 1: One basic call
First, check out an interactive node. "checking out" a node means you're saying you want to work within it/use it. You do **not** want to run `wikiq` from the login node, which is the node you start at when you first ssh and log into Hyak. DON'T DO IT. To go to an interactive node, run the commands:

```
tmux new -s wikiq # you want to do this on a tmux session
int_machine
```

Now, you will see that the terminal shows that you are in a node in front of your working directory path (e.g. from `sohw@mox2:` to `sohw@n2347:`). Yay, we're in the node! Now try running just *one* call of `wikiq` with the parameters you desire from the project directory (in the file organization above, that's `project_dir`). For example, let's say I want to process a file in the `enwiki` subdirectory in `test_data` to find all instances of some regular expression. I would do the following (the square brackets are placeholders for some arbitrary task):

```
cd ~/project_dir

```

or if I want to also test regular expression detection:
```
python3 ./mediawiki_dump_tools/wikiq -u -o ./output_test/test1 ./test_data/[FILE_NAME].7z -RP [REVISION_PATTERN_REGEX] -RPL [LABEL_FOR_REVISION_PATTERN_REGEX]
```

Now go to the appropriate output directory (here, that is `output_test`). You should see a .tsv file with a matching name with the input file. Check it out and see if it looks right (assuming that running the wikiq call went through!)

Exit the interactive node with `Ctrl+D`.

**Note:** For testing, I usually first make a small copy of a dump file and edit the content so that I know for sure if `wikiq` will properly process the cases I am thinking of. For example, if I want to detect all instances of the word "tubby", I insert "tubby" a few times to a pared down version of a dump file and run my first test with that modified file. That way, when I check my output it isn't a blind check. I might do this for multiple files so I can test a small version of my batch job as well.

### Test 2: A few calls across language editions
Assuming that we got the single run going OK, we now know that the way we've configured our wikiq call is all good. Now we want to test running this multiple times (e.g. one or two files per language edition) via a series of handy scripts. This is basically a much smaller version of the actual batch job we want to submit.

There are three main things you'll want:
```
wikiq # which we have, as ~/mediawiki_dump_tools/wikiq
run_wikiq.sh # a bash script to run the baseline wikiq call
test_task_list.sh # a list of run_wikiq.sh calls
```

`run_wikiq.sh` makes a call to `wikiq` with the parameters that are consistent across all calls. It literally just has one line, which you will note looks very similar to the line in Test 1:

```
python3 ./mediawiki_dump_tools/wikiq -u -o ./output_test/test2
```

* `-u` is a recommended parameter. See documentation
* `-o ./output_test` specifies where the output should go
* You can make `run_wikiq.sh` executable by running: `chmod +x run_wikiq.sh`

`test_task_list.sh` is a list of calls to `run_wikiq.sh`, with the appropriate input file and any other parameters that need to change (e.g. if I have distinct regular expressions to run for English vs. Japanese Wikipedia, that's where this would be in). For this test, I have just four lines in it that all follow this structure: `./run_wikiq.sh [INPUT FILE PATH] -RP [REGEX] -RPL [REGEX_LABEL]`, to test some files from 3 different language editions (2 files from English).

To run our test let's do the following:
```
tmux new -s wikiq #or attach to your existing tmux session
int_machine
```

First let's try running just one of the calls we would put in `./test_task_list.sh` to make sure that it runs right:
```
./run_wikiq.sh ./test_data/enwiki/enwiki-20220401-pages-meta-history27.xml-p63975910p64276293.7z -RP [REGEX] -RPL [REGEX_LABEL]
```

Now let's just run `./test_task_list.sh` (make sure it is executable):
```
./test_task_list.sh
```

Check the outputs and confirm things got processed right. Basically, Test 2 ensures that the tasks in your task list is OK. Exit the interactive node with `Ctrl+D`.

### Test 3: Testing a small batch job

You can now archive `test_task_list.sh` somewhere else. Now that we know that the wikiq calls should work, we'll test a very small version of the batch job we want to submit. Baby parallelizing, in a sense. Here are the main files/directories to be concerned about:

```
wikiq # same as before
run_wikiq.sh # same as before, assuming you are doing the same type of call
task_list.sh
run_jobs.sbatch
```

`task_list.sh` is again a series of `run_wikiq.sh` calls, you don't need the `#!/bin/bash` at the first line, though.

`run_jobs.sbatch` is the slurm sbatch script to submit a job to Hyak and tell it to allocate the jobs efficiently. Note that jobs must be submitted via the login nodes, so we won't need to do int_machine and check out a node first. You can find a simple sample job script on the Hyak documentation [here](https://wiki.cac.washington.edu/display/hyakusers/Hyak+mox+Overview). 

We set up this script so that we can parallelize tasks via [job arrays](https://slurm.schedmd.com/job_array.html). A way you can use job arrays is to make a task list, with one command on each line (which we did) and then run (once you set up `run_jobs.sbatch` of course, see below):

```
tmux new -s wikiq
sbatch --wait --array=1-$(shell cat task_list | wc -l) run_jobs.sbatch 0
```

Now we just wait for it to process and check the outputs when it's done. The script sample below is set up so that you'll get an email notification when it is ready.

`run_jobs.sbatch` looks something like this:
```
#!/bin/bash
## wikipi dump parse
#SBATCH --job-name="cdsc_sohw_wikipi; parse wikipedia dumps"
## Allocation Definition
#SBATCH --account=comdata-ckpt
#SBATCH --partition=ckpt
## Resources
## Nodes. This should always be 1 for parallel-sql.
#SBATCH --nodes=1
## Number of cores per node
#SBATCH --ntasks-per-node=28
## Walltime (12 hours)
#SBATCH --time=24:00:00
## Memory per node
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=28 # run multiple tasks on a node at once
#SBATCH --chdir /gscratch/comdata/users/sohw/wikipi
#SBATCH --output=jobs/%A_%a.out
#SBATCH --error=jobs/%A_%a.out
##turn on e-mail notification
#SBATCH --mail-type=ALL
#SBATCH --mail-user=sohyeonhwang@u.northwestern.edu

source ./bin/activate
TASK_NUM=$(( SLURM_ARRAY_TASK_ID + $1))
TASK_CALL=$(sed -n ${TASK_NUM}p ./task_list.sh)
${TASK_CALL}
```

The purpose of Test 3 is to make sure your batch script works properly, so adjust as needed.

**Note from Nate:** "Wikiq runs single-threaded. So your sbatch script should be configured to run jobs that use just 1 core on the nodes in our allocation. The nice thing about slurm compared to torque is that you can run muliple jobs on a single node by assigning them to use just 1/28th of a node (on mox; 1/40th of a node on klone). Using a job array will give you good parallelism by starting a new job when each job finishes. The annoying limitation with job arrays is that they limit the number of jobs in array. I hit the limit running wikiq on wikia; but I don't think you'll hit that limit with the Wikipedia dumps. The limit on klone was 5000, but they changed it because of the storage issues, and I don't know what the limit on mox is."

## Set up and submit your batch job for running

You can now archive your test scripts to some subdirectory if you want to save them as reference, or just delete them. We'll be making our actual task list and scripts now. Like before, the main things we need to care about for the batch job are:
```
wikiq # same as before
run_wikiq.sh # TODO! make sure the output is now output and not output_test
task_list.sh # TODO! make sure the input is now the actual data, and we are excluding the ones we completed in testing
run_jobs.sbatch # same as before
```

The most important thing is to create the new and actual task list we want to run. This uses the data in the `raw_data` directory, not the `test_data` directory we've been on fiddling around with.

You can construct the task list however you want. I like to use a simple Python script to get it all together:

```
file = "task_list.sh"
pattern_labels_regexes = dict()
all_regex_parameters = ""

processed_in_testing = []

with open(file, 'w') as f:
    for lang in raw_data:
        for dumpfile in lang:
            if dumpfile not in processed_in_testing:
                task = "./run_wikiq.sh {} {}".format(dumpfile,all_regex_parameters)
                f.write("{}\n".format(task))
```

Now go to your tmux session and submit the job:
```
sbatch --wait --array=1-$(shell cat task_list | wc -l) run_jobs.sbatch 0
```

### Checking progress and outputs
You can check progress of your job by using `htop`.

When the job is done, you should get an email.

## Next steps
Now that you have a bunch of processed files, you can use Spark to do more processing (e.g. create summary stats, get rid of rows that you don't need).

