# How to run wikiq to process Wikipedia dumps on Hyak

Let's say you want to run `wikiq` on a bunch of Wikipedia data dump files, perhaps for multiple language editions. Depending on how you are running it, this could be quite time-consuming, so you want to parallelize the processing of dumps as much as possible. 

This assumes that you are using Hyak, and comes from Sohyeon's experience learning to run a big job on mox (klone may have slightly different configurations, which I try to note below). In particular, the main task at hand is to run many calls of `wikiq` on dumps from several language editions, processing the raw dumps and detecting certain regular expressions in dump files using the revision pattern arguments of `wikiq`.

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
--- /output # this is where your wikiq outputs will go. you can create a separate output_test folder if you like.
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
# activate any venvs you have
cd ~/project_dir
python3 ./mediawiki_dump_tools/wikiq -u -o ./output/test1 ./raw_data/[FILE_NAME].7z
```

or if I want to also test regular expression detection:
```
python3 ./mediawiki_dump_tools/wikiq -u -o ./output/test1 ./raw_data/[FILE_NAME].7z -RP [REVISION_PATTERN_REGEX] -RPL [LABEL_FOR_REVISION_PATTERN_REGEX]
```

Now go to the appropriate output directory (here, that is `output`). You should see a .tsv file with a matching name with the input file. Check it out and see if it looks right (assuming that running the wikiq call went through!)

Exit the interactive node with `Ctrl+D`.

**Note:** For very early testing, I usually first make a small copy of a dump file and edit the content so that I know for sure if `wikiq` will properly process the cases I am thinking of. For example, if I want to detect all instances of the word "tubby", I insert "tubby" a few times to a pared down version of a dump file and run my first test with that modified file. That way, when I check my output it isn't a blind check. I might do this for multiple files so I can test a small version of my batch job as well.

### Test 2: A few calls with a bash script
Assuming that we got the single run going OK, we now know that the way we've configured our wikiq call is all good. Now we want to test running this multiple times (e.g. if you have multiple language editions, perhaps one or two files per language edition) via a series of handy scripts. This is basically a much smaller version of the actual batch job we want to submit.

There are three main things you'll want:
```
wikiq # which we have, as ~/mediawiki_dump_tools/wikiq
test_task_list.sh # a short list of run_wikiq.sh calls
```

`test_task_list.sh` is a list of calls to `wikiq`, with the appropriate input file and any other parameters that need to change (e.g. if I have distinct regular expressions to run for English vs. Japanese Wikipedia, that's where this would be in). For this test, I have just four lines in it that all follow this structure: `python3 ./mediawiki_dump_tools/wikiq -u -o ./output/test2 [INPUT FILE PATH] -RP [REGEX] -RPL [REGEX_LABEL]` to test the code in sequence. You will note that this is very similar to what we ran in Test 1.

* `-u` is a recommended parameter. See `wikiq` documentation.
* `-o ./output/test2` specifies where the output should go.

To run our test let's do the following:
```
tmux new -s wikiq #or attach to your existing tmux session
int_machine
```

Now let's just run `./test_task_list.sh` (make sure it is executable with `chmod u+x test_task_list.sh`):
```
./test_task_list.sh
```

Check the outputs and confirm things got processed right. Basically, Test 2 ensures that the running a list of wikiq calls with a bash script works. Exit the interactive node with `Ctrl+D`.

Here is an example of what test_task_list.sh might look like:
```
#!/bin/bash
python3 ./mediawiki_dump_tools/wikiq -u -o ./output/test1 ./raw_data/[FILE_NAME].7z
python3 ./mediawiki_dump_tools/wikiq -u -o ./output/test1 ./raw_data/[FILE_NAME].7z
python3 ./mediawiki_dump_tools/wikiq -u -o ./output/test1 ./raw_data/[FILE_NAME].7z
python3 ./mediawiki_dump_tools/wikiq -u -o ./output/test1 ./raw_data/[FILE_NAME].7z 
echo 'Done Test 2'
```

### Test 3: Testing a small batch job

Now that we know that the wikiq calls should work, we'll test a very small version of the batch job we want to submit. Baby parallelizing, in a sense. Here are the main files/directories to be concerned about:

```
wikiq # same as before
task_list.sh
run_jobs.sh
```

`run_jobs.sh` is the slurm sbatch script to submit a job to Hyak and tell it to allocate the jobs efficiently. Note that jobs must be submitted via the login nodes, so we won't need to do int_machine and check out a node first. You can find a simple sample job script on the Hyak documentation [here](https://wiki.cac.washington.edu/display/hyakusers/Hyak+mox+Overview). 

We set up this script so that we can parallelize tasks via [job arrays](https://slurm.schedmd.com/job_array.html). A way you can use job arrays is to make a task list (`task_list.sh`), with one command on each line (which we did) and then run (once you set up `run_jobs.sh` of course, see below).

Here, `task_list.sh` is again essentially a series of `run_wikiq.sh` calls, you don't need the `#!/bin/bash` at the first line, though. However, if you have a complicated regular expression, that might mess up the way slurm reads your lines - so, there are some slight changes to the task list and the sbatch script. You can see examples below. Once you have the task list and sbatch scripts ready, you can submit the job like so:

```
tmux new -s wikiq
sbatch --wait --array=1-$(cat task_list.sh | wc -l) run_jobs.sh 0
```

Now we just wait for it to process and check the outputs when it's done. The script sample below is set up so that you'll get an email notification when it is ready. The purpose of Test 3 is to make sure your batch job submission script works properly, so adjust as needed.

**Note from Nate 2022/06:** "Wikiq runs single-threaded. So your sbatch script should be configured to run jobs that use just 1 core on the nodes in our allocation. The nice thing about slurm compared to torque is that you can run multiple jobs on a single node by assigning them to use just 1/28th of a node (on mox; 1/40th of a node on klone). Using a job array will give you good parallelism by starting a new job when each job finishes. The annoying limitation with job arrays is that they limit the number of jobs in array. I hit the limit running wikiq on wikia; but I don't think you'll hit that limit with the Wikipedia dumps. The limit on klone was 5000, but they changed it because of the storage issues, and I don't know what the limit on mox is."

**Note from Sohyeon 2022/07:** Unfortunately, it looks like running with a job array on mox will *not* allow you to run multiple jobs on a single node; however, it will effectively allow you to run jobs across any available nodes (we have 3 on mox).

#### Example script 1: on `mox`, with a complicated regular expression
AKA what Sohyeon ran.

`task_list.sh` looks something like this: 
```
-u -o /gscratch/comdata/raw_data/sohw_wikiq_outputs_202207 /gscratch/comdata/users/sohw/wikipi/raw_data/enwiki/enwiki-20220401-pages-meta-history27.xml-p67057339p67417312.7z -RP "VERY LONG REGULAR EXPRESSION" -RPl "REGEX_WIDE"
-u -o /gscratch/comdata/raw_data/sohw_wikiq_outputs_202207 /gscratch/comdata/users/sohw/wikipi/raw_data/enwiki/enwiki-20220401-pages-meta-history16.xml-p19104156p19210159.7z -RP "VERY LONG REGULAR EXPRESSION" -RPl "REGEX_WIDE"
...
```

`run_jobs.sh` to run looks something like this:
```
#!/bin/bash
## wikipi dump parse
#SBATCH --job-name="YOUR JOB NAME GOES HERE"
## Allocation Definition
#SBATCH --account=comdata
#SBATCH --partition=comdata
## Resources
## Nodes. This should always be 1 for parallel-sql.
#SBATCH --nodes=1
## Walltime (12 hours)
#SBATCH --time=24:00:00
## Memory per node
#SBATCH --mem=6G
#SBATCH --cpus-per-task=1
#SBATCH --ntasks-per-node=28
#SBATCH
#SBATCH --chdir /gscratch/comdata/users/sohw/wikipi
#SBATCH --output=jobs/%A_%a.out
#SBATCH --error=jobs/%A_%a.out
##turn on e-mail notification
#SBATCH --mail-type=ALL
#SBATCH --mail-user=sohyeonhwang@u.northwestern.edu

TASK_NUM=$(( SLURM_ARRAY_TASK_ID + $1))
TASK_CALL=($(sed -n ${TASK_NUM}p ./tasklist_en.sh))
echo "python3 ./mediawiki_dump_tools/wikiq ${TASK_CALL[@]}"
python3 ./mediawiki_dump_tools/wikiq "${TASK_CALL[@]}
```

The command to run is:
```
sbatch --wait --array=1-$(cat task_list.sh | wc -l) run_jobs.sh 0
```

#### Example script 2: on `klone`
The `submissions_task_list.sh` looks something like this:
```
#TODO INSERT EXAMPLE CALLS THAT WORKS FOR THE BELOW SCRIPT SHARED BY NATE
```

The `run_jobs.sbatch` to run looks something like this:
```
#!/bin/bash
## tf reddit comments
#SBATCH --job-name="cdsc_reddit; parse submission dumps"
## Allocation Definition
#SBATCH --account=comdata-ckpt
#SBATCH --partition=ckpt
## Resources
## Nodes. This should always be 1 for parallel-sql.
#SBATCH --nodes=1    
## Walltime (12 hours)
#SBATCH --time=24:00:00
## Memory per node
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1
#SBATCH --ntasks=1
#SBATCH 
#SBATCH --chdir /gscratch/comdata/users/nathante/cdsc_reddit/datasets
#SBATCH --output=submissions_jobs/%A_%a.out
#SBATCH --error=submissions_jobs/%A_%a.out

TASK_NUM=$(( SLURM_ARRAY_TASK_ID + $1))
TASK_CALL=$(sed -n ${TASK_NUM}p ./submissions_task_list.sh)
${TASK_CALL}
```

The command to run:
```
sbatch --wait --array=1-$(shell cat task_list.sh | wc -l) run_jobs.sbatch 0
```

## Set up and submit your batch job for running

We are now ready to run the job. If you have Test 3 working, this is a VERY fast/simple transition. Like before, the main things we need to care about for the batch job are:
```
wikiq # same as before
task_list.sh # your full task list
run_jobs.sbatch # same as before
```

The most important thing is to create the new and actual task list we want to run.

You can construct the task list however you want. I like to use a simple Python script to get it all together.

Now go to your tmux session and submit the job with the appropriate `sbatch` command.

### Checking progress and outputs
You can check progress of your job by using `squeue -p comdata`.

If there is an error, you will get an email pretty fast.

When the job is done, you should get an email that says the job completed with a good exit code.

## Next steps
Now that you have a bunch of processed files, you can use Spark to do more processing (e.g. create summary stats, get rid of rows that you don't need). You could also use DuckDB instead. Many options are out there.