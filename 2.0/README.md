This folder contains two main things:

* Code for processing wiki data dumps to get instances of policy invocation
* Exploratory examining these instances of policy invocation

# Data collection

For now, I'm looking at policy invocation in the five largest language editions. However, it would be good to extend this to the 10 largest language editions and then

1. Download the wiki data dumps.
2. Create the list of regular expressions we want to search for, from the list of rules and their relevant shorthands.
3. Generate the bash script task lists to process/slurm the data dumps with `wikiq`. Run in `hyak`.
4. The raw outputs can be/should be retained to do more nuanced analysis. Otherwise, we can use `pyspark` to process the data, e.g. crunching to get monthly counts of invocations over time.

## Download dumps

The dumps (en,es,fr,ja,de) are stored on `hyak`. To get more updated versions (or extend analysis to more language editions), you can download the complete revision histories by following instructions [here](https://meta.wikimedia.org/wiki/Data_dumps/Download_tools). The tldr is that on `hyak`, using `build_machine`, you run:

```
wget --recursive --no-parent --no-directories --continue --accept 7z [URL]
```

The short version is:

```
wget -r -np -nd -c -A 7z [URL]
```

* I only extract the .7z zipped files (not .bmz) because they are faster and the 7zip tool is readily handy.
* The `[URL]` is placeholder for the download link for the dump file. E.g., `https://dumps.wikimedia.org/enwiki/20220401/` will get you all the revision history files for the 2022 April 1 dump of English Wikipedia.

I recommend using a bash script to do this and output a language edition's dump per its own subfolder.

## Regular expression of shorthands

I've constructed a list of rules per language edition (in the `rules` folder that is a sibling directory to this one) for the top 5 language editions (see below). For this project, we'll focus on rules that are widely shared across language editions, or have many interlanguage links. We could just use the rule lists, or we could also just grab the rules linked across all these langauge editions.

As of June 2022, there are 316 active language editions. 30 of those have at least 1,000 active users (registered users who have made at least one edit in the last thirty days). 6 of those have at least 10,000 active users. The 10 most active language editions, per number of active users [[ref]](https://en.wikipedia.org/w/index.php?title=List_of_Wikipedias&oldid=1093143826), are:

1. English
2. French
3. German
4. Japanese
5. Spanish
6. Russian
7. Mandarin
8. Italian
9. Portuguese
10. Persian

See the `get_most_ill_rules.ipynb` for getting the rules with most ILLs across the top 5 and 10 language editions.

Once we have that list of rules, we create the regular expressions to detect invocations of them. We use a Perl script for that. See the `regex` subfolder.

Now we can process with [wikiq](https://wiki.communitydata.science/Wikiq).

# Processing dumps on hyak with wikiq

We need (1) the dumps and (2) the regular expressions. We can find the dumps in on `hyak` under `~/wikipi/raw_data/dumps` (there should be a subfolder per language edition) and the regular expressions in this repo `~/2.0/regex/output`.

## Generate the tasklists

For each language edition, we want to process a dump exactly one time and look for all the relevant regular expressions. Each line in the task list should look like this:

```
python3 ./mediawiki_dump_tools/wikiq [input] -o ./output  -RPl [regex_label_here] -RP '[regex_goes_here]'
```

Note that on hyak I'll running `wikiq` on the parent wikipi directory, NOT the repo (`~/wikipi`). So `make_taskslists.py` will output to `tasks`

Early test runs with the `giant_regex' indicate that the construction of it is a little buggy, but the size of it makes it very inscrutable to identify the problem. As a result, instead, for searching for all policies with one regular expression, I create "wide" taskslists, basically the basic construction with regex-label pairs for every single rule of that language edition.

This avoids the issue that comes with giant_regex's bugginess, but the downside is that this take a very long time to run the jobs (so far). It should help this time that we are doing a limited search of rules (just the most ILL'd ones).

## Running wikiq

We make a modification to the local version of `wikiq` so that the regex will ignore case:

```
self.pattern = re.compile(pattern,re.IGNORECASE)
```


# Analysis of policy invocation

We analyze policy invocation from: (1) processed data using spark that summarizes policy invocation use, and (2) the raw data of the instances.
