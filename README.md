# Policy Invocation on Wikipedia
*** Please note that this repo is deprecated. It probably doesn't work anymore. ***

This repo contains the code/information and files used to process/parse wiki dump files and extract policy invocations on hyak. Everything should be accessible and executable via hyak (running the the task list _must_ be done on hyak), though code development/writing was primarily done on my local machine, tested, and then pushed. 

## Contents
* [Raw data information (wmf20190901)](https://github.com/sohyeonhwang/wikipi_repo#raw-data)
* [shortcuts_get to get the list of policies and their shortcuts](https://github.com/sohyeonhwang/wikipi_repo#shortcuts_get-lists-of-policies-guidelines-5-pillars-and-their-shortcuts)
  * shortcuts_get - how to run and notes on input/outputs
  * [plus wp:title](https://github.com/sohyeonhwang/wikipi_repo#shortcuts-plus-wptitle)
* [regex to construct regexes from list of shortcuts](https://github.com/sohyeonhwang/wikipi_repo#regex-generating-regular-expressions-from-list-of-shortcuts)
* [batch_tasklists to create tasklists parsing dump files on hyak](https://github.com/sohyeonhwang/wikipi_repo#batch_tasklists-task-list-construction-for-running-wikiq-on-hyak)
* [running wikiq on hyak to get the tabular policy invocation data](https://github.com/sohyeonhwang/wikipi_repo#running-wikiq-on-hyak)
* [other data](https://github.com/sohyeonhwang/wikipi_repo#other-data-for-generating-figures)
  * active editor counts over time (from wikistats)
  * #TODO interlanguage links
* [#TODO](https://github.com/sohyeonhwang/wikipi_repo#TODO)...
  * interlanguage-link data

-----------------------------------
This README document assumes that the working environemnt and file directory set-up is what I have (in hyak):
- batch_jobs/wikipi
	- input 
	- output
	- task_list --> linked to output from wikipi_repo/batch_tasklists/output
- wikipi
	- wikipi_repo
		- ...
		- batch_tasklists 
			- output
-----------------------------------
## Raw data
As the complete WMF dumps on the CDSC group repo didn't have pages-meta-history files for every language, I manually got the dump files from Wikimedia. Info about how to download [here](https://meta.wikimedia.org/wiki/Data_dumps/Download_tools).

The 20190901 WMF dump has complete edit histories for all pages of the language editions I'm looking at (en,es,fr,ja,de):
* https://dumps.wikimedia.your.org/enwiki/20190901/
* https://dumps.wikimedia.your.org/frwiki/20190901/
* https://dumps.wikimedia.your.org/dewiki/20190901/
* https://dumps.wikimedia.your.org/eswiki/20190901/
* https://dumps.wikimedia.your.org/jawiki/20190901/


On hyak, using build_machine, run: <br />
  <code>wget --recursive --no-parent --no-directories --continue --accept 7z [URL]</code><br />
  shortversion: <code>wget -r -np -nd -c -A 7z [URL]</code>

I only extract the .7z zipped files (not .bmz) because they are faster and the 7zip tool is readily handy.

## shortcuts_get: lists of policies, guidelines, 5 pillars and their shortcuts
This repo contains code that gets a list of all the policies/guidelines for each language edition and gets all the shortcuts (e.g. WP:OR) used in policy invocation. This is in *shortcuts_get* directory.

Since this has all been run, technically this no longer needs to be repeated to extract the data. Information is mostly notes about the procedure/exceptions. However, in the case of redoing it:

The general process:
1. get a list of all the policies, guidelines, and 5 pillar pages for each language edition
2. retrieve all the shortcuts from each rule pages
3. cross-check the list of rules and their shortcuts with other pages on the language edition (namely: category lists, 'shortcuts' list)

* * * UPDATE: now also need to run additional for a new case of shortcuts. See [shortcuts plus wptitle](https://github.com/sohyeonhwang/wikipi_repo#shortcuts-plus-wptitle)

### Running shortcuts_get
**ENGLISH**<br />
run: <code>python3 shortcuts_get_en.py</code>
* WP:5P needs to be manually fixed for each run of shortcuts_get_en. It should be:
<code>[https://en.wikipedia.org/wiki/Wikipedia:Five_pillars]	Wikipedia:Five Pillars	['Wikipedia:Five Pillars','WP:5P','WP:PILLARS','w.wiki/5']</code>

**SPANISH**<br />
run: <code>python3 shortcuts_get_es.py</code>

**FRENCH**<br />
run: <code>python3 shortcuts_get_fr.py</code>

**JAPANESE** <br />
run: <code>python3 shortcuts_get_ja.py</code>
* 5P page shortcuts_get_ja output should be fixed with each run to this:
<code>NA	https://ja.wikipedia.org/wiki/Wikipedia:%E4%BA%94%E6%9C%AC%E3%81%AE%E6%9F%B1	Wikipedia:五本の柱	['Wikipedia:五本の柱', 'WP:5', 'WP:5P']</code>
* In jawiki, [WP:IGNORE ALL RULES](https://ja.wikipedia.org/wiki/Wikipedia:%E3%83%AB%E3%83%BC%E3%83%AB%E3%81%99%E3%81%B9%E3%81%A6%E3%82%92%E7%84%A1%E8%A6%96%E3%81%97%E3%81%AA%E3%81%95%E3%81%84) is a proposal, therefore not included in the list. All other five pillar policies (as seen in enwiki) were captured.

#### Notes about the shortcuts_get inputs and outputs

**ENGLISH**<br />
List constructed from:
* https://en.wikipedia.org/wiki/Wikipedia:List_of_policies
* https://en.wikipedia.org/wiki/Wikipedia:List_of_guidelines

Cross-checked results with category pages:
https://en.wikipedia.org/wiki/Category:Wikipedia_policies
*Issues (Resolved)*
* https://en.wikipedia.org/wiki/Wikipedia:Non-free_content --> actually in guidelines
* Wikipedia:Signatures --> actually in guidelines

https://en.wikipedia.org/wiki/Category:Wikipedia_guidelines
*Issues (resolved)*
* https://en.wikipedia.org/wiki/Wikipedia:Content_assessment --> is "Version 1.0 Editorial Team assessment"

* added in missed pages of guidelines from cross-check into shortcuts_get_en.py code:
  * https://en.wikipedia.org/wiki/Wikipedia:Scientific_citation_guidelines
  * https://en.wikipedia.org/wiki/Wikipedia:Artist%27s_impressions_of_astronomical_objects
  * https://en.wikipedia.org/wiki/Wikipedia:In_the_news/Recurring_items
  * https://en.wikipedia.org/wiki/Wikipedia:Identifying_reliable_sources_(medicine)
  * https://en.wikipedia.org/wiki/Wikipedia:Indic_transliteration
  * https://en.wikipedia.org/wiki/Wikipedia:Non-free_use_rationale_guideline
  * https://en.wikipedia.org/wiki/Wikipedia:Public_domain
  * ... and so on. See file for all of them. There are about 30-35.

----
**SPANISH**<br />
List constructed from:
* https://es.wikipedia.org/wiki/Categor%C3%ADa:Wikipedia:Pol%C3%ADticas
* https://es.wikipedia.org/wiki/Categor%C3%ADa:Wikipedia:Convenciones

Cross-checked results with shortcuts list: https://es.wikipedia.org/wiki/Ayuda:Lista_de_atajos

Added Manual of Style Links manually into the shortcuts_get_es.py code, based on cross-check:
* https://es.wikipedia.org/wiki/Categor%C3%ADa:Wikipedia:Manual_de_estilo
* See file for all.

----
**FRENCH**<br />
List constructed from:
* https://fr.wikipedia.org/wiki/Cat%C3%A9gorie:Wikip%C3%A9dia:R%C3%A8gle
* https://fr.wikipedia.org/wiki/Cat%C3%A9gorie:Wikip%C3%A9dia:Recommandation

Cross-checked results with shortcuts list: https://fr.wikipedia.org/wiki/Aide:Raccourcis_Wikip%C3%A9dia

Added in 5 principles to the policies list via manual inclusion in shortcuts_get_fr.py code: 
* https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Principes_fondateurs
* https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:R%C3%A8gles_de_savoir-vivre
* https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Interpr%C3%A9tation_cr%C3%A9ative_des_r%C3%A8gles
* https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Droit_d%27auteur
* https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Neutralit%C3%A9_de_point_de_vue
* https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Wikip%C3%A9dia_est_une_encyclop%C3%A9die

----
**JAPANESE**<br />
List constructed from:
* [Wikipedia:方針とガイドラインの一覧
 aka Wikipedia: List of policies and guidelines
, which is more thorough than their separate policy + guideline pages](https://ja.wikipedia.org/wiki/Wikipedia:%E6%96%B9%E9%87%9D%E3%81%A8%E3%82%AC%E3%82%A4%E3%83%89%E3%83%A9%E3%82%A4%E3%83%B3%E3%81%AE%E4%B8%80%E8%A6%A7)

Cross-checked results with Category pages.

Added in aggregated file with manual includeion in shortcuts_get_ja.py code:
* https://ja.wikipedia.org/wiki/Wikipedia:%E4%BA%94%E6%9C%AC%E3%81%AE%E6%9F%B1
* https://ja.wikipedia.org/wiki/Wikipedia:%E8%A8%98%E4%BA%8B%E5%90%8D%E3%81%AE%E4%BB%98%E3%81%91%E6%96%B9/%E6%97%A5%E6%9C%AC%E3%81%AE%E7%9A%87%E6%97%8F
* https://ja.wikipedia.org/wiki/Wikipedia:%E8%91%97%E4%BD%9C%E6%A8%A9/20080630%E8%BF%84
* https://ja.wikipedia.org/wiki/Wikipedia:%E3%82%A2%E3%82%AB%E3%82%A6%E3%83%B3%E3%83%88%E4%BD%9C%E6%88%90%E8%80%85
* https://ja.wikipedia.org/wiki/Wikipedia:IP%E3%83%96%E3%83%AD%E3%83%83%E3%82%AF%E9%81%A9%E7%94%A8%E9%99%A4%E5%A4%96

### shortcuts plus wptitle
For enwiki, eswiki, and frwiki, we also need to add the case of WP:TITLE, wherein the title is the title of the page. This is a redirect that works but isn't included in the shortcuts. This case was caught later. In order to avoid manually correcting the exceptions noted in shortcuts_get output notes, we just go through the shortcuts_get output for each language edition and add the WP:TITLE case to the list of shortcuts. This is done with <code>../shortcuts_get/shortcuts_plus_wptitle.py</code>

Outputs go in the outputs folder as: <code><lang>_all_plus.tsv</code>

Simply <code>nano shortcuts_plus_wptitle.py</code> to check that the language edition for input/output files is correct, and run <code>python3 shortcuts_plus_wptitle.py</code>.

This also means we now need to re-generate the regular expressions for enwiki,eswiki,and frwiki. See [regex plus wptitle](https://github.com/sohyeonhwang/wikipi_repo#regex-plus-wptitle)

**language edition specific notes**<br/>
**ENGLISH**<br />
* WP:5P needs to be manually fixed for each run of shortcuts_get_en. It should be:
<code>[https://en.wikipedia.org/wiki/Wikipedia:Five_pillars]	Wikipedia:Five Pillars	['Wikipedia:Five Pillars','WP:5P','WP:PILLARS','w.wiki/5','WP:Five Pillars']</code>

## regex: generating regular expressions from list of shortcuts
Run <code>regex_generator.pl</code> in regex, which goes through the lists of shortcuts and creates regexes with [Regex::Assemble](https://metacpan.org/pod/Regexp::Assemble).

In the code, there is code for every language edition but you must comment lines in/out accordingly to get the output for the desired language edition. There are 3 parts of the code that must be adjusted and they are clearly marked. This will generate the tsv files in regex-lists. <code>giant_regex</code> is printed to standard out for now; I copy and pasted them into text files and have no re-generated them. At minimum, the shortcuts will always include the title of the policy/guidelines page.

There are some bugs in the code still:
* Namely, the perl regex scripts generates regexes that starts with: <code>(?^:</code>. The ^ causes an error in Python, which is what is used in other data extraction.
* In the giant regexes, the ':' is placed in a way that causes error, same as '?' at the end. Placement of ':' needs to be fixed and '?' needs to be deleted. The regex should end with '\b)'. This must be manually added if the giant regex is generated again.

**language edition specific notes**

_Extra ^ symbol before :_ <br />
**SPANISH**<br />
Error in regex for Usuario:Userbox/Documentación de userboxes in guidelines --> (?^:^\b...<br />
... is corrected. This needs to be corrected in each generation of eswiki shortcuts.

**JAPANESE** <br />
Regex errors:<br />
<code>プロジェクト:フィクション/登場人物と設定の記述	NA	(?^:^\b     --> "Project: description of fiction / characters and setting"</code><br />
<code>プロジェクト:キリスト教/キリスト教の記事名と用語表記のガイドライン	NA	(?^:^\b     --> "Project: Christian / Christian article title and terminology guidelines"</code><br />
... have been corrected but should be corrected with each generation of regex.

### regex plus wptitle
This only applies for enwiki, eswiki, and frwiki. Note that this means that the giant regex for those language editions have also changed. The new output can be found in <code>../regex/allregex_plus</code>. This output was created with <code>regex_plus_generator.pl</code>. Then task lists for the three language editions were generated again. 

New giant regexes for enwiki, eswiki, and frwiki are saved in: <code>../giant-regexes/LANG_all_plus_giant.txt</code>

As in the previous regex generation:
* The perl regex scripts generates regexes that starts with: <code>(?^:</code>. The ^ causes an error in Python, which is what is used in other data extraction. The regex should end with '\b)'. The former has been corrected, the latter has been integrated into the code generation so it doesn't need to be manualyl added.
* UNSURE IF THIS IS TRUE: In the giant regexes, the ':' is placed in a way that causes error, same as '?' at the end. Placement of ':' needs to be fixed and '?' needs to be deleted. 
* The giant regex should end with '\b)'. This is currently done but must be manually added if the giant regex is generated again.


## batch_tasklists: task list construction for running wikiq on hyak
Now that we have (1) the raw data and (2) the regexes, we can use [wikiq](https://wiki.communitydata.science/Wikiq) to parse the dump files and generate tabular datasets from the dumpfiles. I helped build extended functionality onto the tool (pattern matching) to do this. I had to correct some bugs. These are not merged to master yet, just on regex_scanner branch of wikiq's git. That's the version I have on my file set-up in hyak for this project.

A modification on wikiq that's in the local version for this project is adding the <code>re.I</code> flag in the <code>re.compile</code>:<br/>
<code>133:         self.pattern = re.compile(pattern,re.IGNORECASE)</code>

To run this, I would want to run a task lsit in parallel on hyak on <code>any_machine</code>.

The basic construction would look like: <br />
<code>python3 ./mwdumptools/wikiq [input] -o ./output  -RPl [regexlabelhere] -RP '[regexgoeshere]'</code>

**giant_regex (giant) vs. many regexes (wide):**
Test runs with the giant_regex indicate that the construction of it is a little buggy, but the size of it makes it very inscrutable to identify the problem. As a result, instead, for searching for all policies, I create "wide" taskslists, basically the basic construction with regex-label pairs for every single policy/guidelines of that language edition. This avoids the issue that comes with giant_regex's bugginess downside is that this take a very long time to run the jobs (so far).

## Running wikiq on hyak
Note that local version of wikiq is modified on line 133 to include the re.IGNORECASE flag.

Japanese: <code>parallel < tasklist_wide_ja</code><br/>
French: <code>parallel < tasklist_wideplus_fr </code><br/>
Spanish: <code>parallel < tasklist_wideplus_es</code>

English has too many tasks for it to run on one node. Since batch parallel isn't working well for me, I create four tasklists of English and run them on their own any_machine.

English: 
* <code>parallel < tasklist_wideplus_en1</code>
* <code>parallel < tasklist_wideplus_en2</code>
* <code>parallel < tasklist_wideplus_en3</code>
* <code>parallel < tasklist_wideplus_en4</code>

## other data for generating figures
* **active editors** (5 or more edits in the given month): from https://stats.wikimedia.org/
  * time range: all
  * monthly (daily would not load for en and fr with the all setting in time)
* interlanguage link information - will be extracted from tabular outputs from wikiq

------
## TODO
<code>

###GERMAN <br />
Unclear how to construct: https://de.wikipedia.org/wiki/Wikipedia:Richtlinien

###INTERLANGUAGE LINK INFORMATION
1. get a list of every single rule page in this set
2. go to every link and check the ILL against the other pages;
3. generate the ILL matrix (dictionary?) - maybe in JSON?

###CHECK HYAK OUTPUTS
...
basic flow to remember: login. run tmux. run any machine. run parallel

###MISC NOTES:
generating figures... use Spark: https://wiki.communitydata.science/CommunityData:Hyak_Spark
- only care about article and user namespaces... 
	- by namespace (+ comment vs revision), so there are 4 namespaces

- only care about the revisions that the regexes _have_ found
- we want to know the _difference_ (+how many policy invocations since the last one???) +X policy invocations with the regex matches

- linked scores: by date, sum the +X each day, plot over time
	- just number
	- keeping the actually policy in mind
- policy invocations / # active users 
- interlanguage link info: for each policy page, look at the revisions and find when the ILL was linked

- might need to correct the regexes (es and ja)
</code>
