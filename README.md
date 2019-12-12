# wikipedia policy invocation
supporting code for wiki policy invocation project:
- policies and guidelines lists (+code to extract)
- regular expressions


#TODO
- scripting for bash / hyak wikiq to extract cases
  - creating tasklist
- interlanguage link retrieval
- number of active editors data 


... CONTAINS
    SCRIPTS
        shortcuts_get
        to run the script the input and output must be in the same directory (as of now). 
        i have not greated path i/o to grab and output to particular directories, they have been moved manually

        regex_generator

    SHORTCUTS GET OUTPUT 
    REGEXES
    ILL INFORMATION


POLICY EXTRACTION AND REGEX PROCESSING
1. get a list of all the policies, guidelines, and 5 pillar pages for each language edition
2. retrieve all the shortcuts from each rule pages
3. cross-check the list of rules and their shortcuts with other pages on the language edition (namely: category lists, 'shortcuts' list)

    ENGLISH
        NEED TO - RUN CODE AGAIN AND GENERATE ALL REGEXES

        https://en.wikipedia.org/wiki/Wikipedia:List_of_policies
        https://en.wikipedia.org/wiki/Wikipedia:List_of_guidelines

        cross-checked results with category pages:
            https://en.wikipedia.org/wiki/Category:Wikipedia_policies
                ???
                    https://en.wikipedia.org/wiki/Wikipedia:Non-free_content --> in guidelines
                    signatures --> in guidelines

            https://en.wikipedia.org/wiki/Category:Wikipedia_guidelines
                ???
                    https://en.wikipedia.org/wiki/Wikipedia:Content_assessment --> is "Version 1.0 Editorial Team assessment"

            added (into guidelines):
                https://en.wikipedia.org/wiki/Wikipedia:Scientific_citation_guidelines
                https://en.wikipedia.org/wiki/Wikipedia:Artist%27s_impressions_of_astronomical_objects
                https://en.wikipedia.org/wiki/Wikipedia:In_the_news/Recurring_items
                https://en.wikipedia.org/wiki/Wikipedia:Identifying_reliable_sources_(medicine)
                https://en.wikipedia.org/wiki/Wikipedia:Indic_transliteration
                https://en.wikipedia.org/wiki/Wikipedia:Non-free_use_rationale_guideline
                https://en.wikipedia.org/wiki/Wikipedia:Public_domain
                https://en.wikipedia.org/wiki/Wikipedia:WikiProject_Economics/Reliable_sources_and_weight 
                https://en.wikipedia.org/wiki/Wikipedia:Shortcut
                https://en.wikipedia.org/wiki/Wikipedia:Project_namespace
                https://en.wikipedia.org/wiki/Wikipedia:Reference_desk/Guidelines/Medical_advice
                https://en.wikipedia.org/wiki/Wikipedia:Talk_page_templates
                https://en.wikipedia.org/wiki/Wikipedia:Userboxes
                https://en.wikipedia.org/wiki/Wikipedia:Template_namespace
                https://en.wikipedia.org/wiki/Wikipedia:Miscellany_for_deletion/Speedy_redirect
                https://en.wikipedia.org/wiki/Wikipedia:As_of
                https://en.wikipedia.org/wiki/Wikipedia:Extended_image_syntax
                https://en.wikipedia.org/wiki/Wikipedia:Disambiguation/PrimaryTopicDefinition
                https://en.wikipedia.org/wiki/Wikipedia:Overcategorization/User_categories
                https://en.wikipedia.org/wiki/Wikipedia:Spellchecking
                https://en.wikipedia.org/wiki/Wikipedia:TemplateStyles
                https://en.wikipedia.org/wiki/Wikipedia:Template_namespace
                https://en.wikipedia.org/wiki/Wikipedia:Wikimedia_sister_projects
                https://en.wikipedia.org/wiki/Wikipedia:WikiProject_Council/Guide
                https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Hidden_text
                https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Military_history
                https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Indonesia-related_articles
                https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Pakistan-related_articles
                https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Blazon
                https://en.wikipedia.org/wiki/Wikipedia:Edit_filter
                https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(US_stations)
                https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(Irish_stations)
                https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(Canadian_stations)
                https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(places_in_Bangladesh)
                https://en.wikipedia.org/wiki/Wikipedia:Naming_conventions_(Football_in_Australia)
                https://en.wikipedia.org/wiki/Wikipedia:Five_pillars

        Note > Five pillars needs to be manually fixed for each run of shortcuts_get_en
            See also	https://en.wikipedia.org/wiki/Wikipedia:Five_pillars]	Wikipedia:Five Pillars	['Wikipedia:Five Pillars','WP:5P','WP:PILLARS','w.wiki/5']

    SPANISH
        cross-checked outputs with shortcuts list: https://es.wikipedia.org/wiki/Ayuda:Lista_de_atajos
        ran again with manual of style links (in guidelines):
            https://es.wikipedia.org/wiki/Categor%C3%ADa:Wikipedia:Manual_de_estilo        

        error in regex for
            Usuario:Userbox/Documentación de userboxes	NA	(?^:^\b...      --> in guidelines
        corrected.


    FRENCH
        cross-checked with shortcuts list: https://fr.wikipedia.org/wiki/Aide:Raccourcis_Wikip%C3%A9dia
        added in 5 principles to the policies list: 
            https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Principes_fondateurs
            https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:R%C3%A8gles_de_savoir-vivre
            https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Interpr%C3%A9tation_cr%C3%A9ative_des_r%C3%A8gles
            https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Droit_d%27auteur
            https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Neutralit%C3%A9_de_point_de_vue
            https://fr.wikipedia.org/wiki/Wikip%C3%A9dia:Wikip%C3%A9dia_est_une_encyclop%C3%A9die

    JAPANESE
        added in aggregated file
            https://ja.wikipedia.org/wiki/Wikipedia:%E4%BA%94%E6%9C%AC%E3%81%AE%E6%9F%B1
            https://ja.wikipedia.org/wiki/Wikipedia:%E8%A8%98%E4%BA%8B%E5%90%8D%E3%81%AE%E4%BB%98%E3%81%91%E6%96%B9/%E6%97%A5%E6%9C%AC%E3%81%AE%E7%9A%87%E6%97%8F
            https://ja.wikipedia.org/wiki/Wikipedia:%E8%91%97%E4%BD%9C%E6%A8%A9/20080630%E8%BF%84
            https://ja.wikipedia.org/wiki/Wikipedia:%E3%82%A2%E3%82%AB%E3%82%A6%E3%83%B3%E3%83%88%E4%BD%9C%E6%88%90%E8%80%85
            https://ja.wikipedia.org/wiki/Wikipedia:IP%E3%83%96%E3%83%AD%E3%83%83%E3%82%AF%E9%81%A9%E7%94%A8%E9%99%A4%E5%A4%96
        
        error in regex for:
            プロジェクト:フィクション/登場人物と設定の記述	NA	(?^:^\b     --> "Project: description of fiction / characters and setting"
            プロジェクト:キリスト教/キリスト教の記事名と用語表記のガイドライン	NA	(?^:^\b     --> "Project: Christian / Christian article title and terminology guidelines"
        corrected.

        NOTES:
        https://ja.wikipedia.org/wiki/Wikipedia:%E3%83%AB%E3%83%BC%E3%83%AB%E3%81%99%E3%81%B9%E3%81%A6%E3%82%92%E7%84%A1%E8%A6%96%E3%81%97%E3%81%AA%E3%81%95%E3%81%84
        "Ignore all rules" is a proposal in the japanese wiki, will not include in japanese policy lists
        other 5P were captured

        5P page shortcuts_get_ja output should be fixed with each run to this:
            NA	https://ja.wikipedia.org/wiki/Wikipedia:%E4%BA%94%E6%9C%AC%E3%81%AE%E6%9F%B1	Wikipedia:五本の柱	['Wikipedia:五本の柱', 'WP:5', 'WP:5P']

    GERMAN
        NEED TO DO EVERYTHING...
        https://de.wikipedia.org/wiki/Wikipedia:Richtlinien

REGEX
the perl regex scripts generates regexes that starts with:
'(?^:...'. The ^ causes an error in structure in python. it should be '(?:\b...' to work the way we imagine...
WP: or Wikipedia: --> fix placement of ':' and get rid of ? at the end

INTERLANGUAGE LINK INFORMATION
    1. get a list of every single rule page in this set
    2. go to every link and check the ILL against the other pages;
    3. generate the ILL matrix (dictionary?) - maybe in JSON?



GETTING DATA FROM DUMPS
== HYAK INSTRUCTIONS ==
1. run over data dumps with the giant regexes for each language edition (test with english). this should generate a file with all of the invocations
2. ...


1. get the input dumpfiles for each language edition 20190901
1.5 fix the regex outputs
2. get the inputs for the regexes
3. test run wikiq i/o with a smaller regex + test file
    > enwiki-20190901-pages-meta-history5.xml-p471239p482985.7z

4. write script for each language edition to run wikiq on all dumps with the giant regexes 