#!/usr/bin/perl

use strict;
use warnings;
use Regexp::Assemble;
use File::Basename;
use Cwd;

my $path = getcwd;
my $wikipi_repo_dir = dirname( $path );

print "$wikipi_repo_dir/shortcuts_get/output/*\n";

## 1/ CHOOSE AN INPUT FOR FILES
#EN - ("en_policies.tsv","en_guidelines.tsv");
#FR - ("fr_policies.tsv","fr_guidelines.tsv");
#ES - ("es_policies.tsv","es_guidelines.tsv");
#JA - ("ja_policies-guidelines.tsv");
my @input_f = ("fr_policies.tsv","fr_guidelines.tsv");
my $len = @input_f;

## 2/ CHOOSE THE CORRESPONDING OUTPUT FILES TO INITIALIZE
#EN - 'en_allregex_list.tsv';
#FR - 'fr_allregex_list.tsv';
#ES - 'es_allregex_list.tsv';
#JA - 'ja_allregex_list.tsv';
my $output_f = 'fr_allregex_list.tsv';
my $output_dir_path = "$path/regex-lists";
print "$output_dir_path/$output_f\n\n";
open(my $output_allregex , '>', "$output_dir_path/$output_f");


my $giant_regex = Regexp::Assemble->new;


for ( my $i = 0; $i < $len ; $i = $i + 1 ) {
    my $pre = "<";
    my $filepath = "$wikipi_repo_dir/shortcuts_get/output/";
    my $filename =  "$input_f[$i]";
    my $combined = $filepath.$filename;
    my $arg = $pre.$combined;
    
    print "FILE: $arg\n";

    open(my $data, $arg) or die "Couldn't open the '$filename' for some reason, $!\n";

    #for each input file, we prep an output file of list of regexes (in the case that there are multiple)
    my $output_small_base = substr $filename, 0, -4;
    my $output_small_name = "$output_small_base\_list.tsv";
    my $output_small_path = "$output_dir_path/$output_small_name";
    print "!!! $output_small_path\n";


    open(my $policyguideline_list , '>', $output_small_path) or die "Couldn't open $!";


    while (my $line = <$data>) {
        chomp $line;
        (my $cat,my $url,my $title,my $abbrevs) = split /\t/, $line;
        chomp $abbrevs;
        #print $abbrevs;
        
        my @regexes;

        # 3/ CHANGE BASED ON LANGUAGE EDITION
        # ENGLISH
        #while ( $abbrevs =~ m/((WP|wp|Wikipedia|wikipedia|w\.wiki)[^']*)/gi ) {
        #    push @regexes, $1;
        #};

        my @tester = split /, /, $abbrevs;

        # JAPANESE
        #foreach (@tester) {
        #    my $messy = $_;
        #    chomp $messy;
            #print "mess: $messy\n";
        #    while ( $messy =~ m/((WP|wp|Wikipedia|wikipedia|プロジェクト)[^']*)/g ) {
        #        chomp $1;
        #       print "cleaner: <$1>\n";
        #        my $loc1 = index($1, "Wikipedia");
        #        my $loc2 = index($1, "WP");
        #        my $loc3 = index($1, "プロジェクト");
        #        my $sum = $loc1 + $loc2;
        
        #        if ( $sum > -3 ) {
        #            print "$1\n";
        #            push @regexes, $1;
        #        };
        #    };
        #};

        # FRENCH AND SPANISH
        foreach ( @tester ) {
            my $messy = $_;

            #FRENCH
            while ( $messy =~ m/((WP|wp|Wikipedia|wikipedia|Wikipédia|wikipédia|w\.wiki|Aide|aide|Utilisateur|utilisateur)[^"]*\b)/g ) {    
            #SPANISH
        #    while ( $messy =~ m/((WP|wp|Wikipedia|wikipedia|Usuaria|usuaria|Usuario|usuario)[^"]*\b)/g ) {
                push @regexes, $1;
            };
        };

        # for german
        # ...

        my $ra = Regexp::Assemble->new;
        foreach ( @regexes ) {
            #print "$_ \n";
            $ra->add( $_ );
            $giant_regex->add( $_ );
            #print "adding $_ to the assembled regex \n";
        }
        my $expression = $ra -> re;
        print "EXPRESSED0: $expression\n";
        my $clipped = substr $expression, 0, -1;
        my $end = "\\b)";
        my $formatted_expression = $clipped.$end;
        print "EXPRESSED1: $formatted_expression\n\n";
        

        #output the regex with the relevant cat, url, title in the output file

        print $output_allregex "$title\t$cat\t$formatted_expression\n";
        print $policyguideline_list "$title\t$cat\t$formatted_expression\n";

    };

    close $data;
    close $policyguideline_list;
    
};
close $output_allregex;

#print $giant_regex -> re