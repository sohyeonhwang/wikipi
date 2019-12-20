#!/usr/bin/perl

use strict;
use warnings;
use Regexp::Assemble;
use File::Basename;
use Cwd;

my $path = getcwd;
my $wikipi_repo_dir = dirname( $path );
my $output_dir_path = "$path/allregex_plus";
my $giant_output_dir_path = "$path/giant-regexes";

#print "$wikipi_repo_dir/shortcuts_get/output/*\n";

## INPUT FILES
my @input_f = ("en_all_plus.tsv","fr_all_plus.tsv","es_all_plus.tsv");
my $len = @input_f;

my $giant_regex = Regexp::Assemble->new;


#for each language edition
for ( my $i = 0; $i < $len ; $i = $i + 1 ) {
    # opening up the input to read
    my $pre = "<";
    my $filepath = "$wikipi_repo_dir/shortcuts_get/output/";
    my $filename =  "$input_f[$i]";
    my $arg = $pre.$filepath.$filename;
    
    print " INPUT FILE: $arg\n";

    open(my $data, $arg) or die "Couldn't open the input file '$filename' for some reason, $!\n";

    #for each input file (e.g. language edition), we prep an output file of list of regexes
    my $output_base = substr $filename, 0, -4;
    my $output_name = "$output_base\_list.tsv";
    my $output_path = "$output_dir_path/$output_name";
    print "OUTPUT FILE: $output_path\n\n";

    open(my $output_data , '>', $output_path) or die "Couldn't open the output file '$!'";

    while (my $line = <$data>) {
        chomp $line;
        (my $cat,my $url,my $title,my $abbrevs) = split /\t/, $line;
        chomp $abbrevs;
        
        my @regexes;

        # ENGLISH
        my $lang_en = index($filename,"en");
        if ( $lang_en > -1 ) {
            #print("Getting regexes for enwiki now...\n");
            while ( $abbrevs =~ m/((WP|wp|Wikipedia|wikipedia|w\.wiki)[^']*)/gi ) {
                push @regexes, $1;
            };
        };

        # FRENCH
        my $lang_fr = index($filename,"fr");
        if ( $lang_fr > -1 ) {
            #print("Getting regexes for frwiki now...\n");
            my @tester = split /, /, $abbrevs;
            foreach ( @tester ) {
                my $messy = $_;
                while ( $messy =~ m/((WP|wp|Wikipedia|wikipedia|Wikipédia|wikipédia|w\.wiki|Aide|aide|Utilisateur|utilisateur)[^"]*\b)/g ) {    
                    push @regexes, $1;
                };
            };
        };

        # SPANISH
        my $lang_es = index($filename,"es");
        if ( $lang_es > -1 ) {
            #print("Getting regexes for eswiki now...\n");
            my @tester = split /, /, $abbrevs;
            foreach ( @tester ) {
                my $messy = $_;

                while ( $messy =~ m/((WP|wp|Wikipedia|wikipedia|Usuaria|usuaria|Usuario|usuario)[^"]*\b)/g ) {
                    push @regexes, $1;
                };
            };
        };


        my $ra = Regexp::Assemble->new;
        foreach ( @regexes ) {
            #print "$_ \n";
            $ra->add( $_ );
            $giant_regex->add( $_ );
            #print "adding $_ to the assembled regex \n";
        }

        my $expression = $ra -> re;
        #print "EXPRESSED0: $expression\n";
        my $clipped = substr $expression, 0, -1;
        my $end = "\\b)";
        my $formatted_expression = $clipped.$end;
        #print "EXPRESSED1: $formatted_expression\n\n";

        #output the regex with the relevant cat, url, title in the output file
        print $output_data "$title\t$cat\t$formatted_expression\n";

    };

    close $data;
    close $output_data;

    #output the giant regex to a text file <lang>_all_plus_giant.txt
    my $giant_output_base = substr $filename, 0, -4;
    my $giant_output_name = "$giant_output_base\_giant.tsv";
    my $giant_output_path = "$giant_output_dir_path/$giant_output_name";
    print "GIANT OUTPUT FILE: $giant_output_path\n\n";

    open(my $output_giant , '>', $giant_output_path) or die "Couldn't open the output file '$!'";

    my $dannoli = $giant_regex -> re;
    print $output_giant "$dannoli\n";

};