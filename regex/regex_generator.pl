#!/usr/bin/perl

use strict;
use warnings;
use Regexp::Assemble;

#input file(s)
# cases we would want multiple input files:
    # english policies and guidelines, they have separte lists
    # french policies and guidelines - automatically generated list from templates in the category pages is more comprehensive
    # spanish policies and guidelines - similar situation as french
# german language edition has things in one file, because of how the rules are organized on the page and need to be carefully crawled...
# japanese edition is from one page; more thorough than the category pages


#CHANGE BASED ON LANGUAGE EDITION
#english
#my @files = ("en_policies.tsv","en_guidelines.tsv");
#french
#my @files = ("fr_policies.tsv","fr_guidelines.tsv");
#spanish
#my @files = ("es_policies.tsv","es_guidelines.tsv");
#japanese
my @files = ("ja_policies-guidelines.tsv");

my $len = @files;

# giant_regex is the regex from every single shortcut/link from all the input files. use it for doing counts of ~everything~
my $giant_regex = Regexp::Assemble->new;


#CHANGE BASED ON LANGUAGE EDITION
#initialize output files
# output file of list of regexes; change the string based on the input files
# for english policy and guidelines
#my $out_file = 'en_pol-gl_list.tsv';
# for french policy and guidelines
#my $out_file = 'fr_pol-gl_list.tsv';
# for spanish
#my $out_file = 'es_pol-gl_list.tsv';
# for japanese
my $out_file = 'ja_pol_gl_list.tsv';

open(my $o_fh0 , '>', $out_file);

for ( my $i = 0; $i < $len ; $i = $i + 1 ) {
    my $pre = "<";
    my $filename =  $files[$i];
    my $arg = $pre.$filename;

    print "$filename\n";

    open(my $data, $arg) or die "Couldn't open the '$filename' for some reason, $!\n";

    #for each input file, we prep an output file of list of regexes (in the case that there are multiple)
    my $out_file1 = substr $filename, 0, -4;
    $out_file1 = "$out_file1\_list.tsv";
    open(my $o_fh1 , '>', $out_file1);

    while (my $line = <$data>) {
        chomp $line;
        (my $cat,my $url,my $title,my $abbrevs) = split /\t/, $line;
        chomp $abbrevs;
        #print $abbrevs;
        
        my @regexes;

        #CHANGE BASED ON LANGUAGE EDITION
        #for english
        #while ( $abbrevs =~ m/((WP|wp|Wikipedia|wikipedia|w\.wiki)[^']*)/gi ) {
        #    push @regexes, $1;
        #};

        my @tester = split /, /, $abbrevs;
        #print "@tester\n";

        #for japanese
        foreach (@tester) {
            my $messy = $_;
            chomp $messy;
            #print "mess: $messy\n";
            while ( $messy =~ m/((WP|wp|Wikipedia|wikipedia|プロジェクト)[^']*)/g ) {
                chomp $1;
               print "cleaner: <$1>\n";
                my $loc1 = index($1, "Wikipedia");
                my $loc2 = index($1, "WP");
                my $loc3 = index($1, "プロジェクト");
                my $sum = $loc1 + $loc2;
        
                if ( $sum > -3 ) {
                    print "$1\n";
                    push @regexes, $1;
                };
            };
        };

        #print "\n\n";
        
        #for french
        #print "These lines should match:\n$abbrevs\n@tester\n";
        #foreach ( @tester ) {
        #    my $messy = $_;
        #    while ( $messy =~ m/((WP|wp|Wikipedia|wikipedia|Wikipédia|wikipédia|w\.wiki|Aide|aide|Utilisateur|utilisateur)[^"]*\b)/g ) {
        #        push @regexes, $1;
        #    };
        #};
        #print "\n";


        ##for spanish
        #foreach ( @tester ) {
        #    my $messy = $_;
        #    while ( $messy =~ m/((WP|wp|Wikipedia|wikipedia|Usuaria|usuaria|Usuario|usuario)[^"]*\b)/g ) {
        #        push @regexes, $1;
        #    };
        #};

        # for german

        my $ra = Regexp::Assemble->new;
        foreach ( @regexes ) {
            #print "$_ \n";
            $ra->add( $_ );
            $giant_regex->add( $_ );
            #print "adding $_ to the assembled regex \n";
        }
        my $expression = $ra -> re;
        #print "$expression\n\n";

        #output the regex with the relevant cat, url, title in the output file
        print $o_fh0 "$title\t$cat\t$expression\n";
        print $o_fh1 "$title\t$cat\t$expression\n";

    };

    close $data;
    close $o_fh1;
    
};
close $o_fh0;

print $giant_regex -> re;