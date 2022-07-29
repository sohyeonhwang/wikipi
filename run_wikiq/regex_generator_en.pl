#!/usr/bin/perl

use strict;
use warnings;
use Regexp::Assemble;
use File::Basename;
use String::Clean;
use Cwd;
use Text::CSV;

my $currentdir = getcwd;
my $inputfile = "/rule_regex.tsv";
my $arg = $currentdir.$inputfile;

my $outputfile = "/rule_regex_generated.tsv";
my $arg_out = $currentdir.$outputfile;
open(my $output_data , '>:encoding(UTF-8)', $arg_out) or die "Couldn't open the '$arg' for some reason, $!\n";

my $tsv = Text::CSV->new({
    sep_char        => "\t",
    eol             => "\n",
    #encoding => "UTF-8",
    quote_space     => 0,
    quote_null      => 0,
});

my $regex_all_rules = Regexp::Assemble->new;

open(my $data, '<:encoding(UTF-8)', $arg) or die "Couldn't open the '$arg' for some reason, $!\n";

while (my $line = <$data>) {
    chomp $line;
    # lang, label, rule, shortcuts, regex
    (my $lang,my $label,my $rule,my $shortcuts) = split /\t/, $line;
    chomp $shortcuts;

    my @regexes;

    my @spl = split(', ', $shortcuts);

    if ($lang eq "ja")
    {
        foreach my $i (@spl) 
        {
            #print "$i\n";
            while ($i =~ m/((WP|Wikipedia|Wikipédia|w\.wiki)[^']*)/g) 
            {
                my $substring = $1;
                #print "$substring\n";
                push @regexes, $substring;
            }

        }
    }
    if ($lang eq "en")
    {
        foreach my $i (@spl) 
        {
            #print "$i\n";
            while ($i =~ m/((WP|wp|Wikipedia|wikipedia|w\.wiki)[^']*)/gi)
            {
                my $substring = $1;
                #print "$substring\n";
                push @regexes, $substring;
            }

        }
    }
    if ($lang eq "es")
    {
        foreach my $i (@spl) 
        {
            #print "$i\n";
            while ($i =~ m/((WP|Wikipedia|Wikipédia|w\.wiki)[^"|']*)/g) 
            {
                my $substring = $1;
                #print "$substring\n";
                push @regexes, $substring;
            }

        }
    }
    if ($lang eq "fr")
    {
        foreach my $i (@spl) 
        {
            my $letter = substr($i, 0, 1);
            if ($letter eq '['){
                $letter = substr($i, 1, 1);
            }
            #print "$i\n";
            if ($letter eq '"')
            {
                while ($i =~ m/((WP|Wikipédia)[^"]*)/g) 
                {
                    my $substring = $1;
                    print "$substring\n";
                    push @regexes, $substring;
                }
            }
            else 
            {
                while ($i =~ m/((WP|Wikipédia)[^']*)/g) 
                {
                    my $substring = $1;
                    print "$substring\n";
                    push @regexes, $substring;
                }
            }
        }
    }
    if ($lang eq "de")
    {
        foreach my $i (@spl) 
        {
            #print "$i\n";
            while ($i =~ m/((WP|wp|Wikipedia|wikipedia|w\.wiki)[^']*)/gi) 
            {
                my $substring = $1;
                if ($substring =~ m/Störe Wikipedia nicht/) 
                {
                    $substring = "$substring, um etwas zu beweisen"
                }
                #print "$substring\n\n";
                push @regexes, $substring;
            }
        }
    }

    my $regex_this_rule = Regexp::Assemble->new;
    foreach ( @regexes ) {
        #print "$_ \n";
        $regex_this_rule->add( $_ );
        $regex_all_rules->add( $_ );
        #print "adding $_ to the assembled regex \n";
    }

    my $expression = $regex_this_rule -> re;
    #print "EXPRESSED0: $expression\n";
    my $clipped = substr $expression, 0, -1;
    my $end = "\\b)";
    my $formatted_expression = $clipped.$end;
    #print "EXPRESSED1: $formatted_expression\n\n\n\n";

    #print "$lang\t$label\t$rule\n";

    my @row = ($lang, $label, $rule, $shortcuts, $formatted_expression);
    $tsv->bind_columns (\(@row));
    $tsv->print ($output_data, [ @row ]);
}

close $data;
close $output_data;

## Make sure the fix the (?^u:(?:W --> (?:\b(?:W and (?^u:W --> (?:\bW