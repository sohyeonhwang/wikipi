#!/usr/bin/perl

use strict;
use warnings;
use Regexp::Assemble;
use File::Basename;
use Cwd;

my $regex = Regexp::Assemble->new;

#my $npov = '(?:\bW(?:P:(?:(?:S(?:UB(?:STANTIAT|JECTIV)|TRUCTUR)|FALSEBALANC|(?:UN)?DU)E|N(?:P(?:OV(?:(?:VIE|HO)W|FACT)?|V)|EUTRAL)|A(?:(?:CHIEVE N|TTRIBUTE)POV|ESTHETIC)|P(?:OV(?:NAMING)?|ROPORTION|SCI)|B(?:ALA(?:NCED?|SP)|ESTSOURCES)|W(?:IKIVOICE|EIGHT)|(?:IMPARTI|GEV)AL|(?:YES|RN)POV|TINFOILHAT|VALID|MNA)|ikipedia:Neutral point of view)\b)';
#my $v = '(?:\bW(?:P:(?:S(?:YN(?:TH(?:ESIS)?)?|TICKTOSOURCE|ECONDARY)|T(?:RANSCRIPTION|ERTIARY)|O(?:R(?:IGINAL)?|I)|P(?:RIMARY|STS)|(?:VERIFY|N)OR|AEIS|CALC)|ikipedia:No original research)\b)';
#my $nor = '(?:\bW(?:P:(?:S(?:O(?:URCE(?:(?:ACCES)?S)?|CIALMEDIA)|ELFPUB(?:LISH)?|PS)|N(?:O(?:TR(?:ELIABLE|S)|N?ENG)|EWSBLOG)|R(?:E(?:D(?:FLAG|DIT)|FLOOP)|SUE)|EX(?:TRAORDINARY|CEPTIONAL)|P(?:AY(?:SITE|WALL)|ROVEIT)|C(?:IRC(?:ULAR)?|HALLENGE)|V(?:ER(?:IFY)?|NOTSUFF)?|FAILEDVERIFICATION|U(?:NSOURCED|GS)|B(?:URDEN|LOGS)|YTCOPYRIGHT|(?:ONU|Q)S|ABOUTSELF|TWITTER)|ikipedia:Verifiability)\b)';

# core regex NPOV, V, NOR regex:
# (?:W(?:P:(?:S(?:(?:T(?:ICKTOSOURC|RUCTUR)|UB(?:STANTIAT|JECTIV))E|O(?:URCE(?:(?:ACCES)?S)?|CIALMEDIA)|E(?:LFPUB(?:LISH)?|CONDARY)|YN(?:TH(?:ESIS)?)?|PS)|N(?:P(?:OV(?:(?:VIE|HO)W|FACT)?|V)|O(?:TR(?:ELIABLE|S)|N?ENG|R)|eutral point of view|o original research|E(?:WSBLOG|UTRAL))|P(?:R(?:O(?:PORTION|VEIT)|IMARY)|AY(?:SITE|WALL)|OV(?:NAMING)?|S(?:CI|TS))|A(?:(?:CHIEVE N|TTRIBUTE)POV|E(?:STHETIC|IS)|BOUTSELF)|V(?:ER(?:IFY(?:OR)?)?|erifiability|NOTSUFF|ALID)?|B(?:(?:ESTSOURCE|LOG)S|ALA(?:NCED?|SP)|URDEN)|T(?:RANSCRIPTION|INFOILHAT|ERTIARY|WITTER)|R(?:E(?:D(?:FLAG|DIT)|FLOOP)|NPOV|SUE)|FA(?:ILEDVERIFICATION|LSEBALANCE)|C(?:IRC(?:ULAR)?|HALLENGE|ALC)|EX(?:TRAORDINARY|CEPTIONAL)|U(?:N(?:SOURCED|DUE)|GS)|O(?:R(?:IGINAL)?|NUS|I)|Y(?:TCOPYRIGHT|ESPOV)|W(?:IKIVOICE|EIGHT)|(?:IMPARTI|GEV)AL|DUE|MNA|QS)|ikipedia:(?:N(?:eutral point of view|o original research)|Verifiability)))

my @npov = ('WP:RNPOV', 'WP:BALANCED', 'WP:NPV', 'WP:MNA', 'WP:NPOVVIEW', 'WP:NPOV', 'WP:WEIGHT', 'WP:SUBJECTIVE', 'WP:NEUTRAL', 'WP:BALASP', 'WP:BESTSOURCES', 'WP:STRUCTURE', 'WP:ACHIEVE NPOV', 'WP:ATTRIBUTEPOV', 'WP:YESPOV', 'WP:VALID', 'WP:WIKIVOICE', 'WP:IMPARTIAL', 'WP:UNDUE', 'Wikipedia:Neutral point of view', 'WP:NPOVFACT', 'WP:AESTHETIC', 'WP:DUE', 'WP:GEVAL', 'WP:TINFOILHAT', 'WP:POVNAMING', 'WP:SUBSTANTIATE', 'WP:POV', 'WP:FALSEBALANCE', 'WP:PSCI', 'WP:BALANCE', 'WP:NPOVHOW', 'WP:PROPORTION', 'WP:Neutral point of view');
my @v = ('WP:TERTIARY', 'WP:PRIMARY', 'WP:TRANSCRIPTION', 'WP:ORIGINAL', 'WP:CALC', 'WP:SYNTH', 'Wikipedia:No original research', 'WP:OI', 'WP:OR', 'WP:SECONDARY', 'WP:VERIFYOR', 'WP:AEIS', 'WP:PSTS', 'WP:SYNTHESIS', 'WP:SYN', 'WP:STICKTOSOURCE', 'WP:NOR', 'WP:No original research');
my @nor = ('WP:UNSOURCED', 'WP:UGS', 'WP:CIRC', 'WP:PAYWALL', 'WP:BURDEN', 'WP:SOURCEACCESS', 'WP:VNOTSUFF', 'WP:NEWSBLOG', 'WP:NOTRS', 'WP:PROVEIT', 'WP:PAYSITE', 'WP:SOURCES', 'WP:EXCEPTIONAL', 'WP:SELFPUB', 'WP:CHALLENGE', 'WP:CIRCULAR', 'Wikipedia:Verifiability', 'WP:V', 'WP:SPS', 'WP:REDFLAG', 'WP:RSUE', 'WP:REFLOOP', 'WP:VER', 'WP:QS', 'WP:BLOGS', 'WP:FAILEDVERIFICATION', 'WP:REDDIT', 'WP:TWITTER', 'WP:YTCOPYRIGHT', 'WP:VERIFY', 'WP:SOURCE', 'WP:SELFPUBLISH', 'WP:SOCIALMEDIA', 'WP:NOENG', 'WP:EXTRAORDINARY', 'WP:ABOUTSELF', 'WP:NOTRELIABLE', 'WP:NONENG', 'WP:ONUS', 'WP:Verifiability');

my @regexes = ();
print "Current empty: @regexes";

foreach ( @npov ) {
    print"$_ \n";
    push @regexes, $_;
};

foreach ( @v ) {
    print"$_ \n";
    push @regexes, $_;
};

foreach ( @nor ) {
    print"$_ \n";
    push @regexes, $_;
};

print "the phrases to now: @regexes \n\n";

foreach ( @regexes ) {
    $regex->add( $_ );
}

my $expression = $regex -> re;
print "REGEX: $expression\n";
