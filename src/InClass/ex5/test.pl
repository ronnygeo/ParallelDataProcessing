#!/usr/bin/perl
use 5.16.0;
use warnings FATAL => 'all';

use Test::Simple tests => 5;
my @jars = `ls *.jar`;
ok(scalar @jars > 0, "jar file exits");
ok(-e "output", "produces output directory");

sub distance {
    my ($aa, $bb) = @_;
    my $dx = $aa->{x} - $bb->{x};
    my $dy = $aa->{y} - $bb->{y};
    return sqrt($dx * $dx + $dy * $dy);
}

my %points = ();
open my $data, "-|", "bzip2 -cd data.tsv.bz2";
while (<$data>) {
    chomp;
    s/\s+/ /g;
    $points{$_} = 0;
}
close $data;

my @centers = ();
open my $ctrs, "<", "output/part-r-00000";
while (<$ctrs>) {
    my ($cc, undef) = split /\t/;
    my ($xx, $yy, undef) = split /\s+/, $cc;
    push @centers, { x => $xx, y => $yy };
}
close $ctrs;

my $max_count = 0;
my $max_labels = 0;
my $max_bads = 0;

for my $ii (1..5) {
    my %labels = ();
    my $center;
    my $count = 0;
    my $bads = 0;

    open my $outf, "<", "output/part-r-0000$ii";
    while (<$outf>) {
        chomp;
        my ($ctr, $dpt) = split /\t/;
        $center = $ctr;

        my ($cx, $cy, undef) = split /\s+/, $ctr;
        my $ptcc = { x => $cx, y => $cy }; 

        unless (defined $points{$dpt}) {
            say "Bad point: [$dpt]";
            $points{$dpt}++;
        }

        my ($xx, $yy, $lab) = split /\s+/, $dpt;
        $labels{$lab} ||= 0;
        $labels{$lab} += 1;

        $count += 1;

        my $pt = { x => $xx, y => $yy };

        my $cdst = distance($ptcc, $pt);

        for my $cc (@centers) {
            if (distance($cc, $pt) < $cdst) {
                $bads++;
            } 
        }
    }
    close $outf;

    my $labs = scalar(keys(%labels));
    $max_count  = $count if ($count > $max_count);
    $max_bads   = $bads  if ($bads  > $max_bads);
    $max_labels = $labs  if ($labs  > $max_labels);
}

ok($max_count < 30000, "not too many points / group");
ok($max_bads == 0, "no points in wrong cluster");
ok($max_labels < 4, "not too many labels per cluster");

