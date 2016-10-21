#!/usr/bin/perl
use 5.16.0;
use warnings FATAL => 'all';

my $K = 5;  # Groups
my $N = 10000;  # Average points per group

# Shuffle an array
sub shuffle {
    my ($xs) = @_;
    my @tmp = @$xs;
    @$xs = ();

    while (scalar @tmp) {
        my $ii = int(rand(scalar @tmp));
        push @$xs, splice(@tmp, $ii, 1); 
    }
}

# Get colors
open my $rgb, "<", "/usr/share/X11/rgb.txt";
my @colors = ();
while (<$rgb>) {
    chomp;
    next if /^!/;
    my (undef, $color) = split /\t+/, $_, 2;
    next unless $color;
    next if $color =~ /\s/;
    next if $color =~ /\d/;
    push @colors, $color;
}
shuffle(\@colors);
close $rgb;

my @points = ();
for (my $cc = 0; $cc < $K; ++$cc) {
    my $cX = 200 + rand(600);
    my $cY = 200 + rand(600);

    my $nn = ($N/2) + int(rand($N));

    for (my $ii = 0; $ii < $nn; ++$ii) {
        my $xx = $cX + rand(100) - 50;
        my $yy = $cY + rand(100) - 50;
       
        push @points, {
            x => $xx,
            y => $yy,
            c => $colors[$cc],
        };
    }
}

#shuffle(\@points);

for my $pp (@points) {
    printf("%.02f\t%.02f\t%s\n", $pp->{x}, $pp->{y}, $pp->{c});
}
