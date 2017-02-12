#!/usr/bin/perl

use strict;
use warnings;

use File::Basename qw(dirname);
use Cwd qw(abs_path);

use lib dirname(dirname abs_path $0) . '/lib';
use plsql2pg;

my $input = $ARGV[0] || '';
if (!$input || !-f $ARGV[0]) {
    die "FATAL: you must give a DDL/DML file to convert as argument.\n";
}
open(my $fh, $input) or die "FATAL: can not open file $input\n";
my @content = <$fh>;
close($fh);
$input = join('', @content);

my $converter = new plsql2pg();
my $translated = $converter->convert($input);

print $translated;

exit 0;
