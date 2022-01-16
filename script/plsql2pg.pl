#!/usr/bin/perl
#------------------------------------------------------------------------------
# Project  : Multidatabase to PostgreSQL SQL converter
# Name     : sql2pg
# Author   : Julien Rouhaud, julien.rouhaud (AT) free (DOT) fr
# Copyright: Copyright (c) 2016-2022 : Julien Rouhaud - All rights reserved
#------------------------------------------------------------------------------

use strict;
use warnings;

use File::Basename qw(basename dirname);
use Cwd qw(abs_path);
use Getopt::Long;

# XXX remove this when a proper Makefile.pl will be added
use lib dirname(dirname abs_path $0) . '/lib';
use sql2pg;

my %cfg;
my @options = (
    'help|h!',
    'version|v!'
);

sub show_help_and_die {
    my $rc = shift;
    my $program_name = basename( $0 );

    print qq{
Usage: $program_name [options] file.sql

Arguments:

    file.sql must be a text file containing the query to convert.

Options:

    -h | --help         : show this message
    -v | --version      : show the version of $program_name
};

    exit $rc;
}

show_help_and_die (1) unless (GetOptions(\%cfg, @options));
show_help_and_die (0) if ($cfg{help});

if ($cfg{version}) {
    print sql2pg::version() if ($cfg{version});
    exit 0;
}

# XXX handle multiple files input
my $input = $ARGV[0] || '';

if ($input eq '') {
    print "FATAL: you must give an SQL file to convert as argument.\n";
    show_help_and_die (1);
}
if (!-f $ARGV[0]) {
    print "FATAL: \"$input\" does not exists.\n";
    show_help_and_die (1);
}

open(my $fh, $input) or die "FATAL: can not open file $input\n";
my @content = <$fh>;
close($fh);
$input = join('', @content);

my $converter = new sql2pg();
my $translated = $converter->convert($input, 'plsql');

print $translated;

exit 0;
