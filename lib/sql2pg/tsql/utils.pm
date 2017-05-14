package sql2pg::tsql::utils;
#------------------------------------------------------------------------------
# Project  : Multidatabase to PostgreSQL SQL converter
# Name     : sql2pg
# Author   : Julien Rouhaud, julien.rouhaud (AT) free (DOT) fr
# Copyright: Copyright (c) 2016-2017 : Julien Rouhaud - All rights reserved
#------------------------------------------------------------------------------

require Exporter;

BEGIN {
    @ISA = qw(Exporter);
    @EXPORT = qw(quote_ident);
}


use Data::Dumper;
use sql2pg::format;
use sql2pg::common;


sub handle_datatype {
    my ($node) = @_;
    my $out;
    my $ident = $node->{ident};

    # Handle identity clause
    if ($node->{identity}) {
        assert((not defined $ident->{table}),
            "Column with IDENTITY should not be schema qualified", $node);

        if (lc($ident->{attribute}) eq 'int') {
            $out .= 'serial';
        } elsif (lc($ident->{attribute}) eq 'bigint') {
            $out .= 'bigserial';
        }
    }
    $out = format_node($node->{ident}) unless($out);

    $out .= '(' . format_array($node->{typmod}, ',') . ')' if ($node->{typmod});
    $out .= format_node($node->{nullnotnull}) if ($node->{nullnotnull});

    return $out;
}
# Handle ident conversion from oracle to pg
sub quote_ident {
    my ($ident) = @_;

    return undef if not defined($ident);

    # remove square brackets if any
    if (substr($ident, 0, 1) eq '[') {
        $ident = substr($ident, 1, -1);
    }

    # original ident was quoted
    if (substr($ident, 0, 1) eq '"') {
        if ((substr($ident, 1, -1) =~ /^[a-zA-Z_][a-zA-Z0-9_]*$/) and (lc($ident) eq $ident)) {
            return lc(substr($ident, 1, -1));
        } else {
            return $ident;
        }
    }
    # original ident wasn't quoted, check if we have to quote it
    else {
        if ($ident =~/[\$#]/) {
            return '"' . lc($ident) . '"';
        } else {
            return lc($ident);
        }
    }
}

1;
