package sql2pg;
#------------------------------------------------------------------------------
# Project  : Multidatabase to PostgreSQL SQL converter
# Name     : sql2pg
# Author   : Julien Rouhaud, julien.rouhaud (AT) free (DOT) fr
# Copyright: Copyright (c) 2016-2017 : Julien Rouhaud - All rights reserved
#------------------------------------------------------------------------------

require Exporter;

BEGIN {
    @ISA = qw(Exporter);
    @EXPORT = qw(inc_stmtno get_comment);
}

use strict;
use warnings;
use 5.010;

use Marpa::R2;
# need at least Marpa::R2 2.076000 for case insensitive L0 rules

use Data::Dumper;
use sql2pg::format;
use sql2pg::common;
use sql2pg::plsql::grammar;
use sql2pg::tsql::grammar;

my $VERSION = '0.1';
our $input;

# Ugly way to try to preserve comments: keep track of statement count, and push
# all found comments in the according hash element.
my %comments;
my $stmtno = 1;

my $query_started = 0;

sub new {
    my ($class, %options) = @_;

    # This create an OO perl object
    my $self = {};
    bless ($self, $class);

    # Initialize this object
    $self->init(%options);

    return($self);
}

sub init {
    my ($self, %options) = @_;
}

sub convert {
    my ($self, $sql, $lang) = @_;
    my $dsl;
    my $func = "sql2pg::" . $lang . "::grammar::dsl";

    no strict;
    $dsl = &$func();
    use strict;

    $input = $sql;

    my $grammar = Marpa::R2::Scanless::G->new( {
        default_action => '::first',
        source => \$dsl
    } );

    my $slr = Marpa::R2::Scanless::R->new( {
        grammar => $grammar,
        semantics_package => 'sql2pg::' . $lang . '::grammar'
    } );

    my $length = length $input;
    my $pos = $slr->read( \$input );

    READ: while(1) {
        for my $event ( @{ $slr->events() } ) {
            my ($name, $start, $end, undef) = @{$event};

            if ($name eq 'new_query') {
                inc_stmtno();
                $query_started = 0;
            } elsif ($name eq 'keyword') {
                $query_started = 1;
            } else {
                if ($query_started) {
                    push(@{$comments{$stmtno}{fixme}}, $event);
                } else {
                    push(@{$comments{$stmtno}{ok}}, $event);
                }
            }
        }

        if ($pos < $length) {
            $pos = $slr->resume();
            next READ;
        }
        last READ;
    }

    # set the counter to 0, format_stmt() will increment it at its beginning
    $stmtno = 0;

    # Grammar is ambiguous at least for function_arg rule and nested function
    # calls.  Don't complain before this is fixed
    #if ( my $ambiguous_status = $slr->ambiguous() ) {
    #    chomp $ambiguous_status;
    #    die "Parse is ambiguous\n", $ambiguous_status;
    #}

    my $value_ref = $slr->value;
    my $value = ${$value_ref};

    # Append any comment found after final query
    inc_stmtno();

    if (defined($comments{$stmtno}{ok})) {
        my $comment = format_comments(get_comment('ok'));
        push (@{$value}, $comment) if ($comment ne '');
    }

    return join('', @{$value});
}

sub inc_stmtno {
    $stmtno++;

    return $stmtno;
}

sub get_comment {
    my ($kind) = @_;

    return $comments{$stmtno}{$kind};
}

sub version {
    return "sql2pg version $VERSION\n";
}

sub version_num {
    return $VERSION;
}

1;
