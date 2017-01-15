#!/usr/bin/env perl
#----------------------------------------
#
# An oracle sql/plsql parser
#
#----------------------------------------

use strict;
use warnings;
use 5.010;

use Marpa::R2;
# need at least Marpa::R2 2.076000 for case insensitive L0 rules

use Data::Dumper;

my $dsl = <<'END_OF_DSL';
lexeme default = latm => 1

stmtmulti ::=
    stmtmulti ';' stmt action => ::first
    | stmtmulti ';' action => ::first # handle terminal ; FIXME
    | stmt action => ::first

stmt ::=
    SelectStmt action => print_node

SelectStmt ::=
    SELECT target_list FROM from_list action => make_select
    | SELECT target_list FROM from_list WHERE where_list action => make_select

target_list ::=
    target_list ',' target_el action => append_ident
    | target_el action => ::first

target_el ::=
    a_expr AS IDENT action => ::first
    | a_expr action => ::first

a_expr ::=
    '*' action => make_ident
    | IDENT action => make_ident
    | number action => make_ident


from_list ::=
    from_list ',' from_elem action => append_from
    | from_elem action => ::first

from_elem ::=
    IDENT AS IDENT action => make_ident
    | IDENT action => make_ident
    | '(' SelectStmt ')' AS IDENT action => make_subselect
    | '(' SelectStmt ')' action => make_subselect

where_list ::=
    where_list QUAL_OP qual action => append_qual
    | qual action => ::first

qual ::=
    a_expr OPERATOR a_expr action => make_qual

# keywords
AND     ~ 'AND':ic
AS      ~ 'AS':ic
IS      ~ 'IS':ic
#FALSE   ~ 'FALSE':ic
FROM    ~ 'FROM':ic
NOT     ~ 'NOT':ic
OR      ~ 'OR':ic
SELECT  ~ 'SELECT':ic
#TRUE    ~ 'TRUE':ic
WHERE   ~ 'WHERE':ic

# everything else
number  ~ digits | float
digits  ~ [0-9]+
float   ~ digits '.' digits
        | '.' digits

IDENT   ~ unquoted_start unquoted_chars
        | quoted_ident
unquoted_start ~ [a-zA-Z_]
unquoted_chars ~ [a-zA-Z_0-9]*
quoted_ident ~ '"' quoted_chars '"'
quoted_chars ~ [^"]+

QUAL_OP     ~ AND | OR

OPERATOR    ~ '=' | '<' | '<=' | '>' | '>=' | '%' | IS | IS NOT

:discard ~ whitespace
whitespace ~ [\s]+

END_OF_DSL

my $grammar = Marpa::R2::Scanless::G->new( { source => \$dsl } );

my $input = 'SElect 1 from DUAL; SELECT * from "t1";';
$input .= 'SELECT 1, abc, "DEF" from "toto" as "TATA;";';
#$input .= " SELECT 1, 'test me', t.* from tbl t WHERE 1 > 2 OR b < 3;";
#$input .= " select * from (
#select 1 from dual
#) as t";


my $value_ref = $grammar->parse( \$input, 'plsql2pg' );

sub plsql2pg::make_ident {
    my (undef, $name, undef, $alias) = @_;
    my $idents = [];
    my $ident = make_node('ident');

    $ident->{name} = quote_ident($name);
    $ident->{alias} = quote_ident($alias);

    push(@{$idents}, $ident);

    return $idents;
}

sub plsql2pg::append_ident {
    my (undef, $idents, undef, $ident) = @_;

    push(@{$idents}, @{$ident});

    return $idents;
}

sub plsql2pg::make_qual {
    my (undef, $left, $op, $right) = @_;
    my $quals = [];
    my $qual = make_node('qual');

    $qual->{left} = pop(@{$left});
    $qual->{op} = $op;
    $qual->{right} = pop(@{$right});

    push(@{$quals}, $qual);

    return $quals;
}

sub plsql2pg::append_qual {
    my (undef, $quals, $qualop, $qual) = @_;

    push(@{$quals}, $qualop);
    push(@{$quals}, @{$qual});

    return $quals;
}

sub plsql2pg::append_from {
    my (undef, $froms, undef, $node) = @_;

    push(@{$froms}, @{$node});

    return $froms;
}

sub plsql2pg::make_select {
    my (undef, undef, $targetlist, undef, $fromlist, undef, $wherelist) = @_;
    my $stmts = [];
    my $stmt = make_node('select');

    $stmt->{targetlist} = $targetlist;
    $stmt->{fromlist} = $fromlist;
    $stmt->{wherelist} = $wherelist;

    push(@{$stmts}, $stmt);

    return $stmts;
}

sub plsql2pg::make_subselect {
    my (undef, undef, $stmt, undef, undef, $alias) = @_;
    my $node = pop(@{$stmt});

    $node->{type} = 'subselect';
    $node->{alias} = $alias if defined($alias);

    push(@{$stmt}, $node);

    return $stmt;
}

sub format_select {
    my ($stmt) = @_;
    my $select = undef;
    my $from = undef;
    my $where = undef;
    my $out;

    foreach my $node (@{$stmt->{targetlist}}) {
        $select .= ', ' if defined($select);
        $select .= format_node($node);
    }

    foreach my $node (@{$stmt->{fromlist}}) {
        $from .= ', ' if defined($from);
        $from .= format_node($node);
    }

    foreach my $node (@{$stmt->{wherelist}}) {
        $where .= "WHERE " unless defined($where);
        if (ref($node)) {
            $where .= format_node($node);
        } else {
            $where .= ' ' . $node . ' ';
        }
    }


    $out  = "SELECT $select";
    $out .= " FROM $from" if ($from ne 'dual'); # only if this is the only table
    $out .= " $where" if defined($where);

    return $out;
}

sub format_subselect {
    my ($stmt) = @_;
    my $out = "( ";

    $stmt->{type} = 'select';

    $out .= format_node($stmt);
    $out .= " )";
    $out .= " AS $stmt->{alias}" if defined($stmt->{alias});

    return $out;
}

sub plsql2pg::print_node {
    my (undef, $node) = @_;

    print format_node($node) . ";\n";

}

# Handle ident conversion from oracle to pg
sub quote_ident {
    my ($ident) = @_;

    return undef if not defined($ident);

    if (substr($ident, 0, 1) eq '"') {
        if ((substr($ident, 1, -1) =~ /[a-zA-Z_]/) and (lc($ident) eq $ident)) {
            return lc(substr($ident, 1, -1));
        } else {
            return $ident;
        }
    } else {
        return lc($ident);
    }
}

sub make_node {
    my ($type) = @_;
    my $node = {};

    $node->{type} = $type;

    return $node;
}

sub format_node {
    my ($node) = @_;
    my $func;

    if (ref($node) eq 'ARRAY') {
        my $out;
        foreach my $n (@{$node}) {
            $out .= format_node($n) . " ";
        }

        return $out;
    }

    if (ref($node) ne 'HASH') {
        error("wrong object type", $node);
    }

    $func = "format_" . $node->{type};

    # XXX should I handle every node type explicitely instead?
    no strict;
    return &$func($node);
}

sub format_ident {
    my ($ident) = @_;
    my $out;

    $out = $ident->{name};
    if (defined($ident->{alias})) {
        $out .= " AS $ident->{alias}";
    }

    return $out;
}

sub format_qual {
    my ($qual) = @_;
    my $out;
    my $tmp = $qual->{left};

    $out = format_node($qual->{left}) . ' ' . $qual->{op} . ' '
         . format_node($qual->{right});

    return $out;
}

sub error {
    my ($msg, $node) = @_;

    print "ERROR: $msg\n";
    print Dumper($node);
    exit 1;
}
