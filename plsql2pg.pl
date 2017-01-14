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
    SelectStmt action => ::first

SelectStmt ::=
    SELECT target_list FROM from_list action => selectfromwhere
    | SELECT target_list FROM from_list WHERE where_list action => selectfromwhere

target_list ::=
    target_list ',' target_el action => append_ident
    | target_el action => ::first

target_el ::=
    a_expr AS IDENT action => ::first
    | a_expr action => ::first

a_expr ::=
    '*' action => new_ident
    | IDENT action => new_ident
    | number action => new_ident


from_list ::=
    from_list ',' IDENT action => append_ident
    | IDENT AS IDENT action => new_ident
    | IDENT action => new_ident

where_list ::=
    where_list QUAL_OP qual action => append_qual
    | qual action => ::first

qual ::=
    a_expr OPERATOR a_expr action => new_qual

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

my $input = 'SElect 1 from DUAL; SELECT 1, abc from "toto"; select * from "TOTO" as "TATA";';
$input .= " SELECT 1 from dual WHERE 1 > 2;";
$input .= " SELECT 1, * from dual WHERE 1 > 2 OR b > 2;";


my $value_ref = $grammar->parse( \$input, 'plsql2pg' );

sub plsql2pg::new_ident {
    my (undef, $ident, undef, $alias) = @_;
    my $idents = [];

    push (@{$idents}, make_ident($ident, $alias));
    return $idents;
}

sub plsql2pg::append_ident {
    my (undef, $idents, undef, $ident) = @_;

    push (@{$idents}, @{$ident});

    #if (ref($ident) eq 'ARRAY') {
    #    push (@{$idents}, @{$ident});
    #} else {
    #    push (@{$idents}, make_ident($ident, undef));
    #}

    return $idents;
}

sub plsql2pg::new_qual {
    my (undef, $left, $op, $right) = @_;
    my $quals = [];

    push(@{$quals}, make_qual($left, $op, $right));

    return $quals;
}

sub plsql2pg::append_qual {
    my (undef, $quals, $qualop, $qual) = @_;

    push(@{$quals}, $qualop);
    push (@{$quals}, @{$qual});

    #if (ref($qual) eq 'ARRAY') {
    #    push (@{$quals}, @{$qual});
    #} else {
    #    push (@{$quals}, $qual);
    #}

    return $quals;
}

sub plsql2pg::selectfromwhere {
    my (undef, undef, $targetlist, undef, $fromlist, undef, $wherelist) = @_;
    my $select = undef;
    my $from = undef;
    my $where = undef;

    foreach my $elem (@{$targetlist}) {
        $select .= ', ' if defined($select);
        $select .= format_ident($elem);
    }

    foreach my $elem (@{$fromlist}) {
        $from .= ', ' if defined($from);
        $from .= format_ident($elem);
    }

    foreach my $elem (@{$wherelist}) {
        $where .= "WHERE " unless defined($where);
        if (ref($elem)) {
            $where .= format_qual($elem);
        } else {
            $where .= ' ' . $elem . ' ';
        }
    }


    print "SELECT $select";
    print " FROM $from" if ($from ne 'dual'); # only if this is the only table
    print " $where" if defined($where);
    print ";\n";
}

# Handle ident conversion from oracle to pg
sub quote_ident {
    my ($ident) = @_;

    return undef if not defined($ident);

    if (substr($ident, 0, 1) eq '"') {
        if (uc($ident) ne $ident) {
            return $ident;
        } else {
            return lc(substr($ident, 1, -1));
        }
    } else {
        return lc($ident);
    }
}

sub make_ident {
    my ($name, $alias) = @_;
    my $elem = {};

    $elem->{type} = 'ident';
    $elem->{name} = quote_ident($name);
    $elem->{alias} = quote_ident($alias);

    return $elem;
}

sub make_qual {
    my ($left, $op, $right) = @_;
    my $elem = {};

    $elem->{type} = 'qual';
    $elem->{left} = pop(@{$left});
    $elem->{op} = $op;
    $elem->{right} = pop(@{$right});

    return $elem;
}

sub format_elem {
    my ($elem) = @_;

    if (ref($elem) ne 'HASH') {
        print "Error: wrong object type\n";
        print Dumper($elem);
        exit 1;
    }

    return format_ident($elem) if ($elem->{type} eq 'ident');
    return format_qual($elem) if ($elem->{type} eq 'qual');

    print "Error: unhandled object type $elem->{type}\n";
    exit 1;
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

    $out = format_elem($qual->{left}) . ' ' . $qual->{op} . ' '
         . format_elem($qual->{right});

    return $out;
}
