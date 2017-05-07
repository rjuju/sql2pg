package sql2pg::tsql::grammar;

use strict;
use warnings;
use 5.010;

use Data::Dumper;
use sql2pg::tsql::utils;
use sql2pg::format;
use sql2pg::common;

#$sql2pg::dsl = <<'END_OF_DSL';
sub dsl {
    return <<'END_OF_DSL';
lexeme default = latm => 1

:start ::= stmtmulti

stmtmulti ::=
    stmt* separator => SEPARATOR action => ::array

stmt ::=
    raw_stmt action => call_format_stmts

raw_stmt ::=
    UseStmt
    | CreateStmt
    | AlterStmt

UseStmt ::=
    USE IDENT action => make_usestmt

CreateStmt ::=
    CreateDbStmt

CreateDbStmt ::=
    CREATE DATABASE IDENT action => make_createdb

AlterStmt ::=
    AlterDbStmt

AlterDbStmt ::=
    ALTER DATABASE IDENT SET alter_db_param alter_db_value action => make_alterdb

alter_db_param ::=
    COMPATIBILITY_LEVEL
    | ANSI_WARNINGS
    | ARITHABORT
    | READ_WRITE

alter_db_value ::=
    '=' INTEGER action => second
    | OFF action => make_keyword
    | EMPTY

EMPTY ::= action => ::undef

sign ::=
    '-'
    | '+'
    | EMPTY

INTEGER ::=
    sign integer action => make_number
##     | bindvar action => make_bindvar
##
## FLOAT ::=
##     sign float action => make_number
##     | bindvar action => make_bindvar
##
## NUMBER ::=
##     INTEGER
##     | FLOAT

# keywords
ALTER       ~ 'ALTER':ic
:lexeme     ~ ALTER pause => after event => keyword
ARITHABORT  ~ 'ARITHABORT':ic
ANSI_WARNINGS ~ 'ANSI_WARNINGS':ic
COMPATIBILITY_LEVEL ~ 'COMPATIBILITY_LEVEL':ic
CREATE      ~ 'CREATE':ic
:lexeme     ~ CREATE pause => after event => keyword
DATABASE    ~ 'DATABASE':ic
GO          ~ 'GO':ic
## IS          ~ 'IS':ic
OFF         ~ 'OFF':ic
READ_WRITE  ~ 'READ_WRITE':ic
SEPARATOR   ~ ';' GO
            | GO
:lexeme     ~ SEPARATOR pause => after event => new_query
SET         ~ 'SET':ic
USE         ~ 'USE':ic

# everything else
digits      ~ [0-9]+
integer     ~ digits
##             | digits expcast
## float       ~ digits '.' digits
##             | digits '.' digits expcast
##             | '.' digits
##             | '.' digits expcast
# exponent and type are done in L0 since no whitespace is allowed here
## expcast     ~ exponent
##             | cast
##             | exponent cast
## exponent    ~ 'e' digits
##             | 'E' digits
##             | 'e-' digits
##             | 'E-' digits
##             | 'e+' digits
##             | 'E+' digits
## cast        ~ 'd'
##             | 'D'
##             | 'f'
##             | 'F'

IDENT ::=
    ident '.' ident '.' ident action => make_ident
    | ident '.' ident action => make_ident
    | ident action => make_ident

ident   ~ unquoted_ident
        | quoted_ident
        | bracketed_ident
        # only for a_expr, but assuming original SQL is valid
        | '*'

## ALIAS   ~ unquoted_start unquoted_chars
##         | quoted_ident
##         | bracketed_ident

unquoted_ident ~ unquoted_start unquoted_chars
unquoted_start ~ [a-zA-Z]
unquoted_chars ~ [a-zA-Z_0-9_$#]*

quoted_ident    ~ '"' quoted_chars '"'
quoted_chars    ~ [^"]+

bracketed_ident ~ '[' bracketed_chars ']'
bracketed_chars ~ [^\]]+

# literal         ~ literal_delim literal_chars literal_delim
# literal_delim   ~ [']
# literal_chars   ~ [^']*

## OPERATOR    ~ '=' | '!=' | '<>' | '<' | '<=' | '>' | '>=' | '%'
##             | '+' | '-' | '*' | '/' | '||' | IS

:discard                    ~ discard
discard                     ~ whitespace
whitespace                  ~ [\s]+
:discard                     ~ comment event => add_comment
comment                     ~ dash_comment | c_comment
dash_comment                ~ '--' dash_chars
dash_chars                  ~ [^\n]*
# see https://gist.github.com/jeffreykegler/5011021
c_comment                   ~ '/*' c_comment_interior '*/'
c_comment_interior          ~ o_non_stars o_star_prefixed_segments o_pre_final_stars
o_non_stars                 ~ [^*]*
o_star_prefixed_segments    ~ star_prefixed_segment*
star_prefixed_segment       ~ stars [^/*] o_star_free_text
stars                       ~ [*]+
o_star_free_text            ~ [^*]*
o_pre_final_stars           ~ [*]*


END_OF_DSL
}


sub alias_node {
    my (undef, $node, $alias) = @_;

    assert_one_el($node);

    @{$node}[0]->{alias} = $alias;

    return $node;
}

sub append_el_1_2 {
    my (undef, $nodes, $node) = @_;

    push(@{$nodes}, @{$node});

    return $nodes;
}

sub append_el_1_3 {
    my (undef, $nodes, undef, $node) = @_;

    push(@{$nodes}, @{$node});

    return $nodes;
}

sub call_format_stmts {
    my (undef, $stmts) = @_;

    return format_stmts($stmts, $sql2pg::input);
}

sub combine_parens_select {
    my (undef, $nodes, $raw_op, undef, $stmt, undef) = @_;

    return sql2pg::common::combine_and_parens_select($nodes, $raw_op, $stmt);
}

sub combine_select {
    my (undef, $nodes, $raw_op, $stmt) = @_;

    return sql2pg::common::combine_and_parens_select($nodes, $raw_op, $stmt);
}

sub concat {
    my (undef, @args) = @_;
    my $out;

    foreach my $kw (@args) {
        $out .= ' ' if (defined($out));
        $out .= uc($kw);
    }

    return $out;
}

# Simple function to discard unhandled elements that doesn't need to be
# deparsed
sub discard {
    return 0;
}

sub make_alias {
    my (undef, $as, $alias) = @_;

    return quote_ident(get_alias($as, $alias));
}

sub make_alterdb {
    my (undef, undef, undef, $ident, undef, $param, $val) = @_;
    my $node = make_node('alterobject');

    if (($param eq 'COMPATIBILITY_LEVEL')
        or ($param eq 'ANSI_WARNINGS')
        or ($param eq 'ARITHABORT')
    ) {
        add_fixme("ALTER DATABASE ignored: $param unhandled");
        return;
    }

    $node->{kind} = 'DATABASE';
    $node->{ident} = $ident;
    $node->{param} = $param;
    $node->{val} = $val;

    return node_to_array($node);
}

sub make_createdb {
    my (undef, undef, undef, $db) = @_;
    my $node = make_node('createobject');

    $node->{kind} = 'DATABASE';
    $node->{ident} = $db;

    return node_to_array($node);
}

sub make_ident {
    my (undef, $table, undef, $schema, undef, $attribute) = @_;
    my @atts = ('schema', 'table', 'attribute');
    my $ident = make_node('ident');

    if (defined($attribute)) {
        $ident->{pop(@atts)} = quote_ident($attribute);
    }

    if (defined($schema)) {
        $ident->{pop(@atts)} = quote_ident($schema);
    }

    if (defined($table)) {
        $ident->{pop(@atts)} = quote_ident($table);
    }

    return node_to_array($ident);
}

sub make_keyword {
    my (undef, $kw1, $kw2) = @_;
    my $node = make_node('keyword');

    $node->{val} = uc($kw1);
    $node->{val} .= ' ' . uc($kw2) if (defined($kw2));

    return node_to_array($node);
}

sub make_literal {
    my (undef, $value, $alias) = @_;
    my $literal = make_node('literal');

    $literal->{value} = substr($value, 1, -1); # remove the quotes
    $literal->{alias} = $alias;

    return node_to_array($literal);
}

sub make_number {
    my (undef, $sign, $val) = @_;
    my $number = make_node('number');

    # extract possible float/double type indicator
    if ($val =~ /[df]$/i) {
        $number->{cast} = lc(substr($val, -1));
        $val = substr($val, 0, -1);
    }

    $number->{sign} = $sign;
    $number->{val} .= $val;

    return node_to_array($number);
}

sub make_usestmt {
    my (undef, undef, $ident) = @_;

    add_fixme("Statement USE " . format_node($ident) . " ignored");

    return;
}

sub parens_node {
    my (undef, undef, $node, undef) = @_;

    return _parens_node($node);
}

sub second {
    my (undef, undef, $node) = @_;

    return $node;
}

sub upper {
    my (undef, $a) = @_;

    return uc($a);
}

1;
