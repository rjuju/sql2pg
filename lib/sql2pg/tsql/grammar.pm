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
    CombinedSelectStmt
    | IgnoredStmt
    | CreateStmt
    | AlterStmt
    | ExecSql

CombinedSelectStmt ::=
    SelectStmt order_clause action => append_orderbyclause

SelectStmt ::=
    '(' SelectStmt ')' action => parens_node
    | SelectStmt combine_op SelectStmt action => combine_select
    | SingleSelectStmt

combine_op ::=
    UNION
    | UNION ALL action => concat
    | INTERSECT
    | MINUS

IgnoredStmt ::=
    UseStmt
    | SetStmt

UseStmt ::=
    USE IDENT action => make_usestmt

SetStmt ::=
    SET set_param ON_OFF action => discard

set_param ::=
    ANSI_NULLS
    | ANSI_PADDING
    | QUOTED_IDENTIFIER

CreateStmt ::=
    CreateDbStmt
    | CreateRoleStmt
    | CreateSchema

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
    | ON_OFF action => make_keyword
    | EMPTY

ON_OFF ::=
    ON action => make_keyword
    | OFF action => make_keyword

CreateRoleStmt ::=
    INE CREATE ROLE IDENT action => make_createrole

CreateSchema ::=
    CREATE SCHEMA IDENT authorization_clause action => make_createschema

authorization_clause ::=
    AUTHORIZATION IDENT action => second
    | EMPTY

ExecSql ::=
    INE EXEC executesql LITERAL_DELIM raw_stmt LITERAL_DELIM action => extract_sql

SingleSelectStmt ::=
    SELECT select_clause from_clause where_clause action => make_select

select_clause ::=
    target_list action => make_selectclause

target_list ::=
    node_target_list action => make_target_list

node_target_list ::=
    node_target_list ',' alias_target_el action => append_el_1_3
    | alias_target_el

alias_target_el ::=
    target_el ALIAS_CLAUSE action => alias_node

unalias_target_list ::=
    unalias_node_target_list action => make_target_list

unalias_node_target_list ::=
    target_list ',' target_el action => append_el_1_3
    | target_el

# aliasing is done in a different rule, so target_el can be used in qual rule,
# to allow function in quals without ambiguity (having function in a_expr is
# ambiguous)
target_el ::=
    parens_target_el
    | no_parens_target_el

parens_target_el ::=
    '(' no_parens_target_el ')' action => parens_node
    # prune extra parens in the grammar
    | '(' parens_target_el ')' action => second

no_parens_target_el ::=
    target_el OPERATOR target_el action => make_opexpr
    | target_el like_clause action => make_likeexpr
    | simple_target_el
    | '(' target_list ')' action => parens_node
    | '(' SelectStmt ')' action => parens_node
    | qual_list action => make_target_qual_list

simple_target_el ::=
    a_expr

like_clause ::=
    LIKE target_el action => make_like
    | LIKE target_el ESCAPE LITERAL action => make_like

from_clause ::=
    FROM from_list action => make_fromclause

from_list ::=
    from_list ',' from_elem action => append_el_1_3
    | from_elem

from_elem ::=
    aliased_simple_from_elem join_clause action => append_joinlist

aliased_simple_from_elem ::=
    simple_from_elem ALIAS_CLAUSE action => alias_node

simple_from_elem ::=
    IDENT
    | '(' SelectStmt ')' action => make_subquery
    # ONLY is not valid for DELETE, assume original query is valid
    | ONLY '(' simple_from_elem ')' action => make_fromonly
    | '(' subjoin ')' action => second

subjoin ::=
    subjoin_ident join_list action => make_subjoin
    | '(' subjoin ')' action => second

subjoin_ident ::=
    ALIASED_IDENT
    | '(' subjoin ')' action => second

join_clause ::=
    join_list
    | EMPTY action => ::undef

join_list ::=
    join_list join_elem action => append_el_1_2
    | join_elem

join_elem ::=
    join_type JOIN join_ident ALIAS_CLAUSE join_cond action => make_normaljoin
    | special_join_type JOIN join_ident ALIAS_CLAUSE action => make_specialjoin

join_ident ::=
    IDENT
    | '(' SelectStmt ')' action => make_subquery

join_type ::=
    normal_join_type
    | EMPTY action => make_jointype

normal_join_type ::=
    INNER action => make_jointype
    | LEFT action => make_jointype
    | LEFT OUTER action => make_jointype
    | RIGHT action => make_jointype
    | RIGHT OUTER action => make_jointype
    | FULL action => make_jointype
    | FULL OUTER action => make_jointype

special_join_type ::=
    NATURAL action => make_jointype
    | NATURAL normal_join_type action => concat
    | CROSS action => make_jointype

join_cond ::=
    USING '(' using_list ')' action => make_joinusing
    | ON qual_list action => make_joinon

using_list ::=
    parens_using_list
    | '(' using_list ')' action => second

parens_using_list ::=
    parens_using_list ',' using_el action => append_el_1_3
    | using_el

using_el ::=
    a_expr ALIAS_CLAUSE action => alias_node

where_clause ::=
    WHERE qual_list action => make_whereclause
    | EMPTY action => ::undef

qual_list ::=
    qual_list_no_parens
    | qual_list_with_parens

qual_list_with_parens ::=
    '(' qual_list_no_parens ')' action => parens_node
    | '(' qual_list_with_parens ')' action => second

qual_list_no_parens ::=
    qual_list qual_op qual action => append_qual
    | qual_no_parens

qual_op ::=
    AND action => upper
    | OR action => upper

# use a specific G1 rule, the NOT IN seems to conflict with IS NOT operator
qual_inop ::=
    IN
    | NOT IN action => concat

qual_exists ::=
    EXISTS action => upper
    | NOT EXISTS action => concat

qual_no_parens ::=
    qual_elem OPERATOR qual_elem action => make_qual
    | qual_elem qual_inop qual_elem action => make_qual
    | qual_exists '(' SelectStmt ')' action => make_existsqual
    | qual_elem like_clause action => make_likeexpr
    | qual_elem BETWEEN qual_elem AND qual_elem action => make_betweenqual

qual_elem ::=
    target_el
    | '(' unalias_target_list ')' action => parens_node
    | '(' SelectStmt ')' action => parens_node

qual ::=
    qual_no_parens
    | '(' qual_list ')' action => parens_node

order_clause ::=
    ORDER BY order_list action => make_orderbyclause
    | EMPTY action => ::undef

order_list ::=
    simple_order_list
    | '(' simple_order_list ')' action => second

simple_order_list ::=
    simple_order_list ',' order_elem action => append_el_1_3
    | order_elem

order_elem ::=
    a_expr ordering nulls_pos action => make_orderby

ordering ::=
    ASC
    | DESC
    | EMPTY action => ::undef

nulls_pos ::=
    NULLS FIRST action => concat
    | NULLS LAST action => concat
    | EMPTY action => ::undef




a_expr ::=
    IDENT
    | NUMBER
    | LITERAL
    | NULL action => make_keyword
    # use "NOT NULL" as a_expr instead of using "IS NOT" as an operator avoid
    # ambiguity (otherwise NOT would be considered as an ident)
    | NOT NULL action => make_keyword



ALIAS_CLAUSE ::=
    AS ALIAS action => make_alias
    | ALIAS action => make_alias
    | EMPTY action => ::undef

EMPTY ::= action => ::undef

IDENT ::=
    ident '.' ident '.' ident action => make_ident
    | ident '.' ident action => make_ident
    | ident action => make_ident

ALIASED_IDENT ::=
    IDENT ALIAS_CLAUSE action => alias_node

LITERAL ::=
    literal action => make_literal
    | bindvar action => make_bindvar

sign ::=
    '-'
    | '+'
    | EMPTY

INTEGER ::=
    sign integer action => make_number
    | bindvar action => make_bindvar

FLOAT ::=
    sign float action => make_number
    | bindvar action => make_bindvar

NUMBER ::=
    INTEGER
    | FLOAT

INE ::=
    # for now, just ignore the stmt and assume it's the intended query for a
    # postgres' IF NOT EXISTS utlity statement
    IF NOT EXISTS '(' SingleSelectStmt ')'
    | EMPTY

LITERAL_DELIM ::=
    [']
    | 'N' [']

# keywords
ALTER       ~ 'ALTER':ic
:lexeme     ~ ALTER pause => after event => keyword
ALL         ~ 'ALL':ic
AND         ~ 'AND':ic
ARITHABORT  ~ 'ARITHABORT':ic
ANSI_NULLS  ~ 'ANSI_NULLS':ic
ANSI_PADDING  ~ 'ANSI_PADDING':ic
ANSI_WARNINGS ~ 'ANSI_WARNINGS':ic
AS          ~ 'AS':ic
ASC         ~ 'ASC':ic
AUTHORIZATION ~ 'AUTHORIZATION':ic
BETWEEN     ~ 'BETWEEN':ic
BY          ~ 'BY':ic
COMPATIBILITY_LEVEL ~ 'COMPATIBILITY_LEVEL':ic
CREATE      ~ 'CREATE':ic
:lexeme     ~ CREATE pause => after event => keyword
CROSS       ~ 'CROSS':ic
DATABASE    ~ 'DATABASE':ic
DESC        ~ 'DESC':ic
ESCAPE      ~ 'ESCAPE':ic
EXEC        ~ 'EXEC':ic
EXISTS      ~ 'EXISTS':ic
FIRST       ~ 'FIRST':ic
FROM        ~ 'FROM':ic
FULL        ~ 'FULL':ic
GO          ~ 'GO':ic
JOIN        ~ 'JOIN':ic
IF          ~ 'IF':ic
IN          ~ 'IN':ic
INNER       ~ 'INNER':ic
INTERSECT   ~ 'INTERSECT':ic
## IS          ~ 'IS':ic
LAST        ~ 'LAST':ic
LEFT        ~ 'LEFT':ic
LIKE        ~ 'LIKE':ic
MINUS       ~ 'MINUS':ic
NATURAL     ~ 'NATURAL':ic
NOT         ~ 'NOT':ic
NULL        ~ 'NULL':ic
NULLS       ~ 'NULLS':ic
OFF         ~ 'OFF':ic
ON          ~ 'ON':ic
ONLY        ~ 'ONLY':ic
OR          ~ 'OR':ic
ORDER       ~ 'ORDER':ic
OUTER       ~ 'OUTER':ic
QUOTED_IDENTIFIER ~ 'QUOTED_IDENTIFIER':ic
READ_WRITE  ~ 'READ_WRITE':ic
RIGHT       ~ 'RIGHT':ic
ROLE        ~ 'ROLE':ic
SCHEMA      ~ 'SCHEMA':ic
SELECT      ~ 'SELECT':ic
SEPARATOR   ~ ';' GO
            | GO
:lexeme     ~ SEPARATOR pause => after event => new_query
SET         ~ 'SET':ic
USE         ~ 'USE':ic
UNION       ~ 'UNION':ic
USING       ~ 'USING':ic
WHERE       ~ 'WHERE':ic

# everything else
digits      ~ [0-9]+
integer     ~ digits
#             | digits expcast
float       ~ digits '.' digits
#            | digits '.' digits expcast
            | '.' digits
#            | '.' digits expcast
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

ALIAS   ~ unquoted_start unquoted_chars
        | quoted_ident
        | bracketed_ident

unquoted_ident ~ unquoted_start unquoted_chars
unquoted_start ~ [a-zA-Z]
unquoted_chars ~ [a-zA-Z_0-9_$#]*

quoted_ident    ~ '"' quoted_chars '"'
quoted_chars    ~ [^"]+

bracketed_ident ~ '[' bracketed_chars ']'
bracketed_chars ~ [^\]]+

ident   ~ unquoted_ident
        | quoted_ident
        | bracketed_ident
        # only for a_expr, but assuming original SQL is valid
        | '*'

bindvar ~ '@' unquoted_ident

executesql ~ 'sys.sp_executesql'

literal         ~ literal_delim literal_chars literal_delim
                | 'N' literal_delim literal_chars literal_delim
literal_delim   ~ [']
literal_chars   ~ [^']*

OPERATOR    ~ '=' | '!=' | '<>' | '<' | '<=' | '>' | '>=' | '%'
            | '+' | '-' | '*' | '/' | '||' # | IS

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

sub append_joinlist {
    my (undef, $from, $joins, $pivot) = @_;

    return $from unless (defined($joins));

    push(@{$from}, @{$joins});

    return $from;
}

sub append_orderbyclause {
    my (undef, $stmt, $clause) = @_;

    push(@{$stmt}, $clause) if (defined($clause));

    return $stmt;
}

sub append_qual {
    my (undef, $quals, $qualop, $qual) = @_;

    push(@{$quals}, $qualop);
    push(@{$quals}, @{$qual});

    return $quals;
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

sub extract_sql {
    my (undef, $ine, undef, undef, undef, $stmt, undef) = @_;

    assert_one_el($stmt);

    @{$stmt}[0]->{ine} = 1 if ($ine);

    return $stmt;
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

sub make_betweenqual {
    my (undef, $left, undef, $right, undef, $right2) = @_;
    my $qual = make_node('qual');

    $qual->{left} = pop(@{$left});
    $qual->{op} = 'BETWEEN';
    $qual->{right} = pop(@{$right});
    $qual->{op2} = 'AND';
    $qual->{right2} = pop(@{$right2});

    return node_to_array($qual);
}

sub make_bindvar {
    my (undef, $var) = @_;
    my $node = make_node('bindvar');

    # Conversion to numbered parameter will be done on formatting: there's no
    # guarantee that every bindvar will be used, so we can't do the conversion
    # now. Instead save the original bindvar name in a hash
    $node->{var} = $var;

    # Also, declare by default this bindvar as useless.  This way, we can warn
    # automatically about all bindvars that won't be used (this function will
    # take care of not declaring useless a bindvar that was already used in the
    # current statement).
    useless_bindvar($node);

    return node_to_array($node);
}

sub make_createdb {
    my (undef, undef, undef, $db) = @_;
    my $node = make_node('createobject');

    $node->{kind} = 'DATABASE';
    $node->{ident} = $db;

    return node_to_array($node);
}

sub make_createrole {
    my (undef, $ine, undef, undef, $ident) = @_;
    my $node = make_node('createobject');

    # ignore INE
    $node->{kind} = 'ROLE';
    $node->{ident} = $ident;
    $node->{ine} = $ine;

    return node_to_array($node);
}

sub make_createschema {
    my (undef, undef, undef, $ident, $auth) = @_;
    my $node = make_node('createobject');

    $node->{kind} = 'SCHEMA';
    $node->{ident} = $ident;
    $node->{auth} = $auth;

    return node_to_array($node);
}

sub make_existsqual {
    my (undef, $op, undef, $subq, undef) = @_;
    my $qual = make_node('qual');
    my $tmp = pop(@{$subq});

    $qual->{op} = $op;
    if (isA($tmp, 'parens')) {
        $qual->{right} = $tmp;
    } else {
        # make sure there's a parens node, without writing too much grammar
        $qual->{right} = _parens_node($tmp);
    }

    return node_to_array($qual);
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

sub make_joinon {
    my (undef, undef, $quallist) = @_;
    my $node = make_node('join_on');

    $node->{quallist} = $quallist;
    return $node;
}

sub make_jointype {
    my (undef, $kw1, $kw2) = @_;

    return 'INNER' if (not defined($kw1));

    $kw1 = uc($kw1);
    $kw2 = uc($kw2) if defined($kw2);

    # just a personal preference
    return 'FULL OUTER' if ($kw1 eq 'FULL');
    return $kw1;
}

sub make_joinusing {
    my (undef, undef, undef, $targetlist) = @_;
    my $node = make_node('using');

    $node->{content} = $targetlist;
    return $node;
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

sub make_normaljoin {
    my (undef, $partitionby, $jointype, undef, $ident, $alias, $cond) = @_;

    if (defined($partitionby)) {
        add_fixme('Partition clause ignored for outer join on table '
                 . format_node($ident)
                 .': ' . format_node($partitionby));
    }

    return make_join($jointype, $ident, $alias, $cond);
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

sub make_orderby {
    my (undef, $elem, $order, $nulls) = @_;
    my $orderby = make_node('orderby');

    assert_one_el($elem);

    if (not defined($order)) {
        $order = 'ASC' unless defined($order);
    } else {
        $order = uc($order);
    }

    $orderby->{elem} = pop(@{$elem});
    $orderby->{order} = $order;
    $orderby->{nulls} = $nulls;

    return node_to_array($orderby);
}

sub make_orderbyclause {
    my (undef, undef, $orderbys) = @_;

    return make_clause('ORDERBY', $orderbys);
}

sub make_qual {
    my (undef, $left, $op, $right) = @_;
    my $qual = make_node('qual');

    # uc the operator in case it has alpha char (IN, IS...)
    $qual->{op} = uc($op);

    $qual->{left} = pop(@{$left});
    $qual->{right} = pop(@{$right});

    return node_to_array($qual);
}

sub make_select {
    my (undef, undef, @args) = @_;
    my $stmt = make_node('select');
    my $token;

    while (scalar @args > 0) {
        $token = shift(@args);
        $stmt->{$token->{type}} = $token if defined($token);
    }

    $stmt->{hook} = undef;

    return node_to_array($stmt);
}

sub make_selectclause {
    my (undef, $tlist) = @_;

    return make_clause('SELECT', $tlist);
}

sub make_specialjoin {
    my (undef, $jointype, undef, $ident, $alias, $cond) = @_;

    return make_join($jointype, $ident, $alias, $cond);
}

sub make_subjoin {
    my (undef, $ident, $list) = @_;
    my $node = make_node('subjoin');
    my $clause;

    $node->{ident} = $ident;
    $node->{joins} = $list;

    $clause = make_clause('SUBQUERY', $node);

    return node_to_array($clause);
}

sub make_subquery {
    my (undef, undef, $stmts, undef, $alias) = @_;
    my $clause;
    my $node = make_node('subquery');

    $node->{alias} = $alias;
    $node->{stmts} = $stmts;
    $clause = make_clause('SUBQUERY', $node);

    return node_to_array($clause);
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

sub make_fromclause {
    my (undef, undef, $froms) = @_;

    return make_clause('FROM', $froms);
}

sub make_fromonly {
    my (undef, undef, undef, $node, undef) = @_;
    my $only = make_node('from_only');

    $only->{node} = $node;

    return node_to_array($only);
}

sub make_like {
    my (undef, undef, $like, undef, $escape) = @_;
    my $out = 'LIKE ' . format_node($like);

    assert_one_el($escape) if (defined($escape));

    $out .= ' ESCAPE ' . format_node(pop(@{$escape})) if (defined($escape));

    return $out;
}

sub make_likeexpr {
    my (undef, $el, $like) = @_;
    my $node = make_node('likeexpr');

    $node->{el} = $el;
    $node->{like} = $like;

    return node_to_array($node);
}

sub make_opexpr {
    my (undef, $left, $op, $right) = @_;

    return make_node_opexpr($left, $op, $right);
}

sub make_target_qual_list {
    my (undef, $quals) = @_;
    my $node = make_node('target_quallist');

    $node->{quallist} = $quals;

    return node_to_array($node);
}

sub make_target_list {
    my (undef, $tlist) = @_;
    my $node = make_node('target_list');

    $node->{tlist} = $tlist;

    return $node;
}

sub make_whereclause {
    my (undef, undef, $quals) = @_;
    my $quallist = make_node('quallist');

    $quallist->{quallist} = $quals;

    return make_clause('WHERE', $quallist);
}

sub upper {
    my (undef, $a) = @_;

    return uc($a);
}

1;
