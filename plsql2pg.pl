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

my $alias_gen = 0;

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
    SELECT select_clause from_clause join_clause
        where_clause group_clause order_clause action => make_select

ALIAS_CLAUSE ::=
    AS ALIAS action => make_alias
    | ALIAS action => make_alias
    | EMPTY action => ::undef

EMPTY ::= action => ::undef

select_clause ::=
    target_list action => make_selectclause

target_list ::=
    target_list ',' target_el action => append_ident
    | target_el action => ::first

target_el ::=
    a_expr ALIAS_CLAUSE action => alias_node
    | function ALIAS_CLAUSE action => alias_node

a_expr ::=
    IDENT action => ::first
    | number action => make_ident
    | LITERAL action => make_literal

function ::=
    IDENT '(' function_args ')' action => make_function

function_args ::=
    function_args ',' function_arg action => append_function_arg
    | function_arg action => ::first

function_arg ::=
    a_expr action => ::first
    | function action => ::first

from_clause ::=
    FROM from_list action => make_fromclause

from_list ::=
    from_list ',' from_elem action => append_from
    | from_elem action => ::first

from_elem ::=
    IDENT ALIAS_CLAUSE action => alias_node
    | '(' SelectStmt ')' ALIAS_CLAUSE action => make_subquery

join_clause ::=
    join_list action => make_joinclause
    | EMPTY action => ::undef

join_list ::=
    join_list join_elem action => append_join
    | join_elem action => ::first

join_elem ::=
    join_type JOIN IDENT ALIAS_CLAUSE join_cond action => make_join

join_type ::=
    INNER action => make_jointype
    | LEFT action => make_jointype
    | LEFT OUTER action => make_jointype
    | RIGHT action => make_jointype
    | RIGHT OUTER action => make_jointype
    | FULL OUTER action => make_jointype
    | EMPTY action => make_jointype

join_cond ::=
    USING '(' target_list ')' action => make_joinusing
    | ON qual_list action => make_joinon

where_clause ::=
    WHERE qual_list action => make_whereclause
    | EMPTY action => ::undef

qual_list ::=
    qual_list QUAL_OP qual action => append_qual
    | qual action => ::first

IDENT ::=
    ident '.' ident '.' ident '.' ident action => make_ident
    | ident '.' ident '.' ident action => make_ident
    | ident '.' ident action => make_ident
    | ident action => make_ident

qual ::=
    a_expr OPERATOR a_expr join_op action => make_qual

join_op ::=
    '(+)' action => ::first
    | EMPTY action => ::undef

group_clause ::=
    GROUP BY group_list action => make_groupbyclause
    | EMPTY action => ::undef

group_list ::=
    group_list ',' group_elem action => append_groupby
    | group_elem action => ::first

group_elem ::=
    IDENT action => make_groupby

order_clause ::=
    ORDER BY order_list action => make_orderbyclause
    | EMPTY action => ::undef

order_list ::=
    order_list ',' order_elem action => append_orderby
    | order_elem action => ::first

order_elem ::=
    a_expr ordering action => make_orderby

ordering ::=
    ASC action => ::first
    | DESC action => ::first
    | EMPTY action => ::undef

# keywords
AND     ~ 'AND':ic
AS      ~ 'AS':ic
ASC     ~ 'ASC':ic
BY      ~ 'BY':ic
DESC    ~ 'DESC':ic
INNER   ~ 'INNER':ic
IS      ~ 'IS':ic
#FALSE   ~ 'FALSE':ic
FULL    ~ 'FULL':ic
FROM    ~ 'FROM':ic
GROUP   ~ 'GROUP':ic
JOIN    ~ 'JOIN':ic
LEFT    ~ 'LEFT':ic
:lexeme ~ LEFT priority => 1
NOT     ~ 'NOT':ic
OR      ~ 'OR':ic
ORDER   ~ 'ORDER':ic
ON      ~ 'ON':ic
OUTER   ~ 'OUTER':ic
RIGHT   ~ 'RIGHT':ic
:lexeme ~ RIGHT priority => 1
SELECT  ~ 'SELECT':ic
USING   ~ 'USING':ic
#TRUE    ~ 'TRUE':ic
WHERE   ~ 'WHERE':ic

# everything else
number  ~ digits | float
digits  ~ [0-9]+
float   ~ digits '.' digits
        | '.' digits

ident   ~ unquoted_start unquoted_chars
        | quoted_ident
        # only for a_expr, but assuming original SQL is valid
        | '*'
ALIAS   ~ unquoted_start unquoted_chars
        | quoted_ident
unquoted_start ~ [a-zA-Z_]
unquoted_chars ~ [a-zA-Z_0-9]*
quoted_ident ~ '"' quoted_chars '"'
quoted_chars ~ [^"]+

LITERAL         ~ literal_delim literal_chars literal_delim
literal_delim   ~ [']
literal_chars   ~ [^']*

QUAL_OP     ~ AND | OR

OPERATOR    ~ '=' | '<' | '<=' | '>' | '>=' | '%' | IS | IS NOT

:discard ~ whitespace
whitespace ~ [\s]+

END_OF_DSL

my $grammar = Marpa::R2::Scanless::G->new( { source => \$dsl } );

my $input = <<'SAMPLE_QUERIES';
SElect 1 nb from DUAL; SELECT * from TBL t order by a, b desc, tbl.c asc;
SELECT nvl(val, 'null') "vAl",1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1, 'test me', t.* from tbl t WHERE 1 > 2 OR b < 3 GROUP BY a, t.b;
 select * from (
select 1 from dual
);
select * from a,c join b using (id,id2) left join d using (id);
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2;
select round(sum(count(*)), 2), 1 from a,b t1 where a.id = t1.id(+);
SAMPLE_QUERIES


print "Original:\n---------\n" . $input . "\n\nConverted:\n----------\n";
my $value_ref = $grammar->parse( \$input, 'plsql2pg' );

sub plsql2pg::make_alias {
    my (undef, $as, $alias) = @_;

    return get_alias($as, $alias);
}

sub plsql2pg::make_ident {
    my (undef, $db, undef, $table, undef, $schema, undef, $attribute) = @_;
    my $idents = [];
    my @atts = ('db', 'schema', 'table', 'attribute');
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

    if (defined($db)) {
        $ident->{pop(@atts)} = quote_ident($db);
    }

    push(@{$idents}, $ident);

    return $idents;
}

sub plsql2pg::append_ident {
    my (undef, $idents, undef, $ident) = @_;

    push(@{$idents}, @{$ident});

    return $idents;
}

sub plsql2pg::make_literal {
    my (undef, $value, $alias) = @_;
    my $literals = [];
    my $literal = make_node('literal');

    $literal->{value} = $value;
    $literal->{alias} = $alias;

    push(@{$literals}, $literal);

    return $literals;
}

sub plsql2pg::make_function {
    my (undef, $ident, undef, $args, undef) = @_;
    my $nodes = [];
    my $func = make_node('function');

    $func->{ident} = $ident;
    $func->{args} = $args;
    push(@{$nodes}, $func);

    return $nodes;
}

sub plsql2pg::append_function_arg {
    my (undef, $args, undef, $arg, undef) = @_;

    push(@{$args}, @{$arg});

    return $args;
}

sub format_function {
    my ($func) = @_;
    my $out = undef;
    my $ident;

    foreach my $arg (@{$func->{args}}) {
        $out .= ', ' if defined($out);
        $out .= format_node($arg);
    }

    $ident = pop(@{$func->{ident}});
    $ident->{attribute} = translate_function_name($ident->{attribute});
    $out = format_ident($ident) . '(' . $out . ')';
    $out .= format_alias($func->{alias});

    return $out;
}

sub plsql2pg::make_selectclause {
    my (undef, $tlist) = @_;

    return make_clause('SELECT', $tlist);
}

sub plsql2pg::make_fromclause {
    my (undef, undef, $froms) = @_;

    return make_clause('FROM', $froms);
}

sub plsql2pg::make_whereclause {
    my (undef, undef, $quals) = @_;
    my $quallist = make_node('quallist');

    $quallist->{quallist} = $quals;

    return make_clause('WHERE', $quallist);
}

sub plsql2pg::make_qual {
    my (undef, $left, $op, $right, $join_op) = @_;
    my $quals = [];
    my $qual = make_node('qual');

    $qual->{left} = pop(@{$left});
    $qual->{op} = $op;
    $qual->{right} = pop(@{$right});
    $qual->{join_op} = $join_op;

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
    my (undef, undef, @args) = @_;
    my $stmts = [];
    my $stmt = make_node('select');
    my $token;

    while (scalar @args > 0) {
        $token = shift(@args);
        $stmt->{$token->{type}} = $token if defined($token);
    }

    push(@{$stmts}, $stmt);

    return $stmts;
}

sub plsql2pg::make_subquery {
    my (undef, undef, $stmt, undef, $alias) = @_;
    my $clause;

    error("Node should contain only one item", $stmt) if (scalar @{$stmt} != 1);

    @{$stmt}[0]->{alias} = $alias;
    $clause = make_clause('subquery', $stmt);

    #return $stmt;
    return $clause;
}

sub plsql2pg::alias_node {
    my (undef, $node, $alias) = @_;

    if (scalar @{$node} != 1) {
        error("Node should contain only one item", $node);
    }

    @{$node}[0]->{alias} = $alias;

    return $node;
}

sub plsql2pg::make_joinclause {
    my (undef, $joins) = @_;

    return make_clause('JOIN', $joins);
}

sub plsql2pg::make_join {
    my (undef, $jointype, undef, $ident, $alias, $cond) = @_;
    my $join = make_node('join');
    my $joins = [];

    $join->{jointype} = $jointype;
    $join->{ident} = pop(@{$ident});
    $join->{ident}->{alias} = $alias;
    $join->{cond} = $cond;

    push (@{$joins}, $join);

    return $joins;
}

sub plsql2pg::append_join {
    my (undef, $joins, $join) = @_;

    push(@{$joins}, $join);

    return $joins;
}

sub plsql2pg::make_jointype {
    my (undef, $kw1, $kw2) = @_;

    return 'INNER' if (not defined($kw1));

    $kw1 = uc($kw1);
    $kw2 = uc($kw2);

    return 'INNER' if ($kw1 eq 'INNER');
    return 'LEFT' if ($kw1 eq 'LEFT');
    return 'RIGHT' if ($kw1 eq 'RIGHT');
    return 'FULL OUTER' if ($kw1 eq 'FULL') and ($kw2 eq 'OUTER');
}

sub plsql2pg::make_joinusing {
    my (undef, undef, undef, $targetlist) = @_;
    my $node = make_node('using');

    $node->{content} = $targetlist;
    return $node;
}

sub plsql2pg::make_joinon {
    my (undef, undef, $quallist) = @_;
    my $node = make_node('on');

    $node->{quallist} = $quallist;
    return $node;
}

sub plsql2pg::make_groupby {
    my (undef, $elem) = @_;
    my $groupbys = [];
    my $groupby = make_node('groupby');

    $groupby->{elem} = $elem;

    push(@{$groupbys}, $groupby);

    return $groupbys;
}

sub plsql2pg::append_groupby {
    my (undef, $groupbys, undef, $groupby) = @_;

    push(@{$groupbys}, @{$groupby});
    return $groupbys;
}

sub plsql2pg::make_groupbyclause {
    my (undef, undef, undef, $groupbys) = @_;

    return make_clause('GROUPBY', $groupbys);
}

sub format_groupby {
    my ($groupby) = @_;
    my $out;

    return format_node(@{$groupby->{elem}}[0]);
}

sub plsql2pg::make_orderby {
    my (undef, $elem, $order) = @_;
    my $orderbys = [];
    my $orderby = make_node('orderby');

    if (not defined($order)) {
        $order = 'ASC' unless defined($order);
    } else {
        $order = uc($order);
    }

    $orderby->{elem} = $elem;
    $orderby->{order} = $order;

    push(@{$orderbys}, $orderby);

    return $orderbys;
}

sub plsql2pg::append_orderby {
    my (undef, $orderbys, undef, $orderby) = @_;

    push(@{$orderbys}, @{$orderby});
    return $orderbys;
}

sub plsql2pg::make_orderbyclause {
    my (undef, undef, undef, $orderbys) = @_;

    return make_clause('ORDERBY', $orderbys);
}

sub format_orderby {
    my ($orderby) = @_;
    my $out;

    return format_node(@{$orderby->{elem}}[0]) . ' ' . $orderby->{order};
}

sub make_clause {
    my ($type, $content) = @_;
    my $clause = make_node($type);

    $clause->{content} = $content;

    return $clause;
}

sub get_alias {
    my ($as, $alias) = @_;

    return quote_ident($alias) if defined($alias);
    return quote_ident($as) if defined ($as);
    return undef;
}

sub format_select {
    my ($stmt) = @_;
    my @clauselist = ('SELECT', 'FROM', 'JOIN', 'WHERE', 'GROUPBY', 'ORDERBY');
    my $nodes;
    my $out = '';

    $nodes = handle_nonsqljoin($stmt->{FROM}, $stmt->{WHERE});

    if (defined($nodes)) {
        $stmt->{join} = [] unless defined($stmt->{JOIN});
        push(@{$stmt->{JOIN}}, @{$nodes});
    }

    foreach my $current (@clauselist) {
        if (defined($stmt->{$current})) {
            my $format = format_node($stmt->{$current});
            if (defined($format)) {
                $out .= ' ' if ($out ne '');
                $out .= $format;
            }
        }
    }

    return $out;
}

sub handle_nonsqljoin {
    my ($from, $where) = @_;
    my $joinlist = undef;
    my $i;

    return if not defined($where->{content});

    for ($i=0; $i<(scalar @{$where->{content}->{quallist}} > 0); $i++) {
        if ((ref @{$where->{content}->{quallist}}[$i] eq 'HASH') and
            defined(@{$where->{content}->{quallist}}[$i]->{join_op})
        ) {
            my $node;
            my $w = splice(@{$where->{content}->{quallist}}, $i, 1);
            my $t;
            my $ident = [];
            my $cond = [];

            $joinlist = [] unless defined($joinlist);
            push(@{$cond}, $w);

            $t = splice_table_from_fromlist($from->{content}, $w->{right}->{table});
            push(@{$ident}, $t);

            $node = plsql2pg::make_join(undef, 'LEFT', undef, $ident,
                $t->{alias}, plsql2pg::make_joinon(undef, undef, $cond));

            push(@{$joinlist}, @{$node});
        }
    }

    return $joinlist;
}

sub splice_table_from_fromlist {
    my ($from, $name) = @_;
    my $elem = undef;
    my $i;

    for ($i=0; $i<(scalar @{$from}); $i++) {
        my $t = @{$from}[$i];
        if (
            (defined($t->{alias}) and $t->{alias} eq $name)
            or
            ($t->{attribute} eq $name)
        ) {
            $elem = splice(@{$from}, $i, 1);
            last;
        }
    }

    return $elem;
}

sub format_quallist {
    my ($quallist) = @_;
    my $out = '';

    foreach my $node (@{$quallist}) {
        if (ref($node)) {
            $out .= format_node($node);
        } else {
            $out .= ' ' . $node . ' ';
        }
    }

    return $out;
}

sub format_using {
    my ($node) = @_;
    my $out = undef;

    foreach my $ident (@{$node->{content}}) {
        $out .= ', ' if defined($out);
        $out .= format_node($ident);
    }

    $out = 'USING (' . $out;
    $out .= ')';
    return $out;
}

sub format_on {
    my ($node) = @_;
    my $out = undef;

    $out = 'ON '. format_quallist($node->{quallist});
    return $out;
}

sub plsql2pg::print_node {
    my (undef, $node) = @_;

    print format_node($node) . " ;\n";

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
        my $out = undef;
        foreach my $n (@{$node}) {
            $out .= " " if defined($out);
            $out .= format_node($n);
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
    my @atts = ('attribute', 'table', 'schema', 'db');
    my $out;

    while (my $elem = pop(@atts)) {
        if (defined ($ident->{$elem})) {
            $out .= "." if defined($out);
            $out .= $ident->{$elem};
        }
    }

    $out .= format_alias($ident->{alias});

    return $out;
}

sub format_literal {
    my ($literal) = @_;
    my $out;

    $out = $literal->{value};
    $out .= format_alias($literal->{alias});

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

sub format_join {
    my ($join) = @_;
    my $out;

    $out = $join->{jointype} . " JOIN ";

    $out .= format_ident($join->{ident});
    $out .= ' ' . format_node($join->{cond});

    return $out;
}

sub format_SELECT {
    my ($select) = @_;

    return "SELECT " . format_standard_clause($select, ', ');
}

sub format_FROM {
    my ($from) = @_;
    my $out = format_standard_clause($from, ', ');

    return undef if ($out eq 'dual');
    return "FROM " . $out;
}

sub format_WHERE {
    my ($where) = @_;

    # handle_nonsqljoin can leave an empty array here
    if (scalar @{$where->{content}->{quallist}} == 0) {
    } else {
        return "WHERE " . format_quallist($where->{content}->{quallist});
    }
}

sub format_JOIN {
    my ($join) = @_;

    return format_standard_clause($join, ' ');
}

sub format_GROUPBY {
    my ($group) = @_;

    return "GROUP BY " . format_standard_clause($group, ', ');
}

sub format_ORDERBY {
    my ($orderby) = @_;

    return "ORDER BY " . format_standard_clause($orderby, ', ');
}

sub format_subquery {
    my ($query) = @_;
    my $alias;
    my $out;

    $out .= '( ' . format_standard_clause($query) . ' )';

    $alias = format_alias(@{$query->{content}}[0]->{alias});

    # alias on subquery in mandatory in pg
    if ($alias eq '') {
        $alias = " AS " . generate_alias();
    }

    $out .= $alias;

    return $out;
}

sub format_standard_clause {
    my ($clause, $delim) = @_;
    my $content = $clause->{content};
    my $out = undef;

    if (ref($content) eq 'HASH') {
        $out = format_node($content);
    } else {
        foreach my $node (@{$content}) {
            $out .= $delim if defined($out);
            $out .= format_node($node);
        }
    }

    return $out;
}

sub format_alias {
    my ($alias) = @_;

    return ' AS ' . quote_ident($alias) if defined($alias);
    return '';
}

sub generate_alias {
    $alias_gen++;

    return "subquery$alias_gen";
}

sub translate_function_name {
    my ($name) = @_;

    return 'COALESCE' if ($name eq 'nvl');

    return $name;
}

sub error {
    my ($msg, $node) = @_;

    print "ERROR: $msg\n";
    print Dumper($node);
    exit 1;
}
