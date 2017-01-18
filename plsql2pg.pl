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
    stmtmulti ';' stmt
    | stmtmulti ';' # handle terminal ; FIXME
    | stmt

stmt ::=
    CombinedSelectStmt action => print_stmts

CombinedSelectStmt ::=
    CombinedSelectStmt combine_op '(' SelectStmt ')' action => combine_select
    | SelectStmt

combine_op ::=
    UNION
    | UNION ALL action => concat
    | INTERSECT
    | MINUS

SelectStmt ::=
    SELECT select_clause from_clause join_clause
        where_clause group_clause having_clause
        order_clause action => make_select

ALIAS_CLAUSE ::=
    AS ALIAS action => make_alias
    | ALIAS action => make_alias
    | EMPTY action => ::undef

EMPTY ::= action => ::undef

select_clause ::=
    target_list action => make_selectclause

target_list ::=
    target_list ',' alias_target_el action => append_el_1_3
    | alias_target_el

alias_target_el ::=
    target_el ALIAS_CLAUSE action => alias_node

# aliasing is done in a different rule, so target_el can be used in qual rule,
# to allow function in quals without ambiguity (having function in a_expr is
# ambiguous)
target_el ::=
    a_expr
    | function

a_expr ::=
    IDENT
    | number action => make_number
    | LITERAL action => make_literal

function ::=
    IDENT '(' function_args ')' window_clause action => make_function

function_args ::=
    function_args ',' function_arg action => append_el_1_3
    | function_arg
    | EMPTY action => ::undef

function_arg ::=
    a_expr
    | function

window_clause ::=
    OVER '(' partition_clause order_clause frame_clause ')' action => make_overclause
    | EMPTY

partition_clause ::=
    PARTITION BY partition_list action => make_partitionclause
    | EMPTY action => ::undef

partition_list ::=
    partition_list ',' partition_elem action => append_el_1_3
    | partition_elem

partition_elem ::=
    a_expr action => make_partitionby

# should only be legal with an order_clause
frame_clause ::=
    RANGE_ROWS frame_start action => make_frame_simple
    | RANGE_ROWS BETWEEN frame_start AND frame_end action => make_frame_between
    | EMPTY action => ::undef

RANGE_ROWS ::=
    RANGE action => upper
    | ROWS action => upper

frame_start ::=
    UNBOUNDED PRECEDING action => concat
    | CURRENT ROW action => concat
    | number PRECEDING action => concat

frame_end ::=
    UNBOUNDED FOLLOWING action => concat
    | CURRENT ROW action => concat
    | number FOLLOWING action => concat

from_clause ::=
    FROM from_list action => make_fromclause

from_list ::=
    from_list ',' from_elem action => append_el_1_3
    | from_elem

from_elem ::=
    IDENT ALIAS_CLAUSE action => alias_node
    | '(' SelectStmt ')' ALIAS_CLAUSE action => make_subquery

join_clause ::=
    join_list action => make_joinclause
    | EMPTY action => ::undef

join_list ::=
    join_list join_elem action => append_join
    | join_elem

join_elem ::=
    join_type JOIN IDENT ALIAS_CLAUSE join_cond action => make_join
    | special_join_type JOIN IDENT ALIAS_CLAUSE action => make_join

join_type ::=
    INNER action => make_jointype
    | LEFT action => make_jointype
    | LEFT OUTER action => make_jointype
    | RIGHT action => make_jointype
    | RIGHT OUTER action => make_jointype
    | FULL OUTER action => make_jointype
    | EMPTY action => make_jointype

special_join_type ::=
    NATURAL action => make_jointype
    | CROSS action => make_jointype

join_cond ::=
    USING '(' using_list ')' action => make_joinusing
    | ON qual_list action => make_joinon

using_list ::=
    using_list ',' using_el action => append_el_1_3
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
    '(' qual_list_no_parens ')' action => parens_qual
    | '(' qual_list_with_parens ')' action => parens_qual

qual_list_no_parens ::=
    qual_list qual_op qual_list action => append_qual
    #| '(' qual_list qual_op parens_qual ')' action => parens_qual
    | qual

qual_op ::=
    AND action => upper
    | OR action => upper

IDENT ::=
    ident '.' ident '.' ident '.' ident action => make_ident
    | ident '.' ident '.' ident action => make_ident
    | ident '.' ident action => make_ident
    | ident action => make_ident

qual ::=
    target_el OPERATOR target_el join_op action => make_qual

join_op ::=
    '(+)'
    | EMPTY action => ::undef

group_clause ::=
    GROUP BY group_list action => make_groupbyclause
    | EMPTY action => ::undef

group_list ::=
    group_list ',' group_elem action => append_el_1_3
    | group_elem

group_elem ::=
    IDENT action => make_groupby

having_clause ::=
    HAVING qual_list action => make_havingclause
    | EMPTY action => ::undef

order_clause ::=
    ORDER BY order_list action => make_orderbyclause
    | EMPTY action => ::undef

order_list ::=
    order_list ',' order_elem action => append_el_1_3
    | order_elem

order_elem ::=
    a_expr ordering action => make_orderby

ordering ::=
    ASC
    | DESC
    | EMPTY action => ::undef

# keywords
ALL         ~ 'ALL':ic
AND         ~ 'AND':ic
AS          ~ 'AS':ic
ASC         ~ 'ASC':ic
BETWEEN     ~ 'BETWEEN':ic
BY          ~ 'BY':ic
CROSS       ~ 'CROSS':ic
CURRENT     ~ 'CURRENT':ic
DESC        ~ 'DESC':ic
INNER       ~ 'INNER':ic
INTERSECT   ~ 'INTERSECT':ic
IS          ~ 'IS':ic
FOLLOWING   ~ 'FOLLOWING':ic
FULL        ~ 'FULL':ic
FROM        ~ 'FROM':ic
GROUP       ~ 'GROUP':ic
HAVING      ~ 'HAVING':ic
JOIN        ~ 'JOIN':ic
LEFT        ~ 'LEFT':ic
:lexeme     ~ LEFT priority => 1
MINUS       ~ 'MINUS':ic
NATURAL     ~ 'NATURAL':ic
NOT         ~ 'NOT':ic
OR          ~ 'OR':ic
ORDER       ~ 'ORDER':ic
ON          ~ 'ON':ic
OUTER       ~ 'OUTER':ic
OVER        ~ 'OVER':ic
PARTITION   ~ 'PARTITION':ic
PRECEDING   ~ 'PRECEDING':ic
RANGE       ~ 'RANGE':ic
RIGHT       ~ 'RIGHT':ic
:lexeme     ~ RIGHT priority => 1
ROW         ~ 'ROW':ic
ROWS        ~ 'ROWS':ic
SELECT      ~ 'SELECT':ic
UNBOUNDED   ~ 'UNBOUNDED':ic
UNION       ~ 'UNION':ic
USING       ~ 'USING':ic
WHERE       ~ 'WHERE':ic

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

OPERATOR    ~ '=' | '<' | '<=' | '>' | '>=' | '%' | IS | IS NOT

:discard ~ whitespace
whitespace ~ [\s]+

END_OF_DSL

my $grammar = Marpa::R2::Scanless::G->new( {
    default_action => '::first',
    source => \$dsl
} );

my $input = <<'SAMPLE_QUERIES';
SElect 1 nb from DUAL WHERE rownum < 2; SELECT * from TBL t order by a, b desc, tbl.c asc;
SELECT nvl(val, 'null') "vAl",1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1, 'test me', t.* from tbl t WHERE (((a > 2)) and rownum < 10) OR b < 3 GROUP BY a, t.b;
 select * from (
select 1 from dual
) union (select 2 from dual) minus (select 3 from dual) interSECT (select 4 from dual) union all (select 5 from dual);
select * from a,c join b using (id,id2) left join d using (id) WHERE rownum >10 and rownum <= 20;
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2 naturaL join d CROSS JOIN e cj;
select round(sum(count(*)), 2), 1 from a,b t1 where a.id = t1.id(+);
SELECT id, count(*) FROM a GROUP BY id HAVING count(*)< 10;
SELECT val, rank() over (partition by id) rank, lead(val) over (order by val rows CURRENT ROW), lag(val) over (partition by id,val order by val range between 2 preceding and unbounded following) as lag from t;
SAMPLE_QUERIES


print "Original:\n---------\n" . $input . "\n\nConverted:\n----------\n";
my $value_ref = $grammar->parse( \$input, 'plsql2pg' );

sub plsql2pg::make_alias {
    my (undef, $as, $alias) = @_;

    return get_alias($as, $alias);
}

sub plsql2pg::concat {
    my (undef, $a, $b) = @_;

    return uc("$a $b");
}

sub plsql2pg::upper {
    my (undef, $a) = @_;

    return uc($a);
}

sub plsql2pg::append_el_1_3 {
    my (undef, $nodes, undef, $node) = @_;

    push(@{$nodes}, @{$node});

    return $nodes;
}

sub plsql2pg::make_number {
    my (undef, $val) = @_;
    my $number = make_node('number');

    $number->{val} = $val;

    return to_array($number);
}

sub format_number {
    my ($number) = @_;

    return $number->{val} . format_alias($number->{alias});
}

sub plsql2pg::make_ident {
    my (undef, $db, undef, $table, undef, $schema, undef, $attribute) = @_;
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

    return to_array($ident);
}

sub plsql2pg::make_literal {
    my (undef, $value, $alias) = @_;
    my $literal = make_node('literal');

    $literal->{value} = $value;
    $literal->{alias} = $alias;

    return to_array($literal);
}

sub plsql2pg::make_function {
    my (undef, $ident, undef, $args, undef, $windowclause) = @_;
    my $func = make_node('function');

    $func->{ident} = $ident;
    $func->{args} = $args;
    $func->{window} = $windowclause;

    return to_array($func);
}

sub format_function {
    my ($func) = @_;
    my $out = undef;
    my $ident;

    foreach my $arg (@{$func->{args}}) {
        $out .= ', ' if defined($out);
        $out .= format_node($arg);
    }
    # arguments are optional
    $out = '' unless defined($out);

    $ident = pop(@{$func->{ident}});
    $ident->{attribute} = translate_function_name($ident->{attribute});
    $out = format_ident($ident) . '(' . $out . ')';

    $out .= format_node($func->{window}) if defined($func->{window});
    $out .= format_alias($func->{alias});

    return $out;
}

sub plsql2pg::make_overclause {
    my (undef, undef, undef, $partition, $order, $frame, undef) = @_;
    my $clause;
    my $content = [];

    push(@{$content}, $partition) if defined($partition);
    push(@{$content}, $order) if defined($order);
    push(@{$content}, $frame) if defined($frame);
    $clause = make_clause('OVERCLAUSE', $content);

    return $clause;
}

sub plsql2pg::make_partitionclause {
    my (undef, undef, undef, $partitionbys) = @_;

    return make_clause('PARTITIONBY', $partitionbys);
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
    my $qual = make_node('qual');

    $qual->{left} = pop(@{$left});
    $qual->{op} = $op;
    $qual->{right} = pop(@{$right});
    $qual->{join_op} = $join_op;

    return to_array($qual);
}

sub plsql2pg::append_qual {
    my (undef, $quals, $qualop, $qual) = @_;

    push(@{$quals}, $qualop);
    push(@{$quals}, @{$qual});

    return $quals;
}

sub plsql2pg::make_select {
    my (undef, undef, @args) = @_;
    my $stmt = make_node('select');
    my $token;

    while (scalar @args > 0) {
        $token = shift(@args);
        $stmt->{$token->{type}} = $token if defined($token);
    }

    return to_array($stmt);
}

sub plsql2pg::combine_select {
    my (undef, $nodes, $raw_op, undef, $stmt, undef) = @_;
    my $op = make_node('combine_op');

    $op->{op} = $raw_op;

    push(@{$nodes}, $op);
    push(@{$nodes}, @{$stmt});

    return $nodes;
}

sub format_combine_op {
    my ($op) = @_;

    return 'EXCEPT' if (uc($op->{op}) eq 'MINUS');
    return uc($op->{op});
}

sub plsql2pg::make_subquery {
    my (undef, undef, $stmt, undef, $alias) = @_;
    my $clause;

    assert_one_el($stmt);

    @{$stmt}[0]->{alias} = $alias;
    $clause = make_clause('subquery', $stmt);

    #return $stmt;
    return $clause;
}

sub plsql2pg::alias_node {
    my (undef, $node, $alias) = @_;

    assert_one_el($node);

    @{$node}[0]->{alias} = $alias;

    return $node;
}

sub plsql2pg::parens_qual {
    my (undef, undef, $node, undef) = @_;
    my $parens = make_node('parens');

    $parens->{node} = $node;
    return to_array($parens);
}

sub plsql2pg::make_joinclause {
    my (undef, $joins) = @_;

    return make_clause('JOIN', $joins);
}

sub plsql2pg::make_join {
    my (undef, $jointype, undef, $ident, $alias, $cond) = @_;
    my $join = make_node('join');

    $join->{jointype} = $jointype;
    $join->{ident} = pop(@{$ident});
    $join->{ident}->{alias} = $alias;
    $join->{cond} = $cond;

    return to_array($join);
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

    return 'FULL OUTER' if ($kw1 eq 'FULL') and ($kw2 eq 'OUTER');
    return $kw1;
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
    my $groupby = make_node('groupby');

    $groupby->{elem} = $elem;

    return to_array($groupby);
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

sub plsql2pg::make_havingclause {
    my (undef, undef, $quals) = @_;
    my $quallist = make_node('quallist');

    $quallist->{quallist} = $quals;

    return make_clause('HAVING', $quallist);
}

sub format_HAVING {
    my ($having) = @_;
    my $out;

    return "HAVING " . format_quallist($having->{content}->{quallist});
}

sub plsql2pg::make_partitionby {
    my (undef, $elem) = @_;
    my $partitionby = make_node('partitionby');

    $partitionby->{elem} = $elem;

    return to_array($partitionby);
}

sub format_partitionby {
    my ($partitionby) = @_;
    my $out;

    return format_node(@{$partitionby->{elem}}[0]);
}

sub plsql2pg::make_frame_simple {
    my (undef, $rangerows, $frame_start) = @_;
    my $node = make_node('frame');

    $node->{rangerows} = $rangerows;
    $node->{frame_start} = $frame_start;

    return $node;
}

sub plsql2pg::make_frame_between {
    my (undef, $rangerows, undef, $frame_start, undef, $frame_end) = @_;
    my $node = make_node('frame');

    $node->{rangerows} = $rangerows;
    $node->{frame_start} = $frame_start;
    $node->{frame_end} = $frame_end;

    return $node;
}

sub format_frame {
    my ($frame) = @_;

    if (defined($frame->{frame_end})) {
        return $frame->{rangerows} . " BETWEEN "
            . $frame->{frame_start} . " AND " . $frame->{frame_end};
    } else {
        return $frame->{rangerows} . " " . $frame->{frame_start};
    }
}

sub plsql2pg::make_orderby {
    my (undef, $elem, $order) = @_;
    my $orderby = make_node('orderby');

    if (not defined($order)) {
        $order = 'ASC' unless defined($order);
    } else {
        $order = uc($order);
    }

    $orderby->{elem} = $elem;
    $orderby->{order} = $order;

    return to_array($orderby);
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
    my @clauselist = ('SELECT', 'FROM', 'JOIN', 'WHERE', 'GROUPBY', 'HAVING',
        'ORDERBY', 'LIMIT', 'OFFSET');
    my $nodes;
    my $out = '';

    $nodes = handle_nonsqljoin($stmt->{FROM}, $stmt->{WHERE});

    if (defined($nodes)) {
        $stmt->{join} = [] unless defined($stmt->{JOIN});
        push(@{$stmt->{JOIN}}, @{$nodes});
    }

    handle_rownum($stmt);

    foreach my $current (@clauselist) {
        if (defined($stmt->{$current})) {
            my $format = format_node($stmt->{$current});
            if (defined($format)) {
                next if ($format eq '');
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

    for ($i=0; $i<(scalar @{$where->{content}->{quallist}}); $i++) {
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

# This function will transform any rownum OPERATOR number to a LIMIT/OFFSET
# clause.  No effort is done to make sure OR-ed or overlapping rownum
# expressions will have expected result, such query would be stupid anyway.
# This grammar also allows a float number, but as everywhere else I assume
# original query is valid to keep the grammar simple.
sub handle_rownum {
    my ($node, $stmt) = @_;
    my $i;

    return undef if (not defined($node));
    if (isA($node, 'select')) {
        return handle_rownum($node->{WHERE}->{content}, $node) if (defined($node->{WHERE}));
        return undef;
        }
    return handle_rownum($node->{node}, $stmt) if isA($node, 'parens');
    return handle_rownum($node->{quallist}, $stmt) if isA($node, 'quallist');

    for ($i=0; $i<(scalar @{$node}); $i++) {
        my $qual = @{$node}[$i];
        next if (ref($qual) ne 'HASH');
        if (isA($qual, 'parens')) {
            handle_rownum($qual, $stmt);
            next;
        }
        if ((
                (isA($qual->{left}, 'number') and isA($qual->{right}, 'ident')
                and ($qual->{right}->{attribute} eq 'rownum')
                and not defined($qual->{right}->{table}))
            ) or (
                (isA($qual->{right}, 'number') and isA($qual->{left}, 'ident')
                and ($qual->{left}->{attribute} eq 'rownum')
                and not defined($qual->{left}->{table}))
            )
        ){
            my $number;
            my $clause;

            # Remove any preceding AND/OR op, too bad if it was an OR
            if (($i>0) and (ref(@{$node}[$i-1]) ne 'HASH'))
            {
                @{$node}[$i-1] = undef;
            }

            if (isA($qual->{left}, 'ident')) {
                $number = $qual->{right};
            } else {
                $number = $qual->{left};
            }

            if (($qual->{op} eq '<') or ($qual->{op} eq '<=')) {
                $number->{val} -= 1 if ($qual->{op} eq '<');
                $clause = make_clause('LIMIT', $number);

            } else {
                $number->{val} -= 1 if ($qual->{op} eq '>=');
                $clause = make_clause('OFFSET', $number);
            }

            # Finally remove the qual
            @{$node}[$i] = undef;

            # XXX Should I handle stupid queries with multiple rownum clauses?
            $stmt->{$clause->{type}} = $clause;
            }
        }
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
        next unless(defined($node));
        if (ref($node)) {
            $out .= format_node($node);
        } else {
            $out .= ' ' . $node . ' ' if ($node ne '');
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

sub plsql2pg::print_stmts {
    my (undef, $stmts) = @_;
    my $first = 1;

    foreach my $stmt (@{$stmts}) {
        print "\n" if (not $first);
        $first = 0;
        print format_node($stmt);
    }
    print " ;\n";
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
            next unless defined($n);
            $out .= " " if defined($out);
            $out .= format_node($n);
        }

        return $out;
    }

    return $node if (not ref($node));

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

sub format_parens {
    my ($parens) = @_;

    return '(' . format_node($parens->{node}) . ')';
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
    $out .= ' ' . format_node($join->{cond}) if defined($join->{cond});

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
    my $out = format_quallist($where->{content}->{quallist});

    return '' if($out eq '');
    return "WHERE " . $out;
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

sub format_OVERCLAUSE {
    my ($windowclause) = @_;

    return " OVER (" . format_standard_clause($windowclause, ' ') . ")";
}

sub format_PARTITIONBY {
    my ($partitionby) = @_;

    return "PARTITION BY " . format_standard_clause($partitionby, ', ');
}

sub format_LIMIT {
    my ($limit) = @_;

    return "LIMIT " . format_node($limit->{content});
}

sub format_OFFSET {
    my ($offset) = @_;

    return "OFFSET " . format_node($offset->{content});
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

sub isA {
    my ($node, $type) = @_;

    return 0 if (not ref($node));
    return 0 if (ref $node ne 'HASH');

    return ($node->{type} eq $type);
}

sub assert_one_el {
    my ($arr) = @_;

    error('Element is not an array', $arr) if (ref($arr) ne 'ARRAY');
    error('Array contains more than one element', $arr) if (scalar @{$arr} != 1);
}

sub to_array {
    my ($node) = @_;
    my $nodes = [];

    push(@{$nodes}, $node);
    return $nodes;
}

sub error {
    my ($msg, $node) = @_;

    print "ERROR: $msg\n";
    print Dumper($node);
    exit 1;
}
