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
my @fixme;

use Data::Dumper;

my $dsl = <<'END_OF_DSL';
lexeme default = latm => 1

stmtmulti ::=
    stmt* separator => SEMICOLON

stmt ::=
    SelectStmt action => print_stmts
    | UpdateStmt action => print_stmts
    | DeleteStmt action => print_stmts
    | InsertStmt action => print_stmts

SelectStmt ::=
    SelectStmt combine_op '(' SingleSelectStmt ')' action => combine_select
    | SingleSelectStmt

combine_op ::=
    UNION
    | UNION ALL action => concat
    | INTERSECT
    | MINUS

SingleSelectStmt ::=
    with_clause SELECT select_clause from_clause join_clause
        where_clause hierarchical_clause group_clause having_clause
        order_clause forupdate_clause action => make_select

UpdateStmt ::=
    UPDATE update_from_clause update_set_clause where_clause
        returning_clause error_logging_clause action => make_update

DeleteStmt ::=
    DELETE delete_from_clause  where_clause returning_clause
        error_logging_clause action => make_delete

InsertStmt ::=
    # parens_column_list should only allow single col name, not FQN ident
    INSERT INTO from_elem parens_column_list insert_data error_logging_clause
        action => make_insert

ALIAS_CLAUSE ::=
    AS ALIAS action => make_alias
    | ALIAS action => make_alias
    | EMPTY action => ::undef

EMPTY ::= action => ::undef

with_clause ::=
    WITH with_list action => make_withclause
    | EMPTY action => ::undef

with_list ::=
    with_list ',' with_elem action => append_el_1_3
    | with_elem

with_elem ::=
    ident AS '(' SelectStmt ')' action => make_with

distinct_elem ::=
    ALL
    | UNIQUE
    | DISTINCT

select_clause ::=
    distinct_elem target_list action => make_distinct_selectclause
    | target_list action => make_selectclause

target_list ::=
    target_list ',' alias_target_el action => append_el_1_3
    | alias_target_el

alias_target_el ::=
    target_el ALIAS_CLAUSE action => alias_node

# aliasing is done in a different rule, so target_el can be used in qual rule,
# to allow function in quals without ambiguity (having function in a_expr is
# ambiguous)
target_el ::=
    target_el_no_parens
    | target_el_with_parens

target_el_with_parens ::=
    '(' target_el_no_parens ')' action => parens_node
    | '(' target_el_with_parens ')' action => parens_node

target_el_no_parens ::=
    target_el OPERATOR simple_target_el action => make_opexpr
    | simple_target_el_no_parens

simple_target_el ::=
    simple_target_el_no_parens
    | '(' target_el ')' action => parens_node

simple_target_el_no_parens ::=
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
    # AS not legal here, assume origina query is correct
    PARTITION BY target_list action => make_partitionclause
    | EMPTY action => ::undef

# should only be legal if an order_clause is present in the OVER clause
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
    flashback_from_elem ALIAS_CLAUSE action => alias_node

flashback_from_elem ::=
    simple_from_elem flashback_clause action => add_flashback

simple_from_elem ::=
    IDENT
    | '(' SelectStmt ')' action => make_subquery
    # ONLY is not valid for DELETE, assume original query is valid
    | ONLY '(' simple_from_elem ')' action => make_fromonly

join_clause ::=
    join_list action => make_joinclause
    | EMPTY action => ::undef

join_list ::=
    join_list join_elem action => append_join
    | join_elem

join_elem ::=
    join_type JOIN join_ident ALIAS_CLAUSE join_cond action => make_join
    | special_join_type JOIN join_ident ALIAS_CLAUSE action => make_join

join_ident ::=
    IDENT
    | '(' SelectStmt ')' action => make_subquery

join_type ::=
    normal_join_type
    | special_join_type
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
    '(' qual_list_no_parens ')' action => parens_node
    | '(' qual_list_with_parens ')' action => parens_node

qual_list_no_parens ::=
    qual_list qual_op qual action => append_qual
    | qual_no_parens

qual_op ::=
    AND action => upper
    | OR action => upper

IDENT ::=
    ident '.' ident '.' ident '.' ident action => make_ident
    | ident '.' ident '.' ident action => make_ident
    | ident '.' ident action => make_ident
    | ident action => make_ident

# PRIOR is only legal in hierarchical clause, assume original query is valid
qual_no_parens ::=
    target_el OPERATOR target_el join_op action => make_qual
    | PRIOR target_el OPERATOR target_el join_op action => make_priorqual
    | target_el OPERATOR PRIOR target_el join_op action => make_qualprior

qual ::=
    qual_no_parens
    | '(' qual_list ')' action => parens_node

join_op ::=
    '(+)'
    | EMPTY action => ::undef

hierarchical_clause ::=
    startwith_clause connectby_clause action => make_hierarchicalclause
    | connectby_clause action => make_hierarchicalclause
    | EMPTY action => ::undef

startwith_clause ::=
    START WITH qual_list action => make_startwith

connectby_clause ::=
    CONNECT BY qual_list action => make_connectby

group_clause ::=
    GROUP BY group_list action => make_groupbyclause
    | EMPTY action => ::undef

group_list ::=
    group_list ',' group_elem action => append_el_1_3
    | group_elem

group_elem ::=
    a_expr action => make_groupby
    | ROLLUP '(' target_list ')' action => make_rollupcube
    | CUBE '(' target_list ')' action => make_rollupcube
    | GROUPING SETS '(' group_list ')' action => make_groupingsetsclause

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
    a_expr ordering nulls_pos action => make_orderby

ordering ::=
    ASC
    | DESC
    | EMPTY action => ::undef

nulls_pos ::=
    NULLS FIRST action => concat
    | NULLS LAST action => concat
    | EMPTY action => ::undef

forupdate_clause ::=
    FOR UPDATE forupdate_content forupdate_wait_clause action => make_forupdateclause
    | EMPTY

forupdate_content ::=
    OF forupdate_list action => second
    | EMPTY

forupdate_list ::=
    forupdate_list ',' IDENT action => append_el_1_3
    | IDENT

forupdate_wait_clause ::=
    NOWAIT action => make_forupdate_wait
    | WAIT integer action => make_forupdate_wait
    | EMPTY

update_from_clause ::=
    from_elem

update_set_clause ::=
    SET update_set_list action => make_update_set_clause

update_set_list ::=
    update_set_list ',' update_set_elem action => append_el_1_3
    | update_set_elem

update_set_elem ::=
    IDENT '=' target_el action => make_opexpr
    | '(' update_set_elems ')' '=' '(' SelectStmt ')' action => make_update_set_set
    | IDENT '=' '(' SelectStmt ')' action => make_update_ident_set

update_set_elems ::=
    update_set_elems ',' IDENT action => append_el_1_3
    | IDENT

delete_from_clause ::=
    FROM from_elem action => second

parens_column_list ::=
    '(' column_list ')' action => make_parens_column_list
    | EMPTY

column_list ::=
    column_list ',' IDENT action => append_el_1_3
    | IDENT

insert_data ::=
    VALUES '(' target_list ')' returning_clause action => make_values
    | SelectStmt

flashback_clause ::=
    VERSIONS BETWEEN flashback_kind flashback_start AND flashback_end
        action => make_flashback_clause
    | AS OF flashback_kind target_el action => make_flashback_clause
    | EMPTY

flashback_kind ::=
    SCN action => upper
    | TIMESTAMP action => upper

flashback_start ::=
    target_el
    | MINVALUE action => upper

flashback_end ::=
    target_el
    | MAXVALUE action => upper

returning_clause ::=
    RETURNING target_list INTO data_items action => make_returning_clause
    | EMPTY

data_items ::=
    data_items ',' data_item action => append_el_1_3
    | data_item

data_item ::=
    # not sure yet
    IDENT

error_logging_clause ::=
    LOG ERRORS err_log_into err_log_list action => make_errlog_clause1
    | REJECT LIMIT number action => make_errlog_clause2
    | REJECT LIMIT UNLIMITED action => make_errlog_clause2
    | EMPTY

err_log_into ::=
    INTO IDENT action => second
    | EMPTY

err_log_list ::=
    '(' target_list ')' action => second
    | EMPTY


# keywords
ALL         ~ 'ALL':ic
AND         ~ 'AND':ic
AS          ~ 'AS':ic
ASC         ~ 'ASC':ic
BETWEEN     ~ 'BETWEEN':ic
BY          ~ 'BY':ic
CONNECT     ~ 'CONNECT':ic
CROSS       ~ 'CROSS':ic
CUBE        ~ 'CUBE':ic
CURRENT     ~ 'CURRENT':ic
DELETE      ~ 'DELETE':ic
:lexeme     ~ DELETE pause => after event => keyword
DESC        ~ 'DESC':ic
DISTINCT    ~ 'DISTINCT':ic
ERRORS      ~ 'ERRORS':ic
FIRST       ~ 'FIRST':ic
FOLLOWING   ~ 'FOLLOWING':ic
FOR         ~ 'FOR':ic
FULL        ~ 'FULL':ic
FROM        ~ 'FROM':ic
GROUP       ~ 'GROUP':ic
GROUPING    ~ 'GROUPING':ic
HAVING      ~ 'HAVING':ic
INNER       ~ 'INNER':ic
INSERT      ~ 'INSERT':ic
:lexeme     ~ INSERT pause => after event => keyword
INTERSECT   ~ 'INTERSECT':ic
INTO        ~ 'INTO':ic
IS          ~ 'IS':ic
JOIN        ~ 'JOIN':ic
LAST        ~ 'LAST':ic
LEFT        ~ 'LEFT':ic
:lexeme     ~ LEFT priority => 1
LIMIT       ~ 'LIMIT':ic
LOG         ~ 'LOG':ic
MAXVALUE    ~ 'MAXVALUE':ic
:lexeme     ~ MAXVALUE priority => 1
MINUS       ~ 'MINUS':ic
MINVALUE    ~ 'MINVALUE':ic
:lexeme     ~ MINVALUE priority => 1
NATURAL     ~ 'NATURAL':ic
NOWAIT      ~ 'NOWAIT':ic
NOT         ~ 'NOT':ic
NULLS       ~ 'NULLS':ic
OF          ~ 'OF':ic
ONLY        ~ 'ONLY':ic
OR          ~ 'OR':ic
ORDER       ~ 'ORDER':ic
ON          ~ 'ON':ic
OUTER       ~ 'OUTER':ic
OVER        ~ 'OVER':ic
PARTITION   ~ 'PARTITION':ic
PRECEDING   ~ 'PRECEDING':ic
PRIOR       ~ 'PRIOR':ic
RANGE       ~ 'RANGE':ic
REJECT      ~ 'REJECT':ic
RETURNING   ~ 'RETURNING':ic
RIGHT       ~ 'RIGHT':ic
:lexeme     ~ RIGHT priority => 1
ROLLUP      ~ 'ROLLUP':ic
ROW         ~ 'ROW':ic
ROWS        ~ 'ROWS':ic
SCN         ~ 'SCN':ic
SELECT      ~ 'SELECT':ic
:lexeme     ~ SELECT pause => after event => keyword
SET         ~ 'SET':ic
SETS        ~ 'SETS':ic
START       ~ 'START':ic
TIMESTAMP   ~ 'TIMESTAMP':ic
UNBOUNDED   ~ 'UNBOUNDED':ic
UNIQUE      ~ 'UNIQUE':ic
UNION       ~ 'UNION':ic
UNLIMITED   ~ 'UNLIMITED':ic
UPDATE      ~ 'UPDATE':ic
:lexeme     ~ UPDATE pause => after event => keyword
USING       ~ 'USING':ic
VALUES      ~ 'VALUES':ic
VERSIONS    ~ 'VERSIONS':ic
WHERE       ~ 'WHERE':ic
WAIT        ~ 'WAIT':ic
WITH        ~ 'WITH':ic
:lexeme     ~ WITH pause => after event => keyword

SEMICOLON   ~ ';'
:lexeme     ~ SEMICOLON pause => after event => new_query

# everything else
number  ~ digits | float
integer ~ digits
digits ~ [0-9]+
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
            | '+' | '-' | '*' | '/' | '||'

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

my $input = <<'SAMPLE_QUERIES';
SElect 1 nb from DUAL WHERE rownum < 2; SELECT DISTINCT * from TBL t order by a nulls last, b desc, tbl.c asc;
SELECT (1), nvl(val, 'null') || ' '|| nvl(val2, 'empty') "vAl",1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1 + 2 * ((t.v) - 2) % 4 meh, 'test me', t.* from tbl t WHERE (((((a > 2)) and (rownum < 10)) OR ((((b < 3)))))) GROUP BY a, t.b;
 select * from (
select 1 from dual
) union (select 2 from dual) minus (select 3 from dual) interSECT (select 4 from dual) union all (select 5 from dual);
select * from a,only (c) join b using (id,id2) left join d using (id) WHERE rownum >10 and rownum <= 20;
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2 naturaL join d CROSS JOIN e cj natural left outer join f natural full outer join g;
select round(sum(count(*)), 2), 1 from a,b t1 where a.id = t1.id(+);
SELECT id, log2(id), log10(id), count(*) FROM a GROUP BY id HAVING count(*)< 10;
SELECT val, rank() over (partition by id) rank, lead(val) over (order by val rows CURRENT ROW), lag(val) over (partition by id,val order by val range between 2 preceding and unbounded following) as lag from t;
WITH s1 as (with s3 as (select 1 from dual) select * from s3), s AS (SELECT * FROM s1 where rownum < 2) SELECT * From s, (with t as (select 3 from t) select * from t) cross join (with u as (select count(*) nb from dual) select nb from u union all (select 0 from dual)) where rownum < 2;
with s as (select 1 from dual) SELECT employee_id, last_name, manager_id
FROM employees   -- this is the FROM clause
WHERE salary > 0   --should not happen
start with /* hard coded value */ employee_id = 1 CONNECT BY isvalid = 1 and PRIOR employee_id = manager_id;
SELECT a,b,c FROM foo bar group by grouping sets(a, cube(a,b), rollup(c,a), cube(rollup(a,b,c)));
SELECT * FROM tbl t, t2 natural join t3 FOR UPDATE OF t2.a, col wait 1;
update t set a = 1, (b,c) = (select * from t2 WHERE id = 1), d = (SELECT 1 from dual) where (a < 10);
delete from public.t tbl where nvl(tbl.col, 'todel') = 'todel';
insert into public.t ins values (2+1, 'tt');
insert   into public.t ins (a,b) values (2+1, 'tt');
insert into public.t ins (a,b) select id, count(*) from t group by 1;
-- now unsupported stuff
SELECT 1 FROM t1 VERSIONS BETWEEN TIMESTAMP MINVALUE AND CURRENT_TIMESTAMP t WHERE id < 10;
UPDATE t1 SET val = 't' WHERE id = 1 LOG ERRORS INTO err.log (to_char(SYSDATE), id);
UPDATE t1 SET val = 't' WHERE id = 1 RETURNING (id%2), * INTO a,b REJECT LIMIT 3;
-- I don't belong to any query
SAMPLE_QUERIES


print "Original:\n---------\n" . $input . "\n\nConverted:\n----------\n";

my $grammar = Marpa::R2::Scanless::G->new( {
    default_action => '::first',
    source => \$dsl
} );
#
my $slr = Marpa::R2::Scanless::R->new(
        { grammar => $grammar, semantics_package => 'plsql2pg' } );

my $length = length $input;
my $pos = $slr->read( \$input );


# Ugly way to try to preserve comments: keep track of statement count, and push
# all found comments in the according hash element.
my %comments;
my $stmtno = 1;

my $query_started = 0;
READ: while(1) {
    for my $event ( @{ $slr->events() } ) {
        my ($name, $start, $end, undef) = @{$event};

        if ($name eq 'new_query') {
            $stmtno++;
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

# set the counter to 0, print_stmt() will increment it at its beginning
$stmtno = 0;

if ( my $ambiguous_status = $slr->ambiguous() ) {
    chomp $ambiguous_status;
    die "Parse is ambiguous\n", $ambiguous_status;
}

my $value_ref = $slr->value;
my $value = ${$value_ref};

# FIXME change here when real output will be added
$stmtno++;
print format_comment($comments{$stmtno}{ok}) if defined($comments{$stmtno}{ok});


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

sub plsql2pg::second {
    my (undef, undef, $node) = @_;

    return $node;
}

sub format_comment {
    my ($comments) = @_;
    my $out = '';

    foreach my $event (@{$comments}) {
        my ($name, $start, $end, undef) = @{$event};
        $out .= substr($input, $start, $end - $start) . "\n";
    }

    return $out;
}

sub plsql2pg::append_el_1_3 {
    my (undef, $nodes, undef, $node) = @_;

    push(@{$nodes}, @{$node});

    return $nodes;
}

sub plsql2pg::make_opexpr {
    my (undef, $left, $op, $right) = @_;

    return make_opexpr($left, $op, $right);
}

sub format_opexpr {
    my ($opexpr) = @_;

    return format_node($opexpr->{left}) . format_node($opexpr->{op})
         . format_node($opexpr->{right}) . format_alias($opexpr->{alias});
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

    assert_one_el($ident);
    $func->{ident} = pop(@{$ident});
    $func->{args} = $args;
    $func->{window} = $windowclause;

    return to_array($func);
}

sub format_function {
    my ($func) = @_;
    my $out = undef;
    my $ident;

    # first transform function to pg compatible if needed
    handle_function($func);

    foreach my $arg (@{$func->{args}}) {
        $out .= ', ' if defined($out);
        $out .= format_node($arg);
    }
    # arguments are optional
    $out = '' unless defined($out);

    $out = format_ident($func->{ident}) . '(' . $out . ')';

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
    my (undef, undef, undef, $tlist) = @_;

    return make_clause('PARTITIONBY', $tlist);
}

sub plsql2pg::make_distinct_selectclause {
    my (undef, $distinct, $tlist) = @_;
    my $node = make_node('target_list');

    $node->{tlist} = $tlist;
    $node->{distinct} = uc($distinct);

    return make_clause('SELECT', $node);
}

sub plsql2pg::make_selectclause {
    my (undef, $tlist) = @_;
    my $node = make_node('target_list');

    $node->{tlist} = $tlist;
    return make_clause('SELECT', $node);
}

sub plsql2pg::make_fromclause {
    my (undef, undef, $froms) = @_;

    return make_clause('FROM', $froms);
}

sub plsql2pg::make_fromonly {
    my (undef, undef, undef, $node, undef) = @_;
    my $only = make_node('from_only');

    $only->{node} = $node;

    return to_array($only);
}

sub format_from_only {
    my ($only) = @_;

    return 'ONLY (' . format_node($only->{node}) . ')';
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

sub plsql2pg::make_priorqual {
    my (undef, undef, $left, $op, $right, $join_op) = @_;
    my $node = plsql2pg::make_qual(undef, $left, $op, $right, $join_op);

    assert_one_el($node);

    $node= pop(@{$node});

    $node->{prior} = 'left';

    return to_array($node);
}

sub plsql2pg::make_qualprior {
    my (undef, $left, $op, undef, $right, $join_op) = @_;
    my $node = plsql2pg::make_qual(undef, $left, $op, $right, $join_op);

    assert_one_el($node);

    $node = pop(@{$node});
    $node->{prior} = 'right';

    return to_array($node);
}

sub plsql2pg::append_qual {
    my (undef, $quals, $qualop, $qual) = @_;

    push(@{$quals}, $qualop);
    push(@{$quals}, @{$qual});

    return $quals;
}

sub plsql2pg::make_withclause {
    my (undef, undef, $list) = @_;

    return make_clause('WITH', $list);
}

sub plsql2pg::make_with {
    my (undef, $alias, undef, undef, $select, undef) = @_;
    my $with = make_node('with');

    $with->{alias} = $alias;
    $with->{select} = $select;

    return to_array($with);
}

sub format_with {
    my ($with) = @_;

    return quote_ident($with->{alias})
           . ' AS ( ' . format_node($with->{select})
           . ' )';
}

sub plsql2pg::make_select {
    my (undef, $with, undef, @args) = @_;
    my $stmt = make_node('select');
    my $token;

    $stmt->{WITH} = $with if defined($with);

    while (scalar @args > 0) {
        $token = shift(@args);
        $stmt->{$token->{type}} = $token if defined($token);
    }

    return to_array($stmt);
}

sub plsql2pg::combine_select {
    my (undef, $nodes, $raw_op, undef, $stmt, undef) = @_;
    my $op = make_node('combine_op');

    assert_one_el($stmt);

    @{$stmt}[0]->{combined} = 1;

    $op->{op} = $raw_op;

    push(@{$nodes}, $op);
    push(@{$nodes}, @{$stmt});

    return $nodes;
}

sub format_combine_op {
    my ($op) = @_;

    return ' EXCEPT ' if (uc($op->{op}) eq 'MINUS');
    return ' ' . uc($op->{op}) . ' ';
}

sub plsql2pg::make_subquery {
    my (undef, undef, $stmts, undef, $alias) = @_;
    my $clause;
    my $node = make_node('subquery');

    $node->{alias} = $alias;
    $node->{stmts} = $stmts;
    $clause = make_clause('SUBQUERY', $node);

    return to_array($clause);
}

sub plsql2pg::alias_node {
    my (undef, $node, $alias) = @_;

    assert_one_el($node);

    @{$node}[0]->{alias} = $alias;

    return $node;
}

sub plsql2pg::parens_node {
    my (undef, undef, $node, undef) = @_;

    return parens_node($node);
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

    # just a personal preference
    return 'FULL OUTER' if ($kw1 eq 'FULL');
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
    my $node = make_node('join_on');

    $node->{quallist} = $quallist;
    return $node;
}

sub plsql2pg::make_startwith {
    my (undef, undef, undef, $quals) = @_;

    return make_clause('STARTWITH', $quals);
}

sub plsql2pg::make_connectby {
    my (undef, undef, undef, $quals) = @_;

    return make_clause('CONNECTBY', $quals);
}

sub plsql2pg::make_hierarchicalclause {
    my (undef, $clause1, $clause2) = @_;
    my $node = make_node('connectby');

    if (defined($clause2)) {
        $node->{startwith} = $clause1;
        $node->{connectby} = $clause2;
    } else {
        $node->{connectby} = $clause1;
    }

    return make_clause('HIERARCHICAL', $node);
}

sub plsql2pg::make_groupby {
    my (undef, $elem) = @_;
    my $groupby = make_node('groupby');

    assert_one_el($elem);
    $groupby->{elem} = pop(@{$elem});

    return to_array($groupby);
}

sub format_groupby {
    my ($groupby) = @_;
    my $out;

    return format_node($groupby->{elem});
}

sub plsql2pg::make_rollupcube {
    my (undef, $keyword, undef, $tlist, undef) = @_;
    my $rollbupcube = make_node('rollupcube');

    $rollbupcube->{keyword} = uc($keyword);
    $rollbupcube->{tlist} = $tlist;

    return to_array($rollbupcube);
}

sub format_rollupcube {
    my ($node) = @_;

    return $node->{keyword} . ' (' . format_target_list($node) . ')';
}

sub plsql2pg::make_groupingsetsclause {
    my (undef, undef, undef, undef, $group_list, undef) = @_;

    return to_array(make_clause('GROUPINGSETS', $group_list));
}

sub plsql2pg::make_groupbyclause {
    my (undef, undef, undef, $groupbys) = @_;

    return make_clause('GROUPBY', $groupbys);
}

sub plsql2pg::make_update {
    my (undef, undef, $from, $set, $where, $returning, $error_logging) = @_;
    my $stmt = make_node('update');

    $stmt->{FROM} = $from;
    $stmt->{SET} = $set;
    $stmt->{WHERE} = $where;

    return to_array($stmt);
}

sub format_update {
    my ($stmt) = @_;
    my $out = 'UPDATE ';

    $out .= format_node($stmt->{FROM});
    $out .= ' SET ';
    $out .= format_standard_clause($stmt->{SET}, ', ');
    $out .= ' ' . format_node($stmt->{WHERE}) if (defined($stmt->{WHERE}));

    return $out;
}

sub plsql2pg::make_update_set_set {
    my (undef, undef, $left, undef, $op, undef, $right, undef) = @_;
    my $tlist = make_node('target_list');

    $tlist->{tlist} = $left;

    return make_opexpr(parens_node($tlist), $op, parens_node($right));
}

sub plsql2pg::make_update_ident_set {
    my (undef, $left, $op, undef, $right) = @_;

    return make_opexpr($left, $op, parens_node($right));
}

sub plsql2pg::make_update_set_clause {
    my (undef, undef, $set) = @_;

    return make_clause('UPDATESET', $set);
}

sub plsql2pg::make_delete {
    my (undef, undef, $from, $where, $returning, $error_logging) = @_;
    my $stmt = make_node('delete');

    $stmt->{FROM} = $from;
    $stmt->{WHERE} = $where;

    return to_array($stmt);
}

sub format_delete {
    my ($stmt) = @_;
    my $out = 'DELETE FROM ';

    $out .= format_node($stmt->{FROM});
    $out .= ' ' . format_node($stmt->{WHERE}) if (defined($stmt->{WHERE}));

    return $out;
}

sub plsql2pg::make_insert {
    my (undef, undef, undef, $from, $cols, $data, $error_logging) = @_;
    my $stmt = make_node('insert');

    $stmt->{from} = $from;
    $stmt->{cols} = $cols;
    $stmt->{data} = $data;

    return to_array($stmt);
}

sub format_insert {
    my ($stmt) = @_;
    my $out = 'INSERT INTO';

    $out .= ' ' . format_node($stmt->{from});
    $out .= ' ' . format_node($stmt->{cols}) if defined($stmt->{cols});
    $out .= ' ' . format_node($stmt->{data});

    return $out;
}

sub plsql2pg::make_values {
    my (undef, undef, undef, $tlist, undef, $returning) = @_;
    my $values = make_node('values');

    $values->{tlist} = $tlist;

    return $values;
}

sub format_values {
    my ($values) = @_;

    return 'VALUES (' . format_target_list($values) . ')';
}

sub plsql2pg::make_parens_column_list {
    my (undef, undef, $cols, undef) = @_;
    my $clist = make_node('target_list');

    $clist->{tlist} = $cols;

    return parens_node($clist);
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

    return to_array($orderby);
}

sub plsql2pg::make_orderbyclause {
    my (undef, undef, undef, $orderbys) = @_;

    return make_clause('ORDERBY', $orderbys);
}

sub format_orderby {
    my ($orderby) = @_;
    my $out;

    $out = format_node($orderby->{elem}) . ' ' . $orderby->{order};
    $out .= ' ' . $orderby->{nulls} if (defined ($orderby->{nulls}));

    return $out;
}

sub plsql2pg::make_forupdate_wait {
    my (undef, $kw, $delay) = @_;

    return undef if ($kw eq 'NOWAIT');
    add_fixme("Clause \"WAIT $delay\" converted to \"NOWAIT\"");
}

sub plsql2pg::make_forupdateclause {
    my (undef, undef, undef, $tlist, $wait) = @_;
    my $forupdate = make_node('forupdate');

    $forupdate->{tlist} = $tlist;
    $forupdate->{nowait} = $wait;

    return make_clause('FORUPDATE', $forupdate);
}

sub format_FORUPDATE {
    my ($clause) = @_;
    my $out = 'FOR UPDATE';
    my $content = $clause->{content};

    $out .= ' OF ' . format_target_list($content) if (defined($content->{tlist}));
    $out .= ' NOWAIT' if (defined($content->{nowait}));

    return $out;
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
    my @clauselist = ('WITH', 'SELECT', 'FROM', 'JOIN', 'WHERE', 'GROUPBY',
        'HAVING', 'ORDERBY', 'FORUPDATE', 'LIMIT', 'OFFSET');
    my $nodes;
    my $out = '';

    $out .= '(' if ($stmt->{combined});

    $nodes = handle_nonsqljoin($stmt->{FROM}, $stmt->{WHERE});

    if (defined($nodes)) {
        $stmt->{join} = [] unless defined($stmt->{JOIN});
        push(@{$stmt->{JOIN}}, @{$nodes});
    }

    handle_rownum($stmt);

    $stmt = handle_connectby($stmt);

    handle_forupdate_clause($stmt);

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

    $out .= ' )' if ($stmt->{combined});

    return $out;
}

sub handle_function {
    my ($func) = @_;
    # FIXME handle packages overloading default funcs?
    my $funcname = $func->{ident}->{attribute};

    if ($funcname eq 'nvl') {
        $func->{ident}->{attribute} = 'COALESCE';
    } elsif (($funcname eq 'log10') or ($funcname eq 'log2')) {
        my $arg = make_node('number');

        assert_one_el($func->{args});

        $arg->{val} = 2;
        $arg->{val} = 10 if ($funcname eq 'log10');

        $func->{ident}->{attribute} = 'log';
        unshift(@{$func->{args}}, $arg);
    }

    return $func;
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
        my $todel = $i-1;;
        next if (ref($qual) ne 'HASH');

        # We'll need to remove any preceding AND/OR op (or following if first
        # el), too bad if it was an OR
        $todel = $i+1 if ($i == 0);

        if (isA($qual, 'parens')) {
            handle_rownum($qual, $stmt);
            if ((parens_is_empty($qual)) and (ref(@{$node}[$todel])) ne 'HASH') {
                @{$node}[$todel] = undef;
            }
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

            if (ref(@{$node}[$todel]) ne 'HASH') {
                @{$node}[$todel] = undef;
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

# This function will transform an oracle hierarchical query (CONNECT BY) to a
# standard recursive query (WITH RECURSIVE).  A new statement is returned that
# must replace the original one.
sub handle_connectby {
    my ($ori) = @_;
    my $lhs = {};
    my $rhs = {};
    my $combine;
    my $quals;
    my $with = make_node('with');
    my $select = make_node('ident');
    my $from = make_node('ident');
    my $stmt = make_node('select');
    my $previous_with;
    my $clause;

    return $ori unless(defined($ori->{HIERARCHICAL}));
    $clause = $ori->{HIERARCHICAL};
    $stmt->{WITH} = $ori->{WITH} if (defined($ori->{WITH}));
    $ori->{WITH} = undef;

    # make two quick copies of the original statement to generate both sides of
    # the UNION ALL part que the recursive query (LHS and RHS).
    while (my ($k, $v) = each %$ori) {
        next if ($k eq 'HIERARCHICAL');
        $lhs->{$k} = $v;
        $rhs->{$k} = $v;
    }

    # start constructing the WITH clause from scratch
    $with->{alias} = 'recur';
    $with->{recursive} = 1;

    # if a START WITH clause was present, transfer it to the WHERE clause of
    # the LHS
    if (defined($clause->{content}->{startwith})) {
        $quals = make_node('quallist');
        $quals->{quallist} = $clause->{content}->{startwith}->{content};
        $lhs->{WHERE} = make_clause('WHERE', $quals);
    }

    # add the LHS to the WITH clause
    $with->{select} = to_array($lhs);

    # add the UNION ALL to the WITH clause
    $combine = make_node('combine_op');
    $combine->{op} = 'UNION ALL';
    push(@{$with->{select}}, $combine);

    # transfer the CONNECT BY clause to the RHS
    $quals = make_node('quallist');
    $quals->{quallist} = $clause->{content}->{connectby}->{content};

    # if a qual's ident is tagged as PRIOR, qualify it with the recursion alias
    foreach my $q (@{$quals->{quallist}}) {
        next if (ref($q) ne 'HASH');
        $q->{$q->{prior}}->{table} = 'recur' if (defined($q->{prior}));
    }

    $rhs->{WHERE} = make_clause('WHERE', $quals);
    $rhs->{combined} = 1;
    # and finally add the RHS to the WITH clause
    push(@{$with->{select}}, to_array($rhs));

    # create a dummy select statement to attach the WITH to
    $select->{attribute} = '*';
    $stmt->{SELECT} = make_clause('SELECT', to_array($select));
    $from->{attribute} = 'recur';
    $stmt->{FROM} = make_clause('FROM', to_array($from));
    $stmt->{WHERE} = $ori->{WHERE} if (defined($ori->{WHERE}));
    # append the recursive WITH to original one if present, otherwise create a
    # new WITH clause
    if (defined($stmt->{WITH})) {
    push(@{$stmt->{WITH}->{content}}, $with);
    } else {
        $stmt->{WITH} = make_clause('WITH', to_array($with));
    }

    return $stmt;
}

# Oracle wants column name, pg wants table name, try to figure it out.  It's
# done here just in case original query only provided column name without
# reference to column and there was only one table ref.
sub handle_forupdate_clause {
    my ($stmt) = @_;
    my $forupdate = $stmt->{FORUPDATE};
    my $from = $stmt->{FROM};
    my $join = $stmt->{JOIN};
    my $tbl_count = 0;
    my $tbl_name;

    return if (not defined($forupdate));
    return if (not defined($forupdate->{content}->{tlist}));

    # get the number of table reference
    foreach my $w (@{$from->{content}}) {
        next unless defined($w);
        $tbl_count++;
        # no need to check other tables if already more than 1 found
        last if ($tbl_count > 1);
        if (defined($w->{alias})) {
            $tbl_name = $w->{alias}
        } else {
            $tbl_name = $w->{attribute}
        }
    }

    # no need to check other tables if already more than 1 found
    if (defined($join) and $tbl_count eq 1) {
        foreach my $w (@{$join->{content}}) {
            next unless defined($w);
            $tbl_count++;
            last if ($tbl_count > 1);
        }
    }
    foreach my $ident (@{$forupdate->{content}->{tlist}}) {
        if (not defined($ident->{table})) {
            if ($tbl_count == 1) {
                $ident->{attribute} = $tbl_name;
            } else {
                add_fixme("FOR UPDATE OF $ident->{attribute} must be changed ot its table name/alias");
            }
        } else {
            # remove column reference
            $ident->{attribute} = $ident->{table};
            $ident->{table} = $ident->{schema};
            $ident->{schema} = $ident->{database};
            $ident->{database} = undef;
        }
    }
}

sub parens_is_empty {
    my ($parens) = @_;

    return 0 unless (ref($parens->{node}) eq 'ARRAY');

    foreach my $node (@{$parens->{node}}) {
        if (defined($node)) {
            if ((ref($node) eq 'HASH') and (isA($node, 'parens'))) {
                my $rc = parens_is_empty($node);
                return $rc if (not $rc);
            }
        }
        return 0 if (defined($node));
    }
    return 1;
}

sub parens_get_new_node {
    my ($parens) = @_;
    my $node = undef;
    my $cpt = 0;

    return undef unless(ref($parens->{node}) eq 'ARRAY');

    foreach my $el (@{$parens->{node}}) {
        next unless(defined($el));
        return undef unless(isA($el, 'parens'));
        $node = $el->{node};
        $cpt++;
        last if($cpt > 1);
    }

    return undef if ($cpt != 1);
    return $node;
}

sub prune_parens {
    my ($parens) = @_;
    my $node;

    return if (not isA($parens, 'parens'));

    if (parens_is_empty($parens)) {
        $parens->{node} = undef;
        return;
    }

    $node = parens_get_new_node($parens);
    if (defined($node)) {
        $parens->{node} = $node;
        prune_parens($parens);
    }
}

sub splice_table_from_fromlist {
    my ($from, $name) = @_;
    my $i;

    # first, check if a table has the wanted name as alias to avoid returning
    # the wrong one
    for ($i=0; $i<(scalar @{$from}); $i++) {
        my $t = @{$from}[$i];
        if (defined($t->{alias}) and $t->{alias} eq $name) {
            return(splice(@{$from}, $i, 1));
        }
    }

    # no, then look for real table name
    for ($i=0; $i<(scalar @{$from}); $i++) {
        my $t = @{$from}[$i];
        if ($t->{attribute} eq $name) {
            return(splice(@{$from}, $i, 1));
        }
    }

    # not found, say it to caller
    return undef;
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

sub format_join_on {
    my ($node) = @_;
    my $out = undef;

    $out = 'ON '. format_quallist($node->{quallist});
    return $out;
}

sub plsql2pg::print_stmts {
    my (undef, $stmts) = @_;
    my $nbfix = 0;

    $stmtno++;

    print format_comment($comments{$stmtno}{ok})
        if (defined($comments{$stmtno}{ok}));

    foreach my $stmt (@{$stmts}) {
        print format_node($stmt);
    }
    print " ;\n";

    $nbfix += (scalar(@fixme)) if (scalar(@fixme > 0));
    $nbfix += (scalar(@{$comments{$stmtno}{fixme}}))
        if (defined($comments{$stmtno}{fixme}));

    print '-- ' . $nbfix ." FIXME for this statement\n" if ($nbfix > 0);
    foreach my $f (@fixme) {
        print "-- FIXME: $f\n";
    }
    undef(@fixme);

    if (defined($comments{$stmtno}{fixme})) {
        print '-- ' . (scalar @{$comments{$stmtno}{fixme}})
            . " comments for this statement must be replaced:\n";
        print format_comment($comments{$stmtno}{fixme});
    }
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

    return '' unless (defined($node));

    if (ref($node) eq 'ARRAY') {
        my $out = undef;
        foreach my $n (@{$node}) {
            next unless defined($n);
            $out .= format_node($n);
        }

        return $out;
    }

    return ' ' . $node . ' ' if (not ref($node));

    prune_parens($node);
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
    my $out = format_node($parens->{node});

    return '(' . $out . ')' if (defined($out) and ($out ne ''));
    return '';
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

    $out .= format_node($join->{ident});
    $out .= ' ' . format_node($join->{cond}) if defined($join->{cond});

    return $out;
}

sub format_WITH {
    my ($with) = @_;
    my $recursive = '';

    # Make the WITH recursive if any of the elem is recursive
    foreach my $w (@{$with->{content}}) {
        if (defined($w->{recursive})) {
            $recursive = 'RECURSIVE ';
            last;
        }
    }

    return "WITH " . $recursive . format_standard_clause($with, ', ');
}

sub format_target_list {
    my ($tlist) = @_;
    my $out = '';

    $out = $tlist->{distinct} . ' ' if (defined($tlist->{distinct}));
    $out .= format_array($tlist->{tlist}, ', ');

    return $out;
}

sub format_array {
    my ($arr, $delim) = @_;
    my $out = '';

    foreach my $elem (@{$arr}) {
        $out .= $delim if ($out ne '');
        $out .= format_node($elem);
    }

    return $out;
}
sub format_SELECT {
    my ($select) = @_;

    return "SELECT " . format_node($select->{content});
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

sub format_GROUPINGSETS {
    my ($node) = @_;

    return 'GROUPING SETS (' . format_standard_clause($node, ', ') . ')';
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

sub format_SUBQUERY {
    my ($query) = @_;
    my $stmts = $ query->{content}->{stmts};
    my $alias = $ query->{content}->{alias};
    my $out;

    $out .= '( ' . format_node($stmts) . ' )';

    $alias = format_alias($alias);

    # alias on subquery in mandatory in pg
    if ($alias eq '') {
        $alias = " AS " . generate_alias();
    }

    $out .= $alias;

    return $out;
}

sub plsql2pg::add_flashback {
    my (undef, $node, $flashback) = @_;

    return $node unless (defined($flashback));
    my $info = 'Flashback clause ignored for table "'
             . format_node($node) . '"';

    $info .= ': "' . format_node($flashback) . '"';

    add_fixme($info);

    return $node;
}

sub plsql2pg::make_flashback_clause {
    my (undef, @args) = @_;
    my $node = make_node('deparse');
    my $msg = shift(@args);

    if ($msg eq 'VERSIONS') {
        $msg .= ' ' . format_node(shift(@args)); # BETWEEN
        $msg .= ' ' . format_node(shift(@args)); # kind
        $msg .= ' ' . format_node(shift(@args)); # start
        $msg .= ' ' . format_node(shift(@args)); # AND
        $msg .= ' ' . format_node(shift(@args)); # end
    } else {
        $msg .= ' ' . shift(@args); # OF
        $msg .= ' ' . format_node(shift(@args)); # kind
        $msg .= ' ' . shift(@args); # expr
    }

    # keyword handling will add many spaces
    $msg =~ s/\s+/ /g;

    $node->{deparse} = $msg;

    return make_clause('FLASHBACK', $node);
}

sub format_FLASHBACK {
    my ($node) = @_;

    return format_node($node->{content});
}

sub plsql2pg::make_returning_clause {
    my (undef, undef, $list, undef, $items) = @_;
    my $msg = 'RETURNING ';

    $msg .= format_array($list, ', ') . ' INTO ' . format_array($items, ', ');

    add_fixme('Returning clause ignored: "' . $msg . '"');
}

sub plsql2pg::make_errlog_clause1 {
    my (undef, undef, undef, $into, $list) = @_;
    my $msg = 'LOG ERRORS';

    $msg .= ' INTO ' . format_node($into) if (defined($into));
    $msg .= ' (' . format_array($list, ', ') . ')' if (defined($list));

    add_fixme('Error logging clause ignored: "' . $msg . '"');

    # only add a fixme
    return undef;
}

sub plsql2pg::make_errlog_clause2 {
    my (undef, undef, undef, $limit) = @_;
    my $msg = 'REJECT LIMIT ' . uc($limit);

    add_fixme('Error logging clause ignored: "' . $msg . '"');

    # only add a fixme
    return undef;
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

sub format_deparse {
    my ($deparse) = @_;

    return $deparse->{deparse};
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

sub parens_node {
    my ($node) = @_;
    my $parens = make_node('parens');

    $parens->{node} = $node;
    return to_array($parens);
}

sub make_opexpr {
    my ($left, $op, $right) = @_;
    my $opexpr = make_node('opexpr');

    $opexpr->{left} = $left;
    $opexpr->{op} = $op;
    $opexpr->{right} = $right;

    return to_array($opexpr);
}

sub isA {
    my ($node, $type) = @_;

    return 0 if (not ref($node));
    return 0 if (ref $node ne 'HASH');

    return ($node->{type} eq $type);
}

sub assert_one_el {
    my ($arr) = @_;
    my $func = (caller(1))[3];

    error("Element is not an array in $func", $arr) if (ref($arr) ne 'ARRAY');
    error("Array contains more than one element in $func", $arr) if (scalar @{$arr} != 1);
}

sub to_array {
    my ($node) = @_;
    my $nodes = [];

    push(@{$nodes}, $node);
    return $nodes;
}

sub add_fixme {
    my ($msg) = @_;

    push(@fixme, $msg);
}

sub error {
    my ($msg, $node) = @_;

    print "ERROR: $msg\n";
    print Dumper($node);
    exit 1;
}
