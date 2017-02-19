package plsql2pg::grammar;

use strict;
use warnings;
use 5.010;

use Data::Dumper;
use plsql2pg::format;
use plsql2pg::utils;

our $dsl = <<'END_OF_DSL';
lexeme default = latm => 1

:start ::= stmtmulti

stmtmulti ::=
    stmt* separator => SEMICOLON action => ::array

stmt ::=
    raw_stmt action => call_format_stmts
    | ExplainStmt action => call_format_stmts

raw_stmt ::=
    CombinedSelectStmt
    | UpdateStmt
    | DeleteStmt
    | InsertStmt
    | ExplainStmt

ExplainStmt ::=
    EXPLAIN PLAN explain_set explain_into FOR raw_stmt
        action => make_explainplan

explain_set ::=
    SET STATEMENT_ID '=' literal action => make_explain_set
    | EMPTY

explain_into ::=
    INTO IDENT action => make_explain_into
    | EMPTY

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

SingleSelectStmt ::=
    with_clause SELECT select_clause from_clause where_clause
        hierarchical_clause group_clause having_clause
        model_clause order_clause forupdate_clause action => make_select

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
    ident parens_field_list AS '(' SelectStmt ')' search_clause cycle_clause
        action => make_with

parens_field_list ::=
    '(' field_list ')' action => second
    | EMPTY

field_list ::=
    field_list ',' IDENT action => append_el_1_3
    | IDENT

search_clause ::=
    SEARCH DEPTH FIRST BY simple_order_list SET IDENT
        action => make_searchclause
    | SEARCH BREADTH FIRST BY simple_order_list SET IDENT
        action => make_searchclause
    | EMPTY

cycle_clause ::=
    CYCLE IDENTS SET IDENT TO cycle_value DEFAULT cycle_value
        action => make_cycleclause
    | EMPTY

cycle_value ::=
    LITERAL
    | NUMBER

select_clause ::=
    distinct_elem target_list action => make_distinct_selectclause
    | target_list action => make_selectclause

distinct_elem ::=
    ALL
    | UNIQUE
    | DISTINCT

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
    | at_tz_prefix_opt target_el at_time_zone action => make_at_time_zone
    | at_tz_prefix target_el action => make_at_time_zone
    | simple_target_el
    | '(' target_list ')' action => parens_node
    | '(' SelectStmt ')' action => parens_node
    | qual_list action => make_target_qual_list

at_tz_prefix_opt ::=
    at_tz_prefix
    | EMPTY

at_tz_prefix ::=
    TIMESTAMP
    | DATE

like_clause ::=
    LIKE target_el action => make_like
    | LIKE target_el ESCAPE LITERAL action => make_like

at_time_zone ::=
    AT TIME ZONE simple_target_el action => make_timezoneexpr

simple_target_el ::=
    a_expr
    | interval

a_expr ::=
    IDENT
    | NUMBER
    | LITERAL
    | case_when
    | function
    | NULL action => make_keyword
    # use "NOT NULL" as a_expr instead of using "IS NOT" as an operator avoid
    # ambiguity (otherwise NOT would be considered as an ident)
    | NOT NULL action => make_keyword

case_when ::=
    CASE when_expr else_expr END action => make_case_when

when_expr ::=
    when_list
    | function_arg when_list action => append_el_1_2

when_list ::=
    when_list when_elem action => append_el_1_2
    | when_elem

when_elem ::=
    WHEN function_arg THEN function_arg action => make_when

else_expr ::=
    ELSE target_el action => make_else
    | EMPTY

interval ::=
    INTERVAL LITERAL interval_kind action => make_interval
    | INTERVAL LITERAL interval_kind TO interval_kind action => make_interval

interval_kind ::=
    interval_unit interval_typmod action => make_intervalkind

interval_unit ::=
    DAY
    | HOUR
    | MINUTE
    | SECOND

interval_typmod ::=
    '(' number_list ')' action => second
    | EMPTY

number_list ::=
    number_list ',' NUMBER action => append_el_1_3
    | NUMBER

function ::=
    IDENT '(' function_args ')' respect_ignore_nulls
        keep_clause window_clause action => make_function

function_args ::=
    function_args ',' function_arg action => append_el_1_3
    | function_arg
    | EMPTY action => ::undef

function_arg ::=
    # this is ambiguous for nested function call
    function_arg target_el action => append_function_arg
    | target_el action => make_function_arg

# this clause is only legal in some cases (LAG, FIRST_VALUE...), and the
# RESPECT variant isn't legal in all cases, but I couldn't find any doc that
# enumerates all the cases. Anyway, assume original query is correct
respect_ignore_nulls ::=
    RESPECT NULLS action => make_respect_ignore_nulls_clause
    | IGNORE NULLS action => make_respect_ignore_nulls_clause
    | EMPTY

keep_clause ::=
    # order_clause is mandatory here, assume original query is correct
    KEEP '(' DENSE_RANK FIRST_LAST order_clause ')' action => make_keepclause
    | EMPTY

FIRST_LAST ::=
    FIRST
    | LAST

window_clause ::=
    OVER '(' partition_clause order_clause frame_clause ')' action => make_overclause
    | EMPTY

partition_clause ::=
    # alias not legals in this target_list, assume original query is correct
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
    UNBOUNDED PRECEDING action => make_frame_boundary
    | CURRENT ROW action => make_frame_boundary
    | NUMBER PRECEDING action => make_frame_boundary

frame_end ::=
    UNBOUNDED FOLLOWING action => make_frame_boundary
    | CURRENT ROW action => make_frame_boundary
    | NUMBER FOLLOWING action => make_frame_boundary

from_clause ::=
    FROM from_list action => make_fromclause

from_list ::=
    from_list ',' from_elem action => append_el_1_3
    | from_elem

from_elem ::=
    aliased_flashback_from_elem join_clause action => append_joinlist

aliased_flashback_from_elem ::=
    flashback_from_elem ALIAS_CLAUSE action => alias_node

flashback_from_elem ::=
    simple_from_elem flashback_clause action => add_flashback

simple_from_elem ::=
    IDENT sample_clause action => add_sample_clause
    | '(' SelectStmt ')' action => make_subquery
    # ONLY is not valid for DELETE, assume original query is valid
    | ONLY '(' simple_from_elem ')' action => make_fromonly
    | '(' subjoin ')' action => second

sample_clause ::=
    SAMPLE sample_block '(' NUMBER ')' sample_seed
        action => make_sample_clause
    | EMPTY

sample_block ::=
    BLOCK
    | EMPTY

sample_seed ::=
    SEED '(' NUMBER ')' action => make_sample_seed
    | EMPTY

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
    # partition_clause only legal if followed with an outer join, assume
    # original query is correct
    partition_clause join_type JOIN join_ident ALIAS_CLAUSE join_cond action => make_normaljoin
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

IDENT ::=
    ident '.' ident '.' ident action => make_ident
    | ident '.' ident action => make_ident
    | ident action => make_ident

IDENTS ::=
    IDENTS ',' IDENT action => append_el_1_3
    | IDENT

ALIASED_IDENT ::=
    IDENT ALIAS_CLAUSE action => alias_node

LITERAL ::=
    literal action => make_literal
    | bindvar action => make_bindvar

LITERALS ::=
    LITERALS ',' LITERAL action => append_el_1_3
    | LITERAL

# PRIOR is only legal in hierarchical clause, assume original query is valid
# join_op can't be on the LHS and RHS at the same time, assume original query
# is valid
qual_no_parens ::=
    qual_elem join_op OPERATOR qual_elem join_op action => make_qual
    | qual_elem join_op qual_inop qual_elem join_op action => make_qual
    | qual_exists '(' SelectStmt ')' action => make_existsqual
    | qual_elem like_clause action => make_likeexpr
    | qual_elem BETWEEN qual_elem AND qual_elem action => make_betweenqual
    | PRIOR qual_elem OPERATOR qual_elem join_op action => make_priorqual
    | qual_elem OPERATOR PRIOR qual_elem join_op action => make_qualprior

qual_elem ::=
    target_el
    | '(' unalias_target_list ')' action => parens_node
    | '(' SelectStmt ')' action => parens_node

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
    CONNECT BY nocycle qual_list action => make_connectby

nocycle ::=
    NOCYCLE
    | EMPTY

group_clause ::=
    GROUP BY group_list action => make_groupbyclause
    | EMPTY action => ::undef

group_list ::=
    group_list ',' group_elem action => append_el_1_3
    | group_elem

group_elem ::=
    target_list action => make_groupby
    | ROLLUP '(' target_list ')' action => make_rollupcube
    | CUBE '(' target_list ')' action => make_rollupcube
    | GROUPING SETS '(' group_list ')' action => make_groupingsetsclause
    # only legal in GROUPING SETS list elem
    | '()' action => make_keyword # simple way to have empty parens as-is, not pruned

having_clause ::=
    HAVING qual_list action => make_havingclause
    | EMPTY action => ::undef

model_clause ::=
    MODEL cell_reference_options returns_rows_clause reference_models
        main_model action => make_modelclause
    | EMPTY

cell_reference_options ::=
    ignore_keep_nav dimension_single_reference action => discard
    | EMPTY

ignore_keep_nav ::=
    IGNORE NAV action => discard
    | KEEP NAV action => discard

dimension_single_reference ::=
    UNIQUE DIMENSION action => discard
    | UNIQUE SINGLE REFERENCE action => discard
    | EMPTY

returns_rows_clause ::=
    RETURN UPDATED ROWS action => discard
    | RETURN ALL ROWS action => discard
    | EMPTY

reference_models ::=
    reference_models reference_model action => discard
    | reference_model action => discard
    | EMPTY

reference_model ::=
    REFERENCE IDENT ON '(' SelectStmt ')' model_column_clauses
        cell_reference_options model_rules_clause
        action => discard

model_column_clauses ::=
    # partition_clause can be empty, assume original query is valid
    partition_clause DIMENSION BY '(' target_list ')'
        MEASURES '(' target_list ')' action => discard

main_model ::=
    main_model_name model_column_clauses cell_reference_options
        model_rules_clause action => discard

main_model_name ::=
    MAIN IDENT action => discard
    | EMPTY

model_rules_clause ::=
    rules_update_upsert iterate_clause '(' model_rules_clause_elems ')'
        action => discard

rules_update_upsert ::=
    RULES update_upsert auto_seq_order action => discard
    | EMPTY

update_upsert ::=
    UPDATE action => discard
    | UPSERT action => discard
    | UPSERT ALL action => discard
    | EMPTY

iterate_clause ::=
    ITERATE '(' NUMBER ')' until_clause action => discard
    | EMPTY

auto_seq_order ::=
    AUTOMATIC ORDER action => discard
    | SEQUENTIAL ORDER action => discard
    | EMPTY

until_clause ::=
    UNTIL '(' qual_list ')' action => discard
    | EMPTY

model_rules_clause_elems ::=
    model_rules_clause_elems ',' model_rules_clause_elem action => discard
    | model_rules_clause_elem

model_rules_clause_elem ::=
    update_upsert cell_assignment order_clause '=' target_el
        action => discard

cell_assignment ::=
    IDENT '[' cell_assignment_elems ']' action => discard

cell_assignment_elems ::=
    cell_assignment_elems ',' cell_assignment_elem action => discard
    | cell_assignment_elem action => discard

cell_assignment_elem ::=
    target_el
    | single_column_for_loop
    # multiple multi_column_for_loop is not legal, assume original query is valid
    | multi_column_for_loop

single_column_for_loop ::=
    FOR IDENT single_column_for_loop_elem action => discard

single_column_for_loop_elem ::=
    IN '(' LITERALS ')' action => discard
    | IN '(' SelectStmt ')'  action => discard
    | like_clause FROM LITERAL TO LITERAL inc_dec LITERAL action => discard
    | FROM LITERAL TO LITERAL inc_dec LITERAL action => discard

inc_dec ::=
    INCREMENT action => discard
    | DECREMENT action => discard

multi_column_for_loop ::=
    FOR '(' IDENTS IN '(' multi_column_elems ')' action => discard
    | FOR '(' IDENTS IN '(' SelectStmt ')' action => discard

multi_column_elems ::=
    multi_column_elems ',' multi_column_elem
    | multi_column_elem

multi_column_elem ::=
    '(' LITERAL ')' action => discard
    | LITERAL action => discard

order_clause ::=
    ORDER siblings_clause BY order_list action => make_orderbyclause
    | EMPTY action => ::undef

siblings_clause ::=
    SIBLINGS
    | EMPTY

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
    NOWAIT action => upper
    | WAIT INTEGER action => make_forupdate_wait
    | SKIP LOCKED action => concat
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
    | REJECT LIMIT NUMBER action => make_errlog_clause2
    | REJECT LIMIT UNLIMITED action => make_errlog_clause2
    | EMPTY

err_log_into ::=
    INTO IDENT action => second
    | EMPTY

err_log_list ::=
    '(' target_list ')' action => second
    | EMPTY

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

# keywords
ALL         ~ 'ALL':ic
AND         ~ 'AND':ic
AS          ~ 'AS':ic
ASC         ~ 'ASC':ic
AT          ~ 'AT':ic
AUTOMATIC   ~ 'AUTOMATIC':ic
BETWEEN     ~ 'BETWEEN':ic
BLOCK       ~ 'BLOCK':ic
BREADTH     ~ 'BREADTH':ic
BY          ~ 'BY':ic
CASE        ~ 'CASE':ic
CONNECT     ~ 'CONNECT':ic
CROSS       ~ 'CROSS':ic
CUBE        ~ 'CUBE':ic
:lexeme     ~ CUBE priority => 1
CURRENT     ~ 'CURRENT':ic
CYCLE       ~ 'CYCLE':ic
DATE        ~ 'DATE':ic
DAY         ~ 'DAY':ic
DECREMENT   ~ 'DECREMENT':ic
DEFAULT     ~ 'DEFAULT':ic
DELETE      ~ 'DELETE':ic
:lexeme     ~ DELETE pause => after event => keyword
DENSE_RANK  ~ 'DENSE_RANK':ic
DEPTH       ~ 'DEPTH':ic
DESC        ~ 'DESC':ic
DIMENSION   ~ 'DIMENSION':ic
DISTINCT    ~ 'DISTINCT':ic
ELSE        ~ 'ELSE':ic
END         ~ 'END':ic
ERRORS      ~ 'ERRORS':ic
ESCAPE      ~ 'ESCAPE':ic
EXISTS      ~ 'EXISTS':ic
EXPLAIN     ~ 'EXPLAIN':ic
:lexeme     ~ EXPLAIN pause => after event => keyword
FIRST       ~ 'FIRST':ic
FOLLOWING   ~ 'FOLLOWING':ic
FOR         ~ 'FOR':ic
FULL        ~ 'FULL':ic
FROM        ~ 'FROM':ic
GROUP       ~ 'GROUP':ic
GROUPING    ~ 'GROUPING':ic
HAVING      ~ 'HAVING':ic
HOUR        ~ 'HOUR':ic
IGNORE      ~ 'IGNORE':ic
IN          ~ 'IN':ic
INCREMENT   ~ 'INCREMENT':ic
INNER       ~ 'INNER':ic
INTERVAL    ~ 'INTERVAL':ic
INSERT      ~ 'INSERT':ic
:lexeme     ~ INSERT pause => after event => keyword
INTERSECT   ~ 'INTERSECT':ic
INTO        ~ 'INTO':ic
IS          ~ 'IS':ic
ITERATE     ~ 'ITERATE':ic
JOIN        ~ 'JOIN':ic
KEEP        ~ 'KEEP':ic
LAST        ~ 'LAST':ic
LEFT        ~ 'LEFT':ic
:lexeme     ~ LEFT priority => 1
LIKE        ~ 'LIKE':ic
LIMIT       ~ 'LIMIT':ic
LOCKED       ~ 'LOCKED':ic
LOG         ~ 'LOG':ic
MAIN        ~ 'MAIN':ic
MAXVALUE    ~ 'MAXVALUE':ic
:lexeme     ~ MAXVALUE priority => 1
MEASURES    ~ 'MEASURES':ic
MINUS       ~ 'MINUS':ic
MINUTE      ~ 'MINUTE':ic
MINVALUE    ~ 'MINVALUE':ic
:lexeme     ~ MINVALUE priority => 1
MODEL       ~ 'MODEL':ic
NATURAL     ~ 'NATURAL':ic
NAV         ~ 'NAV':ic
NOCYCLE     ~ 'NOCYCLE':ic
# this one is unsed in qual_inop G1 rule
NOT         ~ 'NOT':ic
# this one is used in OPERATOR L0 rule
NOWAIT      ~ 'NOWAIT':ic
NULL        ~ 'NULL':ic
:lexeme     ~ NULL priority => 1
NULLS       ~ 'NULLS':ic
OF          ~ 'OF':ic
ONLY        ~ 'ONLY':ic
OR          ~ 'OR':ic
ORDER       ~ 'ORDER':ic
ON          ~ 'ON':ic
OUTER       ~ 'OUTER':ic
OVER        ~ 'OVER':ic
PARTITION   ~ 'PARTITION':ic
PLAN        ~ 'PLAN':ic
PRECEDING   ~ 'PRECEDING':ic
PRIOR       ~ 'PRIOR':ic
RANGE       ~ 'RANGE':ic
REFERENCE   ~ 'REFERENCE':ic
REJECT      ~ 'REJECT':ic
RESPECT     ~ 'RESPECT':ic
RETURN      ~ 'RETURN':ic
RETURNING   ~ 'RETURNING':ic
RIGHT       ~ 'RIGHT':ic
:lexeme     ~ RIGHT priority => 1
ROLLUP      ~ 'ROLLUP':ic
:lexeme     ~ ROLLUP priority => 1
ROW         ~ 'ROW':ic
ROWS        ~ 'ROWS':ic
RULES       ~ 'RULES':ic
SAMPLE      ~ 'SAMPLE':ic
SCN         ~ 'SCN':ic
SEARCH      ~ 'SEARCH':ic
SECOND      ~ 'SECOND':ic
SEED        ~ 'SEED':ic
SEQUENTIAL  ~ 'SEQUENTIAL':ic
SELECT      ~ 'SELECT':ic
:lexeme     ~ SELECT pause => after event => keyword
SET         ~ 'SET':ic
SETS        ~ 'SETS':ic
SIBLINGS    ~ 'SIBLINGS':ic
SINGLE      ~ 'SINGLE':ic
SKIP        ~ 'SKIP':ic
START       ~ 'START':ic
STATEMENT_ID~ 'STATEMENT_ID':ic
THEN        ~ 'THEN':ic
:lexeme     ~ THEN priority => 1
TIME        ~ 'TIME':ic
TIMESTAMP   ~ 'TIMESTAMP':ic
TO          ~ 'TO':ic
UNBOUNDED   ~ 'UNBOUNDED':ic
UNIQUE      ~ 'UNIQUE':ic
UNION       ~ 'UNION':ic
UNLIMITED   ~ 'UNLIMITED':ic
UNTIL       ~ 'UNTIL':ic
UPDATE      ~ 'UPDATE':ic
:lexeme     ~ UPDATE pause => after event => keyword
UPDATED     ~ 'UPDATED':ic
UPSERT      ~ 'UPSERT':ic
USING       ~ 'USING':ic
VALUES      ~ 'VALUES':ic
VERSIONS    ~ 'VERSIONS':ic
WHEN        ~ 'WHEN':ic
WHERE       ~ 'WHERE':ic
WAIT        ~ 'WAIT':ic
WITH        ~ 'WITH':ic
:lexeme     ~ WITH pause => after event => keyword
ZONE        ~ 'ZONE':ic

SEMICOLON   ~ ';'
:lexeme     ~ SEMICOLON pause => after event => new_query

# everything else
digits      ~ [0-9]+
integer     ~ digits
            | digits expcast
float       ~ digits '.' digits
            | digits '.' digits expcast
            | '.' digits
            | '.' digits expcast
# exponent and type are done in L0 since no whitespace is allowed here
expcast     ~ exponent
            | cast
            | exponent cast
exponent    ~ 'e' digits
            | 'E' digits
            | 'e-' digits
            | 'E-' digits
            | 'e+' digits
            | 'E+' digits
cast        ~ 'd'
            | 'D'
            | 'f'
            | 'F'

ident   ~ unquoted_ident
        | quoted_ident
        # only for a_expr, but assuming original SQL is valid
        | '*'

bindvar ~ ':' unquoted_ident
        | ':' digits
        | ':' quoted_ident

ALIAS   ~ unquoted_start unquoted_chars
        | quoted_ident

unquoted_ident ~ unquoted_start unquoted_chars
unquoted_start ~ [a-zA-Z]
unquoted_chars ~ [a-zA-Z_0-9_$#]*

quoted_ident ~ '"' quoted_chars '"'
quoted_chars ~ [^"]+

literal         ~ literal_delim literal_chars literal_delim
literal_delim   ~ [']
literal_chars   ~ [^']*

OPERATOR    ~ '=' | '!=' | '<>' | '<' | '<=' | '>' | '>=' | '%'
            | '+' | '-' | '*' | '/' | '||' | IS

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

sub add_flashback {
    my (undef, $node, $flashback) = @_;

    return $node unless (defined($flashback));
    my $info = 'Flashback clause ignored for table "'
             . format_node($node) . '"';

    $info .= ': "' . format_node($flashback) . '"';

    add_fixme($info);

    return $node;
}

sub add_sample_clause {
    my (undef, $ident, $sample) = @_;

    assert_one_el($ident);

    @{$ident}[0]->{sample} = $sample if (defined($sample));

    return $ident;
}

sub alias_node {
    my (undef, $node, $alias) = @_;

    plsql2pg::utils::assert_one_el($node);

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

sub append_function_arg {
    my (undef, $nodes, $el) = @_;

    plsql2pg::utils::assert_one_el($nodes);
    assert_isA(@{$nodes}[0], 'function_arg');

    push(@{@{$nodes}[0]->{arg}}, @{$el});

    return $nodes;
}

sub append_joinlist {
    my (undef, $from, $joins) = @_;

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

    return format_stmts($stmts, $plsql2pg::input);
}

sub combine_parens_select {
    my (undef, $nodes, $raw_op, undef, $stmt, undef) = @_;

    return plsql2pg::utils::combine_and_parens_select($nodes, $raw_op, $stmt);
}

sub combine_select {
    my (undef, $nodes, $raw_op, $stmt) = @_;

    return plsql2pg::utils::combine_and_parens_select($nodes, $raw_op, $stmt);
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

    return plsql2pg::utils::get_alias($as, $alias);
}

sub make_at_time_zone {
    my (undef, $kw, $el, $expr) = @_;
    my $node = make_node('at_time_zone');

    $node->{kw} = $kw;
    $node->{el} = $el;
    $node->{expr} = $expr;

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

sub make_case_when {
    my (undef, undef, $whens, $else, undef) = @_;
    my $node = make_node('case_when');

    $node->{whens} = $whens;
    $node->{else} = $else;

    return node_to_array($node);
}

sub make_connectby {
    my (undef, undef, undef, $nocycle, $quals) = @_;

    if (defined($nocycle)) {
        add_fixme('NOCYCLE clause ignored for clause: ' . format_node($quals));
    }

    return make_clause('CONNECTBY', $quals);
}

sub make_cycleclause {
    my (undef, undef, $idents, undef, $ident, undef, $value, undef, $default) = @_;
    my $out = 'CYCLE ';

    $out .= format_array($idents, ', ') . ' SET ' . format_node($ident)
        . ' TO ' . format_node($value) . ' DEFAULT ' . format_node($default);

    add_fixme('CYCLE clause ignored: ' . $out);
}

sub make_delete {
    my (undef, undef, $from, $where, $returning, $error_logging) = @_;
    my $stmt = make_node('delete');

    $stmt->{FROM} = $from;
    $stmt->{WHERE} = $where;

    return node_to_array($stmt);
}

sub make_distinct_selectclause {
    my (undef, $distinct, $tlist) = @_;

    $tlist->{distinct} = uc($distinct);

    return make_clause('SELECT', $tlist);
}

sub make_else {
  my (undef, undef, $el) = @_;
  my $node = make_node('else');

  $node->{el} = $el;

  # There can only be one else, so don't array it
  return $node;
}

sub make_errlog_clause1 {
    my (undef, undef, undef, $into, $list) = @_;
    my $msg = 'LOG ERRORS';

    $msg .= ' INTO ' . format_node($into) if (defined($into));
    $msg .= ' (' . format_node($list) . ')' if (defined($list));

    add_fixme('Error logging clause ignored: "' . $msg . '"');

    # only add a fixme
    return undef;
}

sub make_errlog_clause2 {
    my (undef, undef, undef, $limit) = @_;
    my $msg = 'REJECT LIMIT ';

    if (not ref($limit)) {
        $msg .= uc($limit);
    } else {
        $msg .= format_node($limit);
    }

    add_fixme('Error logging clause ignored: "' . $msg . '"');

    # only add a fixme
    return undef;
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

sub make_explain_into {
    my (undef, undef, $ident) = @_;

    return $ident;
}

sub make_explainplan {
    my (undef, undef, undef, $set, $into, undef, $stmts) = @_;
    my $node = make_node('explain');
    my $msg = '';

    $node->{stmts} = $stmts;

    $msg .= 'SET STATEMENT_ID = ' . $set if defined ($set);
    if (defined($into)) {
        $msg .= ' ' unless($msg eq '');
        $msg .= 'INTO ' . format_node($into);
    }

    add_fixme('EXPLAIN clause ignored: ' . $msg) if ($msg ne '');

    return node_to_array($node);
}

sub make_explain_set {
    my (undef, undef, undef, undef, $literal) = @_;

    return $literal;
}

sub make_flashback_clause {
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

sub make_forupdateclause {
    my (undef, undef, undef, $tlist, $wait) = @_;
    my $forupdate = make_node('forupdate');

    $forupdate->{tlist} = $tlist;
    $forupdate->{wait_clause} = $wait;

    return make_clause('FORUPDATE', $forupdate);
}

sub make_forupdate_wait {
    my (undef, $kw, $delay) = @_;

    add_fixme('Clause "WAIT '
                . format_node($delay)
                . '" converted to "NOWAIT"');

    return 'NOWAIT';
}

sub make_frame_boundary {
    my (undef, $el1, $el2) = @_;
    my $node = make_node('frame_boundary');

    # use keyword node to avoid extra space around them
    $el1 = make_keyword(undef, $el1) if (not ref($el1));
    $el2 = make_keyword(undef, $el2);

    $node->{el1} = $el1;
    $node->{el2} = $el2;

    return $node;
}

sub make_frame_between {
    my (undef, $rangerows, undef, $frame_start, undef, $frame_end) = @_;
    my $node = make_node('frame');

    $node->{rangerows} = $rangerows;
    $node->{frame_start} = $frame_start;
    $node->{frame_end} = $frame_end;

    return $node;
}

sub make_frame_simple {
    my (undef, $rangerows, $frame_start) = @_;
    my $node = make_node('frame');

    $node->{rangerows} = $rangerows;
    $node->{frame_start} = $frame_start;

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

sub make_function {
    my (undef, $ident, undef, $args, undef, undef, undef, $windowclause) = @_;
    my $func = make_node('function');

    plsql2pg::utils::assert_one_el($ident);
    $func->{ident} = pop(@{$ident});
    $func->{args} = $args;
    $func->{window} = $windowclause;

    return node_to_array($func);
}

sub make_function_arg {
    my (undef, $el) = @_;
    my $node = make_node('function_arg');

    plsql2pg::utils::assert_one_el($el);

    $node->{arg} = $el;

    return node_to_array($node);
}

sub make_groupby {
    my (undef, $elem) = @_;
    my $groupby = make_node('groupby');

    if (isA($elem, 'target_list')) {
        $groupby->{elem} = $elem;
    } else {
        plsql2pg::utils::assert_one_el($elem);
        $groupby->{elem} = pop(@{$elem});
    }

    return node_to_array($groupby);
}

sub make_groupbyclause {
    my (undef, undef, undef, $groupbys) = @_;

    return make_clause('GROUPBY', $groupbys);
}

sub make_groupingsetsclause {
    my (undef, undef, undef, undef, $group_list, undef) = @_;

    return node_to_array(make_clause('GROUPINGSETS', $group_list));
}

sub make_havingclause {
    my (undef, undef, $quals) = @_;
    my $quallist = make_node('quallist');

    $quallist->{quallist} = $quals;

    return make_clause('HAVING', $quallist);
}

sub make_hierarchicalclause {
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

sub make_ident {
    my (undef, $table, undef, $schema, undef, $attribute) = @_;
    my @atts = ('schema', 'table', 'attribute');
    my $ident = make_node('ident');

    if (defined($attribute)) {
        $ident->{pop(@atts)} = plsql2pg::utils::quote_ident($attribute);
    }

    if (defined($schema)) {
        $ident->{pop(@atts)} = plsql2pg::utils::quote_ident($schema);
    }

    if (defined($table)) {
        $ident->{pop(@atts)} = plsql2pg::utils::quote_ident($table);
    }

    return node_to_array($ident);
}

sub make_insert {
    my (undef, undef, undef, $from, $cols, $data, $error_logging) = @_;
    my $stmt = make_node('insert');

    $stmt->{from} = $from;
    $stmt->{cols} = $cols;
    $stmt->{data} = $data;

    return node_to_array($stmt);
}

sub make_interval {
    my (undef, undef, $literal, $kind, undef, $kind2) = @_;
    my $node = make_node('interval');

    plsql2pg::utils::assert_one_el($literal);

    $node->{literal} = pop(@{$literal});
    $node->{kind} = $kind;
    $node->{kind2} = $kind2 if (defined($kind2));

    return node_to_array($node);
}

sub make_intervalkind {
    my (undef, $unit, $typmod) = @_;
    my $node = make_node('keyword');

    $node->{val} = uc($unit);

    # the typmod is only legal for SECOND in postgres, because not necessary
    # for other units. Also, only the 2nd digit is legal/needed
    if ( (defined($typmod)) and (uc($unit) eq 'SECOND') ) {
        $node->{val} .= '(' . format_node(pop(@{$typmod})) . ')';
    }

    return $node;
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

sub make_keepclause {
    my (undef, undef, undef, undef, $firstlast, $order) = @_;
    my $sql = "KEEP (DENSE_RANK $firstlast ";

    $sql .= format_node($order);

    add_fixme('KEEP clause ignored: "' . $sql . '"');

    return undef;
}

sub make_keyword {
    my (undef, $kw1, $kw2) = @_;
    my $node = make_node('keyword');

    $node->{val} = uc($kw1);
    $node->{val} .= ' ' . uc($kw2) if (defined($kw2));

    return node_to_array($node);
}

sub make_like {
    my (undef, undef, $like, undef, $escape) = @_;
    my $out = 'LIKE ' . format_node($like);

    plsql2pg::utils::assert_one_el($escape) if (defined($escape));

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

sub make_literal {
    my (undef, $value, $alias) = @_;
    my $literal = make_node('literal');

    $literal->{value} = substr($value, 1, -1); # remove the quotes
    $literal->{alias} = $alias;

    return node_to_array($literal);
}

sub make_modelclause {
    add_fixme('MODEL claused ignored');

    return undef;
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

sub make_opexpr {
    my (undef, $left, $op, $right) = @_;

    return make_node_opexpr($left, $op, $right);
}

sub make_orderby {
    my (undef, $elem, $order, $nulls) = @_;
    my $orderby = make_node('orderby');

    plsql2pg::utils::assert_one_el($elem);

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
    my (undef, undef, $siblings, undef, $orderbys) = @_;

    if (defined($siblings)) {
        add_fixme('SIBLINGS clause of ORDER BY ignored');
    }

    return make_clause('ORDERBY', $orderbys);
}

sub make_overclause {
    my (undef, undef, undef, $partition, $order, $frame, undef) = @_;
    my $clause;
    my $content = [];

    push(@{$content}, $partition) if defined($partition);
    push(@{$content}, $order) if defined($order);
    push(@{$content}, $frame) if defined($frame);
    $clause = make_clause('OVERCLAUSE', $content);

    return $clause;
}

sub make_parens_column_list {
    my (undef, undef, $cols, undef) = @_;
    my $clist = make_node('target_list');

    $clist->{tlist} = $cols;

    return _parens_node($clist);
}

sub make_partitionclause {
    my (undef, undef, undef, $tlist) = @_;

    return make_clause('PARTITIONBY', $tlist);
}

sub make_priorqual {
    my (undef, undef, $left, $op, $right, $join_op) = @_;
    my $node = make_qual(undef, $left, undef, $op, $right, $join_op);

    plsql2pg::utils::assert_one_el($node);

    $node= pop(@{$node});

    $node->{prior} = 'left';

    return node_to_array($node);
}

# Make a new qual node. If this node is an SQL89 join element, always return it
# in the form a = b(+)
sub make_qual {
    my (undef, $left, $join_op1, $op, $right, $join_op2) = @_;
    my $qual = make_node('qual');

    # uc the operator in case it has alpha char (IN, IS...)
    $qual->{op} = uc($op);
    # if the join_op is on the LHS, permute args to simplify further code
    if (defined($join_op1)) {
        $qual->{left} = pop(@{$right});
        $qual->{right} = pop(@{$left});
        $qual->{join_op} = $join_op1;
        $qual->{op} = inverse_operator($op);
    } else {
        $qual->{left} = pop(@{$left});
        $qual->{right} = pop(@{$right});
        $qual->{join_op} = $join_op2;
    }

    return node_to_array($qual);
}

sub make_qualprior {
    my (undef, $left, $op, undef, $right, $join_op) = @_;
    my $node = plsql2pg::make_qual(undef, $left, undef, $op, $right, $join_op);

    plsql2pg::utils::assert_one_el($node);

    $node = pop(@{$node});
    $node->{prior} = 'right';

    return node_to_array($node);
}

sub make_respect_ignore_nulls_clause {
    my (undef, $kw1, $kw2) = @_;
    my $sql = uc($kw1) . ' ' . uc($kw2);

    add_fixme('NULLS clause ignored: "' . $sql . '"');

    return undef;
}

sub make_returning_clause {
    my (undef, undef, $list, undef, $items) = @_;
    my $msg = 'RETURNING ';

    $msg .= format_node($list) . ' INTO ' . format_array($items, ', ');

    add_fixme('Returning clause ignored: "' . $msg . '"');
}

sub make_rollupcube {
    my (undef, $keyword, undef, $tlist, undef) = @_;
    my $rollbupcube = make_node('rollupcube');

    $rollbupcube->{keyword} = uc($keyword);
    $rollbupcube->{tlist} = $tlist;

    return node_to_array($rollbupcube);
}

sub make_sample_clause {
    my (undef, undef, undef, undef, $percent, undef, $seed) = @_;
    my $node = make_node('sample');

    $node->{percent} = $percent;
    $node->{seed} = $seed if (defined($seed));

    return $node;
}

sub make_sample_seed {
    my (undef, undef, undef, $seed, undef) = @_;

    return $seed;
}

sub make_searchclause {
    my (undef, undef, $kw, undef, undef, $list, undef, $ident) = @_;
    my $out = 'SEARCH ';

    $out .= uc($kw) . ' FIRST BY ' . format_array($list, ', ')
        . ' SET ' . format_node($ident);

    add_fixme('SEARCH clause ignored: ' . $out);
}

sub make_select {
    my (undef, $with, undef, @args) = @_;
    my $stmt = make_node('select');
    my $token;

    $stmt->{WITH} = $with if defined($with);

    while (scalar @args > 0) {
        $token = shift(@args);
        $stmt->{$token->{type}} = $token if defined($token);
    }

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

sub make_startwith {
    my (undef, undef, undef, $quals) = @_;

    return make_clause('STARTWITH', $quals);
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

sub make_timezoneexpr {
    my (undef, undef, undef, undef, $val) = @_;

    return $val;
}

sub make_update {
    my (undef, undef, $from, $set, $where, $returning, $error_logging) = @_;
    my $stmt = make_node('update');

    $stmt->{FROM} = $from;
    $stmt->{SET} = $set;
    $stmt->{WHERE} = $where;

    return node_to_array($stmt);
}

sub make_update_ident_set {
    my (undef, $left, $op, undef, $right) = @_;

    return make_node_opexpr($left, $op, _parens_node($right));
}

sub make_update_set_clause {
    my (undef, undef, $set) = @_;

    return make_clause('UPDATESET', $set);
}

sub make_update_set_set {
    my (undef, undef, $left, undef, $op, undef, $right, undef) = @_;
    my $tlist = make_node('target_list');

    $tlist->{tlist} = $left;

    return make_node_opexpr(_parens_node($tlist), $op, _parens_node($right));
}

sub make_values {
    my (undef, undef, undef, $tlist, undef, $returning) = @_;
    my $values = make_node('values');

    $values->{tlist} = $tlist;

    return $values;
}

sub make_when {
    my (undef, undef, $el1, undef, $el2) = @_;
    my $node = make_node('when');

    $node->{el1} = $el1;
    $node->{el2} = $el2;

    return node_to_array($node);
}

sub make_whereclause {
    my (undef, undef, $quals) = @_;
    my $quallist = make_node('quallist');

    $quallist->{quallist} = $quals;

    return make_clause('WHERE', $quallist);
}

sub make_with {
    my (undef, $alias, $fields, undef, undef, $select, undef) = @_;
    my $with = make_node('with');
    my $tlist;

    $with->{alias} = $alias;
    $with->{fields} = $fields;
    $with->{select} = $select;

    return node_to_array($with);
}

sub make_withclause {
    my (undef, undef, $list) = @_;

    return make_clause('WITH', $list);
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
