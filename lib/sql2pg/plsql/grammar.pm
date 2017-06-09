package sql2pg::plsql::grammar;
#------------------------------------------------------------------------------
# Project  : Multidatabase to PostgreSQL SQL converter
# Name     : sql2pg
# Author   : Julien Rouhaud, julien.rouhaud (AT) free (DOT) fr
# Copyright: Copyright (c) 2016-2017 : Julien Rouhaud - All rights reserved
#------------------------------------------------------------------------------

use strict;
use warnings;
use 5.010;

use Data::Dumper;
use sql2pg::plsql::utils;
use sql2pg::format;
use sql2pg::common;

#$sql2pg::dsl = <<'END_OF_DSL';
sub dsl {
    return <<'END_OF_DSL';
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
    | DDLStmt
    | TransacStmt

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
    with_clause SELECT select_clause into_clause from_clause where_clause
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

DDLStmt ::=
    CreateStmt
    | AlterTableStmt
    | TruncateStmt
    | CommentStmt

CreateStmt ::=
    CreateTableAsStmt
    | CreateTableStmt
    | CreateViewAsStmt
    | CreateIndexStmt
    | CreateTblspcStmt
    | CreateSequenceStmt
    | CreateProcStmt
    | CreateTriggerStmt

TransacStmt ::=
    BEGIN action => make_keyword
    | COMMIT action => make_keyword
    | ROLLBACK action => make_keyword

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
    | CONNECT_BY_ROOT IDENT action => make_connectby_ident
    | CONNECT_BY_ISLEAF IDENT action => make_connectby_ident
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
    # only valid in PIVOT clause, assume original query is correct
    | ANY action => make_keyword

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
    interval_unit typmod action => make_intervalkind

interval_unit ::=
    DAY
    | HOUR
    | MINUTE
    | SECOND

typmod ::=
    '(' number_list ')' action => second
    | '(' number_list CHAR ')' action => second
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

into_clause ::=
    INTO IDENTS action => make_intoclause
    | EMPTY

from_clause ::=
    FROM from_list action => make_fromclause

from_list ::=
    from_list ',' from_elem action => append_el_1_3
    | from_elem

from_elem ::=
    aliased_flashback_from_elem join_clause pivot_unpivot
        action => append_joinlist

aliased_flashback_from_elem ::=
    flashback_from_elem ALIAS_CLAUSE action => alias_node

flashback_from_elem ::=
    simple_from_elem pivot_unpivot flashback_clause action => add_flashback

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

pivot_unpivot ::=
    pivot_clause
    | unpivot_clause
    | EMPTY

pivot_clause ::=
    PIVOT xml '(' pivot_list ALIAS_CLAUSE pivot_for pivot_in ')'
        action => make_pivotclause

xml ::=
    XML
    | EMPTY

pivot_list ::=
    pivot_list ',' pivot_elem action => discard
    | pivot_elem action => discard

pivot_elem ::=
    IDENT '(' target_el ')' ALIAS_CLAUSE action => discard

pivot_for ::=
    FOR IDENT action => discard
    | FOR parens_column_list action => discard

pivot_in ::=
    IN '(' target_list ')' action => discard
    | IN '(' SelectStmt ')' action => discard

unpivot_clause ::=
    UNPIVOT inc_exc_nul '(' IDENT pivot_for unpivot_in ')'
        action => make_unpivotclause
    | UNPIVOT inc_exc_nul '(' parens_column_list pivot_for unpivot_in ')'
        action => make_unpivotclause

inc_exc_nul ::=
    INCLUDE NULLS action => discard
    | EXCLUDE NULLS action => discard
    | EMPTY

unpivot_in ::=
    IN '(' unpivot_in_list ')' action => discard

unpivot_in_list ::=
    unpivot_in_list ',' unpivot_in_elem
    | unpivot_in_elem

unpivot_in_elem ::=
    IDENT unpivot_in_alias action => discard
    | parens_column_list unpivot_in_alias action => discard

unpivot_in_alias ::=
    AS LITERAL action => discard
    | AS '(' LITERALS ')' action => discard
    | EMPTY

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
    | connectby_clause startwith_clause action => make_hierarchicalclause
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


CreateTableStmt ::=
    (CREATE) temp_clause (TABLE) IDENT ('(') tbl_cols (_COMMA) (')')
        tbl_att_clauses temp_commit_clause tblspc_clause
        action => make_createtable

temp_clause ::=
    TEMPORARY
    | GLOBAL TEMPORARY action => make_global_temporary
    | EMPTY

temp_commit_clause ::=
    ON COMMIT DELETE ROWS action => concat
    | EMPTY

tbl_cols ::=
    tbl_cols ',' tbl_coldef action => append_el_1_3
    | tbl_coldef

tbl_coldef ::=
    IDENT datatype col_pk col_default (_ENCRYPT) check_clause generated_clause
        NOT_NULL tbl_col_references enable_clause action => make_tbl_coldef
    | IDENT AS ('(') target_el (')') action => make_tbl_coldef_as

tbl_col_references ::=
    CONSTRAINT IDENT REFERENCES IDENT ('(') IDENT (')') fk_on_del fk_on_upd
        deferrable_clause action => make_tbl_col_references
    | EMPTY

generated_clause ::=
    (GENERATED ALWAYS AS) ('(') target_el (')') _VIRTUAL action => ::first
    | EMPTY

tbl_att_clauses ::=
    tbl_att_clause* action => ::array

tbl_att_clause ::=
    PCTFREE INTEGER action => make_tbl_attribute
    | PCTUSED INTEGER action => discard
    | INITRANS INTEGER action => discard
    | MAXTRANS INTEGER action => discard
    | COMPRESS action => discard
    | NOCOMPRESS action => discard
    | LOGGING action => discard
    # doc is unclear, NOLOGGING seems different from pg UNLOGGED
    | NOLOGGING action => discard
    | storage_clause action => discard

storage_clause ::=
    STORAGE '(' storage_clause_els ')' action => discard

storage_clause_els ::=
    storage_clause_el* action => discard

storage_clause_el ::=
    INITIAL SIZE_CLAUSE action => discard
    | NEXT SIZE_CLAUSE action => discard
    | MINEXTENTS INTEGER action => discard
    | MAXEXTENTS INTEGER action => discard
    | PCTINCREASE INTEGER action => discard
    | FREELIST action => discard
    | FREELISTS INTEGER action => discard
    | GROUPS INTEGER action => discard
    | BUFFER_POOL DEFAULT action => discard
    | FLASH_CACHE DEFAULT action => discard
    | CELL_FLASH_CACHE DEFAULT action => discard

tblspc_clause ::=
    TABLESPACE IDENT action => second
    | EMPTY

datatype ::=
    IDENT typmod action => make_datatype

col_pk ::=
    PRIMARY KEY
    | EMPTY

col_default ::=
    DEFAULT ('(') target_el (')') action => make_coldefault
    | DEFAULT target_el action => make_coldefault
    | EMPTY

check_clause ::=
    CHECK ('(') target_el (')') deferrable_clause action => make_check_clause
    | EMPTY

deferrable_clause ::=
    DEFERRABLE action => upper
    | DEFERRABLE INITIALLY DEFERRED action => concat
    | DEFERRABLE INITIALLY IMMEDIATE action => concat
    | NOT DEFERRABLE action => upper
    | NOT DEFERRABLE INITIALLY DEFERRED action => concat
    | NOT DEFERRABLE INITIALLY IMMEDIATE action => concat
    | EMPTY

CreateTableAsStmt ::=
    CREATE TABLE IDENT CT_options AS SelectStmt action => make_createtableas

CT_options ::=
    CT_option* action => discard

CT_option ::=
    PARALLEL
    | COMPRESS

CreateViewAsStmt ::=
    CREATE or_replace_clause (_FORCE) VIEW IDENT view_cols AS SelectStmt
        view_opts action => make_createviewas

view_cols ::=
    '(' view_cols_elems ')' action => second
    | EMPTY

view_cols_elems ::=
    view_cols_elems ',' view_cols_elem action => append_el_1_3
    | view_cols_elem

view_cols_elem ::=
    # drop everything except the IDENT
    IDENT _UNIQUE _RELY enable_clause validate_clause action => ::first
    | AT_constraint_def

view_opts ::=
    WITH CHECK OPTION action => concat
    | EMPTY

or_replace_clause ::=
    OR REPLACE action => concat
    | EMPTY

CreateIndexStmt ::=
    CREATE _UNIQUE INDEX IDENT ON IDENT ('(') simple_order_list (')')
        tblspc_clause action => make_createindex

CreateTblspcStmt ::=
    # ignore BIG|SMALL and tablespace kind
    (CREATE tblspc_bigsmall tblspc_kind TABLESPACE) IDENT tblspc_options
        action => make_tblspc

tblspc_bigsmall ::=
    SMALLFILE
    | BIGFILE
    | EMPTY

tblspc_kind ::=
    TEMPORARY
    | UNDO
    | EMPTY

tblspc_options ::=
    tblspc_option*

tblspc_option ::=
    DATAFILE LITERAL action => second
    | TEMPFILE LITERAL action => second
    # drop everything else
    | MINIMUM EXTENT SIZE_CLAUSE action => discard
    | BLOCKSIZE SIZE_CLAUSE action => discard
    | FORCE LOGGING action => discard
    | LOGGING action => discard
    | ONLINE action => discard
    | OFFLINE action => discard
    | extend_mgmt_clause action => discard
    | segment_mgmt_clause action => discard
    | flashback__mode_clause action => discard
    | MAXSIZE SIZE_CLAUSE action => discard
    | SIZE SIZE_CLAUSE action => discard
    | REUSE action => discard
    | AUTOEXTEND ON action => discard
    | AUTOEXTEND OFF action => discard
    | NEXT SIZE_CLAUSE action => discard
    | RETENTION GUARANTEE action => discard
    | RETENTION NOGUARANTEE action => discard

extend_mgmt_clause ::=
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE action => discard
    | EXTENT MANAGEMENT LOCAL UNIFORM SIZE SIZE_CLAUSE action => discard
    | EXTENT MANAGEMENT DICTIONNARY action => discard

segment_mgmt_clause ::=
    SEGMENT SPACE MANAGEMENT AUTO action => discard
    | SEGMENT SPACE MANAGEMENT MANUAL action => discard

flashback__mode_clause ::=
    FLASHBACK ON action => discard
    | FLASHBACK OFF action => discard

CreateSequenceStmt ::=
    CREATE SEQUENCE IDENT seq_options action => make_createsequence

seq_options ::=
    seq_option* action => ::array

#documentation is unclear, but looks like any order is valid
seq_option ::=
    (START WITH) INTEGER action => make_seq_startwith
    | (INCREMENT BY) INTEGER action => make_seq_incby
    | (MINVALUE) INTEGER action => make_seq_minval
    | (MAXVALUE) INTEGER action => make_seq_maxval
    | (CACHE) INTEGER action => make_seq_cache
    | (NOCACHE) action => make_seq_nocache
    | (CYCLE) action => make_seq_cycle
    | (NOCYCLE) action => make_seq_nocycle

CreateProcStmt ::=
    (CREATE) or_replace_clause (pl_type) IDENT pl_arglist pl_return_clause
        (AS_IS) pl_block action => make_createpl_func

pl_type ::=
    PROCEDURE
    | FUNCTION

pl_return_clause ::=
    RETURN datatype action => second
    | EMPTY

pl_arglist ::=
    ('(') pl_args (')') action => ::first
    | EMPTY

pl_args ::=
    pl_arg* separator => COMMA action => ::array

pl_arg ::=
    IDENT argmode datatype col_default action => make_pl_arg

argmode ::=
    IN action => upper
    | OUT action => upper
    | IN OUT action => concat
    | EMPTY

pl_declareblock ::=
    # in a <<IDENT>> block
    DECLARE pl_declarelist action => second
    # in the PROCEDURE block
    | pl_declarelist action => ::first
    | EMPTY

pl_declarelist ::=
    pl_var* separator => SEMICOLON action => ::array

pl_var ::=
    IDENT datatype col_default action => make_pl_var
    | IDENT datatype col_default (':=') target_el action => make_pl_var

pl_stmts ::=
    pl_stmt* separator => SEMICOLON action => ::array

pl_stmt ::=
    raw_stmt
    | function
    | IfThenElse
    | pl_block
    | pl_raise_exc
    | pl_return
    | pl_set
    | pl_for
    | NULL action => make_keyword

pl_exception ::=
    EXCEPTION pl_exception_list action => second
    | EMPTY

pl_exception_list ::=
    pl_exception_when+ separator => SEMICOLON action => ::array

pl_exception_when ::=
    WHEN IDENT THEN pl_stmts action => make_pl_exception_when

pl_block ::=
    pl_block_ident pl_declareblock (BEGIN) pl_stmts pl_exception (END) _IDENT
        action => make_pl_block

pl_block_ident ::=
    ('<<') IDENT ('>>') action => ::first
    | EMPTY

IfThenElse ::=
    (IF) target_el (THEN) pl_stmts (END IF) action => make_pl_ifthenelse
    | (IF) target_el (THEN) pl_stmts (ELSE) pl_stmts (END IF)
        action => make_pl_ifthenelse

pl_raise_exc ::=
    RAISE IDENT action => make_pl_raise

pl_return ::=
    RETURN action => make_pl_ret
    | RETURN target_el action => make_pl_ret

pl_set ::=
    IDENT ':=' target_el action => make_pl_set

pl_for ::=
    FOR IDENT IN pl_for_cond pl_loop action => make_pl_for

pl_for_cond ::=
    SelectStmt
    # a_expr is probably not restrictive enough, assume original query is valid
    | _REVERSE a_expr '..' a_expr action => make_pl_dotdot

pl_loop ::=
    LOOP pl_stmts END LOOP action => make_pl_loop

CreateTriggerStmt ::=
    (CREATE) or_replace_clause (TRIGGER) IDENT trigger_when (ON) IDENT
        trigger_for pl_block action => make_createtrigger

trigger_when ::=
    AFTER trigger_event action => concat
    | BEFORE trigger_event action => concat
    | INSTEAD OF trigger_event action => concat

trigger_event ::=
    INSERT
    | UPDATE
    | DELETE

trigger_for ::=
    FOR EACH ROW action => concat
    | FOR EACH STATEMENT action => concat

AlterTableStmt ::=
    ALTER TABLE IDENT AT_action action => make_altertable

AT_action ::=
    ADD AT_constraint_def action => make_AT_add_constraints
    | ADD ('(') AT_constraints (')') action => make_AT_add_constraints

AT_constraint_def ::=
    CONSTRAINT IDENT AT_constraint tblspc_clause action => make_AT_constraint_def

_USING_INDEX ::=
    USING INDEX action => discard
    | USING INDEX IDENT action => third
    | EMPTY

AT_constraint ::=
    AT_constraint_clause _USING_INDEX (_RELY) enable_clause validate_clause
        action => make_AT_constraint_add_clauses

AT_constraints ::=
    AT_constraints ',' AT_constraint_def action => append_el_1_3
    | AT_constraint_def

AT_constraint_clause ::=
    check_clause
    | unique_clause
    | pk_clause
    | fk_clause

enable_clause ::=
    ENABLE action => discard
    | DISABLE
    | EMPTY

validate_clause ::=
    VALIDATE action => upper
    | NOVALIDATE action => upper
    | EMPTY

unique_clause ::=
    UNIQUE ('(') IDENTS (')') action => make_unique_clause

pk_clause ::=
    PRIMARY KEY ('(') IDENTS (')') action => make_pk_clause

fk_clause ::=
    (FOREIGN KEY) ('(') IDENTS (')') (REFERENCES) IDENT ('(') IDENTS (')')
        fk_on_del fk_on_upd deferrable_clause action => make_fk_clause

fk_on_del ::=
    (ON DELETE) fk_action
    | EMPTY

fk_on_upd ::=
    (ON UPDATE) fk_action
    | EMPTY

fk_action ::=
    NO ACTION action => concat
    | RESTRICT action => upper
    | CASCADE action => upper
    | SET NULL action => upper
    | SET DEFAULT action => concat

TruncateStmt ::=
    TRUNCATE TABLE IDENT truncate_opts action => make_truncatestmt

truncate_opts ::=
    truncate_opt* action => discard

truncate_opt ::=
    PRESERVE MATERIALIZED VIEW LOG action => discard
    | PURGE MATERIALIZED VIEW LOG action => discard

CommentStmt ::=
    COMMENT ON comment_obj IDENT IS LITERAL action => make_comment

comment_obj ::=
    TABLE action => upper
    | COLUMN action => upper

NOT_NULL ::=
    NOT NULL action => concat
    # only valid in check constraints, assume original query is valid
    | IS NOT NULL action => concat
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

SIZE_CLAUSE ::=
    # for now, all usage of this isn't translated
    integer_unit action => discard
    | UNLIMITED action => discard

AS_IS ::=
    AS
    | IS

_IDENT ::=
    IDENT
    | EMPTY

_UNIQUE ::=
    UNIQUE
    | EMPTY

_RELY ::=
    RELY
    | EMPTY

_FORCE ::=
    FORCE
    | EMPTY

_ENCRYPT ::=
    ENCRYPT
    | EMPTY

_REVERSE ::=
    REVERSE
    | EMPTY

_VIRTUAL ::=
    VIRTUAL
    | EMPTY

_COMMA ::=
    COMMA
    | EMPTY

COMMA ::=
    ','

OPERATOR ::=
    operator action => upper

# keywords
ACTION              ~ 'ACTION':ic
ADD                 ~ 'ADD':ic
AFTER               ~ 'AFTER':ic
ALWAYS              ~ 'ALWAYS':ic
ALL                 ~ 'ALL':ic
ALTER               ~ 'ALTER':ic
:lexeme             ~ ALTER pause => after event => keyword
AND                 ~ 'AND':ic
ANY                 ~ 'ANY':ic
AS                  ~ 'AS':ic
ASC                 ~ 'ASC':ic
AT                  ~ 'AT':ic
AUTO                ~ 'AUTO':ic
AUTOALLOCATE        ~ 'AUTOALLOCATE':ic
AUTOEXTEND          ~ 'AUTOEXTEND':ic
AUTOMATIC           ~ 'AUTOMATIC':ic
BEFORE              ~ 'BEFORE':ic
BEGIN               ~ 'BEGIN':ic;
:lexeme             ~ BEGIN pause => after event => keyword
BETWEEN             ~ 'BETWEEN':ic
BIGFILE             ~ 'BIGFILE':ic
BLOCK               ~ 'BLOCK':ic
BLOCKSIZE           ~ 'BLOCKSIZE':ic
BREADTH             ~ 'BREADTH':ic
BUFFER_POOL         ~ 'BUFFER_POOL':ic
BY                  ~ 'BY':ic
CACHE               ~ 'CACHE':ic
CASCADE             ~ 'CASCADE':ic
CASE                ~ 'CASE':ic
CELL_FLASH_CACHE    ~ 'CELL_FLASH_CACHE':ic
CHAR                ~ 'CHAR':ic
CHECK               ~ 'CHECK':ic
COLUMN              ~ 'COLUMN':ic
COMMENT             ~ 'COMMENT':ic
:lexeme             ~ COMMENT pause => after event => keyword
COMMIT              ~ 'COMMIT';
:lexeme             ~ COMMIT pause => after event => keyword
COMPRESS            ~ 'COMPRESS':ic
CONNECT             ~ 'CONNECT':ic
CONNECT_BY_ROOT     ~ 'CONNECT_BY_ROOT':ic
CONNECT_BY_ISLEAF   ~ 'CONNECT_BY_ISLEAF':ic
CONSTRAINT          ~'CONSTRAINT':ic
CREATE              ~ 'CREATE':ic
:lexeme             ~ CREATE pause => after event => keyword
CROSS               ~ 'CROSS':ic
CUBE                ~ 'CUBE':ic
:lexeme             ~ CUBE priority => 1
CURRENT             ~ 'CURRENT':ic
CYCLE               ~ 'CYCLE':ic
DATAFILE            ~ 'DATAFILE':ic
DATE                ~ 'DATE':ic
DAY                 ~ 'DAY':ic
DECLARE             ~ 'DECLARE':ic
DECREMENT           ~ 'DECREMENT':ic
DEFAULT             ~ 'DEFAULT':ic
DEFERRABLE          ~ 'DEFERRABLE':ic
DEFERRED            ~ 'DEFERRED':ic
DELETE              ~ 'DELETE':ic
:lexeme             ~ DELETE pause => after event => keyword
DENSE_RANK          ~ 'DENSE_RANK':ic
DEPTH               ~ 'DEPTH':ic
DESC                ~ 'DESC':ic
DICTIONNARY         ~ 'DICTIONNARY':ic
DIMENSION           ~ 'DIMENSION':ic
DISABLE             ~ 'DISABLE':ic
DISTINCT            ~ 'DISTINCT':ic
EACH                ~ 'EACH':ic
ELSE                ~ 'ELSE':ic
ENABLE              ~ 'ENABLE':ic
ENCRYPT             ~ 'ENCRYPT':ic
END                 ~ 'END':ic
ERRORS              ~ 'ERRORS':ic
ESCAPE              ~ 'ESCAPE':ic
EXCEPTION           ~ 'EXCEPTION':ic
EXCLUDE             ~ 'EXCLUDE':ic
EXISTS              ~ 'EXISTS':ic
EXPLAIN             ~ 'EXPLAIN':ic
:lexeme             ~ EXPLAIN pause => after event => keyword
EXTENT              ~ 'EXTENT':ic
FLASHBACK           ~ 'FLASHBACK':ic
FLASH_CACHE         ~ 'FLASH_CACHE':ic
FIRST               ~ 'FIRST':ic
FOLLOWING           ~ 'FOLLOWING':ic
FOR                 ~ 'FOR':ic
FORCE               ~ 'FORCE':ic
FOREIGN             ~ 'FOREIGN':ic
FREELIST            ~ 'FREELIST':ic
FREELISTS           ~ 'FREELISTS':ic
FROM                ~ 'FROM':ic
FULL                ~ 'FULL':ic
FUNCTION            ~ 'FUNCTION':ic
GUARANTEE           ~ 'GUARANTEE':ic
GENERATED           ~ 'GENERATED':ic
GLOBAL              ~ 'GLOBAL':ic
GROUP               ~ 'GROUP':ic
GROUPING            ~ 'GROUPING':ic
GROUPS              ~ 'GROUPS':ic
HAVING              ~ 'HAVING':ic
HOUR                ~ 'HOUR':ic
IF                  ~ 'IF':ic
IGNORE              ~ 'IGNORE':ic
IMMEDIATE           ~ 'IMMEDIATE':ic
IN                  ~ 'IN':ic
INCLUDE             ~ 'INCLUDE':ic
INCREMENT           ~ 'INCREMENT':ic
INDEX               ~ 'INDEX':ic
INITIAL             ~ 'INITIAL':ic
INITIALLY           ~ 'INITIALLY':ic
INITRANS            ~ 'INITRANS':ic
INNER               ~ 'INNER':ic
INTERVAL            ~ 'INTERVAL':ic
INSERT              ~ 'INSERT':ic
:lexeme             ~ INSERT pause => after event => keyword
INSTEAD             ~ 'INSTEAD':ic
INTERSECT           ~ 'INTERSECT':ic
INTO                ~ 'INTO':ic
_IS                 ~ 'IS':ic
IS                  ~ 'IS':ic
ITERATE             ~ 'ITERATE':ic
JOIN                ~ 'JOIN':ic
KEEP                ~ 'KEEP':ic
KEY                 ~ 'KEY':ic
LAST                ~ 'LAST':ic
LEFT                ~ 'LEFT':ic
:lexeme             ~ LEFT priority => 1
LIKE                ~ 'LIKE':ic
LIMIT               ~ 'LIMIT':ic
LOCAL               ~ 'LOCAL':ic
LOCKED              ~ 'LOCKED':ic
LOG                 ~ 'LOG':ic
LOGGING             ~ 'LOGGING':ic
LOOP                ~ 'LOOP':ic
MAIN                ~ 'MAIN':ic
MANAGEMENT          ~ 'MANAGEMENT':ic
MANUAL              ~ 'MANUAL':ic
MATERIALIZED        ~ 'MATERIALIZED':ic
MAXEXTENTS          ~ 'MAXEXTENTS':ic
MAXSIZE             ~ 'MAXSIZE':ic
MAXTRANS            ~ 'MAXTRANS':ic
MAXVALUE            ~ 'MAXVALUE':ic
:lexeme             ~ MAXVALUE priority => 1
MEASURES            ~ 'MEASURES':ic
MINEXTENTS          ~ 'MINEXTENTS':ic
MINIMUM             ~ 'MINIMUM':ic
MINUS               ~ 'MINUS':ic
MINUTE              ~ 'MINUTE':ic
MINVALUE            ~ 'MINVALUE':ic
:lexeme             ~ MINVALUE priority => 1
MODEL               ~ 'MODEL':ic
NATURAL             ~ 'NATURAL':ic
NAV                 ~ 'NAV':ic
NEXT                ~ 'NEXT':ic
NO                  ~ 'NO':ic
NOCACHE             ~ 'NOCACHE':ic
NOCOMPRESS          ~ 'NOCOMPRESS':ic
NOCYCLE             ~ 'NOCYCLE':ic
NOGUARANTEE         ~ 'NOGUARANTEE':ic
NOLOGGING           ~ 'NOLOGGING':ic
# this one is unsed in qual_inop G1 rule
NOT                 ~ 'NOT':ic
# this one is used in OPERATOR L0 rule
NOVALIDATE          ~ 'NOVALIDATE':ic
NOWAIT              ~ 'NOWAIT':ic
NULL                ~ 'NULL':ic
:lexeme             ~ NULL priority => 1
NULLS               ~ 'NULLS':ic
OF                  ~ 'OF':ic
OFF                 ~ 'OFF':ic
OFFLINE             ~ 'OFFLINE':ic
ONLINE              ~ 'ONLINE':ic
ONLY                ~ 'ONLY':ic
OPTION              ~ 'OPTION':ic
OR                  ~ 'OR':ic
ORDER               ~ 'ORDER':ic
ON                  ~ 'ON':ic
OUT                 ~ 'OUT':ic
OUTER               ~ 'OUTER':ic
OVER                ~ 'OVER':ic
PARALLEL            ~ 'PARALLEL':ic
PARTITION           ~ 'PARTITION':ic
PCTFREE             ~ 'PCTFREE':ic
PCTINCREASE         ~ 'PCTINCREASE':ic
PCTUSED             ~ 'PCTUSED':ic
PIVOT               ~ 'PIVOT':ic
PLAN                ~ 'PLAN':ic
PRECEDING           ~ 'PRECEDING':ic
PRESERVE            ~ 'PRESERVE':ic
PRIMARY             ~ 'PRIMARY':ic
PRIOR               ~ 'PRIOR':ic
PROCEDURE           ~ 'PROCEDURE':ic
PURGE               ~ 'PURGE':ic
RAISE               ~ 'RAISE':ic
RANGE               ~ 'RANGE':ic
REFERENCE           ~ 'REFERENCE':ic
REFERENCES          ~ 'REFERENCES':ic
REJECT              ~ 'REJECT':ic
RELY                ~ 'RELY':ic
REPLACE             ~ 'REPLACE':ic
RESPECT             ~ 'RESPECT':ic
RESTRICT            ~ 'RESTRICT':ic
RETENTION           ~ 'RETENTION':ic
RETURN              ~ 'RETURN':ic
RETURNING           ~ 'RETURNING':ic
REUSE               ~ 'REUSE':ic
REVERSE             ~ 'REVERSE':ic
RIGHT               ~ 'RIGHT':ic
:lexeme             ~ RIGHT priority => 1
ROLLBACK            ~ 'ROLLBACK';
:lexeme             ~ ROLLBACK pause => after event => keyword
ROLLUP              ~ 'ROLLUP':ic
:lexeme             ~ ROLLUP priority => 1
ROW                 ~ 'ROW':ic
ROWS                ~ 'ROWS':ic
RULES               ~ 'RULES':ic
SAMPLE              ~ 'SAMPLE':ic
SCN                 ~ 'SCN':ic
SEARCH              ~ 'SEARCH':ic
SECOND              ~ 'SECOND':ic
SEED                ~ 'SEED':ic
SEGMENT             ~ 'SEGMENT':ic
SEQUENCE            ~ 'SEQUENCE':ic
SEQUENTIAL          ~ 'SEQUENTIAL':ic
SELECT              ~ 'SELECT':ic
:lexeme             ~ SELECT pause => after event => keyword
SET                 ~ 'SET':ic
SETS                ~ 'SETS':ic
SIBLINGS            ~ 'SIBLINGS':ic
SINGLE              ~ 'SINGLE':ic
SIZE                ~ 'SIZE':ic
SKIP                ~ 'SKIP':ic
SMALLFILE           ~ 'SMALLFILE':ic
SPACE               ~ 'SPACE':ic
START               ~ 'START':ic
STATEMENT           ~ 'STATEMENT':ic
STATEMENT_ID        ~ 'STATEMENT_ID':ic
STORAGE             ~ 'STORAGE':ic
TABLE               ~ 'TABLE':ic
TABLESPACE          ~ 'TABLESPACE':ic
TEMPFILE            ~ 'TEMPFILE':ic
TEMPORARY           ~ 'TEMPORARY':ic
THEN                ~ 'THEN':ic
:lexeme             ~ THEN priority => 1
TIME                ~ 'TIME':ic
TIMESTAMP           ~ 'TIMESTAMP':ic
TO                  ~ 'TO':ic
TRIGGER             ~ 'TRIGGER':ic
TRUNCATE            ~ 'TRUNCATE':ic
UNBOUNDED           ~ 'UNBOUNDED':ic
UNDO                ~ 'UNDO':ic
UNIFORM             ~ 'UNIFORM':ic
UNIQUE              ~ 'UNIQUE':ic
UNION               ~ 'UNION':ic
UNLIMITED           ~ 'UNLIMITED':ic
UNPIVOT             ~ 'UNPIVOT':ic
UNTIL               ~ 'UNTIL':ic
UPDATE              ~ 'UPDATE':ic
:lexeme             ~ UPDATE pause => after event => keyword
UPDATED             ~ 'UPDATED':ic
UPSERT              ~ 'UPSERT':ic
USING               ~ 'USING':ic
VALIDATE            ~ 'VALIDATE':ic
VALUES              ~ 'VALUES':ic
VERSIONS            ~ 'VERSIONS':ic
VIEW                ~ 'VIEW':ic
VIRTUAL             ~ 'VIRTUAL':ic
WHEN                ~ 'WHEN':ic
WHERE               ~ 'WHERE':ic
WAIT                ~ 'WAIT':ic
WITH                ~ 'WITH':ic
:lexeme             ~ WITH pause => after event => keyword
XML                 ~ 'XML':ic
ZONE                ~ 'ZONE':ic

SEMICOLON           ~ ';'
:lexeme             ~ SEMICOLON pause => after event => new_query

# everything         else
digits              ~ [0-9]+
integer             ~ digits
                    | digits expcast
integer_unit        ~ digits 'k'
                    | digits 'K'
                    | digits 'm'
                    | digits 'M'
                    | digits 'g'
                    | digits 'G'
                    | digits 't'
                    | digits 'T'
                    | digits 'p'
                    | digits 'P'
                    | digits 'e'
                    | digits 'E'
float               ~ digits '.' digits
                    | digits '.' digits expcast
                    | '.' digits
                    | '.' digits expcast
# exponent a        nd type are done in L0 since no whitespace is allowed here
expcast             ~ exponent
                    | cast
                    | exponent cast
exponent            ~ 'e' digits
                    | 'E' digits
                    | 'e-' digits
                    | 'E-' digits
                    | 'e+' digits
                    | 'E+' digits
cast                ~ 'd'
                    | 'D'
                    | 'f'
                    | 'F'

ident               ~ unquoted_ident
                    | quoted_ident
                    # only for a_expr, but assuming original SQL is valid
                    | '*'

bindvar             ~ ':' unquoted_ident
                    | ':' digits
                    | ':' quoted_ident

ALIAS               ~ unquoted_start unquoted_chars
                    | quoted_ident

unquoted_ident      ~ unquoted_start unquoted_chars
unquoted_start      ~ [a-zA-Z]
unquoted_chars      ~ [a-zA-Z_0-9_$#]*

quoted_ident        ~ '"' quoted_chars '"'
quoted_chars        ~ [^"]+

literal             ~ literal_delim literal_chars literal_delim
literal_delim       ~ [']
literal_chars       ~ [^']*

operator            ~ '=' | '!=' | '<>' | '<' | '<=' | '>' | '>=' | '%'
                    | '+' | '-' | '*' | '/' | '||' | _IS

:discard                    ~ discard
discard                     ~ whitespace | slash
whitespace                  ~ [\s]+
slash                       ~ [/]
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

sub add_flashback {
    my (undef, $node, $pivot, $flashback) = @_;

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

sub append_function_arg {
    my (undef, $nodes, $el) = @_;

    assert_one_el($nodes);
    assert_isA(@{$nodes}[0], 'function_arg');

    push(@{@{$nodes}[0]->{arg}}, @{$el});

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

sub make_alias {
    my (undef, $as, $alias) = @_;

    return quote_ident(get_alias($as, $alias));
}

sub make_altertable {
    my (undef, undef, undef, $ident, $actions) = @_;
    my $nodes = [];

    foreach my $a (@{$actions}) {
        my $node = make_node('alterobject');

        $node->{kind} = 'TABLE';
        $node->{ident} = $ident;
        $node->{action} = $a;
        push(@{$nodes}, $node);
    }

    return $nodes;
}

sub make_AT_add_constraints {
    my (undef, undef, $defs) =  @_;

    foreach my $def (@{$defs}) {
        $def->{kind} = 'ADD CONSTRAINT';
    }

    return $defs;
}

sub make_AT_constraint_def {
    my (undef, undef, $ident, $constraint, $tblspc) = @_;
    my $node = make_node('AT_action');

    assert_one_el($constraint);
    $constraint = pop(@{$constraint});

    # Remove deferrable clauses when PG don't support them
    if (
        isA($constraint, 'check_clause') or
        isA($constraint, 'unique_clause')
    ) {

        if (exists $constraint->{deferrable}) {
            delete $constraint->{deferrable};
        }
    }

    # this will be updated on AT_action g1 rule
    $node->{kind} = 'TO BE UPDATED';
    $node->{ident} = $ident;
    $node->{action} = $constraint;
    $node->{tblspc} = $tblspc;

    return node_to_array($node);
}

sub make_at_time_zone {
    my (undef, $kw, $el, $expr) = @_;
    my $node = make_node('at_time_zone');

    $node->{kw} = $kw;
    $node->{el} = $el;
    $node->{expr} = $expr;

    return node_to_array($node);
}

sub make_AT_constraint_add_clauses {
    my (undef, $constraint, $using, $enable, $validate) = @_;

    assert_one_el($constraint);
    @{$constraint}[0]->{using} = $using;
    # drop enable clause if any
    if ($validate and $validate eq 'NOVALIDATE') {
        @{$constraint}[0]->{not_valid} = 1;
    }

    return $constraint;
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

sub make_check_clause {
    my (undef, undef, $el, $deferrable) = @_;
    my $node = make_node('check_clause');

    $node->{el} = $el;
    $node->{deferrable} = $deferrable;

    return node_to_array($node);
}

sub make_coldefault {
    my (undef, undef, $el) = @_;
    my $node = make_node('coldefault');

    assert_one_el($el);
    $el = pop(@{$el});

    # Remove extraneous parens if any
    if (isA($el, 'parens') and scalar(@{$el->{node}}) == 1) {
        $el = pop(@{$el->{node}});
    }

    $node->{el} = $el;

    return node_to_array($node);
}

sub make_comment {
    my (undef, undef, undef, $object, $ident, undef, $comment) = @_;
    my $node = make_node('comment');

    $node->{object} = $object;
    $node->{ident} = $ident;
    $node->{comment} = $comment;

    return node_to_array($node);
}

sub make_connectby {
    my (undef, undef, undef, $nocycle, $quals) = @_;

    if (defined($nocycle)) {
        add_fixme('NOCYCLE clause ignored for clause: ' . format_node($quals));
    }

    return make_clause('CONNECTBY', $quals);
}

sub make_connectby_ident {
    my (undef, $kw, $ident) = @_;

    add_fixme('Clause ' . $kw . 'ignored');

    return $ident;
}

sub make_createindex {
    my (undef, undef, $unique, undef, $idxname, undef, $on, $cols, $tblspc) = @_;
    my $node = make_node('createobject');

    $node->{kind} = 'INDEX';
    $node->{unique} = $unique;
    $node->{ident} = $idxname;
    $node->{on} = $on;
    $node->{cols} = $cols;
    $node->{tblspc} = $tblspc;

    return node_to_array($node);
}

sub make_createpl_func {
    my (undef, $replace, $ident, $args, $returns, $block)
        = @_;
    my $node = make_node('pl_func');

    $node->{ident} = $ident;
    $node->{replace} = $replace;
    $node->{args} = $args;
    if ($returns) {
        $node->{returns} = $returns;
    } else {
        $node->{returns} = make_node('keyword');
        $node->{returns}->{val} = 'void';
    }

    assert_one_el($block);
    $node->{block} = pop(@{$block});

    return node_to_array($node);
}

sub make_createsequence {
    my (undef, undef, undef, $ident, $opts) = @_;
    my $node = make_node('createsequence');

    $node->{ident} = $ident;
    foreach my $o (@{$opts}) {
        $node->{$o->{opt}} = $o;
    }

    return node_to_array($node);
}

sub make_createtable {
    my (undef, $temp, $ident, $cols, $storage, $on_commit, $tblspc) = @_;
    my $node = make_node('createobject');
    my $stmts;
    my $ret = [];

    assert_one_el($ident);

    if ($temp) {
        $node->{kind} = 'TEMPORARY TABLE';
    } else {
        $node->{kind} = 'TABLE';
    }
    $node->{ident} = pop(@{$ident});
    $node->{cols} = $cols;
    $node->{storage} = $storage;
    $node->{on_commit} = $on_commit;
    $node->{tblspc} = $tblspc;

    ($node, $stmts) = sql2pg::plsql::utils::createtable_hook($node);

    push(@{$ret}, $node);
    push(@{$ret}, @{$stmts}) if (scalar @{$stmts} > 0);

    return $ret;
}

sub make_createtableas {
    my (undef, undef, undef, $ident, $options, undef, $stmt) = @_;
    my $node = make_node('createobject');

    $node->{kind} = 'TABLE';
    $node->{ident} = $ident;
    $node->{stmt} = $stmt;
    # drop $options

    return node_to_array($node);
}

sub make_createtrigger {
    my (undef, $replace, $ident, $when, $on, $for, $block) = @_;
    my $proc = make_node('pl_func');
    my $trig = make_node('createtrigger');
    my $return = make_node('pl_ret');
    my $body= make_node('pl_block');
    my $stmts = [];

    assert_one_el($block);
    $block = pop(@{$block});

    # Check if original trigger body contained "RETURN ;" instruction, and
    # replace them to "RETURN NEW ;"
    expression_tree_walker($block->{stmts}, 'sql2pg::plsql::utils::force_pl_ret_new', undef);

    # Force final "RETURN NEW ;" by surrounding all code in a new block and
    # appending a RETURN NEW after
    $return->{val}= make_node('ident');
    $return->{val}->{attribute} = 'NEW';
    $body->{stmts} = node_to_array($block);
    push(@{$body->{stmts}}, $return);

    $proc->{ident} = $ident;
    # pg's CREATE TRIGGER doesn't accept "OR REPLACE", so move it to the
    # associated function
    $proc->{replace} = $replace;
    $proc->{returns} = 'trigger';
    $proc->{block} = $body;
    push(@{$stmts}, $proc);

    $trig->{ident} = $ident;
    $trig->{when} = $when;
    $trig->{on} = $on;
    $trig->{for} = $for;
    $trig->{func} = $ident;
    push(@{$stmts}, $trig);

    return $stmts;
}

sub make_createviewas {
    my (undef, undef, $replace, undef, $ident, $atts, undef, $stmt, $opts) = @_;
    my $node = make_node('createobject');

    $node->{kind} = 'VIEW';
    $node->{replace} = $replace;
    $node->{ident} = $ident;
    $node->{view_atts} = $atts;
    $node->{stmt} = $stmt;
    $node->{view_opts} = $opts;

    return node_to_array($node);
}

sub make_cycleclause {
    my (undef, undef, $idents, undef, $ident, undef, $value, undef, $default) = @_;
    my $out = 'CYCLE ';

    $out .= format_array($idents, ', ') . ' SET ' . format_node($ident)
        . ' TO ' . format_node($value) . ' DEFAULT ' . format_node($default);

    add_fixme('CYCLE clause ignored: ' . $out);
}

sub make_datatype {
    my (undef, $ident, $typmod) = @_;
    my $node = make_node('datatype');

    assert_one_el($ident);

    $node->{ident} = pop(@{$ident});
    $node->{typmod} = $typmod;
    $node->{hook} = 'sql2pg::plsql::utils::handle_datatype';

    return node_to_array($node);
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

sub make_fk_clause {
    my (undef, $srcs, $ident, $dsts, $on_del, $on_upd, $deferrable) = @_;
    my $node = make_node('fk_clause');

    $node->{srcs} = $srcs;
    $node->{ident} = $ident;
    $node->{dsts} = $dsts;
    $node->{on_del} = $on_del;
    $node->{on_upd} = $on_upd;
    $node->{deferrable} = $deferrable;

    return node_to_array($node);
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

    assert_one_el($ident);
    $func->{ident} = pop(@{$ident});
    $func->{args} = $args;
    $func->{window} = $windowclause;
    $func->{hook} = 'sql2pg::plsql::utils::handle_function';

    return node_to_array($func);
}

sub make_function_arg {
    my (undef, $el) = @_;
    my $node = make_node('function_arg');

    assert_one_el($el);

    $node->{arg} = $el;
    $node->{hook} = 'sql2pg::plsql::utils::handle_respect_ignore_nulls';

    return node_to_array($node);
}

sub make_global_temporary {
    add_fixme("GLOBAL clause of TEMPORARY TABLE ignored");

    return "TEMPORARY";
}

sub make_groupby {
    my (undef, $elem) = @_;
    my $groupby = make_node('groupby');

    if (isA($elem, 'target_list')) {
        $groupby->{elem} = $elem;
    } else {
        assert_one_el($elem);
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

    $node->{$clause1->{type}} = $clause1;
    $node->{$clause2->{type}} = $clause2 if (defined($clause2));

    return make_clause('HIERARCHICAL', $node);
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

    assert_one_el($literal);

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

sub make_intoclause {
    my (undef, undef, $idents) = @_;

    return make_clause('INTO', $idents);
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

sub make_pivotclause {
    add_fixme('PIVOT clause ignored');
}

sub make_pk_clause {
    my (undef, undef, undef, $idents) = @_;
    my $node = make_node('pk_clause');

    $node->{idents} = $idents;

    return node_to_array($node);
}

sub make_pl_arg {
    my (undef, $ident, $argmode, $datatype, $default) = @_;
    my $node = make_node('pl_arg');

    assert_one_el($datatype);
    $datatype = pop(@{$datatype});

    # PG supports IN OUT, but SQL99 standard requires INOUT
    $argmode = 'INOUT' if ($argmode and $argmode eq 'IN OUT');

    $node->{ident} = $ident;
    $node->{argmode} = $argmode;
    $node->{datatype} = $datatype;
    $node->{default} = $default;

    return $node;
}

sub make_pl_block {
    my (undef, $ident, $declare, $body, $exception, $ident2) = @_;
    my $node = make_node('pl_block');

    $node->{ident} = $ident;
    $node->{declare} = $declare;
    $node->{stmts} = $body;
    $node->{exception} = $exception;

    return node_to_array($node);
}

sub make_pl_dotdot {
    my (undef, $reverse, $lower, undef, $upper) = @_;
    my $node = make_node('pl_dotdot');

    $node->{reverse} = $reverse;
    $node->{lower} = $lower;
    $node->{upper} = $upper;

    return node_to_array($node);
}

sub make_pl_exception_when {
    my (undef, undef, $ident, undef, $stmts) = @_;
    my $node = make_node('pl_exception_when');

    $node->{val} = make_node('literal');
    $node->{val}->{value} = format_node($ident);
    $node->{stmts} = $stmts;

    return $node;
}

sub make_pl_for {
    my (undef, undef, $ident, undef, $select, $loop) = @_;
    my $node = make_node('pl_for');

    $node->{ident} = $ident;
    $node->{cond} = $select;
    $node->{loop} = $loop;

    return node_to_array($node);
}

sub make_pl_ifthenelse {
    my (undef, $if, $then, $else, undef) = @_;
    my $node = make_node('pl_ifthenelse');

    $node->{if} = $if;
    $node->{then} = $then;
    $node->{else} = $else;

    return node_to_array($node);
}

sub make_pl_loop {
    my (undef,undef, $stmts, undef, undef) = @_;
    my $node = make_node('pl_loop');

    $node->{stmts} = $stmts;

    return node_to_array($node);
}

sub make_pl_raise {
    my (undef, undef, $ident) = @_;
    my $node = make_node('pl_raise');

    $node->{level} = 'EXCEPTION';
    $node->{val} = make_node('literal');
    $node->{val}->{value} = format_node($ident);

    return node_to_array($node);
}

sub make_pl_ret {
    my (undef, undef, $el) = @_;
    my $node = make_node('pl_ret');

    $node->{val} = $el;

    return node_to_array($node);
}

sub make_pl_set {
    my (undef, $ident, undef, $el) = @_;
    my $node = make_node('pl_set');

    $node->{ident} = $ident;
    $node->{val} = $el;

    return node_to_array($node);
}

sub make_pl_var {
    my (undef, $ident, $datatype, $default, $val) = @_;
    my $node = make_node('pl_var');

    assert_one_el($datatype);
    $datatype = pop(@{$datatype});

    if ($default) {
        assert_one_el($default);
        $default = pop(@{$default});
    }

    # ignore any exception
    return undef if ($datatype->{ident}->{attribute} eq 'exception');

    $node->{ident} = $ident;
    $node->{datatype} = $datatype;
    # replace DEFAULT clause with simple assignment
    $node->{val} = $default->{el} if ($default);
    $node->{val} = $val if ($val);

    return $node;
}

sub make_priorqual {
    my (undef, undef, $left, $op, $right, $join_op) = @_;
    my $node = make_qual(undef, $left, undef, $op, $right, $join_op);

    assert_one_el($node);

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
    my $node = make_qual(undef, $left, undef, $op, $right, $join_op);

    assert_one_el($node);

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

    $stmt->{hook} = 'sql2pg::plsql::utils::select_hook';

    return node_to_array($stmt);
}

sub make_selectclause {
    my (undef, $tlist) = @_;

    return make_clause('SELECT', $tlist);
}

sub make_seq_cache {
    my (undef, $num) = @_;
    my $node = make_node('seq_opt');

    $node->{value} = 'CACHE ' . format_node($num);
    $node->{opt} = 'cache';

    return $node;
}

sub make_seq_cycle {
    my (undef) = @_;
    my $node = make_node('seq_opt');

    $node->{value} = 'CYCLE';
    $node->{opt} = 'cycle';

    return $node;
}

sub make_seq_incby {
    my (undef, $num) = @_;
    my $node = make_node('seq_opt');

    $node->{value} = 'INCREMENT BY ' . format_node($num);
    $node->{opt} = 'incby';

    return $node;
}

sub make_seq_maxval {
    my (undef, $num) = @_;
    my $node = make_node('seq_opt');

    $node->{value} = 'MAXVALUE ' . format_node($num);
    $node->{opt} = 'maxval';

    return $node;
}

sub make_seq_minval {
    my (undef, $num) = @_;
    my $node = make_node('seq_opt');

    $node->{value} = 'MINVALUE ' . format_node($num);
    $node->{opt} = 'minval';

    return $node;
}

sub make_seq_nocache {
    my (undef) = @_;
    my $node = make_node('seq_opt');

    $node->{value} = 'CACHE 1';
    $node->{opt} = 'cache';

    return $node;
}

sub make_seq_nocycle {
    my (undef) = @_;
    my $node = make_node('seq_opt');

    $node->{value} = 'NO CYCLE';
    $node->{opt} = 'cycle';

    return $node;
}

sub make_seq_startwith {
    my (undef, $num) = @_;
    my $node = make_node('seq_opt');

    $node->{value} = 'START WITH ' . format_node($num);
    $node->{opt} = 'startwith';

    return $node;
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

sub make_tbl_attribute {
    my (undef, $kw, $val) = @_;
    my $node = make_node('tbl_attribute');

    a
    $node->{kw} = uc($kw);
    $node->{val} = $val;
    $node->{hook} = 'sql2pg::plsql::utils::handle_tbl_attribute';

    return $node;
}

sub make_tbl_coldef {
    my (undef, $ident, $datatype, $pk, $default, $check, $expr, $notnull,
        $fk, undef) = @_;
    my $node = make_node('tbl_coldef');

    $node->{pk} = 1 if ($pk);

    assert_one_el($datatype);
    $datatype = pop(@{$datatype});
    assert_one_el($ident);
    $ident = pop(@{$ident});
    if ($expr) {
        assert_one_el($expr);
        $expr = pop(@{$expr});
    }

    # PG doesn't handle DEFERRABLE in such case, drop it if any
    if ($check) {
        assert_one_el($check);
        $check = pop(@{$check});

        if (exists $check->{deferrable}) {
            delete $check->{deferrable};
        }
    }

    # FIXME kludge to replace raw(x) datatype to something else based on
    # default clause if any.
    # NOTE: this is done before any hook is called, so it's based on the
    # original values
    if ($datatype->{ident}->{attribute} eq 'raw' and $default) {
        assert_one_el($default);
        $default = pop(@{$default});
        assert_one_el($default->{el});
        $default->{el} = pop(@{$default->{el}});

        # is the check clause a specific function call?
        if (isA($default->{el}, 'function')) {
            if ($default->{el}->{ident}->{attribute} eq 'sys_guid') {
                $datatype->{ident}->{attribute} = 'uuid';
                delete $datatype->{typmod} if ($datatype->{typmod});
            }
        }
    }

    $node->{ident} = $ident;
    $node->{datatype} = $datatype;
    $node->{default} = $default;
    $node->{fk} = $fk;
    $node->{check} = $check;
    $node->{expr} = $expr;
    $node->{notnull} = $notnull;

    return node_to_array($node);
}

sub make_tbl_coldef_as {
    my (undef, $ident, undef, $expr) = @_;
    my $node = make_node('tbl_coldef');

    assert_one_el($ident);
    $ident = pop(@{$ident});
    assert_one_el($expr);
    $expr = pop(@{$expr});

    $node->{ident} = $ident;
    $node->{expr} = $expr;

    return node_to_array($node);
}

sub make_tbl_col_references {
    my (undef, undef, $fkname, undef, $ident, $dsts, $on_del, $on_upd,
        $deferrable) = @_;
    my $node = make_node('fk_clause');

    $node->{fkname} = $fkname;
    $node->{ident} = $ident;
    $node->{dsts} = $dsts;
    $node->{on_del} = $on_del;
    $node->{on_upd} = $on_upd;
    $node->{deferrable} = $deferrable;

    return node_to_array($node);
}

sub make_tblspc {
    my (undef, $ident, $options) = @_;
    my $node = make_node('createobject');

    $node->{kind} = 'TABLESPACE';
    $node->{ident} = $ident;

    OPTIONS: foreach my $o (@{$options}) {
        next OPTIONS unless (isA($o, 'literal'));
        $node->{tblspc_location} = $o;
        add_fixme('Specify absolute path to tablespace');
    }

    return node_to_array($node);
}

sub make_timezoneexpr {
    my (undef, undef, undef, undef, $val) = @_;

    return $val;
}

sub make_truncatestmt {
    my (undef, undef, undef, $ident, $opts) = @_;
    my $node = make_node('truncate_table');

    $node->{ident} = $ident;
    # discard options, not relevant for PG

    return node_to_array($node);
}

sub make_unique_clause {
    my (undef, undef, $idents) = @_;
    my $node = make_node('unique_clause');

    $node->{idents} = $idents;

    return node_to_array($node);
}

sub make_unpivotclause {
    add_fixme('UNPIVOT clause ignored');
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

sub third {
    my (undef, undef, undef, $node) = @_;

    return $node;
}

sub upper {
    my (undef, $a) = @_;

    return uc($a);
}

1;
