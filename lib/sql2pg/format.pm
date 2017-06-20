package sql2pg::format;
#------------------------------------------------------------------------------
# Project  : Multidatabase to PostgreSQL SQL converter
# Name     : sql2pg
# Author   : Julien Rouhaud, julien.rouhaud (AT) free (DOT) fr
# Copyright: Copyright (c) 2016-2017 : Julien Rouhaud - All rights reserved
#------------------------------------------------------------------------------

require Exporter;

BEGIN {
    @ISA = qw(Exporter);
    @EXPORT = qw(format_node format_comments format_array format_stmts);
}

use strict;
use warnings;
use 5.010;

use Data::Dumper;
use sql2pg;
use sql2pg::common;

our @fixme;
my $tab = "  ";
my $depth = 0;
my %exceptions = ('id' => 50001);

sub format_alias {
    my ($alias) = @_;

    return ' AS ' . $alias if defined($alias);
    return '';
}

sub format_alterobject_set {
    my ($node) = @_;
    my $out = "ALTER $node->{kind} ";

    $out .= format_node($node->{ident});
    $out .= ' SET' . format_node($node->{param});
    $out .= 'TO ' . format_node($node->{val});

    return $out;
}

sub format_appended_quals {
    my ($node) = @_;

    return format_quallist($node->{quallist});
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

sub format_alterobject {
    my ($node) = @_;
    my $out = "ALTER $node->{kind} ";

    $out .= format_node($node->{ident});
    $out .= ' ' . format_node($node->{action});
    $out .= ' TABLESPACE ' . format_node($node->{tblspc}) if ($node->{tblspc});

    return $out;
}

sub format_AT_action {
    my ($node) = @_;
    my $out;

    $out = $node->{kind};
    $out .= ' ' . format_node($node->{ident});
    $out .= ' ' . format_node($node->{action});
    $out .= ' USING INDEX ' . format_node($node->{using}) if ($node->{using});
    # handle NOT VALID here instead of in each AT_action
    $out .= ' NOT VALID' if ($node->{action}->{not_valid});
    # we don't use TABLESPACE information if any was present

    return $out;
}

sub format_at_time_zone {
    my ($node) = @_;
    my $out = '';

    $out = uc($node->{kw}) . ' ' if defined($node->{kw});
    $out .= format_node($node->{el});
    $out .= ' AT TIME ZONE ' . format_node($node->{expr})
        if (defined($node->{expr}));
    $out .= format_alias($node->{alias});

    return $out;
}

sub format_bindvar {
    my ($node) = @_;

    return translate_bindvar($node);
}

sub format_case_when {
    my ($node) = @_;
    my $out = 'CASE';

    foreach my $n (@{$node->{whens}}) {
        $out .= ' ' . format_node($n);
    }

    $out .= ' ' . format_node($node->{else}) if (defined($node->{else}));

    $out .= ' END' . format_alias($node->{alias});

    return $out;
}

sub format_cast_arg {
    my ($node) = @_;

    return format_node($node->{ident})
        . ' AS ' . format_node($node->{datatype});
}

sub format_check_clause {
    my ($node) = @_;
    my $out;

    $out = 'CHECK (' . format_node($node->{el}) . ')';
    $out .= " $node->{deferrable}" if ($node->{deferrable});

    return $out;
}

sub format_coldefault {
    my ($node) = @_;

    return 'DEFAULT (' . format_node($node->{el}) . ')';
}

sub format_combine_op {
    my ($op) = @_;

    return ' EXCEPT ' if (uc($op->{op}) eq 'MINUS');
    return ' ' . uc($op->{op}) . ' ';
}

# Handle COMMENT ON statement
sub format_comment {
    my ($node) = @_;

    return "COMMENT ON "
        . $node->{object} . ' ' . format_node($node->{ident})
        . " IS " . format_node($node->{comment});
}

# Handle comments found in SQL source
sub format_comments {
    my ($comments) = @_;
    my $out = '';

    foreach my $event (@{$comments}) {
        my ($name, $start, $end, undef) = @{$event};
        $out .= substr($sql2pg::input, $start, $end - $start) . "\n";
    }

    return $out;
}

sub format_createobject {
    my ($node) = @_;
    my $out = 'CREATE';

    $out .= ' ' . $node->{replace} if (defined($node->{replace}));
    $out .= ' UNIQUE' if ($node->{unique});
    $out .= ' ' . uc($node->{kind});
    $out .= ' IF NOT EXISTS' if ($node->{ine});
    $out .= ' ' . format_node($node->{ident});

    if ($node->{view_atts}) {
        my $tmp;

        VIEW_ATTS: foreach my $a (@{$node->{view_atts}}) {
            next VIEW_ATTS if (isA($a, 'AT_action'));

            $tmp .= ', ' if ($tmp);
            $tmp .= format_node($a);
        }

        if ($tmp) {
            $out .= "($tmp)";
        }
    }

    if (defined($node->{stmt})) {
        $out .= ' AS ' . format_node($node->{stmt});
    }

    if ($node->{datatype}) {
        $out .= ' AS ' . format_node($node->{datatype});
    }

    if ($node->{on}) {
        $out .= ' ON ' . format_node($node->{on});
    }

    if ($node->{cols}) {
        $out .= " (\n    " . format_array($node->{cols}, ",\n    ") . "\n)";
    }

    if ($node->{auth}) {
        $out .= ' AUTHORIZATION ' . format_node($node->{auth});
    }

    if ($node->{tblspc_location}) {
        $out .= ' LOCATION ' . format_node($node->{tblspc_location});
    }

    if ($node->{storage}) {
        my $tmp;

        ELEM: foreach my $s (@{$node->{storage}}) {
            my $hook;
            # ignore discarded elements
            next ELEM if($s == 0);

            $hook = $s->{hook};
            if ($hook) {
                no strict;
                $s = &$hook($s);
                use strict;
            } else {
            }

            $tmp .= ', ' if ($tmp);
            $tmp .= "$s->{kw}=$s->{val}";
        }

        if ($tmp) {
            $out .= " WITH ($tmp)";
        }
    }

    if ($node->{view_opts}) {
        $out .= " $node->{view_opts}";
    }

    if ($node->{on_commit}) {
        $out .= " $node->{on_commit}";
    }

    if ($node->{tblspc}) {
        $out .= ' TABLESPACE ' . format_node($node->{tblspc});
    }

    return $out;
}

sub format_createsequence {
    my ($node) = @_;
    my $out = 'CREATE SEQUENCE ';

    $out .= format_node($node->{ident});
    $out .= ' ' . $node->{incby}->{value} if ($node->{incby});
    if ($node->{minval}) {
        $out .= ' ' . $node->{minval}->{value};
    } else {
        $out .= ' NO MINVALUE';
    }
    if ($node->{maxval}) {
        $out .= ' ' . $node->{maxval}->{value};
    } else {
        $out .= ' NO MAXVALUE';
    }
    $out .= ' ' . $node->{startwith}->{value} if ($node->{startwith});
    $out .= ' ' . $node->{cache}->{value} if ($node->{cache});
    $out .= ' ' . $node->{cycle}->{value} if ($node->{cycle});

    return $out;
}

sub format_createtrigger {
    my ($node) = @_;
    my $out = 'CREATE TRIGGER ' . format_node($node->{ident});

    $out .= "\n    " . $node->{when};
    $out .= ' ON ' . format_node($node->{on});
    $out .= ' ' . $node->{for};
    $out .= "\n    EXECUTE PROCEDURE " . format_node($node->{func}) . '()';

    return $out;
}

sub format_datatype {
    my ($node) = @_;
    my $hook = $node->{hook};
    my $out;

    if ($hook) {
        no strict;
        $node = &$hook($node);
        use strict;
    }

    $out = format_node($node->{ident});
    $out .= "%$node->{typeref}" if ($node->{typeref});

    $out .= '(' . format_array($node->{typmod}, ',') . ')' if ($node->{typmod});
    $out .= format_node($node->{nullnotnull}) if ($node->{nullnotnull});

    return $out;
}

sub format_delete {
    my ($stmt) = @_;
    my $out = 'DELETE FROM ';

    $out .= format_node($stmt->{FROM});
    $out .= ' ' . format_node($stmt->{WHERE}) if (defined($stmt->{WHERE}));

    return $out;
}

sub format_deparse {
    my ($deparse) = @_;

    return $deparse->{deparse};
}

sub format_else {
    my ($node) = @_;

    return 'ELSE ' . format_node($node->{el});
}

sub format_explain {
    my ($node) = @_;

    return 'EXPLAIN ' . format_node($node->{stmts});
}

sub format_fk_clause {
    my ($node) = @_;
    my $out = '';

    if ($node->{srcs}) {
        $out .= 'FOREIGN KEY (';
        $out .= format_array($node->{srcs}, ', ') . ') ';
    } else {
        $out .= 'CONSTRAINT ' . format_node($node->{fkname}) . ' ';
    }
    $out .= 'REFERENCES ' . format_node($node->{ident});
    $out .= '(' . format_array($node->{dsts}, ', ') . ')';
    $out .= " ON DELETE $node->{on_del}" if ($node->{on_del});
    $out .= " ON UPDATE $node->{on_upd}" if ($node->{on_upd});
    $out .= format_node($node->{deferrable}) if ($node->{deferrable});

    return $out;
}

sub format_FLASHBACK {
    my ($node) = @_;

    return format_node($node->{content});
}

sub format_FORUPDATE {
    my ($clause) = @_;
    my $out = 'FOR UPDATE';
    my $content = $clause->{content};

    $out .= ' OF ' . format_target_list($content)
        if (defined($content->{tlist}));
    $out .= ' ' . $content->{wait_clause}
        if (defined($content->{wait_clause}));

    return $out;
}

sub format_frame {
    my ($frame) = @_;

    if (defined($frame->{frame_end})) {
        return
            $frame->{rangerows}
            . " BETWEEN "
            . format_node($frame->{frame_start})
            . " AND "
            . format_node($frame->{frame_end});
    } else {
        return
            $frame->{rangerows}
            . " "
            . format_node($frame->{frame_start});
    }
}

sub format_frame_boundary {
    my ($node) = @_;
    my $out = '';

    return format_node($node->{el1}) . ' ' . format_node($node->{el2});
}

sub format_FROM {
    my ($from) = @_;
    my $out = '';

    foreach my $node (@{$from->{content}}) {
        if ($out ne '') {
            if (isA($node, 'join')) {
                $out .= ' ';
            } else {
                $out .= ', ';
            }
        }
        $out .= format_node($node);
    }

    return undef if ($out eq 'dual');
    return "FROM " . $out;
}

sub format_from_only {
    my ($only) = @_;

    return 'ONLY (' . format_node($only->{node}) . ')';
}

sub format_function {
    my ($func) = @_;
    my $out = undef;
    my $ident;
    my $hook = $func->{hook};

    # first transform function to pg compatible if needed
    no strict;
    $func = &$hook($func);
    use strict;

    # hook can transform a function call to something else
    if (not isA($func, 'function')) {
        return format_node($func);
    }

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

sub format_function_arg {
    my ($node) = @_;
    my $hook = $node->{hook};
    my $out = '';

    foreach my $a (@{$node->{arg}}) {
        $out .= ' ' unless($out eq '');
        $out .= format_node($a);
    }

    return $out;
}

sub format_groupby {
    my ($groupby) = @_;
    my $out;

    return format_node($groupby->{elem});
}

sub format_GROUPBY {
    my ($group) = @_;

    return "GROUP BY " . format_standard_clause($group, ', ');
}

sub format_GROUPINGSETS {
    my ($node) = @_;

    return 'GROUPING SETS (' . format_standard_clause($node, ', ') . ')';
}

sub format_HAVING {
    my ($having) = @_;
    my $out;

    return "HAVING " . format_quallist($having->{content}->{quallist});
}

sub format_ident {
    my ($ident) = @_;
    my @atts = ('attribute', 'table', 'schema');
    my $out;

    while (my $elem = pop(@atts)) {
        if (defined ($ident->{$elem})) {
            $out .= "." if defined($out);
            $out .= $ident->{$elem};
        }
    }

    $out .= format_node($ident->{sample}) if (defined($ident->{sample}));
    # FIXME handle collation name translation
    $out .= ' COLLATE ' . format_ident($ident->{collation})
        if (defined($ident->{collation}));
    $out .= format_alias($ident->{alias});

    return $out;
}

sub format_insert {
    my ($stmt) = @_;
    my $out = 'INSERT INTO';

    $out .= ' ' . format_node($stmt->{from});
    $out .= ' ' . format_node($stmt->{cols}) if defined($stmt->{cols});
    $out .= ' ' . format_node($stmt->{data});

    return $out;
}

sub format_interval {
    my ($node) = @_;
    my $out = 'INTERVAL ';

    $out .= format_node($node->{literal})
         . ' ' . format_node($node->{kind});

    $out .= ' TO ' . format_node($node->{kind2}) if (defined($node->{kind2}));

    return $out;
}

sub format_INTO {
    my ($node) = @_;

    return "INTO " . format_array($node->{content}, ', ');
}

sub format_join {
    my ($join) = @_;
    my $out;

    $out = $join->{jointype} . " JOIN ";

    $out .= format_node($join->{ident});
    $out .= ' ' . format_node($join->{cond}) if defined($join->{cond});

    return $out;
}

sub format_join_on {
    my ($node) = @_;
    my $out = undef;

    $out = 'ON '. format_quallist($node->{quallist});
    return $out;
}

sub format_keyword {
    my ($node) = @_;

    return $node->{val};
}

sub format_likeexpr {
    my ($node) = @_;

    return format_node($node->{el}) . ' ' . $node->{like};
}

sub format_LIMIT {
    my ($limit) = @_;

    return "LIMIT " . format_node($limit->{content});
}

sub format_literal {
    my ($literal) = @_;
    my $out;

    $out = "'" . $literal->{value} . "'";
    $out .= format_alias($literal->{alias});

    return $out;
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

    sql2pg::common::prune_parens($node);

    $func = "format_" . $node->{type};

    # XXX should I handle every node type explicitely instead?
    no strict;
    return &$func($node);
}

sub format_number {
    my ($num) = @_;
    my $out = '';

    $out .= 'CAST(' if (defined($num->{cast}));
    $out .= $num->{sign} if (defined($num->{sign}) and ($num->{sign} ne '+') );
    $out .= $num->{val};
    if (defined($num->{cast})) {
        $out .= ' AS ' . ($num->{cast} eq 'd' ? 'float' : 'real') . ')';
    }
    $out .= format_alias($num->{alias});

    return $out;
}

sub format_OFFSET {
    my ($offset) = @_;

    return "OFFSET " . format_node($offset->{content});
}

sub format_opexpr {
    my ($opexpr) = @_;

    return format_node($opexpr->{left}) . format_node($opexpr->{op})
         . format_node($opexpr->{right}) . format_alias($opexpr->{alias});
}

sub format_orderby {
    my ($orderby) = @_;
    my $out;

    $out = format_node($orderby->{elem}) . ' ' . $orderby->{order};
    $out .= ' ' . $orderby->{nulls} if (defined ($orderby->{nulls}));

    return $out;
}

sub format_ORDERBY {
    my ($orderby) = @_;

    return "ORDER BY " . format_standard_clause($orderby, ', ');
}

sub format_OVERCLAUSE {
    my ($windowclause) = @_;
    my $window = format_standard_clause($windowclause, ' ') || '';

    return " OVER (" . $window . ")";
}

sub format_parens {
    my ($parens) = @_;
    my $out = format_node($parens->{node});

    return '(' . $out . ')' .format_alias($parens->{alias})
        if (defined($out) and ($out ne ''));
    return '';
}

sub format_PARTITIONBY {
    my ($partitionby) = @_;

    return "PARTITION BY " . format_standard_clause($partitionby, ', ');
}

sub format_pk_clause {
    my ($node) = @_;
    my $out = 'PRIMARY KEY (';

    $out .= format_array($node->{idents}, ', ');
    $out .= ')';

    return $out;
}

sub format_pl_arg {
    my ($node) = @_;
    my $out;

    $out = format_node($node->{ident});
    # IN argmode is default, remove it if any
    $out .= ' ' . $node->{argmode}
        if ($node->{argmode} and $node->{argmode} ne 'IN');
    $out .= ' ' . format_node($node->{datatype});
    $out .= ' ' . format_node($node->{default}) if ($node->{default});

    return $out;
}

sub format_pl_block {
    my ($node) = @_;
    my $out = '';

    # block delimiter if subblock
    if ($node->{ident}) {
        $out .= "\n" . tab() . '<<' . format_node($node->{ident}) .">>\n";
    }

    # var declaration
    if ($node->{declare}) {
        $out .= tab() . "DECLARE\n";
        $depth++;
        VAR: foreach my $v (@{$node->{declare}}) {
            # can be undefined, exception var are removed at parse time
            next VAR unless($v);
            $out .= tab() . format_node($v) . " ;\n";
        }
        $depth--;
    }

    $out .= tab() . "BEGIN\n";
    $depth++;
    foreach my $s (@{$node->{stmts}}) {
        $out .= tab() . format_node($s) . " ;\n";
    }
    $depth--;

    if ($node->{exception}) {
        $out .= tab() . "EXCEPTION\n";
        $depth++;
        foreach my $e (@{$node->{exception}}) {
            $out .= tab() . format_node($e) . "";
        }
        $depth--;
    }

    $out .= tab() . "END";

    return $out;
}

sub format_pl_close {
    my ($node) = @_;

    return 'CLOSE ' . format_node($node->{ident});
}

sub format_pl_dotdot {
    my ($node) = @_;
    my $out;

    $out = uc($node->{reverse}) . ' ' if ($node->{reverse});
    $out .= format_node($node->{lower}) . '..' . format_node($node->{upper});

    return $out;
}

sub format_pl_elsif {
    my ($node) = @_;
    my $out = 'ELSIF ';

    $out .= format_node($node->{el}) . " THEN \n";

    $depth++;
    foreach my $s (@{$node->{stmts}}) {
        $out .= tab() . format_node($s) . " ;\n";
    }
    $depth--;

    return $out;
}

sub format_pl_exception_when {
    my ($node) = @_;
    my $out;
    my $name = format_node($node->{val});

    if ($exceptions{$name}) {
        # Custom exception, rewritten to SQLSTATE 'id'
        $out = "WHEN SQLSTATE '" . $exceptions{$name} . "' THEN\n";
    } else {
        # Standard exception
        $out = "WHEN " . uc($node->{val}->{value}) . " THEN\n";
    }

    $depth++;
    foreach my $s (@{$node->{stmts}}) {
        $out .= tab() . format_node($s) . " ;\n";
    }
    $depth--;

    return $out;
}

sub format_pl_exit {
    my ($node) = @_;

    return 'EXIT' unless($node->{when});
    return 'EXIT WHEN ' . format_node($node->{when});
}

sub format_pl_fetch_into {
    my ($node) = @_;

    return 'FETCH ' . format_node($node->{ident})
        . ' INTO ' . format_array($node->{into}, ', ');
}

sub format_pl_for {
    my ($node) = @_;
    my $out;

    $out = 'FOR ' . format_node($node->{ident}) . ' IN ';
    $out .= format_node($node->{cond});
    $out .= format_node($node->{loop});

    return $out;
}

sub format_pl_func {
    my ($node) = @_;
    my $out = 'CREATE';

    $out .= ' OR REPLACE' if ($node->{replace});
    $out .= ' FUNCTION';
    # function name
    $out .= ' ' . format_node($node->{ident});

    # function args if any
    $out .= '(';
    $out .= format_array($node->{args}, ', ') if ($node->{args});
    $out .= ")\n";

    $out .= "RETURNS " . format_node($node->{returns}) . " AS\n";
    $out .= "\$_\$\n";

    $out .= format_node($node->{block});

    $out .= " ;\n\$_\$ language plpgsql";

    return $out;
}

sub format_pl_ifthenelse {
    my ($node) = @_;
    my $out;

    $out = "\n" . tab() . 'IF ' . format_node($node->{if}) ." THEN\n";

    $depth++;
    foreach my $s (@{$node->{then}}) {
        $out .= tab() . format_node($s) . " ;\n";
    }
    $depth--;

    foreach my $s (@{$node->{elsifs}}) {
        $out .= tab() . format_node($s);
    }

    if ($node->{else}) {
        $out .= tab() . "ELSE\n";

        $depth++;
        foreach my $s (@{$node->{else}}) {
            $out .= tab() . format_node($s) . " ;\n";
        }
        $depth--;
    }

    $out .= tab() . "END IF";

    return $out;
}

sub format_pl_loop {
    my ($node) = @_;
    my $out;

    $out .= "\n" . tab() . "LOOP\n";
    $depth++;
        foreach my $s (@{$node->{stmts}}) {
            $out .= tab() . format_node($s) . " ;\n";
        }
    $depth--;

    $out .= tab() . "END LOOP";

    return $out;
}

sub format_pl_open_cursor {
    my ($node) = @_;
    my $out;

    $out = "\n". tab() . 'OPEN ' . format_node($node->{ident}) . " FOR\n";
    $depth++;
    $out .= tab() . format_node($node->{stmt});
    $depth--;

    return $out;
}

sub format_pl_raise {
    my ($node) = @_;
    my $out;
    my $name = format_node($node->{val});

    $out = "RAISE $node->{level} $name";

    if ($node->{args} and scalar @{$node->{args}} > 0) {
        $out .= ', ' . format_array($node->{args}, ', ');
    }

    if ($node->{level} eq 'EXCEPTION') {
        $exceptions{$name} = $exceptions{id}++
            unless (exists $exceptions{$name});
        $out .= " USING ERRCODE = '$exceptions{$name}'";
    }

    return $out;
}

sub format_pl_ret {
    my ($node) = @_;

    return "\n" . tab() . "RETURN " . format_node($node->{val});
}

sub format_pl_set {
    my ($node) = @_;

    return format_node($node->{ident}) . ' := ' . format_node($node->{val});
}

sub format_pl_var {
    my ($node) = @_;
    my $out;

    $out = format_node($node->{ident});
    $out .= ' ' . format_node($node->{datatype});
    $out .= ' := ' . format_node($node->{val}) if ($node->{val});

    return $out;
}

sub format_pl_while {
    my ($node) = @_;
    my $out;

    $out = 'WHILE ' . format_node($node->{cond});
    $out .= format_node($node->{loop});

    return $out;
}

sub format_proarg {
    my ($arg) = @_;
    my $out;

    $out .= format_node($arg->{name}) . ' ' . format_node($arg->{datatype});

    return $out;
}

sub format_qual {
    my ($qual) = @_;
    my $out = '';

    # EXISTS qual don't have LHS
    $out .= format_node($qual->{left}) . ' ' if (defined($qual->{left}));

    $out .= $qual->{op} . ' ' . format_node($qual->{right});

    $out .= ' ' . $qual->{op2} . ' ' . format_node($qual->{right2})
        if (defined($qual->{op2}));

    $out .= format_alias($qual->{alias});

    return $out;
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

sub format_rollupcube {
    my ($node) = @_;

    return $node->{keyword} . ' (' . format_node($node->{tlist}) . ')';
}

sub format_select {
    my ($stmt) = @_;
    my @clauselist = ('WITH', 'SELECT', 'INTO', 'FROM', 'WHERE', 'GROUPBY',
        'HAVING', 'ORDERBY', 'FORUPDATE', 'LIMIT', 'OFFSET');
    my $hook = $stmt->{hook};
    my $nodes;
    my $out = '';

    # Handle specific language rewriting
    if ($hook) {
        no strict;
        $stmt = &$hook($stmt);
        use strict;
    }

    sql2pg::common::handle_missing_alias($stmt);

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

sub format_SELECT {
    my ($select) = @_;

    return "SELECT " . format_node($select->{content});
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

sub format_SUBQUERY {
    my ($query) = @_;
    my $alias = $ query->{alias};
    my $out;

    $out .= '(' . format_node($query->{content}) . ')';

    $alias = format_alias($alias);

    # alias on subquery in mandatory in pg, should have been generated if
    # needed in handle_missing_alias, but this function doesn't really try hard
    # to reach all possible subqueries, so check here also
    $alias = ' AS ' . sql2pg::common::generate_alias() if ($alias eq '');

    $out .= $alias;

    return $out;
}

sub format_target_quallist {
    my ($node) = @_;

    return format_quallist($node->{quallist}) . format_alias($node->{alias});
}

sub format_target_list {
    my ($tlist) = @_;
    my $out = '';

    $out = $tlist->{distinct} . ' ' if (defined($tlist->{distinct}));
    $out .= format_array($tlist->{tlist}, ', ');

    return $out;
}

sub format_tbl_attribute {
    error('toto');
}

sub format_tbl_coldef {
    my ($node) = @_;
    my $out = format_node($node->{ident});

    $out .= ' ' . format_node($node->{datatype});
    $out .= ' PRIMARY KEY' if ($node->{pk});
    $out .= ' ' . format_node($node->{default}) if ($node->{default});
    $out .= ' ' . format_node($node->{check}) if ($node->{check});
    $out .= ' ' . $node->{notnull} if ($node->{notnull});
    $out .= ' ' . format_node($node->{fk}) if ($node->{fk});

    return $out;
}

sub format_tbl_condef {
    my ($node) = @_;
    my $out = 'CONSTRAINT ';

    $out .= format_node($node->{ident});
    $out .= ' ' . $node->{contype};

    $out .= ' (' . format_array($node->{conlist}, ',') . ')';

    return $out;
}

sub format_trim_arg {
    my ($node) = @_;
    my $out;

    $out = $node->{trim_kind} . ' ' if ($node->{trim_kind});
    $out .= format_node($node->{literal});
    $out .= ' FROM ' . format_node($node->{ident});

    return $out;
}

sub format_truncate_table {
    my ($node) = @_;

    return 'TRUNCATE TABLE ' . format_node($node->{ident});
}

sub format_unique_clause {
    my ($node) = @_;
    my $out = 'UNIQUE (';

    $out .= format_array($node->{idents}, ', ');
    $out .= ')';

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

sub format_sample {
    my ($node) = @_;
    my $out = ' TABLESAMPLE SYSTEM (';

    $out .= format_node($node->{percent}) . ')';
    $out .= ' REPEATABLE (' . format_node($node->{seed}) . ')'
        if (defined($node->{seed}));

    return $out;
}

sub format_stmts {
    my ($stmts) = @_;
    my $nbfix = 0;
    my $stmtno;
    my $comments;
    my $out = '';
    my $first = 1;
    my $semi_colon_needed = 0;

    $stmtno = inc_stmtno();

    $comments = get_comment('ok');

    $out = format_comments($comments) if (defined($comments));

    foreach my $stmt (@{$stmts}) {
        my $tmp;

        # XXX special handling of combined statements with orderby here, should
        # find a better way to deal with it
        if (isA($stmt, 'ORDERBY')) {
            $out .= ' ';
        } elsif (
            isA($stmt, 'alterobject')
            or isA($stmt, 'createobject')
            or isA($stmt, 'createtrigger')
            or isA($stmt, 'pl_func')
        ) {
            # Single statement rewritten in multiple statements
            # for instance AT ADD (...) in plpgsql, GENERATED AS transformed in
            # trigger...
            # Semicolon must be prepended since we only know one is needed when
            # a statement follows another one.
            $out .= " ;\n" if (not $first);
        } else {
        }
        $tmp = format_node($stmt);

        if ($tmp ne '') {
            $out .= $tmp;
            $first = 0;
        }

    }
    $out .= " ;\n";

    # Add a fixme for each unused bindvar
    # FIXME make it specific to oracle sql
    sql2pg::common::warn_useless_bindvars();

    $comments = get_comment('fixme');

    $nbfix += (scalar(@fixme)) if (scalar(@fixme) > 0);
    $nbfix += (scalar(@{$comments})) if (defined($comments));

    $out .= '-- ' . $nbfix ." FIXME for this statement\n" if ($nbfix > 0);
    foreach my $f (@fixme) {
        $out .= "-- FIXME: $f\n";
    }
    undef(@fixme);
    if (defined($comments)) {
        $out .= '-- ' . (scalar @{$comments})
            . " comments for this statement must be replaced:\n";
        $out .= format_comments($comments);
    }

    # reset all neeed global vars, must be at the end the this function to
    # avoid weird corner case resetting vars in the middle of a statement
    sql2pg::common::new_statement();

    return $out;
}

sub format_subquery {
    my ($stmt) = @_;

    return format_node($stmt->{stmts});
}

sub format_subjoin {
    my ($node) = @_;
    my $out = format_node($node->{ident});

    if (defined($node->{joins})) {
        $out .= ' ' . format_node($node->{joins});
    }

    return $out;
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

sub format_values {
    my ($values) = @_;

    return 'VALUES (' . format_node($values->{tlist}) . ')';
}

sub format_when {
    my ($node) = @_;

    return 'WHEN ' . format_node($node->{el1})
        . ' THEN ' . format_node($node->{el2});
}

sub format_WHERE {
    my ($where) = @_;
    my $out = format_quallist($where->{content}->{quallist});

    return '' if($out eq '');
    return "WHERE " . $out;
}

sub format_with {
    my ($with) = @_;
    my $out;

    $out = $with->{alias};
    $out .= ' (' . format_array($with->{fields}, ', ') . ')'
        if (defined($with->{fields}));

    $out .= ' AS (' . format_node($with->{select})
           . ')';

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

sub tab {
    return $tab x $depth;
}

1;
