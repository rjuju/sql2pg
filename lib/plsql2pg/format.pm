package plsql2pg::format;

require Exporter;

BEGIN {
    @ISA = qw(Exporter);
    @EXPORT = qw(format_node format_comments format_array format_stmts);
}

use Data::Dumper;
use plsql2pg;
use plsql2pg::utils;

our @fixme;

sub format_alias {
    my ($alias) = @_;

    return ' AS ' . $alias if defined($alias);
    return '';
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

sub format_combine_op {
    my ($op) = @_;

    return ' EXCEPT ' if (uc($op->{op}) eq 'MINUS');
    return ' ' . uc($op->{op}) . ' ';
}

sub format_comments {
    my ($comments) = @_;
    my $out = '';

    foreach my $event (@{$comments}) {
        my ($name, $start, $end, undef) = @{$event};
        $out .= substr($plsql2pg::input, $start, $end - $start) . "\n";
    }

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
        return $frame->{rangerows} . " BETWEEN "
            . $frame->{frame_start} . " AND " . $frame->{frame_end};
    } else {
        return $frame->{rangerows} . " " . $frame->{frame_start};
    }
}

sub format_FROM {
    my ($from) = @_;
    my $out = format_standard_clause($from, ', ');

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

    # first transform function to pg compatible if needed
    plsql2pg::utils::handle_function($func);

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
    my $out = '';

    plsql2pg::utils::handle_respect_ignore_nulls($node);

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

sub format_join {
    my ($join) = @_;
    my $out;

    $out = $join->{jointype} . " JOIN ";

    $out .= format_node($join->{ident});
    $out .= ' ' . format_node($join->{cond}) if defined($join->{cond});

    return $out;
}

sub format_JOIN {
    my ($join) = @_;

    return format_standard_clause($join, ' ');
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

    plsql2pg::utils::prune_parens($node);

    $func = "format_" . $node->{type};

    # XXX should I handle every node type explicitely instead?
    no strict;
    return &$func($node);
}

sub format_number {
    my ($number) = @_;

    return $number->{val} . format_alias($number->{alias});
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
    my @clauselist = ('WITH', 'SELECT', 'FROM', 'JOIN', 'WHERE', 'GROUPBY',
        'HAVING', 'ORDERBY', 'FORUPDATE', 'LIMIT', 'OFFSET');
    my $nodes;
    my $out = '';

    # transform (+) qual to LEFT JOIN
    plsql2pg::utils::handle_nonsqljoin($stmt);

    # transform ROWNUM where clauses to LIMIT/OFFSET
    plsql2pg::utils::handle_rownum($stmt);

    $stmt = plsql2pg::utils::handle_connectby($stmt);

    plsql2pg::utils::handle_missing_alias($stmt);

    plsql2pg::utils::handle_forupdate_clause($stmt);

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
    $alias = ' AS ' . plsql2pg::utils::generate_alias() if ($alias eq '');

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

    $out .= $node->{percent} . ')';
    $out .= ' REPEATABLE (' . $node->{seed} . ')' if (defined($node->{seed}));

    return $out;
}

sub format_stmts {
    my ($stmts) = @_;
    my $nbfix = 0;
    my $comments;
    my $out = '';

    inc_stmtno();

    $comments = get_comment('ok');

    $out = format_comments($comments) if (defined($comments));

    foreach my $stmt (@{$stmts}) {
        # XXX special handling of combined statements with orderby here, should
        # find a better way to deal with it
        $out .= ' ' if (isA($stmt, 'ORDERBY'));
        $out .= format_node($stmt);
    }
    $out .= " ;\n";

    # Add a fixme for each unused bindvar
    plsql2pg::utils::warn_useless_bindvars();

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
    plsql2pg::utils::new_statement();

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

1;
