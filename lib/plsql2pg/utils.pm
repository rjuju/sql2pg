package plsql2pg::utils;

require Exporter;

BEGIN {
    @ISA = qw(Exporter);
    @EXPORT = qw(make_node make_join make_node_opexpr _parens_node isA
                 make_clause inverse_operator node_to_array assert assert_isA
                 assert_one_el add_fixme useless_bindvar translate_bindvar);
}

use Data::Dumper;
use plsql2pg::format;

my %bindvars = ();
my $alias_gen = 0;

my %walker_actions = (
    joinop  => \&plsql2pg::utils::qual_is_join_op,
    rownum  => \&plsql2pg::utils::qual_is_rownum
);

sub add_fixme {
    my ($msg) = @_;

    push(@plsql2pg::format::fixme, $msg);
}

sub assert {
    my ($ok, $msg, @args) = @_;

    error("Assert error:" . "\n" . $msg, @args) if (not $ok);
}

sub assert_isA {
    my ($node, $type, @args) = @_;

    if (not isA($node, $type)) {
        error("Unexpected node type " . $node->{type} . ", expected $type",
            $node, @args);
    }
}

sub assert_one_el {
    my ($arr) = @_;

    error("Element is not an array", $arr)
        if (ref($arr) ne 'ARRAY');
    error("Array contains more than one element", $arr)
        if (scalar @{$arr} != 1);
}

sub combine_and_parens_select {
    my ($nodes, $raw_op, $stmt) = @_;
    my $op = make_node('combine_op');

    assert_one_el($stmt);

    $stmt = pop(@{$stmt});
    $stmt->{combined} = 1;
    $stmt = _parens_node($stmt);
    prune_parens($stmt);

    $op->{op} = $raw_op;

    push(@{$nodes}, $op);
    push(@{$nodes}, @{$stmt});

    return $nodes;
}

sub error {
    my ($msg, @args) = @_;
    my $i=1;

    print "ERROR: $msg\n";

    while ( (my @c = (caller($i++))) ) {
        print "in $c[3]\n";
    }

    foreach my $node (@args) {
        print Dumper($node);
    }
    exit 1;
}

# Search if the given table name (real name or alias) exists in an array on
# from_elem.  If found, return the matching node and entry position in the
# array, otherwise undef and -1.
sub find_table_in_array {
    my ($name, $array) = @_;
    my $i = 0;

    return (undef, -1) unless (defined($array));

    # first, check if a table has the wanted name as alias to avoid returning
    # the wrong one
    foreach my $t (@{$array}) {
        if (isA($t, 'ident')) {
            if (defined($t->{alias}) and $t->{alias} eq $name) {
                return($t, $i);
            }
        } elsif (isA($t, 'join')) {
            if (defined($t->{ident}->{alias}) and $t->{ident}->{alias} eq $name) {
                return($t, $i);
            }
        } elsif (isA($t, 'SUBQUERY')) {
            if (defined($t->{alias}) and $t->{alias} eq $name) {
                return($t, $i);
            }
        } else {
            error('Node type ' . $t->{type} . ' unexpected', $t);
        }
        $i++;
    }

    $i=0;
    # no, then look for real table name
    foreach my $t (@{$array}) {
        if (isA($t, 'ident')) {
            if ($t->{attribute} eq $name) {
                return($t, $i);
            }
        } elsif (isA($t, 'join')) {
            # ignore if the join ident isn't an ident (probably a subquery),
            # because only the alias can be referred
            next unless(isA($t->{ident}, 'ident'));
            if ($t->{ident}->{attribute} eq $name) {
                return($t, $i);
            }
        } elsif (isA($t, 'SUBQUERY')) {
            # only alias can be used as reference for a SUBQUERY
        } else {
            error("Node unexpected", $t);
        }
        $i++;
    }

    # not found, say it to caller
    return (undef, -1);
}

sub generate_alias {
    $alias_gen++;

    return "subquery" . $alias_gen;
}

sub get_alias {
    my ($as, $alias) = @_;

    return quote_ident($alias) if defined($alias);
    return quote_ident($as) if defined ($as);
    return undef;
}

# This function will transform an oracle hierarchical query (CONNECT BY) to a
# standard recursive query (WITH RECURSIVE).  A new statement is returned that
# must replace the original one.
sub handle_connectby {
    my ($ori) = @_;
    my $lhs = {};
    my $rhs = {};
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
    if (defined($clause->{content}->{STARTWITH})) {
        $quals = make_node('quallist');
        $quals->{quallist} = $clause->{content}->{STARTWITH}->{content};
        $lhs->{WHERE} = make_clause('WHERE', $quals);
    }

    # transfer the CONNECT BY clause to the RHS
    $quals = make_node('quallist');
    $quals->{quallist} = $clause->{content}->{CONNECTBY}->{content};

    # if a qual's ident is tagged as PRIOR, qualify it with the recursion alias
    foreach my $q (@{$quals->{quallist}}) {
        next if (ref($q) ne 'HASH');
        $q->{$q->{prior}}->{table} = 'recur' if (defined($q->{prior}));
    }

    # Add the WHERE clause to the RHS
    $rhs->{WHERE} = make_clause('WHERE', $quals);

    # And finally make a combined statement with the LHS, UNION ALL and the RHS
    $with->{select} = combine_and_parens_select(node_to_array($lhs),
                                                'UNION ALL',
                                                node_to_array($rhs));

    # create a dummy select statement to attach the WITH to
    $select->{attribute} = '*';
    $stmt->{SELECT} = make_clause('SELECT', node_to_array($select));
    $from->{attribute} = 'recur';
    $stmt->{FROM} = make_clause('FROM', node_to_array($from));
    $stmt->{WHERE} = $ori->{WHERE} if (defined($ori->{WHERE}));
    # append the recursive WITH to original one if present, otherwise create a
    # new WITH clause
    if (defined($stmt->{WITH})) {
    push(@{$stmt->{WITH}->{content}}, $with);
    } else {
        $stmt->{WITH} = make_clause('WITH', node_to_array($with));
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
            if ( ($tbl_count == 1) and (defined($tbl_name)) ) {
                $ident->{attribute} = $tbl_name;
            } else {
                add_fixme("FOR UPDATE OF $ident->{attribute} must be changed to its table name/alias");
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
    } elsif ($funcname eq 'trunc') {
        my $nb = (scalar @{$func->{args}});

        if ( ($nb != 1) and ($nb != 2)) {
            add_fixme('Function trunc with too many arguments');
            return;
        }

        if ($nb == 1) {
            my $arg = make_node('literal');

            $arg->{values} = 'day';
            unshift(@{$func->{args}}, $arg);
        } else {
            #swap arguments, replace MM with month
            my $arg = pop(@{$func->{args}});
            my $v;

            assert_isA($arg, 'literal', $func);

            $v = $arg->{value};

            if ($v eq 'Y' or $v eq 'YY' or $v eq 'YYYY' or $v eq 'YEAR') {
                $arg->{value} = 'year';
            } elsif ($v eq 'MM' or $v eq 'MONTH' or $v eq 'MON') {
                $arg->{value} = 'month';
            } elsif ($v eq 'HH' or $v eq 'HH12' or $v eq 'HH24') {
                $arg->{value} = 'hour';
            } elsif ($v eq 'MI') {
                $arg->{value} = 'minute';
            } else {
                error("Cannot understand format $v in trunc function", $func);
            }

            unshift(@{$func->{args}}, $arg);
        }

        $func->{ident}->{attribute} = 'date_trunc';
    }

    return $func;
}

# Generate unique alias for subquery which doesn't have an alias
sub handle_missing_alias {
    my ($stmt) = @_;

    foreach my $w (@{$stmt->{FROM}->{content}}) {
        if ( isA($w, 'SUBQUERY') and (not defined($w->{alias})) ) {
            $w->{alias} = generate_alias();
        }
    }

    if (defined($stmt->{JOIN})) {
        foreach my $w (@{$stmt->{JOIN}->{content}}) {
            if ( isA($w, 'SUBQUERY') and (not defined($w->{alias})) ) {
                $w->{alias} = generate_alias();
            }
        }
    }
}

# This function will remove any "joinop" qual (ident op ident(+)), and will add
# a LEFT JOIN clause corresponding to this qual.  The left-join-ed table will
# also be removed from the where clause.
sub handle_nonsqljoin {
    my ($stmt) = @_;
    my $joins = [];
    my $quals;
    my $last_fail = undef;

    $quals = whereclause_walker('joinop', $stmt, undef);

    return unless (defined($quals));

    foreach my $removed (@{$quals}) {
        my $qual = $removed->{qual};
        my $join;
        my $t;
        my $ident;
        my $cond;

        $t = splice_table_from_fromlist($qual->{right}->{table},
                                        $stmt->{FROM}->{content});

        ($t, undef) = find_table_in_array($qual->{right}->{table}, $joins)
            unless(defined($t));

        assert((defined($t)), "could not find relation "
            . $qual->{right}->{table}, $qual, $joins);

        $ident = node_to_array($t);

        # If this qual is part of mulitple join condition, append the qual
        if (isA($t, 'join')) {
            # if the qual_op is undefined, it's probably because previous qual
            # was first qual and it removed the op.  All the qual_op handling
            # is probably buggy, so if we don't find previous one, just add an
            # AND and go on
            my $op = $removed->{op};
            if (not defined($op)) {
                my $nb = scalar(@{$t->{cond}->{quallist}});

                $op = @{$t->{cond}->{quallist}}[$nb-2]->{saved_op};
                if (not defined($op)) {
                    $op = 'AND';
                    add_fixme('Cannot find operator for JOIN condition: '
                              . format_node($qual)
                              . '. An AND has been forced');
                }
            }
            push(@{$t->{cond}->{quallist}}, $op);
            push(@{$t->{cond}->{quallist}}, $qual);
        }
        # otherwise add a join JOIN clause
        else {
            my $joinon = plsql2pg::grammar::make_joinon(undef, undef,
                                               node_to_array($qual));

            # saved this removed qual qualop in case further quals need it (see
            # above bloc when appending a qual to joinon list)
            @{$joinon->{quallist}}[0]->{saved_op} = $removed->{op};

            $join = make_join('LEFT', $ident, $t->{alias}, $joinon);

            push(@{$joins}, @{$join});
        }
    }

    # We now have all the join node, make sure we add them in the right order
    while (scalar(@{$joins}) > 0) {
        # Take the last element
        my $join = pop(@{$joins});
        my $left;
        my $right;
        my $pos;

        # XXX The quallist can contain multiple entries, assume there are not
        # different dependancies in all of them

        # left node is needed to check dependency
        $left = @{$join->{cond}->{quallist}}[0]->{left};
        assert((isA($left, 'ident')), "left node is not an ident", $left);

        # right node is needed to prevent infinite loop
        $right = @{$join->{cond}->{quallist}}[0]->{right};
        assert((isA($right, 'ident')), "right node is not an ident", $right);

        (undef, $pos) = find_table_in_array($left->{table},
                                            $stmt->{FROM}->{content});
        if ($pos >= 0) {
            # We found the dependency, add the join after it and reset last_fail
            splice @{$stmt->{FROM}->{content}}, $pos+1, 0, $join;
            $last_fail = undef;
        } else {
            # dependency not found, check if we looped all pending join without
            # sucess and error out in this case
            if (defined($last_fail)) {
                error("Coudln't resolve table dependencies for SQL89 JOIN, "
                    . "orignal query is probably broken")
                    if ($last_fail eq $right->{table});
            } else {
                # Initialise the last_fail with the first missed dependency
                $last_fail = $right->{table};
            }
            # Dependency not found, put in first position to handle it after a
            # full loop
            unshift(@{$joins}, $join);
        }
    }
}

# If space-separated arguments is respect_ignore_nulls clause, remove it and
# add a FIXME.  It's way easier to handle it here than in the grammar.
sub handle_respect_ignore_nulls {
    my ($node) = @_;
    my $i = 0;

    assert_isA($node, 'function_arg');

    while ( $i < scalar(@{$node->{arg}}) ) {
        my $cur;
        my $next;

        return if ($i >= (scalar(@{$node->{arg}})-1));

        $cur = @{$node->{arg}}[$i];
        $next = @{$node->{arg}}[$i+1];

        $i++;

        next unless (isA($cur, 'ident') and isA($next, 'ident'));

        $cur = format_node($cur);
        $next = format_node($next);

        if (($cur eq 'respect' or $cur eq 'ignore') and ($next eq 'nulls')) {
            plsql2pg::grammar::make_respect_ignore_nulls_clause(undef, $cur,
                $next);
            splice(@{$node->{arg}}, $i-1, 2);
        }
    }
}

# This function will transform any rownum OPERATOR number to a LIMIT/OFFSET
# clause.  No effort is done to make sure OR-ed or overlapping rownum
# expressions will have expected result, such query would be stupid anyway.
# This grammar also allows a float number, but as everywhere else I assume
# original query is valid to keep the grammar simple.
sub handle_rownum {
    my ($stmt) = @_;
    my $quals;

    $quals = whereclause_walker('rownum', $stmt, undef);

    return unless (defined($quals));

    foreach my $removed (@{$quals}) {
        my $qual = $removed->{qual};
        my $number;
        my $clause;

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

        # XXX Should I handle queries with conflicting rownum clauses?
        $stmt->{$clause->{type}} = $clause;
    }

    # Compute real LIMIT if an OFFSET is also present
    if (defined($stmt->{LIMIT}) and defined($stmt->{OFFSET})) {
        $stmt->{LIMIT}->{content}->{val} -= $stmt->{OFFSET}->{content}->{val};
    }
}

sub inverse_operator {
    my ($op) = @_;

    return '<=' if ($op eq '>');
    return '<' if ($op eq '>=');
    return '>=' if ($op eq '<');
    return '>' if ($op eq '<=');
    return '=' if ($op eq '=');
    return '!=' if ($op eq '!=');
    return '<>' if ($op eq '<>');

    error("Unexpected operator: $op");
}

sub isA {
    my ($node, $type) = @_;

    return 0 if (not defined($node));
    return 0 if (not ref($node));
    return 0 if (ref $node ne 'HASH');

    return ($node->{type} eq $type);
}

sub make_clause {
    my ($type, $content) = @_;
    my $clause = make_node($type);

    $clause->{content} = $content;

    return $clause;
}

sub make_join {
    my ($jointype, $ident, $alias, $cond) = @_;
    my $join = make_node('join');

    $join->{jointype} = $jointype;
    $join->{ident} = pop(@{$ident});
    $join->{ident}->{alias} = $alias;
    $join->{cond} = $cond;

    return node_to_array($join);
}

sub make_node {
    my ($type) = @_;
    my $node = {};

    $node->{type} = $type;

    return $node;
}

sub make_node_opexpr {
    my ($left, $op, $right) = @_;
    my $opexpr = make_node('opexpr');

    $opexpr->{left} = $left;
    $opexpr->{op} = $op;
    $opexpr->{right} = $right;

    return node_to_array($opexpr);
}

sub new_statement {
    $alias_gen = 0;
    %bindvars = ();
}

sub node_to_array {
    my ($node) = @_;
    my $nodes = [];

    push(@{$nodes}, $node);
    return $nodes;
}

# Iterate through a given where_clause or select node, undef any qual matching
# the given function and return all these quals in an array, possibly undef but
# not empty.  Return a removed_qual node, containing the qual and the related
# qual_op (AND/OR).
sub whereclause_walker {
    my ($func, $node, $stmt) = @_;
    my $quals = [];
    my $i;

    return undef if (not defined($node));

    if (isA($node, 'select')) {
        return whereclause_walker($func, $node->{WHERE}->{content}, $node)
            if (defined($node->{WHERE}));
        return undef;
        }

    return whereclause_walker($func, $node->{node}, $stmt)
        if isA($node, 'parens');
    return whereclause_walker($func, $node->{quallist}, $stmt)
        if (isA($node, 'quallist') or isA($node, 'appended_quals'));

    for ($i=0; $i<(scalar @{$node}); $i++) {
        my $qual = @{$node}[$i];
        my $todel = $i-1;;
        next if (ref($qual) ne 'HASH');

        # We'll need to remove any preceding AND/OR op (or following if first
        # el), too bad if it was an OR
        $todel = $i+1 if ($i == 0);

        # Recurse the walker if it's a parens node
        if (isA($qual, 'parens')) {
            my $res = whereclause_walker($func, $qual, $stmt);

            # We found matching quals in the parens node
            if (defined($res)) {
                # save qualop on the first qual in returned array
                @{$res}[0]->{op} =  @{$node}[$todel];

                # Remove the parens node if previous walker call removed all of
                # its content
                if ((parens_is_empty($qual)) and (ref(@{$node}[$todel])) ne 'HASH') {
                    @{$node}[$todel] = undef;
                }

                push(@{$quals}, @{$res});
            }
            next;
        }
        # Otherwise check if the node match the given condition
        elsif ($walker_actions{$func}->($qual)) {
            my $ret = make_node('removed_qual');

            # Remove extraneous QUAL_OP, this is probably really buggy
            if (ref(@{$node}[$todel]) ne 'HASH') {
                $ret->{op} = @{$node}[$todel];
                @{$node}[$todel] = undef;
            }

            $ret->{qual} = $qual;

            # Save the matching qual
            push(@{$quals}, $ret);
            # And remove it from the quallist
            @{$node}[$i] = undef;
        }
    }

    return undef unless(scalar @{$quals} > 0);
    return $quals;
}

# Remove a redundant parens level:
#
# - if the parens content is an array only containing a single parens (any other
#   undef value in the array of ignored), return this inner parens.
# - if the parens content is a parens node, return the inner parens
# - otherwise return undef
#
# - otherwise return undef.
sub parens_get_new_node {
    my ($parens) = @_;
    my $node = undef;
    my $cpt = 0;

    return $parens->{node}->{node} if (isA($parens->{node}, 'parens'));
    return undef unless(ref($parens->{node}) eq 'ARRAY');

    foreach my $el (@{$parens->{node}}) {
        next unless(defined($el));
        return undef unless(isA($el, 'parens'));
        $node = $el->{node};
        $cpt++;
        last if($cpt > 1);
    }

    return undef if ($cpt != 1);
    # The array was only containing a parens, return this parens
    return $node;
}

# Return if the given parens node is empty, meaning only contains empty array,
# or array of undefined or empty parens
sub parens_is_empty {
    my ($parens) = @_;

    # Can't be empty if the parens content isn't an array
    return 0 unless (ref($parens->{node}) eq 'ARRAY');

    foreach my $node (@{$parens->{node}}) {
        if (defined($node)) {
            # Check if this node is an empty parens
            if ((ref($node) eq 'HASH') and (isA($node, 'parens'))) {
                my $rc = parens_is_empty($node);
                # Parens wasn't empty, stop now and say parens isn't empty
                return $rc if (not $rc);
            }
        }
        # The content wasn't empty, stop now and say parens isn't empty
        return 0 if (defined($node));
    }

    # We didn't find any content, so the parens is empty
    return 1;
}

sub _parens_node {
    my ($node) = @_;
    my $parens = make_node('parens');

    $parens->{node} = $node;
    prune_parens($parens);

    return node_to_array($parens);
}

# Walk through possibly nested parens node and remove redundant parens node
sub prune_parens {
    my ($parens) = @_;
    my $node;

    # Sanity check
    return if (not isA($parens, 'parens'));

    # Remove the parens content if the parens is empty
    if (parens_is_empty($parens)) {
        $parens->{node} = undef;
        return;
    }

    # Check for redundant parens level
    $node = parens_get_new_node($parens);
    if (defined($node)) {
        # There was, remove this extraneous level and start pruning again
        $parens->{node} = $node;
        prune_parens($parens);
    }
}

sub qual_is_join_op {
    my ($qual) = @_;

    return (isA($qual,'qual') and defined($qual->{join_op}));
}

sub qual_is_rownum {
    my ($qual) = @_;

    return ((
            (isA($qual->{left}, 'number') and isA($qual->{right}, 'ident')
            and ($qual->{right}->{attribute} eq 'rownum')
            and not defined($qual->{right}->{table}))
        ) or (
            (isA($qual->{right}, 'number') and isA($qual->{left}, 'ident')
            and ($qual->{left}->{attribute} eq 'rownum')
            and not defined($qual->{left}->{table}))
        ));
}

# Handle ident conversion from oracle to pg
sub quote_ident {
    my ($ident) = @_;

    return undef if not defined($ident);

    # original ident was quoted
    if (substr($ident, 0, 1) eq '"') {
        if ((substr($ident, 1, -1) =~ /^[a-zA-Z_][a-zA-Z0-9_]*$/) and (lc($ident) eq $ident)) {
            return lc(substr($ident, 1, -1));
        } else {
            return $ident;
        }
    }
    # original ident wasn't quoted, check if we have to quote it
    else {
        if ($ident =~/[\$#]/) {
            return '"' . lc($ident) . '"';
        } else {
            return lc($ident);
        }
    }
}

sub splice_table_from_fromlist {
    my ($name, $froms) = @_;
    my $i;

    return undef unless (defined($froms));

    # first, check if a table has the wanted name as alias to avoid returning
    # the wrong one
    for ($i=0; $i<(scalar @{$froms}); $i++) {
        my $t = @{$froms}[$i];
        if (defined($t->{alias}) and $t->{alias} eq $name) {
            return(splice(@{$froms}, $i, 1));
        }
    }

    # no, then look for real table name
    for ($i=0; $i<(scalar @{$froms}); $i++) {
        my $t = @{$froms}[$i];
        # for subquery, only the alias can be used as reference
        next if (isA($t, 'SUBQUERY'));
        if ($t->{attribute} eq $name) {
            return(splice(@{$froms}, $i, 1));
        }
    }

    # not found, say it to caller
    return undef;
}

sub translate_bindvar {
    my ($v) = @_;

    assert_isA($v, 'bindvar');

    # if it was a useless bindvar, remove this information
    delete($bindvars{useless}{$v->{var}});

    if (not defined($bindvars{used}{$v->{var}})) {
        # pick up next param number
        $bindvars{used}{$v->{var}} = '$' . (scalar(keys %{$bindvars{used}})+1);

        add_fixme('Bindvar ' . $v->{var}
                . ' has been translated to parameter '
                . $bindvars{used}{$v->{var}});
    }

    return $bindvars{used}{$v->{var}};
}

sub useless_bindvar {
    my ($v) = @_;

    # sanity check
    return unless(isA($v, 'bindvar'));

    # if the bindvar has already been used, it's not a useless bindvar
    return if(defined($bindvars{used}{$v->{var}}));

    # ok, we can add it as a for now useless bindvar
    $bindvars{useless}{$v->{var}} = 1;
}

sub warn_useless_bindvars {

    foreach my $k (keys %{$bindvars{useless}}) {
        add_fixme("Bindvar $k is now useless");
    }
}

1;
