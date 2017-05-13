package sql2pg::plsql::utils;
#------------------------------------------------------------------------------
# Project  : Multidatabase to PostgreSQL SQL converter
# Name     : sql2pg
# Author   : Julien Rouhaud, julien.rouhaud (AT) free (DOT) fr
# Copyright: Copyright (c) 2016-2017 : Julien Rouhaud - All rights reserved
#------------------------------------------------------------------------------

require Exporter;

BEGIN {
    @ISA = qw(Exporter);
    @EXPORT = qw(quote_ident);
}


use Data::Dumper;
use sql2pg::format;
use sql2pg::common;

my %bindvars = ();


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

    return $stmt if (not defined($forupdate));
    return $stmt if (not defined($forupdate->{content}->{tlist}));

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

    return $stmt;
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

# This function will remove any "joinop" qual (ident op ident(+)), and will add
# a LEFT JOIN clause corresponding to this qual.  The left-join-ed table will
# also be removed from the where clause.
sub handle_nonsqljoin {
    my ($stmt) = @_;
    my $joins = [];
    my $quals;
    my $last_fail = undef;

    $quals = whereclause_walker('joinop', $stmt, undef);

    return $stmt unless (defined($quals));

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
            my $joinon = sql2pg::plsql::grammar::make_joinon(undef, undef,
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

    return $stmt;
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
            sql2pg::plsql::grammar::make_respect_ignore_nulls_clause(undef, $cur,
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

    return $stmt unless (defined($quals));

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

    return $stmt;
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

# Hook to handle all global rewriting rules for select statements
sub select_hook {
    my ($stmt) = @_;

    # transform (+) qual to LEFT JOIN
    handle_nonsqljoin($stmt);

    # transform ROWNUM where clauses to LIMIT/OFFSET
    handle_rownum($stmt);

    # transform CONNECT BY clauses to WITH RECURSIVE clauses
    $stmt = handle_connectby($stmt);

    # Handle specific oracle column referenced FOR UPDATE clauses
    handle_forupdate_clause($stmt);

    return $stmt;
}

1;
