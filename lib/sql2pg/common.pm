package sql2pg::common;
#------------------------------------------------------------------------------
# Project  : Multidatabase to PostgreSQL SQL converter
# Name     : sql2pg
# Author   : Julien Rouhaud, julien.rouhaud (AT) free (DOT) fr
# Copyright: Copyright (c) 2016-2017 : Julien Rouhaud - All rights reserved
#------------------------------------------------------------------------------

require Exporter;

BEGIN {
    @ISA = qw(Exporter);
    @EXPORT = qw(make_node make_join make_node_opexpr _parens_node isA error
                 make_clause inverse_operator node_to_array assert assert_isA
                 assert_one_el add_fixme useless_bindvar translate_bindvar
                 get_alias whereclause_walker splice_table_from_fromlist
                 find_table_in_array combine_and_parens_select
                 expression_tree_walker);
}

use strict;
use warnings;
use 5.010;

use Data::Dumper;
use sql2pg::format;

my $alias_gen = 0;
my %bindvars = ();

my %walker_actions = (
    joinop  => \&sql2pg::plsql::utils::qual_is_join_op,
    rownum  => \&sql2pg::plsql::utils::qual_is_rownum
);

sub add_fixme {
    my ($msg) = @_;

    push(@sql2pg::format::fixme, $msg);
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

    return $alias if defined($alias);
    return $as if defined ($as);

    error("Expected alias not found");
}

# Generate unique alias for subquery which doesn't have an alias
sub handle_missing_alias {
    my ($stmt) = @_;

    return unless(defined $stmt->{FROM});

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

# Iterate through a given tree.  Strongly based on postgres' same function,
# though this one is more a quick hack.
sub expression_tree_walker {
    my ($node, $func, $extra) = @_;

    return 0 if (not $node);
    if (ref($node) eq 'ARRAY') {
        foreach my $e (@{$node}) {
            return 1 if (expression_tree_walker($e, $func, $extra));
        }
    } elsif (ref($node) eq 'HASH') {
        if (isA($node, 'opexpr')) {
            return 1 if (expression_tree_walker($node->{left}, $func, $extra));
            return 1 if (expression_tree_walker($node->{op}, $func, $extra));
            return 1 if (expression_tree_walker($node->{right}, $func, $extra));
        } elsif (
            isA($node, 'ident')
            or isA($node, 'number')
        ) {
            no strict;
            return 1 if (&$func($node, $extra));
            use strict;
        } elsif (isA($node, 'function')) {
            return 1 if (expression_tree_walker($node->{args}, $func, $extra));
            return 1 if (expression_tree_walker($node->{ident}, $func, $extra));
            return 1 if (expression_tree_walker($node->{alias}, $func, $extra));
            return 1 if (expression_tree_walker($node->{window}, $func, $extra));
        } elsif (isA($node, 'function_arg')) {
            return 1 if (expression_tree_walker($node->{arg}, $func, $extra));
        } elsif (isA($node, 'parens')) {
            return 1 if (expression_tree_walker($node->{node}, $func, $extra));
            return 1 if (expression_tree_walker($node->{alias}, $func, $extra));
        } else {
            error("Node \"$node->{type}\" not handled.\n"
                . "Please report an issue.");
        }
    } else {
        no strict;
        return &$func($node, $extra);
        use strict;
    }
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

# Take a not qualified ident and qualify it with NEW.  Should be used in
# trigger body statements, like plpgsql GENERATED AS rewriting
sub pl_add_prefix_new {
    my ($node, $cols) = @_;

    if (isA($node, 'ident')) {
        $node->{table} = 'NEW'
            if (not $node->{table} and (exists $cols->{$node->{attribute}}));
    }

    return 0;
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

# FIXME make it specific to oracle sql
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

# FIXME make it specific to oracle sql
sub useless_bindvar {
    my ($v) = @_;

    # sanity check
    return unless(isA($v, 'bindvar'));

    # if the bindvar has already been used, it's not a useless bindvar
    return if(defined($bindvars{used}{$v->{var}}));

    # ok, we can add it as a for now useless bindvar
    $bindvars{useless}{$v->{var}} = 1;
}

# FIXME make it specific to oracle sql
sub warn_useless_bindvars {

    foreach my $k (keys %{$bindvars{useless}}) {
        add_fixme("Bindvar $k is now useless");
    }
}

1;
