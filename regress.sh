#!/bin/bash

echo "Running regression tests..."
perl ./plsql2pg.pl test/sql/sample_queries.sql > test/out/rewritten.sql 2> test/out/stderr
if [[ $? -ne 0 ]]; then
    echo "Error during plsql2pg;"
    cat test/out/stderr
    exit 1
fi
echo ""
echo "Results:"
echo ""

diff test/expected/sample_rewritten.sql test/out/rewritten.sql > test/out/rewritten.diff
if [[ $(cat test/out/rewritten.diff |wc -l) == "0" ]]; then
    echo "No diff in converted SQL"
else
    echo "Differences in converted SQL. See test/out/rewritten.diff:"
    echo ""
    cat test/out/rewritten.diff
    echo ""
fi
