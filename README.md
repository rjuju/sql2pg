plsql2pg
========

Overview
--------

plsql2pg is a parser that converts Oracle SQL to PostgreSQL compatible SQL.

It'll convert automatically most of specific keywords (CONNECT BY, (+)...),
type, functions and so on.

Things that can't be automatically converted will be exported as SQL comments
after its problematic query.

Dependencies
------------

plsql2pg requires Marpa::R2, at least version 2.076000.

Usage
-----

psql2pg queries.sql > rewritten_queries.sql
