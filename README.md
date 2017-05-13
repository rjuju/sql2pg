sql2pg
======

/!\ WARNING /!\

This project is still a work in progress,  SQL grammar are big and complex.  If
you encounter some statements that are not handled, please report an issue with
a self contained case and a link to the grammar reference.

Overview
--------

sql2pg is a parser that converts various flavor of SQL to PostgreSQL compatible
SQL.

It'll convert automatically most of specific keywords (CONNECT BY, (+)...),
type, functions and so on.

Things that can't be automatically converted will be exported as SQL comments
after its problematic query.

Dependencies
------------

sql2pg requires Marpa::R2, at least version 2.076000.

Usage
-----

For Oracle SQL:

```bash
plsql2pg.pl queries.sql > rewritten_queries.sql
```

For SQL Server SQL:

```bash
tsql2pg.pl queries.sql > rewritten_queries.sql
```
