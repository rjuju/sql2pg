plsql2pg
========

Some references:

- [Marpa::R2 main doc](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Marpa_R2.pod)
- [SLIF / Scanless interface](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Scanless/DSL.pod)
- [window functions spec](https://docs.oracle.com/cd/E11882_01/server.112/e25554/analysis.htm#DWHSG021)


Original:
---------
```sql
SElect 1 nb from DUAL WHERE rownum < 2; SELECT DISTINCT * from TBL t order by a nulls last, b desc, tbl.c asc;
SELECT (1), nvl(val, 'null') || ' '|| nvl(val2, 'empty') "vAl",1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1 + 2 * ((t.v) - 2) % 4 meh, 'test me', t.* from tbl t WHERE (((((a > 2)) and (rownum < 10)) OR ((((b < 3)))))) GROUP BY a, t.b;
 select * from (
select 1 from dual
) union (select 2 from dual) minus (select 3 from dual) interSECT (select 4 from dual) union all (select 5 from dual);
select * from a,only (c) join b using (id,id2) left join d using (id) WHERE rownum >10 and rownum <= 20;
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2 naturaL join d CROSS JOIN e cj natural left outer join f natural full outer join g;
select round(sum(count(*)), 2), 1 from a,b t1 where a.id = t1.id(+);
SELECT id, log2(id), log10(id), count(*) FROM a GROUP BY id HAVING count(*)< 10;
SELECT val, rank() over (partition by id) rank, lead(val) over (order by val rows CURRENT ROW), lag(val) over (partition by id,val order by val range between 2 preceding and unbounded following) as lag from t;
WITH s1 as (with s3 as (select 1 from dual) select * from s3), s AS (SELECT * FROM s1 where rownum < 2) SELECT * From s, (with t as (select 3 from t) select * from t) cross join (with u as (select count(*) nb from dual) select nb from u union all (select 0 from dual)) where rownum < 2;
with s as (select 1 from dual) SELECT employee_id, last_name, manager_id
FROM employees   -- this is the FROM clause
WHERE salary > 0   --should not happend
start with /* hard coded value */ employee_id = 1 CONNECT BY isvalid = 1 and PRIOR employee_id = manager_id;
SELECT a,b,c FROM foo bar group by grouping sets(a, cube(a,b), rollup(c,a), cube(rollup(a,b,c)));
SELECT * FROM tbl t, t2 natural join t3 FOR UPDATE OF t2.a, col wait 1;
update t set a = 1, (b,c) = (select * from t2 WHERE id = 1), d = (SELECT 1 from dual) where (a < 10);
delete from public.t tbl where nvl(tbl.col, 'todel') = 'todel';
insert into public.t ins values (2+1, 'tt');
insert   into public.t ins (a,b) values (2+1, 'tt');
insert into public.t ins (a,b) select id, count(*) from t group by 1;
-- now unsupported stuff
SELECT 1 FROM t1 VERSIONS BETWEEN TIMESTAMP MINVALUE AND CURRENT_TIMESTAMP t WHERE id < 10;
UPDATE t1 SET val = 't' WHERE id = 1 LOG ERRORS INTO err.log (to_char(SYSDATE), id);
UPDATE t1 SET val = 't' WHERE id = 1 RETURNING (id%2), * INTO a,b REJECT LIMIT 3;
```

Converted:
----------
```sql
SELECT 1 AS nb LIMIT 1 ;
SELECT DISTINCT * FROM tbl AS t ORDER BY a ASC NULLS LAST, b DESC, tbl.c ASC ;
SELECT (1), COALESCE(val, 'null') || ' ' || COALESCE(val2, 'empty') AS "vAl", 1, abc, "DEF" FROM toto AS "TATA;" ;
SELECT 1 + 2 * ((t.v) - 2) % 4 AS meh, 'test me', t.* FROM tbl AS t WHERE (((a > 2)) OR (b < 3)) GROUP BY a, t.b LIMIT 9 ;
SELECT * FROM ( SELECT 1 ) AS subquery1 UNION ( SELECT 2 ) EXCEPT ( SELECT 3 ) INTERSECT ( SELECT 4 ) UNION ALL ( SELECT 5 ) ;
SELECT * FROM a, ONLY (c) INNER JOIN b USING (id, id2) LEFT JOIN d USING (id) LIMIT 20 OFFSET 10 ;
SELECT * FROM a, c RIGHT JOIN b ON a.id = b.id AND a.id2 = b.id2 NATURAL JOIN d CROSS JOIN e AS cj NATURAL LEFT JOIN f NATURAL FULL OUTER JOIN g ;
SELECT round(sum(count(*)), 2), 1 FROM a LEFT JOIN b AS t1 ON a.id = t1.id ;
SELECT id, log(2, id), log(10, id), count(*) FROM a GROUP BY id HAVING count(*) < 10 ;
SELECT val, rank() OVER (PARTITION BY id) AS rank, lead(val) OVER (ORDER BY val ASC ROWS CURRENT ROW), lag(val) OVER (PARTITION BY id, val ORDER BY val ASC RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS lag FROM t ;
WITH s1 AS ( WITH s3 AS ( SELECT 1 ) SELECT * FROM s3 ), s AS ( SELECT * FROM s1 LIMIT 1 ) SELECT * FROM s, ( WITH t AS ( SELECT 3 FROM t ) SELECT * FROM t ) AS subquery2 CROSS JOIN ( WITH u AS ( SELECT count(*) AS nb ) SELECT nb FROM u UNION ALL ( SELECT 0 ) ) AS subquery3 LIMIT 1 ;
WITH RECURSIVE s AS ( SELECT 1 ), recur AS ( SELECT employee_id, last_name, manager_id FROM employees WHERE employee_id = 1 UNION ALL ( SELECT employee_id, last_name, manager_id FROM employees WHERE isvalid = 1 AND recur.employee_id = manager_id ) ) SELECT * FROM recur WHERE salary > 0 ;
SELECT a, b, c FROM foo AS bar GROUP BY GROUPING SETS (a, CUBE (a, b), ROLLUP (c, a), CUBE (rollup(a, b, c))) ;
SELECT * FROM tbl AS t, t2 NATURAL JOIN t3 FOR UPDATE OF t2, col NOWAIT ;
-- 2 FIXME for this statement
-- FIXME: Clause "WAIT 1" converted to "NOWAIT"
-- FIXME: FOR UPDATE OF col must be changed ot its table name/alias
UPDATE t SET a = 1, (b, c) = (SELECT * FROM t2 WHERE id = 1), d = (SELECT 1) WHERE (a < 10) ;
DELETE FROM public.t AS tbl WHERE COALESCE(tbl.col, 'todel') = 'todel' ;
INSERT INTO public.t AS ins VALUES (2 + 1, 'tt') ;
INSERT INTO public.t AS ins (a, b) VALUES (2 + 1, 'tt') ;
INSERT INTO public.t AS ins (a, b) SELECT id, count(*) FROM t GROUP BY 1 ;
SELECT 1 FROM t1 AS t WHERE id < 10 ;
-- 1 FIXME for this statement
-- FIXME: Flashback clause ignored for table "t1": "VERSIONS BETWEEN TIMESTAMP MINVALUE AND current_timestamp"
UPDATE t1 SET val = 't' WHERE id = 1 ;
-- 1 FIXME for this statement
-- FIXME: Error logging clause ignored: "LOG ERRORS INTO err.log (to_char(sysdate), id)"
UPDATE t1 SET val = 't' WHERE id = 1 ;
-- 2 FIXME for this statement
-- FIXME: Returning clause ignored: "RETURNING (id % 2), * INTO a, b"
-- FIXME: Error logging clause ignored: "REJECT LIMIT 3"
```

