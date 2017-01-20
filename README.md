plsql2pg
========

Some references:

- [Marpa::R2 main doc](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Marpa_R2.pod)
- [SLIF / Scanless interface](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Scanless/DSL.pod)
- [window functions spec](https://docs.oracle.com/cd/E11882_01/server.112/e25554/analysis.htm#DWHSG021)


Original:
---------
```sql
SElect 1 nb from DUAL WHERE rownum < 2; SELECT * from TBL t order by a, b desc, tbl.c asc;
SELECT nvl(val, 'null') || ' '|| nvl(val2, 'empty') "vAl",1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1, 'test me', t.* from tbl t WHERE (((((a > 2)) and (rownum < 10)) OR ((((b < 3)))))) GROUP BY a, t.b;
 select * from (
select 1 from dual
) union (select 2 from dual) minus (select 3 from dual) interSECT (select 4 from dual) union all (select 5 from dual);
select * from a,c join b using (id,id2) left join d using (id) WHERE rownum >10 and rownum <= 20;
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2 naturaL join d CROSS JOIN e cj;
select round(sum(count(*)), 2), 1 from a,b t1 where a.id = t1.id(+);
SELECT id, count(*) FROM a GROUP BY id HAVING count(*)< 10;
SELECT val, rank() over (partition by id) rank, lead(val) over (order by val rows CURRENT ROW), lag(val) over (partition by id,val order by val range between 2 preceding and unbounded following) as lag from t;
WITH s1 as (with s3 as (select 1 from dual) select * from s3), s AS (SELECT * FROM s1 where rownum < 2) SELECT * From s, (with t as (select 3 from t) select * from t) cross join (with u as (select count(*) nb from dual) select nb from u union all (select 0 from dual)) where rownum < 2;
with s as (select 1 from dual) SELECT employee_id, last_name, manager_id
FROM employees
WHERE salary > 0
start with employee_id = 1 CONNECT BY isvalid = 1 and PRIOR employee_id = manager_id;
```

Converted:
----------
```sql
SELECT 1 AS nb LIMIT 1 ;
SELECT * FROM tbl AS t ORDER BY a ASC, b DESC, tbl.c ASC ;
SELECT COALESCE(val, 'null') || ' ' || COALESCE(val2, 'empty') AS "vAl", 1, abc, "DEF" FROM toto AS "TATA;" ;
SELECT 1, 'test me', t.* FROM tbl AS t WHERE (((a > 2)) OR (b < 3)) GROUP BY a, t.b LIMIT 9 ;
SELECT * FROM ( SELECT 1 ) AS subquery1 UNION ( SELECT 2 ) EXCEPT ( SELECT 3 ) INTERSECT ( SELECT 4 ) UNION ALL ( SELECT 5 ) ;
SELECT * FROM a, c INNER JOIN b USING (id, id2) LEFT JOIN d USING (id) LIMIT 20 OFFSET 10 ;
SELECT * FROM a, c RIGHT JOIN b ON a.id = b.id AND a.id2 = b.id2 NATURAL JOIN d CROSS JOIN e AS cj ;
SELECT round(sum(count(*)), 2), 1 FROM a LEFT JOIN b AS t1 ON a.id = t1.id ;
SELECT id, count(*) FROM a GROUP BY id HAVING count(*) < 10 ;
SELECT val, rank() OVER (PARTITION BY id) AS rank, lead(val) OVER (ORDER BY val ASC ROWS CURRENT ROW), lag(val) OVER (PARTITION BY id, val ORDER BY val ASC RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS lag FROM t ;
WITH s1 AS ( WITH s3 AS ( SELECT 1 ) SELECT * FROM s3 ), s AS ( SELECT * FROM s1 LIMIT 1 ) SELECT * FROM s, ( WITH t AS ( SELECT 3 FROM t ) SELECT * FROM t ) AS subquery2 CROSS JOIN ( WITH u AS ( SELECT count(*) AS nb ) SELECT nb FROM u UNION ALL ( SELECT 0 ) ) AS subquery3 LIMIT 1 ;
WITH RECURSIVE s AS ( SELECT 1 ), recur AS ( SELECT employee_id, last_name, manager_id FROM employees WHERE employee_id = 1 UNION ALL ( SELECT employee_id, last_name, manager_id FROM employees WHERE isvalid = 1 AND recur.employee_id = manager_id ) ) SELECT * FROM recur WHERE salary > 0 ;
```

