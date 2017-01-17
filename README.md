plsql2pg
========

Some references:

- [Marpa::R2 main doc](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Marpa_R2.pod)
- [SLIF / Scanless interface](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Scanless/DSL.pod)


Original:
---------
```sql
SElect 1 nb from DUAL; SELECT * from TBL t order by a, b desc, tbl.c asc;
SELECT nvl(val, 'null') "vAl",1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1, 'test me', t.* from tbl t WHERE a > 2 and rownum < 10 OR b < 3 GROUP BY a, t.b;
 select * from (
select 1 from dual
) union (select 2 from dual) minus (select 3 from dual) interSECT (select 4 from dual) union all (select 5 from dual);
select * from a,c join b using (id,id2) left join d using (id) WHERE rownum >10 and rownum <= 20;
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2 naturaL join d CROSS JOIN e cj;
select round(sum(count(*)), 2), 1 from a,b t1 where a.id = t1.id(+);
SELECT id, count(*) FROM a GROUP BY id HAVING count(*)< 10;

```

Converted:
----------
```sql
SELECT 1 AS nb ;
SELECT * FROM tbl AS t ORDER BY a ASC, b DESC, tbl.c ASC ;
SELECT COALESCE(val, 'null') AS "vAl", 1, abc, "DEF" FROM toto AS "TATA;" ;
SELECT 1, 'test me', t.* FROM tbl AS t WHERE a > 2 OR b < 3 GROUP BY a, t.b LIMIT 9 ;
SELECT * FROM ( SELECT 1 ) AS subquery1
UNION
SELECT 2
EXCEPT
SELECT 3
INTERSECT
SELECT 4
UNION ALL
SELECT 5 ;
SELECT * FROM a, c INNER JOIN b USING (id, id2) LEFT JOIN d USING (id) LIMIT 20 OFFSET 10 ;
SELECT * FROM a, c RIGHT JOIN b ON a.id = b.id AND a.id2 = b.id2 NATURAL JOIN d CROSS JOIN e AS cj ;
SELECT round(sum(count(*)), 2), 1 FROM a LEFT JOIN b AS t1 ON a.id = t1.id ;
SELECT id, count(*) FROM a GROUP BY id HAVING count(*) < 10 ;
```

