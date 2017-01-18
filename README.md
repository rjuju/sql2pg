plsql2pg
========

Some references:

- [Marpa::R2 main doc](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Marpa_R2.pod)
- [SLIF / Scanless interface](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Scanless/DSL.pod)
- [window functions spec](https://docs.oracle.com/cd/E11882_01/server.112/e25554/analysis.htm#DWHSG021)


Original:
---------
```sql
SElect 1 nb from DUAL; SELECT * from TBL t order by a, b desc, tbl.c asc;
SELECT nvl(val, 'null') "vAl",1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1, 'test me', t.* from tbl t WHERE (((a > 2)) and rownum < 10) OR b < 3 GROUP BY a, t.b;
 select * from (
select 1 from dual
) union (select 2 from dual) minus (select 3 from dual) interSECT (select 4 from dual) union all (select 5 from dual);
select * from a,c join b using (id,id2) left join d using (id) WHERE rownum >10 and rownum <= 20;
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2 naturaL join d CROSS JOIN e cj;
select round(sum(count(*)), 2), 1 from a,b t1 where a.id = t1.id(+);
SELECT id, count(*) FROM a GROUP BY id HAVING count(*)< 10;
SELECT val, rank() over (partition by id) rank, lead(val) over (order by val rows CURRENT ROW), lag(val) over (partition by id,val order by val range between 2 preceding and unbounded following) as lag from t;
```

Converted:
----------
```sql
SELECT 1 AS nb ;
SELECT * FROM tbl AS t ORDER BY a ASC, b DESC, tbl.c ASC ;
SELECT COALESCE(val, 'null') AS "vAl", 1, abc, "DEF" FROM toto AS "TATA;" ;
SELECT 1, 'test me', t.* FROM tbl AS t WHERE (((a > 2)) AND rownum < 10) OR b < 3 GROUP BY a, t.b ;
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
SELECT val, rank() OVER (PARTITION BY id) AS rank, lead(val) OVER (ORDER BY val ASC ROWS CURRENT ROW), lag(val) OVER (PARTITION BY id, val ORDER BY val ASC RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS lag FROM t ;
```

