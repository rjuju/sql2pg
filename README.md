plsql2pg
========

Some references:

- [Marpa::R2 main doc](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Marpa_R2.pod)
- [SLIF / Scanless interface](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Scanless/DSL.pod)


Original:
---------
```sql
Elect 1 nb from DUAL; SELECT * from TBL t order by a, b desc, tbl.c asc;
SELECT nvl(val, 'null') "vAl",1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1, 'test me', t.* from tbl t WHERE 1 > 2 OR b < 3 GROUP BY a, t.b;
 select * from (
select 1 from dual
);
select * from a,c join b using (id,id2) left join d using (id);
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2;
select round(sum(count(*)), 2), 1 from a,b t1 where a.id = t1.id(+);
```

Converted:
----------
```sql
SELECT 1 AS nb ;
SELECT * FROM tbl AS t ORDER BY a ASC, b DESC, tbl.c ASC ;
SELECT COALESCE(val, 'null') AS "vAl", 1, abc, "DEF" FROM toto AS "TATA;" ;
SELECT 1, 'test me', t.* FROM tbl AS t WHERE 1 > 2 OR b < 3 GROUP BY a, t.b ;
SELECT * FROM ( SELECT 1 ) AS subquery1 ;
SELECT * FROM a, c INNER JOIN b USING (id, id2) LEFT JOIN d USING (id) ;
SELECT * FROM a, c RIGHT JOIN b ON a.id = b.id AND a.id2 = b.id2 ;
SELECT round(sum(count(*)), 2), 1 FROM a LEFT JOIN b AS t1 ON a.id = t1.id ;
```

