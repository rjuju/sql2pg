plsql2pg
========

Some references:

- [Marpa::R2 main doc](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Marpa_R2.pod)
- [SLIF / Scanless interface](http://search.cpan.org/~jkegl/Marpa-R2-3.000000/pod/Scanless/DSL.pod)


Original:
---------
```sql
SElect 1 nb from DUAL; SELECT * from TBL t order by a, b desc, c asc;
SELECT 1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1, 'test me', * from tbl t WHERE 1 > 2 OR b < 3;
 select t.* from (
select 1 from dual
) as t;
select * from a,c join b using (id,id2);
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2;
select 1 from a,b t1 where a.id = t1.id(+);
```

Converted:
----------
```sql
SELECT 1 AS nb FROM dual ;
SELECT * FROM tbl AS t ORDER BY a ASC, b DESC, c ASC ;
SELECT 1, abc, "DEF" FROM toto AS "TATA;" ;
SELECT 1, 'test me', * FROM tbl AS t WHERE 1 > 2 OR b < 3 ;
SELECT t.* FROM ( SELECT 1 FROM dual ) AS ) ;
SELECT * FROM a, c INNER JOIN b USING (id, id2) ;
SELECT * FROM a, c RIGHT JOIN b ON a.id = b.id AND a.id2 = b.id2 ;
SELECT 1 FROM a LEFT JOIN b AS t1 ON a.id = t1.id ;
```
