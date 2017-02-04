SElect 1 nb from DUAL WHERE rownum < 2; SELECT DISTINCT * from TBL t order by a nulls last, b desc, tbl.c asc;
SELECT -1, 1-1, aze aze#, a a$z#e from t;
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
WHERE salary > 0   --should not happen
start with /* hard coded value */ employee_id = 1 CONNECT BY isvalid = 1 and PRIOR employee_id = manager_id;
SELECT a,b,c FROM foo bar group by grouping sets(a, cube(a,b), rollup(c,a), cube(rollup(a,b,c)));
SELECT * FROM tbl t, t2 natural join t3 FOR UPDATE OF t2.a, col wait 1;
update t set a = 1, (b,c) = (select * from t2 WHERE id = 1), d = (SELECT 1 from dual) where (a < 10);
delete from public.t tbl where nvl(tbl.col, 'todel') = 'todel';
insert into public.t ins values (2+1, 'tt');
insert   into public.t ins (a,b) values (2+1, 'tt');
insert into public.t ins (a,b) select id, count(*) from t group by 1;
select a.id from a,d,b,c where b.id = c.id(+) and c.id = d.id (+) and a.id =b.id(+);
select round(t.val /100, 2) from t;
select id, case id when 0 then 'blah' else id % 3 > 1 end as val from t;
select id, case when val = 0 then 'nothing' when val < 100 then 'little' when val >=100 then 'lot' end from t;
select trim(leading ' ' from v) from t where id > (select count(*) from t2);
-- now unsupported stuff
SELECT 1 FROM t1 VERSIONS BETWEEN TIMESTAMP MINVALUE AND CURRENT_TIMESTAMP t WHERE id < 10;
UPDATE t1 SET val = 't' WHERE id = 1 LOG ERRORS INTO err.log (to_char(SYSDATE), id);
UPDATE t1 SET val = 't' WHERE id = 1 RETURNING (id%2), * INTO a,b REJECT LIMIT 3;
SELECT first_value(val ignore nulls) over (partition by deptno order by val) from t;
SELECT lag(val respect nulls, 1, -1) over () from t;
SELECT lag(val, 1) respect nulls over () from t;
SELECT val from t partition by (dt) right outer join t2 on (t2.id = t.id);
-- I don't belong to any query
