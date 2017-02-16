SElect 1 nb from DUAL WHERE rownum < 2; SELECT DISTINCT * from TBL t order by a nulls last, b desc, tbl.c asc;
SELECT -1, + 1-+1, aze aze#, a a$z#e from t;
SELECT (((1), 1, (1, (1,1)))), nvl(val, 'null') || ' '|| nvl(val2, 'empty') "vAl",1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1 + +2 * ((t.v) - 2) % 4 meh, 'test me', t.* from tbl t WHERE (((((a > 2)) and (rownum < 10)) OR ((((b < 3)))))) GROUP BY a, t.b;
 select * from (
((((select 1 from dual))))
) union (select 2 from dual) minus (select 3 from dual) interSECT (select 4 from dual) union all (select 5 from dual) order by 1 asc;
select * from a,only (c) join b using (id,id2) left join d using (id) WHERE rownum >10 and rownum <= 20;
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2 naturaL join d CROSS JOIN e cj natural left outer join f natural full outer join g;
select round(sum(count(*)), 2), 1 from a,b t1 where a.id = t1.id(+);
select t2.* from t2, t1 where t2.id(+) > t1.id;
SELECT id, log2(id), log10(id), count(*) FROM a GROUP BY id HAVING count(*)< 10;
SELECT val, rank() over (partition by id) rank, lead(val) over (order by val rows CURRENT ROW), lag(val) over (partition by id,val order by val range between 2 preceding and unbounded following) as lag from t;
WITH s1 as (with s3 as (select 1 from dual) select * from s3), s AS (SELECT * FROM s1 where rownum < 2) SELECT * From s, (with t as (select 3 from t) select * from t) cross join (with u as (select count(*) nb from dual) select nb from u union all (select 0 from dual)) where rownum < 2;
with s as (select 1 from dual) SELECT employee_id, last_name, manager_id
FROM employees   -- this is the FROM clause
WHERE salary > 0   --should not happen
start with /* hard coded value */ employee_id = 1 CONNECT BY nocycle isvalid = 1 and PRIOR employee_id = manager_id;
SELECT :val,b,c FROM foo bar group by grouping sets((a), (), cube(a,b), rollup(c,a), cube(rollup(a,b,c)), grouping sets (()));
SELECT * FROM tbl t, t2 natural join t3 FOR UPDATE OF t2.a, col wait 1;
update t set a = 1, (b,c) = (select * from t2 WHERE id = 1), d = (SELECT 1 from dual) where (a < 10);
delete from public.t tbl where nvl(tbl.col, 'todel') = 'todel';
insert into public.t ins values (2+1, 'tt');
insert   into public.t ins (a,b) values (2+1, 'tt');
insert into public.t ins (a,b) select id, count(*) from t group by 1;
select a.id from a,d,b,c where b.id = c.id(+) or b.id2 = c.id2(+) and c.id = d.id (+) and a.id =b.id(+);
select round(t.val /100, 2) from t;
select id, case id when 0 then 'blah' else id % 3 > 1 end as val from t;
select id, case when val = 0 then 'nothing' when val < 100 then 'little' when val >=100 then 'lot' end from t;
select trim(leading ' ' from v) from t where id > (select count(*) from t2);
select count(*) from t where val like '%the_val%' escape '\' and rownum <= 5 or val like (fct(val)) for update skip locked;
with s (id, val) as (select 1, 'val' from dual) select * from s;
select * from dual, (((((t t1 left join (select 1 from t2) tt using (((id)))))) right join t using (id)));
select count(*) from t where val in ('a', 'b') or val not in (('test')) or not exists (select 1 from t2 where t2.id = t1.id) or exists (((((select 1 from t3 where t3.id=t1.id)))));
select null, 1, (1, (select count(*) from t)) from t2 where id1 is not and ((((id2 is not null))));
select interval '3' hour, interval '3-6' hour(:"h") to second(:a,:b) from dual where val = :"h";
select INTERVAL'20' DAY - INTERVAL'240' HOUR = INTERVAL'10-0' DAY TO SECOND from t;
select :a, b,:1,d,:1 from t where b = :a;
select to_date('2017-01-01', 'YYYY-MM-DD') at time zone 'Europe/Paris' "paris time", to_date(dt, :fmt) at time zone :tz, timestamp '2017-02-01 23:12:15' at time zone 'CET', date '2017-02-01' from t;
select a,b,c from t sample block (3) seed (.2)"S";
select a = 1 OR b = 2 v1,(a = 1 OR b = 2)v2 from t;
explain plan set statement_id = 'del stmt' into del_tables for delete from t where id < 10;
-- now unsupported stuff
SELECT 1 FROM t1 VERSIONS BETWEEN TIMESTAMP MINVALUE AND CURRENT_TIMESTAMP t WHERE id < 10;
UPDATE t1 SET val = 't' WHERE id = 1 LOG ERRORS INTO err.log (to_char(SYSDATE), id);
UPDATE t1 SET val = 't' WHERE id = 1 RETURNING (id%2), * INTO a,b REJECT LIMIT 3;
SELECT first_value(val ignore nulls) over (partition by deptno order by val) from t;
SELECT lag(val respect nulls, 1, -1) over () from t;
SELECT lag(val, 1) respect nulls over () from t;
SELECT val from t partition by (dt) right outer join t2 on (t2.id = t.id);
select a,b,c from (select * from t1 join b using (id)) model dimension by (a,b) measures (c) rules (f[a,b] = 4, g[a,c] = val(2));
-- I don't belong to any query
