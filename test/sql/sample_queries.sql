SElect 1 nb from DUAL WHERE rownum < 2; SELECT DISTINCT * from TBL t order by a nulls last, b desc, tbl.c asc;
SELECT -1, + 1-+1, aze aze#, a a$z#e from t;
SELECT (((1), 1, (1, (1,1)))), nvl(val, 'null') || ' '|| nvl(val2, 'empty') "vAl",1, abc, "DEF" from "toto" as "TATA;";
 SELECT 1e1d + +2.1F * ((t.v) - 2) % 4 meh, 1dd, 'test me', t.* from tbl t WHERE (((((a > 2)) and (rownum < 10)) OR ((((b < 3)))))) GROUP BY a, t.b;
 select * from (
((((select 1 from dual))))
) union (select 2 from dual) minus (select 3 from dual) interSECT (select 4 from dual) union all (select 5 from dual) order by 1 asc;
select * from a,only (c) join b using (id,id2) left join d using (id) WHERE rownum >10 and rownum <= 20;
select * from a,c right join b on a.id = b.id AND a.id2 = b.id2 naturaL join d CROSS JOIN e cj natural left outer join f natural full outer join g;
select round(sum(count(*)), 2), 1 from a,b t1 where a.id = t1.id(+);
select t2.* from t2, t1 where t2.id(+) > t1.id;
SELECT id, log2(id), log10(id), count(*) FROM a GROUP BY id HAVING count(*)< 10;
-- Oracle allows this syntax
SELECT id, log2(id), log10(id), count(*) FROM a HAVING count(*)< 10 GROUP BY id;
SELECT val, rank() over (partition by id) rank, lead(val) over (order by val rows CURRENT ROW), lag(val) over (partition by id,val order by val range between 2 preceding and unbounded following) as lag from t;
WITH "S1" as (with s3 as (select 1 from dual) select * from s3), s AS (SELECT * FROM s1 where rownum < 2) SELECT * From s, (with t as (select 3 from t) select * from t) cross join (with u as (select count(*) nb from dual) select nb from u union all (select 0 from dual)) where rownum < 2;
with s as (select 1 from dual) SELECT employee_id, last_name, manager_id
FROM employees   -- this is the FROM clause
WHERE salary > 0   --should not happen
start with /* hard coded value */ employee_id = 1 CONNECT BY nocycle isvalid = 1 and PRIOR employee_id = manager_id;
SELECT :val,b,c FROM foo bar group by grouping sets((a), (), cube(a,b), rollup(c,a), cube(rollup(a,b,c)), grouping sets ((())));
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
select trim(leading ' ' from v), cast(t as number(2)) from t where id > (select count(*) from t2);
select count(*) from t where val like '%the_val%' escape '\' and rownum <= 5 or val like (fct(value => val)) for update skip locked;
with s (id, val) as (select 1, 'val' from dual) search breadth first by id nulls first, val asc set id cycle id2,id3 set id4 to '1' default '0' select * from s;
select * from dual, (((((t t1 left join (select 1 from t2) tt using (((id)))))) right join t using (id)));
select count(*) from t where val in ('a', 'b') or val not in (('test')) or not exists (select 1 from t2 where t2.id = t1.id) or exists (((((select 1 from t3 where t3.id=t1.id)))));
select null, 1, (1, (select count(*) from t)) from t2 where id1 is not and ((((id2 is not null))));
select interval '3' hour, interval '3-6' hour(:"h") to second(:a,:b) from dual where val = :"h";
select INTERVAL'20' DAY - INTERVAL'240' HOUR = INTERVAL'10-0' DAY TO SECOND from t;
select :a, b,:1,?,d,:1 from t where b = :a and d > ?;
select to_date('2017-01-01', 'YYYY-MM-DD') at time zone 'Europe/Paris' "paris time", to_date(dt, :fmt) at time zone :tz, timestamp '2017-02-01 23:12:15' at time zone 'CET', date '2017-02-01' from t;
select a,b,c from t sample block (3) seed (.2)"S";
select a = 1 OR b = 2 v1,(a = 1 OR b = 2)v2 from t;
explain plan set statement_id = 'del stmt' into del_tables for delete from t where id < 10;
select * from a, a1, a2, b, b1, b2 where a.id = a1.id(+) and a1.id = a2.id(+) and a.id = b.id and b1.id = b2.id(+) and b2.id2(+) = b1.id2 and b1.id(+) < b.id;
CREAte table nsp.t as select 1 from dual;
comment on table nsp.t is 'this table should have only one line';
create or replace view v as select * from nsp.t;
select '''it''s a ''''lot of quotes''', 'it''s fine' from dual;
merge into t1 t using (select id, val, sysdate from tmp t2 where id = 1) as src on (t.id = src.id and src.id2 = t.id2)
when matched then update set t.val = src.val, t.dt = sysdate where t.id < 100 delete where src.id > 1000
when not matched then insert (id, val, dt) values (1, 'new', sysdate) where id > 0;
merge into t1 t using tmp.data on (t.id = data.id) when matched then update set t.val = data.val;
select - fact(2), + fact, fact(2), fact(3) - fact, fact(3) + - id, - id from t;
show errors
-- now unsupported stuff
SELECT 1 FROM t1 VERSIONS BETWEEN TIMESTAMP MINVALUE AND CURRENT_TIMESTAMP t WHERE id < 10;
UPDATE t1 SET val = 't' WHERE id = 1 LOG ERRORS INTO err.log (to_char(SYSDATE), id);
UPDATE t1 SET val = 't' WHERE id = 1 RETURNING (id%2), * INTO a,b REJECT LIMIT 3;
SELECT first_value(val ignore nulls) over (partition by deptno order by val) from t order siblings by 1;
SELECT lag(val respect nulls, 1, -1) over () from t;
SELECT lag(val, 1) respect nulls over () from t;
SELECT val from t partition by (dt) right outer join t2 on (t2.id = t.id);
select a,b,c from (select * from t1 join b using (id)) model dimension by (a,b) measures (c) rules (f[a,b] = 4, g[a,c] = val(2));
select * from a join b a2 using (id) unpivot (value for value_type in (a,b) ) ORDER BY 1;
create table tbl_virtual(id number primary key, data blob not null, id1 as (to_date(id)), id2 number generated always as (id/10),);
create global temporary table test_glob_tmp (id number(9) constraint tmp_fk references tbl_virtual(id1));
truncate table tst_tbl preserve materialized view log;
create or replace procedure "Test_Proc" (id in out numeric, id2 inout number) is
new text := 'set';
invalid_input exception;
begin
    IF id <= 0 THEN
    DBMS_OUTPUT.put_line
    ('not correct'); raise invalid_input; raise invalid_input2; raise invalid_input;
    <<block1>>
    declare i integer; dt1 timestamp with LOCAL time zone; dt2 TIMESTAMP with TIME zone;
    BEGIN
        truncate table tmp;
        for i in (select i from tbl where pk = id) loop new := 'set'; end loop;
            for i in reverse 1.. i loop dbms_output.put_line('i is ' || i||' and' ||f(a,b)); end loop;
    END block1;
else null;
    end if;
exception when invalid_input then dbms_output.put_line('exception catched'); RAISE;end "Test_Proc";
create or replace trigger test_trg before update on nsp.tbl for each row declare
ok integer; begin select count(*) > 0 into ok from nsp.tbl; NEW.ok := ok;return;end;
create Function "test_func"(id in number default (0), val tbl.val%type) return varchar2 as
declare val varchar2(255) default 'unset';
begin
    if id <= 0 then begin if id < 0 then return 'val is negative'; else return
        'id is zero' end if; end; elsif id = 42 then id:=1;id := 0;return ((  ('true'))) else return 'false' end if; return();
end;
create function toto as
cur mytype;
type mytype is ref Cursor;
id number;
val varchar2(255);
cur2 sys_refcursor;
begin
    select ?,:d,? from t;
    select ? from t2;
    if SQL%ROWCOUNT = 1 then dbms_output.put_line('1 line'); else dbms_output.put_line('1 line'); end if;
    open cur for select * from tbl order by id;
    loop fetch cur into id, val; exit when cur%notfound or id is null or id < 0;end loop; close cur;
        exit; id := 3; while id > 0 loop dbms_output.put_line('id is ' || id); id := id-1;commit;end loop;
        <<nested>>
        declare cur2 mytype; -- should it be handled?
        begin
        end; val := get_select(field => '*', cond => 't'); open cur for val; fct(cur.id);
        useless_cw case cur2.id when null then case when random() = 0 then f1(-1);dbms_output.put_line('bingo') ;else dbms_output.put_line('null'); end case; when 0 then id := 1; else DBMS_OUTPUT.put_line('cur2.id is ' || cur2.id); end case useless_cw;
        execute immediate 'select 1, 1 from' || 'cur.tbl' into id, id; id := sql%rowcount;
end;
create  package pkg is type myrecord is record(id number, val varvhar2(50));procedure set_val(id number, val varchar2); function get_val(id number) return varchar2;
-- TODO/FIXME below is broken, should reference pkg.myrecord
type mytable is table of myrecord index by i;
pi constant number := 3.14;pg constant varchar2(50) := 'PostgreSQL';end pkg;
create or replace package BODY pkg as procedure set_val(id number, val varchar2) is begin update config set value = val where pk = id; end;
function get_val(id number) is val varchar;begin select value into val from config where pk = id; return val; end;end pkg;
show errors
-- I don't belong to any query
