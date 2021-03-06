SELECT 1 AS nb LIMIT 1 ;
SELECT DISTINCT * FROM tbl AS t ORDER BY a ASC NULLS LAST, b DESC, tbl.c ASC ;
SELECT -1, 1 - 1, aze AS "aze#", a AS "a$z#e" FROM t ;
SELECT ((1), 1, (1, (1, 1))), COALESCE(val, 'null') || ' ' || COALESCE(val2, 'empty') AS "vAl", 1, abc, "DEF" FROM toto AS "TATA;" ;
SELECT CAST(1e1 AS float) + CAST(2.1 AS real) * ((t.v) - 2) % 4 AS meh, CAST(1 AS float) AS d, 'test me', t.* FROM tbl AS t WHERE (((a > 2)) OR (b < 3)) GROUP BY a, t.b LIMIT 9 ;
SELECT * FROM ((SELECT 1)) AS subquery1 UNION (SELECT 2) EXCEPT (SELECT 3) INTERSECT (SELECT 4) UNION ALL (SELECT 5) ORDER BY 1 ASC ;
SELECT * FROM a, ONLY (c) INNER JOIN b USING (id, id2) LEFT JOIN d USING (id) LIMIT 10 OFFSET 10 ;
SELECT * FROM a, c RIGHT JOIN b ON a.id = b.id AND a.id2 = b.id2 NATURAL JOIN d CROSS JOIN e AS cj NATURAL LEFT JOIN f NATURAL FULL OUTER JOIN g ;
SELECT round(sum(count(*)), 2), 1 FROM a LEFT JOIN b AS t1 ON a.id = t1.id ;
SELECT t2.* FROM t1 LEFT JOIN t2 ON t1.id <= t2.id ;
SELECT id, log(2, id), log(10, id), count(*) FROM a GROUP BY id HAVING count(*) < 10 ;
-- Oracle allows this syntax
SELECT id, log(2, id), log(10, id), count(*) FROM a GROUP BY id HAVING count(*) < 10 ;
SELECT val, rank() OVER (PARTITION BY id) AS rank, lead(val) OVER (ORDER BY val ASC ROWS CURRENT ROW), lag(val) OVER (PARTITION BY id, val ORDER BY val ASC RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS lag FROM t ;
WITH "S1" AS (WITH s3 AS (SELECT 1) SELECT * FROM s3), s AS (SELECT * FROM s1 LIMIT 1) SELECT * FROM s, (WITH t AS (SELECT 3 FROM t) SELECT * FROM t) AS subquery1 CROSS JOIN (WITH u AS (SELECT count(*) AS nb) SELECT nb FROM u UNION ALL (SELECT 0)) AS subquery2 LIMIT 1 ;
WITH RECURSIVE s AS (SELECT 1), recur AS (SELECT employee_id, last_name, manager_id FROM employees WHERE employee_id = 1 UNION ALL (SELECT employee_id, last_name, manager_id FROM employees WHERE isvalid = 1 AND recur.employee_id = manager_id)) SELECT * FROM recur WHERE salary > 0 ;
-- 4 FIXME for this statement
-- FIXME: NOCYCLE clause ignored for clause: isvalid = 1 AND employee_id = manager_id
-- 3 comments for this statement must be replaced:
-- this is the FROM clause
--should not happen
/* hard coded value */
SELECT $1, b, c FROM foo AS bar GROUP BY GROUPING SETS (a, (), CUBE (a, b), ROLLUP (c, a), CUBE (rollup(a, b, c)), GROUPING SETS (())) ;
-- 1 FIXME for this statement
-- FIXME: Bindvar :val has been translated to parameter $1
SELECT * FROM tbl AS t, t2 NATURAL JOIN t3 FOR UPDATE OF t2, col NOWAIT ;
-- 2 FIXME for this statement
-- FIXME: Clause "WAIT 1" converted to "NOWAIT"
-- FIXME: FOR UPDATE OF col must be changed to its table name/alias
UPDATE t SET a = 1, (b, c) = (SELECT * FROM t2 WHERE id = 1), d = (SELECT 1) WHERE (a < 10) ;
DELETE FROM public.t AS tbl WHERE COALESCE(tbl.col, 'todel') = 'todel' ;
INSERT INTO public.t AS ins VALUES (2 + 1, 'tt') ;
INSERT INTO public.t AS ins (a, b) VALUES (2 + 1, 'tt') ;
INSERT INTO public.t AS ins (a, b) SELECT id, count(*) FROM t GROUP BY 1 ;
SELECT a.id FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id OR b.id2 = c.id2 LEFT JOIN d ON c.id = d.id ;
SELECT round(t.val / 100, 2) FROM t ;
SELECT id, CASE id WHEN 0 THEN 'blah' ELSE id % 3 > 1 END AS val FROM t ;
SELECT id, CASE WHEN val = 0 THEN 'nothing' WHEN val < 100 THEN 'little' WHEN val >= 100 THEN 'lot' END FROM t ;
SELECT trim(LEADING  ' '  FROM  v ), cast(t AS numeric(2)) FROM t WHERE id > (SELECT count(*) FROM t2) ;
SELECT count(*) FROM t WHERE val LIKE '%the_val%' ESCAPE '\' OR val LIKE (fct(value => val)) FOR UPDATE SKIP LOCKED LIMIT 5 ;
WITH s (id, val) AS (SELECT 1, 'val') SELECT * FROM s ;
-- 2 FIXME for this statement
-- FIXME: SEARCH clause ignored: SEARCH BREADTH FIRST BY id ASC NULLS FIRST, val ASC SET id
-- FIXME: CYCLE clause ignored: CYCLE id2, id3 SET id4 TO '1' DEFAULT '0'
SELECT * FROM dual, ((t AS t1 LEFT JOIN (SELECT 1 FROM t2) AS tt USING (id)) AS subquery2 RIGHT JOIN t USING (id)) AS subquery1 ;
SELECT count(*) FROM t WHERE val IN ('a', 'b') OR val NOT IN ('test') OR NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id) OR EXISTS (SELECT 1 FROM t3 WHERE t3.id = t1.id) ;
SELECT NULL, 1, (1, (SELECT count(*) FROM t)) FROM t2 WHERE id1 IS not AND (id2 IS NOT NULL) ;
SELECT INTERVAL '3' HOUR, INTERVAL '3-6' HOUR TO SECOND($1) WHERE val = $2 ;
-- 3 FIXME for this statement
-- FIXME: Bindvar :b has been translated to parameter $1
-- FIXME: Bindvar :"h" has been translated to parameter $2
-- FIXME: Bindvar :a is now useless
SELECT INTERVAL '20' DAY - INTERVAL '240' HOUR = INTERVAL '10-0' DAY TO SECOND FROM t ;
SELECT $1, b, $2, $3, d, $2 FROM t WHERE b = $1 AND d > $4 ;
-- 4 FIXME for this statement
-- FIXME: Bindvar :a has been translated to parameter $1
-- FIXME: Bindvar :1 has been translated to parameter $2
-- FIXME: Prepared statement parameter n°1 has been translated to parameter $3
-- FIXME: Prepared statement parameter n°2 has been translated to parameter $4
SELECT to_date('2017-01-01', 'YYYY-MM-DD') AT TIME ZONE 'Europe/Paris' AS "paris time", to_date(dt, $1) AT TIME ZONE $2, TIMESTAMP '2017-02-01 23:12:15' AT TIME ZONE 'CET', DATE '2017-02-01' FROM t ;
-- 2 FIXME for this statement
-- FIXME: Bindvar :fmt has been translated to parameter $1
-- FIXME: Bindvar :tz has been translated to parameter $2
SELECT a, b, c FROM t TABLESAMPLE SYSTEM (3) REPEATABLE (.2) AS "S" ;
SELECT a = 1 OR b = 2 AS v1, (a = 1 OR b = 2) AS v2 FROM t ;
EXPLAIN DELETE FROM t WHERE id < 10 ;
-- 1 FIXME for this statement
-- FIXME: EXPLAIN clause ignored: SET STATEMENT_ID = 'del stmt' INTO del_tables
SELECT * FROM a LEFT JOIN a1 ON a.id = a1.id LEFT JOIN a2 ON a1.id = a2.id, b LEFT JOIN b1 ON b.id >= b1.id LEFT JOIN b2 ON b1.id = b2.id AND b1.id2 = b2.id2 WHERE  AND a.id = b.id ;
CREATE TABLE nsp.t AS SELECT 1 ;
COMMENT ON TABLE nsp.t IS 'this table should have only one line' ;
CREATE OR REPLACE VIEW v AS SELECT * FROM nsp.t ;
SELECT '''it''s a ''''lot of quotes''', 'it''s fine' ;
INSERT INTO t1 AS t (id, val, dt) SELECT id, val, clock_timestamp() FROM tmp AS t2 WHERE id = 1 ON CONFLICT (id, id2) DO UPDATE SET val = excluded.val, dt = clock_timestamp() WHERE t.id < 100 ;
-- 2 FIXME for this statement
-- FIXME: DELETE clause of the merge_update_clause has been ignored: DELETE WHERE src.id > 1000
-- FIXME: WHERE clause of the merge_insert_clause has been ignored: WHERE id > 0
INSERT INTO t1 AS t SELECT * FROM tmp.data ON CONFLICT (id) DO UPDATE SET val = excluded.val ;
SELECT  - fact(2),  + fact, fact(2), fact(3) - fact, fact(3) +  - id,  - id FROM t ;
-- now unsupported stuff
SELECT 1 FROM t1 AS t WHERE id < 10 ;
-- 1 FIXME for this statement
-- FIXME: Flashback clause ignored for table "t1": "VERSIONS BETWEEN TIMESTAMP MINVALUE AND current_timestamp"
UPDATE t1 SET val = 't' WHERE id = 1 ;
-- 1 FIXME for this statement
-- FIXME: Error logging clause ignored: "LOG ERRORS INTO err.log (to_char(clock_timestamp()), id)"
UPDATE t1 SET val = 't' WHERE id = 1 ;
-- 2 FIXME for this statement
-- FIXME: Returning clause ignored: "RETURNING (id % 2), * INTO a, b"
-- FIXME: Error logging clause ignored: "REJECT LIMIT 3"
SELECT first_value(val) OVER (PARTITION BY deptno ORDER BY val ASC) FROM t ORDER BY 1 ASC ;
-- 2 FIXME for this statement
-- FIXME: NULLS clause ignored: "IGNORE NULLS"
-- FIXME: SIBLINGS clause of ORDER BY ignored
SELECT lag(val, 1, -1) OVER () FROM t ;
-- 1 FIXME for this statement
-- FIXME: NULLS clause ignored: "RESPECT NULLS"
SELECT lag(val, 1) OVER () FROM t ;
-- 1 FIXME for this statement
-- FIXME: NULLS clause ignored: "RESPECT NULLS"
SELECT val FROM t RIGHT JOIN t2 ON (t2.id = t.id) ;
-- 1 FIXME for this statement
-- FIXME: Partition clause ignored for outer join on table t2: PARTITION BY (dt)
SELECT a, b, c FROM (SELECT * FROM t1 INNER JOIN b USING (id)) AS subquery1 ;
-- 1 FIXME for this statement
-- FIXME: MODEL claused ignored
SELECT * FROM a INNER JOIN b AS a2 USING (id) ORDER BY 1 ASC ;
-- 1 FIXME for this statement
-- FIXME: UNPIVOT clause ignored
CREATE TABLE tbl_virtual (
    id numeric PRIMARY KEY,
    data bytea NOT NULL,
    id1 timestamp,
    id2 numeric
) ;
CREATE FUNCTION tbl_virtual_virtual_cols()
RETURNS trigger AS
$_$
BEGIN
  NEW.id1 := to_date(NEW.id) ;
  NEW.id2 := NEW.id / 10 ;
  
  RETURN NEW ;
END ;
$_$ language plpgsql ;
CREATE TRIGGER tbl_virtual_virtual_cols
    BEFORE INSERT OR UPDATE ON tbl_virtual FOR EACH ROW
    EXECUTE PROCEDURE tbl_virtual_virtual_cols() ;
-- 1 FIXME for this statement
-- FIXME: Datatype "timestamp" for column "id1" guessed.  Please check it
CREATE TEMPORARY TABLE test_glob_tmp (
    id numeric(9) CONSTRAINT tmp_fk REFERENCES tbl_virtual(id1)
) ;
-- 1 FIXME for this statement
-- FIXME: GLOBAL clause of TEMPORARY TABLE ignored
TRUNCATE TABLE tst_tbl ;
CREATE OR REPLACE FUNCTION "Test_Proc"(id INOUT numeric, id2 INOUT numeric)
RETURNS void AS
$_$
DECLARE
  new text := 'set' ;
BEGIN
  
  IF id <= 0 THEN
    RAISE NOTICE 'not correct' ;
    RAISE EXCEPTION 'invalid_input' USING ERRCODE = '50001' ;
    RAISE EXCEPTION 'invalid_input2' USING ERRCODE = '50002' ;
    RAISE EXCEPTION 'invalid_input' USING ERRCODE = '50001' ;
    
    <<block1>>
    DECLARE
      i integer ;
      dt1 timestamp with time zone ;
      dt2 timestamp with time zone ;
    BEGIN
      TRUNCATE TABLE tmp ;
      FOR i IN (SELECT i FROM tbl WHERE pk = id)
      LOOP
        new := 'set' ;
      END LOOP ;
      FOR i IN REVERSE 1..i
      LOOP
        RAISE NOTICE 'i is % and%', i, f(a, b) ;
      END LOOP ;
    END ;
  ELSE
    NULL ;
  END IF ;
EXCEPTION
  WHEN SQLSTATE '50001' THEN
    RAISE NOTICE 'exception catched' ;
    RAISE ;
END ;
$_$ language plpgsql ;
CREATE OR REPLACE FUNCTION test_trg()
RETURNS  trigger  AS
$_$
BEGIN
    DECLARE
    ok integer ;
  BEGIN
    SELECT count(*) > 0 INTO ok FROM nsp.tbl ;
    new.ok := ok ;
    
    RETURN NEW ;
  END ;
  
  RETURN NEW ;
END ;
$_$ language plpgsql ;
CREATE TRIGGER test_trg
    BEFORE UPDATE ON nsp.tbl FOR EACH ROW
    EXECUTE PROCEDURE test_trg() ;
CREATE FUNCTION test_func(id numeric DEFAULT (0), val tbl.val%TYPE)
RETURNS varchar AS
$_$
DECLARE
  val varchar(255) := 'unset' ;
BEGIN
  
  IF id <= 0 THEN
        BEGIN
      
      IF id < 0 THEN
        
        RETURN 'val is negative' ;
      ELSE
        
        RETURN 'id is zero' ;
      END IF ;
    END ;
  ELSIF id = 42 THEN 
    id := 1 ;
    id := 0 ;
    
    RETURN (('true')) ;
  ELSE
    
    RETURN 'false' ;
  END IF ;
  
  RETURN  ;
END ;
$_$ language plpgsql ;
CREATE FUNCTION toto()
RETURNS void AS
$_$
DECLARE
  cur refcursor ;
  id numeric ;
  val varchar(255) ;
  cur2 refcursor ;
  sql2pg_rowcount int ;
BEGIN
  SELECT $1, $2, $3 FROM t ;
  SELECT $4 FROM t2 ;
  GET DIAGNOSTICS sql2pg_rowcount := row_count ;
  
  IF sql2pg_rowcount = 1 THEN
    RAISE NOTICE '1 line' ;
  ELSE
    RAISE NOTICE '1 line' ;
  END IF ;
  
  OPEN cur FOR
    SELECT * FROM tbl ORDER BY id ASC ;
  
  LOOP
    FETCH cur INTO id, val ;
    EXIT WHEN NOT FOUND OR id IS NULL OR id < 0 ;
  END LOOP ;
  CLOSE cur ;
  EXIT ;
  id := 3 ;
  WHILE id > 0
  LOOP
    RAISE NOTICE 'id is %', id ;
    id := id - 1 ;
    COMMIT ;
  END LOOP ;
  
  <<nested>>
  DECLARE
    cur2 mytype ;
  BEGIN
  END ;
  val := get_select(field => '*', cond => 't') ;
  
  OPEN cur FOR EXECUTE val ;
  PERFORM fct(cur.id) ;
  CASE cur2.id 
    WHEN NULL THEN
      CASE 
        WHEN random() = 0 THEN
          PERFORM f1(-1) ;
          RAISE NOTICE 'bingo' ;
        ELSE
          RAISE NOTICE 'null' ;
      END CASE ;
 
    WHEN 0 THEN
      id := 1 ;
    ELSE
      RAISE NOTICE 'cur2.id is %', cur2.id ;
  END CASE ;
  EXECUTE 'select 1, 1 from' || 'cur.tbl'
    INTO id, id ;
  GET DIAGNOSTICS sql2pg_rowcount := row_count ;
  id := sql2pg_rowcount ;
END ;
$_$ language plpgsql ;
-- 5 FIXME for this statement
-- FIXME: COMMIT found in function body
-- FIXME: Prepared statement parameter n°1 has been translated to parameter $1
-- FIXME: Bindvar :d has been translated to parameter $2
-- FIXME: Prepared statement parameter n°2 has been translated to parameter $3
-- FIXME: Prepared statement parameter n°3 has been translated to parameter $4
CREATE SCHEMA pkg ;
CREATE TYPE pkg.myrecord AS (
  id numeric, val varvhar2(50)
) ;
CREATE TYPE pkg.mytable AS (
  mytable myrecord[]
) ;
CREATE OR REPLACE FUNCTION pkg.set_val(id numeric, val varchar)
RETURNS void AS
$_$
BEGIN
  UPDATE config SET value = val WHERE pk = id ;
END ;
$_$ language plpgsql ;
CREATE OR REPLACE FUNCTION pkg.get_val(id numeric)
RETURNS void AS
$_$
DECLARE
  val varchar ;
BEGIN
  SELECT value INTO val FROM config WHERE pk = id ;
  
  RETURN val ;
END ;
$_$ language plpgsql ;
