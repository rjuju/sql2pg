SELECT 1 AS nb LIMIT 1 ;
SELECT DISTINCT * FROM tbl AS t ORDER BY a ASC NULLS LAST, b DESC, tbl.c ASC ;
SELECT -1, 1 - 1, aze AS "aze#", a AS "a$z#e" FROM t ;
SELECT (1), COALESCE(val, 'null') || ' ' || COALESCE(val2, 'empty') AS "vAl", 1, abc, "DEF" FROM toto AS "TATA;" ;
SELECT 1 + 2 * ((t.v) - 2) % 4 AS meh, 'test me', t.* FROM tbl AS t WHERE (((a > 2)) OR (b < 3)) GROUP BY a, t.b LIMIT 9 ;
SELECT * FROM ((SELECT 1)) AS subquery1 UNION (SELECT 2) EXCEPT (SELECT 3) INTERSECT (SELECT 4) UNION ALL (SELECT 5) ORDER BY 1 ASC ;
SELECT * FROM a, ONLY (c) INNER JOIN b USING (id, id2) LEFT JOIN d USING (id) LIMIT 20 OFFSET 10 ;
SELECT * FROM a, c RIGHT JOIN b ON a.id = b.id AND a.id2 = b.id2 NATURAL JOIN d CROSS JOIN e AS cj NATURAL LEFT JOIN f NATURAL FULL OUTER JOIN g ;
SELECT round(sum(count(*)), 2), 1 FROM a LEFT JOIN b AS t1 ON a.id = t1.id ;
SELECT t2.* FROM t1 LEFT JOIN t2 ON t1.id <= t2.id ;
SELECT id, log(2, id), log(10, id), count(*) FROM a GROUP BY id HAVING count(*) < 10 ;
SELECT val, rank() OVER (PARTITION BY id) AS rank, lead(val) OVER (ORDER BY val ASC ROWS CURRENT ROW), lag(val) OVER (PARTITION BY id, val ORDER BY val ASC RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS lag FROM t ;
WITH s1 AS (WITH s3 AS (SELECT 1) SELECT * FROM s3), s AS (SELECT * FROM s1 LIMIT 1) SELECT * FROM s, (WITH t AS (SELECT 3 FROM t) SELECT * FROM t) AS subquery2 CROSS JOIN (WITH u AS (SELECT count(*) AS nb) SELECT nb FROM u UNION ALL (SELECT 0)) AS subquery3 LIMIT 1 ;
WITH RECURSIVE s AS (SELECT 1), recur AS (SELECT employee_id, last_name, manager_id FROM employees WHERE employee_id = 1 UNION ALL (SELECT employee_id, last_name, manager_id FROM employees WHERE isvalid = 1 AND recur.employee_id = manager_id)) SELECT * FROM recur WHERE salary > 0 ;
-- 3 FIXME for this statement
-- 3 comments for this statement must be replaced:
-- this is the FROM clause
--should not happen
/* hard coded value */
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
SELECT a.id FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id LEFT JOIN d ON c.id = d.id ;
SELECT round(t.val / 100, 2) FROM t ;
SELECT id, CASE id WHEN 0 THEN 'blah' ELSE id % 3 > 1 END AS val FROM t ;
SELECT id, CASE WHEN val = 0 THEN 'nothing' WHEN val < 100 THEN 'little' WHEN val >= 100 THEN 'lot' END FROM t ;
SELECT trim(leading ' ' from v) FROM t WHERE id > (SELECT count(*) FROM t2) ;
-- now unsupported stuff
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
SELECT first_value(val) OVER (PARTITION BY deptno ORDER BY val ASC) FROM t ;
-- 1 FIXME for this statement
-- FIXME: NULLS clause ignored: "IGNORE NULLS"
SELECT lag(val, 1, -1) OVER () FROM t ;
-- 1 FIXME for this statement
-- FIXME: NULLS clause ignored: "RESPECT NULLS"
SELECT lag(val, 1) OVER () FROM t ;
-- 1 FIXME for this statement
-- FIXME: NULLS clause ignored: "RESPECT NULLS"
SELECT val FROM t RIGHT JOIN t2 ON (t2.id = t.id) ;
-- 1 FIXME for this statement
-- FIXME: Partition clause ignored for outer join on table t2: PARTITION BY (dt)
-- I don't belong to any query
