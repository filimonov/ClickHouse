-- Analyzer passes hash expressions without their column sources. Two columns with the same name
-- and type from different subqueries then share a hash, and only the full comparison tells them
-- apart. Every query below has such a pair as keys of the optimization the pass performs.
SET enable_analyzer = 1;
SET optimize_or_like_chain_min_patterns = 1, optimize_or_like_chain_min_substrings = 1;
SET optimize_syntax_fuse_functions = 1;

CREATE TEMPORARY TABLE l (a UInt64, b UInt64);
CREATE TEMPORARY TABLE r (a UInt64, b UInt64);
INSERT INTO l SELECT number, number % 2 FROM numbers(4);
INSERT INTO r SELECT number * 10, number % 2 FROM numbers(4);

SELECT 'grouping sets: grouping() of each key';
SELECT l.a, r.a, grouping(l.a) AS gl, grouping(r.a) AS gr, count()
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b
GROUP BY GROUPING SETS ((l.a), (r.a)) ORDER BY ALL;

SELECT 'aggregate of a non-key column that collides with a key';
SELECT l.a, min(r.a), max(r.a)
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b
GROUP BY l.a ORDER BY l.a;

SELECT 'order by after group by: the second column is a different key';
SELECT l.a, r.a, count()
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b
GROUP BY l.a, r.a ORDER BY l.a, count(), r.a DESC;

SELECT 'function key of two columns that collide';
SELECT l.a + r.a AS s, l.a, count()
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b
GROUP BY l.a + r.a, l.a ORDER BY ALL;

SELECT 'order by function of a column that collides with an earlier key';
SELECT l.a, r.a
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b
ORDER BY l.a, toString(r.a) DESC;

SELECT 'limit by and order by with colliding keys are not duplicates';
SELECT l.a, r.a
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b
ORDER BY l.a, r.a LIMIT 1 BY l.a, r.a;

SELECT 'or chain to in: one IN per column';
SELECT l.a, r.a
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b
WHERE l.a = 1 OR r.a = 10 OR l.a = 2 OR r.a = 20 ORDER BY ALL;

SELECT 'like chain: one multiSearchAny per column';
SELECT l.a, r.a
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b
WHERE toString(l.a) LIKE '%1%' OR toString(r.a) LIKE '%2%' OR toString(l.a) LIKE '%3%' ORDER BY ALL;

SELECT 'fused sum/count: one sumCount per column';
SELECT sum(l.a), count(l.a), sum(r.a), count(r.a), avg(r.a)
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b;

SELECT 'group_by_use_nulls: each key becomes Nullable on its own';
SELECT l.a, r.a, toTypeName(l.a), toTypeName(r.a), count()
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b
GROUP BY GROUPING SETS ((l.a), (r.a)) ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT 'join_use_nulls: the right column becomes Nullable, the left stays';
SELECT l.a, r.a, toTypeName(l.a), toTypeName(r.a)
FROM (SELECT * FROM l) AS l LEFT JOIN (SELECT * FROM r WHERE a < 20) AS r ON l.a * 10 = r.a ORDER BY ALL
SETTINGS join_use_nulls = 1;

SELECT 'interpolate over a colliding projection column';
SELECT l.a, max(r.a) AS ra
FROM (SELECT * FROM l) AS l JOIN (SELECT * FROM r) AS r ON l.b = r.b
WHERE l.a < 2 GROUP BY l.a ORDER BY l.a WITH FILL FROM 0 TO 4 INTERPOLATE (ra AS ra + 100);

SELECT 'correlated column that collides with an inner column';
SELECT l.a FROM (SELECT * FROM l) AS l WHERE EXISTS (SELECT 1 FROM (SELECT * FROM r) AS r WHERE r.a = l.a * 10 AND r.b = 1) ORDER BY ALL
SETTINGS allow_correlated_subqueries = 1;

SELECT 'key with a subquery, referenced through its alias in projection and HAVING';
SELECT a + (SELECT max(number) FROM numbers(3)) AS k, grouping(k) AS g, count()
FROM (SELECT * FROM l) AS l
GROUP BY GROUPING SETS ((k), ()) HAVING k > 2 OR g = 1 ORDER BY ALL;
