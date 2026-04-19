-- Exotic E2 test for TYPE_GERMAN_STRING — areas not covered by the extended plan.
-- Run as: (echo "SET cbo_enable_low_cardinality_optimize=false;"; cat tmp/e2/test_plan_exotic.sql) | mysql -h127.0.0.1 -P9030 -uroot
-- Expected: identical output with enable_german_string=true vs false.

USE gs_test;

SELECT '=== regexp_extract / regexp_replace ===' AS tag;
SELECT id, regexp_extract(long_s, '[a-z]+', 0) FROM gs_e2 ORDER BY id;
SELECT id, regexp_replace(long_s, '[aeiou]', 'X') FROM gs_e2 ORDER BY id;
SELECT id, regexp_replace(short_s, '(.)(.)', '$2$1') FROM gs_e2 ORDER BY id;

SELECT '=== concat with all-constant args (hits concat_prepare) ===' AS tag;
SELECT id, concat('prefix:', '---', 'suffix') FROM gs_e2 WHERE id <= 3 ORDER BY id;
SELECT concat('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j');

SELECT '=== string arithmetic (nullif / greatest / least / coalesce) ===' AS tag;
SELECT id, nullif(short_s, 'apple'), greatest(short_s, opt_s), least(short_s, opt_s)
FROM gs_e2 ORDER BY id;

SELECT '=== window functions on string ===' AS tag;
SELECT id, short_s, LAG(short_s) OVER (ORDER BY id) AS prev_s,
       LEAD(short_s, 1, 'END') OVER (ORDER BY id) AS next_s
FROM gs_e2 ORDER BY id;
SELECT short_s, COUNT(*) OVER (PARTITION BY short_s) cnt FROM gs_e2 ORDER BY short_s, id;

SELECT '=== multi-col GROUP BY + having ===' AS tag;
SELECT length(short_s), MAX(long_s), COUNT(*)
FROM gs_e2 GROUP BY length(short_s) HAVING COUNT(*) >= 1 ORDER BY 1;
SELECT opt_s, short_s, COUNT(*) FROM gs_e2 GROUP BY opt_s, short_s ORDER BY opt_s, short_s;

SELECT '=== string_agg-ish: group_concat ===' AS tag;
SELECT length(short_s), group_concat(short_s ORDER BY id) FROM gs_e2 GROUP BY length(short_s) ORDER BY 1;

SELECT '=== MAX / MIN / string aggregates ===' AS tag;
SELECT MIN(short_s), MAX(short_s), MIN(long_s), MAX(long_s) FROM gs_e2;
SELECT opt_s, MIN(short_s), MAX(short_s) FROM gs_e2 GROUP BY opt_s ORDER BY opt_s;

SELECT '=== nested scalar subquery with string ===' AS tag;
SELECT id, short_s, (SELECT COUNT(*) FROM gs_e2 b WHERE b.short_s = a.short_s) cnt
FROM gs_e2 a ORDER BY id;

SELECT '=== correlated EXISTS on string ===' AS tag;
SELECT id, short_s FROM gs_e2 a
WHERE EXISTS (SELECT 1 FROM gs_e2 b WHERE b.opt_s = a.short_s AND b.id <> a.id)
ORDER BY id;

SELECT '=== CAST chain (string <-> numeric <-> string) ===' AS tag;
SELECT id, cast(cast(id AS VARCHAR) AS CHAR(4)), cast(length(short_s) AS VARCHAR) FROM gs_e2 ORDER BY id;

SELECT '=== large concat chain ===' AS tag;
SELECT id, concat(short_s, '/', long_s, '/', opt_s, '/', cast(id AS VARCHAR), '/', short_s)
FROM gs_e2 ORDER BY id;

SELECT '=== IN with many literals (forces hash set) ===' AS tag;
SELECT id, short_s FROM gs_e2
WHERE short_s IN ('apple','banana','cherry','pear','BANANA','thirteenchars!','你好','twelvecharss','nomatch1','nomatch2')
ORDER BY id;

SELECT '=== like patterns (% at middle / escape) ===' AS tag;
SELECT id FROM gs_e2 WHERE long_s LIKE '%brown%' ORDER BY id;
SELECT id FROM gs_e2 WHERE long_s LIKE '%_row_%' ORDER BY id;
SELECT id FROM gs_e2 WHERE short_s LIKE '_a%' ORDER BY id;

SELECT '=== full-column equality with CAST sibling ===' AS tag;
SELECT id FROM gs_e2 WHERE short_s = cast(id AS VARCHAR) ORDER BY id;
