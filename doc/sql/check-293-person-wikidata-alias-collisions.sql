-- FASTAPI-TEXT2SQL-293, measure before building: what the Wikidata aliases of persons
-- (T_WC_WIKIDATA_PERSON.ALIASES_JSON, fr + en) would add to T_WC_T2S_PERSON_ALSO_KNOWN_AS,
-- the table the person resolver reads (process 51), and how many of them would be ambiguous.
-- Read-only (SELECT and temporary tables).
--
-- Every comparison is made on the alias-table normalization (PERSON_NAME_NORM of the alias
-- tables, \p{L}\p{M}\p{N}, trimmed here) and under utf8mb4_unicode_ci, which is what the
-- resolver's exact match does (WHERE NORM = %s on that column). Main names of T_WC_T2S_PERSON
-- are re-normalized the same way, their own NORM column using [:alnum:].
--
-- Candidate filters measured, not decided: an alias of one word when the TMDb name has
-- several ("Jennifer" for Jenna Haze), an alias with no capital letter (a description such as
-- "Indonesian singer" is usually written that way on Wikidata... or not: section 5 shows them).

SELECT NOW() AS NOW_;

-- Persons of T2S, main name re-normalized.
DROP TEMPORARY TABLE IF EXISTS tmp_p;
CREATE TEMPORARY TABLE tmp_p (
    ID_PERSON   INT NOT NULL PRIMARY KEY,
    PERSON_NAME VARCHAR(200),
    POPULARITY  DOUBLE,
    ID_WIKIDATA VARCHAR(50),
    NAME_NORM   VARCHAR(255),
    NAME_TOKENS INT,
    KEY IDX_NORM (NAME_NORM),
    KEY IDX_WD (ID_WIKIDATA)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_p (ID_PERSON, PERSON_NAME, POPULARITY, ID_WIKIDATA, NAME_NORM)
SELECT ID_PERSON, PERSON_NAME, POPULARITY, ID_WIKIDATA,
       TRIM(LCASE(REGEXP_REPLACE(REGEXP_REPLACE(PERSON_NAME, '[^\\p{L}\\p{M}\\p{N} ]+', ' '), ' +', ' ')))
FROM T_WC_T2S_PERSON
WHERE COALESCE(DELETED, 0) = 0 AND PERSON_NAME IS NOT NULL AND PERSON_NAME <> '';

UPDATE tmp_p SET NAME_TOKENS = LENGTH(NAME_NORM) - LENGTH(REPLACE(NAME_NORM, ' ', '')) + 1;

-- Current aliases of the resolver (TMDb also_known_as, T2S copy).
DROP TEMPORARY TABLE IF EXISTS tmp_aka;
CREATE TEMPORARY TABLE tmp_aka (
    ID_PERSON INT NOT NULL,
    NORM      VARCHAR(255),
    KEY IDX_NORM (NORM),
    KEY IDX_PERSON_NORM (ID_PERSON, NORM)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_aka (ID_PERSON, NORM)
SELECT ID_PERSON, TRIM(PERSON_NAME_NORM)
FROM T_WC_T2S_PERSON_ALSO_KNOWN_AS
WHERE COALESCE(DELETED, 0) = 0 AND PERSON_NAME_NORM IS NOT NULL AND TRIM(PERSON_NAME_NORM) <> '';

-- Wikidata aliases of the T2S persons, one row per (person, language, alias).
DROP TEMPORARY TABLE IF EXISTS tmp_wd;
CREATE TEMPORARY TABLE tmp_wd (
    ID_PERSON   INT NOT NULL,
    LANG        CHAR(2) NOT NULL,
    ALIAS       VARCHAR(200) NOT NULL,
    NORM        VARCHAR(255),
    TOKENS      INT,
    IS_SELF     TINYINT DEFAULT 0,
    IS_SINGLE   TINYINT DEFAULT 0,
    IS_LOWER    TINYINT DEFAULT 0,
    KEY IDX_PERSON_NORM (ID_PERSON, NORM),
    KEY IDX_NORM (NORM)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_wd (ID_PERSON, LANG, ALIAS)
SELECT p.ID_PERSON, 'fr', LEFT(j.ALIAS, 200)
FROM tmp_p p
INNER JOIN T_WC_WIKIDATA_PERSON wp ON wp.ID_WIKIDATA = p.ID_WIKIDATA AND wp.DELETED = 0
INNER JOIN JSON_TABLE(wp.ALIASES_JSON, '$.fr[*]' COLUMNS (ALIAS VARCHAR(500) PATH '$')) j
WHERE wp.ALIASES_JSON LIKE '%"fr"%' AND j.ALIAS IS NOT NULL AND j.ALIAS <> '';

INSERT INTO tmp_wd (ID_PERSON, LANG, ALIAS)
SELECT p.ID_PERSON, 'en', LEFT(j.ALIAS, 200)
FROM tmp_p p
INNER JOIN T_WC_WIKIDATA_PERSON wp ON wp.ID_WIKIDATA = p.ID_WIKIDATA AND wp.DELETED = 0
INNER JOIN JSON_TABLE(wp.ALIASES_JSON, '$.en[*]' COLUMNS (ALIAS VARCHAR(500) PATH '$')) j
WHERE wp.ALIASES_JSON LIKE '%"en"%' AND j.ALIAS IS NOT NULL AND j.ALIAS <> '';

UPDATE tmp_wd
SET NORM = TRIM(LCASE(REGEXP_REPLACE(REGEXP_REPLACE(ALIAS, '[^\\p{L}\\p{M}\\p{N} ]+', ' '), ' +', ' ')));
DELETE FROM tmp_wd WHERE NORM IS NULL OR NORM = '';
UPDATE tmp_wd SET TOKENS = LENGTH(NORM) - LENGTH(REPLACE(NORM, ' ', '')) + 1;

-- Already held for the same person: equal to its main name, or to one of its aliases.
UPDATE tmp_wd w INNER JOIN tmp_p p ON p.ID_PERSON = w.ID_PERSON
SET w.IS_SELF = 1 WHERE w.NORM = p.NAME_NORM;
UPDATE tmp_wd w
SET w.IS_SELF = 1
WHERE w.IS_SELF = 0
  AND EXISTS (SELECT 1 FROM tmp_aka a WHERE a.ID_PERSON = w.ID_PERSON AND a.NORM = w.NORM);

-- Filter flags.
UPDATE tmp_wd w INNER JOIN tmp_p p ON p.ID_PERSON = w.ID_PERSON
SET w.IS_SINGLE = 1 WHERE w.TOKENS = 1 AND p.NAME_TOKENS > 1;
UPDATE tmp_wd
SET IS_LOWER = 1
WHERE CAST(ALIAS AS BINARY) = CAST(LOWER(ALIAS) AS BINARY)
  AND CAST(ALIAS AS BINARY) <> CAST(UPPER(ALIAS) AS BINARY);

-- The set that would be ingested: new for the person, both filters applied, one row per
-- (person, NORM) under the collation.
DROP TEMPORARY TABLE IF EXISTS tmp_new;
CREATE TEMPORARY TABLE tmp_new (
    ID_PERSON INT NOT NULL,
    NORM      VARCHAR(255) NOT NULL,
    ALIAS     VARCHAR(200),
    UNIQUE KEY UK_PERSON_NORM (ID_PERSON, NORM),
    KEY IDX_NORM (NORM)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO tmp_new (ID_PERSON, NORM, ALIAS)
SELECT ID_PERSON, NORM, ALIAS FROM tmp_wd
WHERE IS_SELF = 0 AND IS_SINGLE = 0 AND IS_LOWER = 0
ORDER BY ID_PERSON, LANG DESC, ALIAS;

-- Copies: a temporary table cannot be opened twice in one query.
DROP TEMPORARY TABLE IF EXISTS tmp_new2;
CREATE TEMPORARY TABLE tmp_new2 LIKE tmp_new;
INSERT INTO tmp_new2 SELECT * FROM tmp_new;
DROP TEMPORARY TABLE IF EXISTS tmp_p2;
CREATE TEMPORARY TABLE tmp_p2 LIKE tmp_p;
INSERT INTO tmp_p2 SELECT * FROM tmp_p;

-- 1. Funnel.
SELECT COUNT(*) AS T2S_PERSONS, SUM(ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '') AS WITH_QID FROM tmp_p;
SELECT COUNT(*) AS WD_ALIAS_ROWS,
       COUNT(DISTINCT ID_PERSON, NORM) AS DISTINCT_PERSON_NORM,
       COUNT(DISTINCT CASE WHEN IS_SELF = 1 THEN CONCAT(ID_PERSON, '|', NORM) END) AS ALREADY_HELD,
       COUNT(DISTINCT CASE WHEN IS_SELF = 0 AND IS_SINGLE = 1 THEN CONCAT(ID_PERSON, '|', NORM) END) AS DROPPED_SINGLE_WORD,
       COUNT(DISTINCT CASE WHEN IS_SELF = 0 AND IS_SINGLE = 0 AND IS_LOWER = 1 THEN CONCAT(ID_PERSON, '|', NORM) END) AS DROPPED_LOWERCASE
FROM tmp_wd;
SELECT COUNT(*) AS KEPT_NEW, COUNT(DISTINCT ID_PERSON) AS KEPT_PERSONS FROM tmp_new;
SELECT COUNT(*) AS CURRENT_ALIAS_ROWS FROM tmp_aka;

-- 2. Baseline: ambiguity already present in the current alias table (one NORM, several persons).
SELECT COUNT(*) AS AMBIGUOUS_NORMS, SUM(N) AS ROWS_INVOLVED, MAX(N) AS MAX_PERSONS
FROM (SELECT NORM, COUNT(DISTINCT ID_PERSON) AS N FROM tmp_aka GROUP BY NORM HAVING N > 1) x;

-- 3. Ambiguity of the kept Wikidata aliases, each (person, alias) counted once per kind.
--   A: the same NORM is a kept Wikidata alias of another person too;
--   B: it is a current alias of another person;
--   C: it is the main name of another person (strategy 1 normally answers first for a Latin
--      name, so the danger is when that strategy misses or the name is not Latin).
SELECT 'A shared with another kept WD alias' AS KIND_, COUNT(*) AS PERSON_ALIASES, COUNT(DISTINCT n.NORM) AS NORMS
FROM tmp_new n
WHERE EXISTS (SELECT 1 FROM tmp_new2 o WHERE o.NORM = n.NORM AND o.ID_PERSON <> n.ID_PERSON)
UNION ALL
SELECT 'B equals a current alias of another person', COUNT(*), COUNT(DISTINCT n.NORM)
FROM tmp_new n
WHERE EXISTS (SELECT 1 FROM tmp_aka a WHERE a.NORM = n.NORM AND a.ID_PERSON <> n.ID_PERSON)
UNION ALL
SELECT 'C equals the main name of another person', COUNT(*), COUNT(DISTINCT n.NORM)
FROM tmp_new n
WHERE EXISTS (SELECT 1 FROM tmp_p p WHERE p.NAME_NORM = n.NORM AND p.ID_PERSON <> n.ID_PERSON)
UNION ALL
SELECT 'any of A, B, C', COUNT(*), COUNT(DISTINCT n.NORM)
FROM tmp_new n
WHERE EXISTS (SELECT 1 FROM tmp_new2 o WHERE o.NORM = n.NORM AND o.ID_PERSON <> n.ID_PERSON)
   OR EXISTS (SELECT 1 FROM tmp_aka a WHERE a.NORM = n.NORM AND a.ID_PERSON <> n.ID_PERSON)
   OR EXISTS (SELECT 1 FROM tmp_p p WHERE p.NAME_NORM = n.NORM AND p.ID_PERSON <> n.ID_PERSON)
UNION ALL
SELECT 'none: unambiguous', COUNT(*), COUNT(DISTINCT n.NORM)
FROM tmp_new n
WHERE NOT EXISTS (SELECT 1 FROM tmp_new2 o WHERE o.NORM = n.NORM AND o.ID_PERSON <> n.ID_PERSON)
  AND NOT EXISTS (SELECT 1 FROM tmp_aka a WHERE a.NORM = n.NORM AND a.ID_PERSON <> n.ID_PERSON)
  AND NOT EXISTS (SELECT 1 FROM tmp_p p WHERE p.NAME_NORM = n.NORM AND p.ID_PERSON <> n.ID_PERSON);

-- 3b. Kind C split by popularity: where the alias owner is MORE popular than the person whose
-- main name it is, a popularity tie-break would hand the name to the alias owner.
SELECT CASE WHEN COALESCE(o.POPULARITY, 0) > COALESCE(p.POPULARITY, 0) THEN 'alias owner more popular'
            ELSE 'name owner more popular or equal' END AS SIDE_,
       COUNT(*) AS PAIRS
FROM tmp_new n
INNER JOIN tmp_p o ON o.ID_PERSON = n.ID_PERSON
INNER JOIN tmp_p2 p ON p.NAME_NORM = n.NORM AND p.ID_PERSON <> n.ID_PERSON
GROUP BY SIDE_;

-- 4. Thirty kind-C collisions, the most popular name owners first.
SELECT n.ALIAS, o.ID_PERSON AS ALIAS_OWNER_ID, o.PERSON_NAME AS ALIAS_OWNER, ROUND(o.POPULARITY, 1) AS OWNER_POP,
       p.ID_PERSON AS NAME_OWNER_ID, p.PERSON_NAME AS NAME_OWNER, ROUND(p.POPULARITY, 1) AS NAME_POP
FROM tmp_new n
INNER JOIN tmp_p o ON o.ID_PERSON = n.ID_PERSON
INNER JOIN tmp_p2 p ON p.NAME_NORM = n.NORM AND p.ID_PERSON <> n.ID_PERSON
ORDER BY p.POPULARITY DESC
LIMIT 30;

-- 5. What the two filters drop, twenty of each, most popular persons first.
SELECT 'single word' AS FILTER_, p.PERSON_NAME, ROUND(p.POPULARITY, 1) AS POP, w.LANG, w.ALIAS
FROM tmp_wd w INNER JOIN tmp_p p ON p.ID_PERSON = w.ID_PERSON
WHERE w.IS_SELF = 0 AND w.IS_SINGLE = 1
ORDER BY p.POPULARITY DESC LIMIT 20;

SELECT 'lowercase' AS FILTER_, p.PERSON_NAME, ROUND(p.POPULARITY, 1) AS POP, w.LANG, w.ALIAS
FROM tmp_wd w INNER JOIN tmp_p p ON p.ID_PERSON = w.ID_PERSON
WHERE w.IS_SELF = 0 AND w.IS_SINGLE = 0 AND w.IS_LOWER = 1
ORDER BY p.POPULARITY DESC LIMIT 20;

-- 6. Thirty kept, unambiguous aliases, most popular persons first, to judge the gain.
SELECT p.PERSON_NAME, ROUND(p.POPULARITY, 1) AS POP, n.ALIAS
FROM tmp_new n INNER JOIN tmp_p p ON p.ID_PERSON = n.ID_PERSON
WHERE NOT EXISTS (SELECT 1 FROM tmp_new2 o WHERE o.NORM = n.NORM AND o.ID_PERSON <> n.ID_PERSON)
  AND NOT EXISTS (SELECT 1 FROM tmp_aka a WHERE a.NORM = n.NORM AND a.ID_PERSON <> n.ID_PERSON)
  AND NOT EXISTS (SELECT 1 FROM tmp_p2 q WHERE q.NAME_NORM = n.NORM AND q.ID_PERSON <> n.ID_PERSON)
ORDER BY p.POPULARITY DESC LIMIT 30;

DROP TEMPORARY TABLE IF EXISTS tmp_new2;
DROP TEMPORARY TABLE IF EXISTS tmp_p2;
DROP TEMPORARY TABLE IF EXISTS tmp_new;
DROP TEMPORARY TABLE IF EXISTS tmp_wd;
DROP TEMPORARY TABLE IF EXISTS tmp_aka;
DROP TEMPORARY TABLE IF EXISTS tmp_p;
