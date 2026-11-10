-- TMDB-MOVIE-PREPROCESS-071, step 1: measure before changing the award link tables.
-- Read-only: every write goes to temporary tables.
--
-- Today T_WC_T2S_MOVIE_AWARD, _SERIE_AWARD and _PERSON_AWARD hold one row per (award, entity):
-- the four Best Actress Oscars of Katharine Hepburn are one row, without year or film. The
-- ticket moves them to one row per P166 statement, with the year (P585), the work (P1686),
-- the ceremony (P805), and gives each category its family (the P31 of the category).
--
-- What this file answers:
--   A. how many P166 statements on T2S entities, and how many rows the pair dedup removes
--   B. qualifier coverage: P585, P1686, P805, and statements carrying several of the same
--   C. the P1686 work resolved to a T2S movie or series
--   D. the P31 parents of the categories: how many, which ones, and what tells a family
--      (Academy Awards) from a generic class (film award)
--   E. the acceptance cases of the ticket, read through this preview
--   F. key stability: STATEMENT_GUID filled, one Wikidata id on several T2S entities
--   G. second pass: the family as an instance of Q107655869, its three routes, several years
--   H. third pass: what the family rule misses (P361 is not loaded on the categories)

SELECT NOW() AS NOW_;

-- ---------------------------------------------------------------------------------------
-- The win set: one row per (P166 statement, T2S entity). Same reading as f_awardlinksql:
-- deprecated rank excluded, no DELETED filter. ID_AWARD is NULL when the category is not a
-- T_WC_T2S_AWARD row (outside the award cone, or not built yet).
-- ---------------------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS tmp_071_win;
CREATE TEMPORARY TABLE tmp_071_win (
  ID_STATEMENT bigint(20) NOT NULL,
  STATEMENT_GUID varchar(255) DEFAULT NULL,
  ID_SUBJECT varchar(50) NOT NULL,
  ID_CATEGORY varchar(50) NOT NULL,
  ID_AWARD int(11) DEFAULT NULL,
  ENTITY_KIND varchar(10) NOT NULL,
  ID_ENTITY int(11) NOT NULL,
  KEY ID_STATEMENT (ID_STATEMENT),
  KEY ID_CATEGORY (ID_CATEGORY),
  KEY ID_SUBJECT (ID_SUBJECT),
  KEY ID_AWARD (ID_AWARD, ID_ENTITY)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_071_win
SELECT s.ID_STATEMENT, s.STATEMENT_GUID, s.ID_WIKIDATA, v.ID_ITEM, a.ID_AWARD, 'person', p.ID_PERSON
FROM T_WC_WIKIDATA_STATEMENT s
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
JOIN T_WC_T2S_PERSON p ON p.ID_WIKIDATA = s.ID_WIKIDATA
LEFT JOIN T_WC_T2S_AWARD a ON a.ID_WIKIDATA = v.ID_ITEM AND a.AWARD_SOURCE = 'P166'
WHERE s.ID_PROPERTY = 'P166'
  AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated')
  AND p.ID_WIKIDATA IS NOT NULL AND p.ID_WIKIDATA <> '';

INSERT INTO tmp_071_win
SELECT s.ID_STATEMENT, s.STATEMENT_GUID, s.ID_WIKIDATA, v.ID_ITEM, a.ID_AWARD, 'movie', m.ID_MOVIE
FROM T_WC_WIKIDATA_STATEMENT s
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
JOIN T_WC_T2S_MOVIE m ON m.ID_WIKIDATA = s.ID_WIKIDATA
LEFT JOIN T_WC_T2S_AWARD a ON a.ID_WIKIDATA = v.ID_ITEM AND a.AWARD_SOURCE = 'P166'
WHERE s.ID_PROPERTY = 'P166'
  AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated')
  AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '';

INSERT INTO tmp_071_win
SELECT s.ID_STATEMENT, s.STATEMENT_GUID, s.ID_WIKIDATA, v.ID_ITEM, a.ID_AWARD, 'serie', t.ID_SERIE
FROM T_WC_WIKIDATA_STATEMENT s
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
JOIN T_WC_T2S_SERIE t ON t.ID_WIKIDATA = s.ID_WIKIDATA
LEFT JOIN T_WC_T2S_AWARD a ON a.ID_WIKIDATA = v.ID_ITEM AND a.AWARD_SOURCE = 'P166'
WHERE s.ID_PROPERTY = 'P166'
  AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated')
  AND t.ID_WIKIDATA IS NOT NULL AND t.ID_WIKIDATA <> '';

-- ---------------------------------------------------------------------------------------
-- A. Statements against pairs: the rows the dedup removes
-- ---------------------------------------------------------------------------------------
SELECT '=== A1. P166 statements on T2S entities, by kind ===' AS section;

SELECT ENTITY_KIND,
       COUNT(*) AS WIN_ROWS,
       SUM(ID_AWARD IS NOT NULL) AS ROWS_ON_A_T2S_AWARD,
       SUM(ID_AWARD IS NULL) AS ROWS_OUTSIDE_T2S_AWARD,
       COUNT(DISTINCT CASE WHEN ID_AWARD IS NOT NULL THEN CONCAT(ID_AWARD, '-', ID_ENTITY) END) AS DISTINCT_PAIRS,
       SUM(ID_AWARD IS NOT NULL)
         - COUNT(DISTINCT CASE WHEN ID_AWARD IS NOT NULL THEN CONCAT(ID_AWARD, '-', ID_ENTITY) END) AS ROWS_THE_DEDUP_REMOVES
FROM tmp_071_win
GROUP BY ENTITY_KIND
ORDER BY ENTITY_KIND;

SELECT '=== A2. the link tables today ===' AS section;

SELECT 'T_WC_T2S_PERSON_AWARD' AS TABLE_NAME, COUNT(*) AS TABLE_ROWS, COUNT(DISTINCT ID_AWARD, ID_PERSON) AS DISTINCT_PAIRS FROM T_WC_T2S_PERSON_AWARD
UNION ALL
SELECT 'T_WC_T2S_MOVIE_AWARD', COUNT(*), COUNT(DISTINCT ID_AWARD, ID_MOVIE) FROM T_WC_T2S_MOVIE_AWARD
UNION ALL
SELECT 'T_WC_T2S_SERIE_AWARD', COUNT(*), COUNT(DISTINCT ID_AWARD, ID_SERIE) FROM T_WC_T2S_SERIE_AWARD;

SELECT '=== A3. T_WC_T2S_AWARD today, by source and type ===' AS section;

SELECT AWARD_SOURCE, AWARD_TYPE, COUNT(*) AS AWARDS FROM T_WC_T2S_AWARD GROUP BY AWARD_SOURCE, AWARD_TYPE;

SELECT '=== A4. pairs with several statements: how many wins a pair hides ===' AS section;

SELECT ENTITY_KIND, WINS_IN_THE_PAIR, COUNT(*) AS PAIRS
FROM (SELECT ENTITY_KIND, ID_AWARD, ID_ENTITY, COUNT(*) AS WINS_IN_THE_PAIR
      FROM tmp_071_win WHERE ID_AWARD IS NOT NULL
      GROUP BY ENTITY_KIND, ID_AWARD, ID_ENTITY) x
GROUP BY ENTITY_KIND, WINS_IN_THE_PAIR
ORDER BY ENTITY_KIND, WINS_IN_THE_PAIR;

-- ---------------------------------------------------------------------------------------
-- B. Qualifiers
-- ---------------------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS tmp_071_stmt;
CREATE TEMPORARY TABLE tmp_071_stmt (PRIMARY KEY (ID_STATEMENT))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
AS SELECT DISTINCT ID_STATEMENT FROM tmp_071_win WHERE ID_AWARD IS NOT NULL;

DROP TEMPORARY TABLE IF EXISTS tmp_071_qual;
CREATE TEMPORARY TABLE tmp_071_qual (PRIMARY KEY (ID_STATEMENT))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
AS
SELECT w.ID_STATEMENT,
       SUM(q.ID_QUALIFIER_PROPERTY = 'P585')  AS N_P585,
       SUM(q.ID_QUALIFIER_PROPERTY = 'P1686') AS N_P1686,
       SUM(q.ID_QUALIFIER_PROPERTY = 'P805')  AS N_P805,
       MIN(CASE WHEN q.ID_QUALIFIER_PROPERTY = 'P585' THEN qt.YEAR_VALUE END) AS YEAR_MIN,
       MAX(CASE WHEN q.ID_QUALIFIER_PROPERTY = 'P585' THEN qt.YEAR_VALUE END) AS YEAR_MAX,
       MIN(CASE WHEN q.ID_QUALIFIER_PROPERTY = 'P1686' THEN qi.ID_ITEM END) AS WORK_MIN,
       MIN(CASE WHEN q.ID_QUALIFIER_PROPERTY = 'P805' THEN qi.ID_ITEM END) AS CEREMONY_MIN
FROM tmp_071_stmt w
JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q
  ON q.ID_STATEMENT = w.ID_STATEMENT AND q.ID_QUALIFIER_PROPERTY IN ('P585', 'P1686', 'P805')
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
LEFT JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qi ON qi.ID_STATEMENT_QUALIFIER = q.ID_STATEMENT_QUALIFIER
GROUP BY w.ID_STATEMENT;

SELECT '=== B1. qualifier coverage of the win rows, by kind ===' AS section;

SELECT w.ENTITY_KIND,
       COUNT(*) AS WIN_ROWS,
       SUM(COALESCE(q.N_P585, 0) > 0)  AS WITH_P585_YEAR,
       ROUND(100 * SUM(COALESCE(q.N_P585, 0) > 0) / COUNT(*), 1) AS PCT_YEAR,
       SUM(COALESCE(q.N_P1686, 0) > 0) AS WITH_P1686_WORK,
       ROUND(100 * SUM(COALESCE(q.N_P1686, 0) > 0) / COUNT(*), 1) AS PCT_WORK,
       SUM(COALESCE(q.N_P805, 0) > 0)  AS WITH_P805_CEREMONY,
       SUM(COALESCE(q.N_P585, 0) = 0 AND COALESCE(q.N_P805, 0) > 0) AS NO_YEAR_BUT_CEREMONY,
       SUM(COALESCE(q.N_P585, 0) > 1)  AS SEVERAL_P585,
       SUM(COALESCE(q.N_P585, 0) > 1 AND q.YEAR_MIN <> q.YEAR_MAX) AS SEVERAL_DISTINCT_YEARS,
       SUM(COALESCE(q.N_P1686, 0) > 1) AS SEVERAL_P1686,
       SUM(COALESCE(q.N_P805, 0) > 1)  AS SEVERAL_P805
FROM tmp_071_win w
LEFT JOIN tmp_071_qual q ON q.ID_STATEMENT = w.ID_STATEMENT
WHERE w.ID_AWARD IS NOT NULL
GROUP BY w.ENTITY_KIND
ORDER BY w.ENTITY_KIND;

SELECT '=== B2. win rows with no year: can the ceremony give it? ===' AS section;
-- The ceremony carries its own P585 (point in time) only when it is a full V2 entity.

SELECT COUNT(*) AS ROWS_NO_YEAR_BUT_CEREMONY,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT cs
                   JOIN T_WC_WIKIDATA_TIME_VALUE ct ON ct.ID_STATEMENT = cs.ID_STATEMENT
                   WHERE cs.ID_WIKIDATA = q.CEREMONY_MIN AND cs.ID_PROPERTY = 'P585')) AS CEREMONY_HAS_P585
FROM tmp_071_win w
JOIN tmp_071_qual q ON q.ID_STATEMENT = w.ID_STATEMENT
WHERE w.ID_AWARD IS NOT NULL AND q.N_P585 = 0 AND q.N_P805 > 0;

SELECT '=== B3. every qualifier property seen on the win statements ===' AS section;

SELECT q.ID_QUALIFIER_PROPERTY, COUNT(DISTINCT q.ID_STATEMENT) AS STATEMENTS
FROM tmp_071_stmt w
JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER q ON q.ID_STATEMENT = w.ID_STATEMENT
GROUP BY q.ID_QUALIFIER_PROPERTY
ORDER BY STATEMENTS DESC
LIMIT 20;

SELECT '=== B4. years out of range (before 1880 or in the future) ===' AS section;

SELECT SUM(YEAR_MIN < 1880) AS BEFORE_1880, SUM(YEAR_MIN > YEAR(CURDATE())) AS IN_THE_FUTURE,
       MIN(YEAR_MIN) AS YEAR_LOWEST, MAX(YEAR_MIN) AS YEAR_HIGHEST
FROM tmp_071_qual WHERE N_P585 > 0;

-- ---------------------------------------------------------------------------------------
-- C. The P1686 work resolved to T2S
-- ---------------------------------------------------------------------------------------
SELECT '=== C1. the work of a win (first P1686), resolved to T2S, by kind of winner ===' AS section;

SELECT w.ENTITY_KIND,
       COUNT(*) AS ROWS_WITH_A_WORK,
       SUM(m.ID_MOVIE IS NOT NULL) AS WORK_IS_A_T2S_MOVIE,
       SUM(t.ID_SERIE IS NOT NULL) AS WORK_IS_A_T2S_SERIE,
       SUM(m.ID_MOVIE IS NOT NULL AND t.ID_SERIE IS NOT NULL) AS WORK_IS_BOTH,
       SUM(m.ID_MOVIE IS NULL AND t.ID_SERIE IS NULL) AS WORK_OUTSIDE_T2S
FROM tmp_071_win w
JOIN tmp_071_qual q ON q.ID_STATEMENT = w.ID_STATEMENT AND q.N_P1686 > 0
LEFT JOIN (SELECT ID_WIKIDATA, MIN(ID_MOVIE) AS ID_MOVIE FROM T_WC_T2S_MOVIE
           WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' GROUP BY ID_WIKIDATA) m ON m.ID_WIKIDATA = q.WORK_MIN
LEFT JOIN (SELECT ID_WIKIDATA, MIN(ID_SERIE) AS ID_SERIE FROM T_WC_T2S_SERIE
           WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' GROUP BY ID_WIKIDATA) t ON t.ID_WIKIDATA = q.WORK_MIN
WHERE w.ID_AWARD IS NOT NULL
GROUP BY w.ENTITY_KIND
ORDER BY w.ENTITY_KIND;

-- ---------------------------------------------------------------------------------------
-- D. Families: the P31 parents of the categories
-- ---------------------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS tmp_071_category;
CREATE TEMPORARY TABLE tmp_071_category (PRIMARY KEY (ID_CATEGORY))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
AS SELECT ID_CATEGORY, COUNT(*) AS WIN_ROWS FROM tmp_071_win WHERE ID_AWARD IS NOT NULL GROUP BY ID_CATEGORY;

DROP TEMPORARY TABLE IF EXISTS tmp_071_parent;
CREATE TEMPORARY TABLE tmp_071_parent (KEY ID_CATEGORY (ID_CATEGORY), KEY ID_PARENT (ID_PARENT))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
AS
SELECT DISTINCT c.ID_CATEGORY, pv.ID_ITEM AS ID_PARENT, c.WIN_ROWS
FROM tmp_071_category c
JOIN T_WC_WIKIDATA_STATEMENT ps ON ps.ID_WIKIDATA = c.ID_CATEGORY AND ps.ID_PROPERTY = 'P31'
JOIN T_WC_WIKIDATA_ITEM_VALUE pv ON pv.ID_STATEMENT = ps.ID_STATEMENT
WHERE (ps.`RANK` IS NULL OR ps.`RANK` <> 'deprecated');

SELECT '=== D1. categories by number of P31 parents ===' AS section;

SELECT COALESCE(x.PARENTS, 0) AS P31_PARENTS, COUNT(*) AS CATEGORIES, SUM(c.WIN_ROWS) AS WIN_ROWS
FROM tmp_071_category c
LEFT JOIN (SELECT ID_CATEGORY, COUNT(*) AS PARENTS FROM tmp_071_parent GROUP BY ID_CATEGORY) x ON x.ID_CATEGORY = c.ID_CATEGORY
GROUP BY COALESCE(x.PARENTS, 0)
ORDER BY P31_PARENTS;

SELECT '=== D2. the parents: how many, in the award cone, themselves a T2S award ===' AS section;

SELECT COUNT(*) AS DISTINCT_PARENTS,
       SUM(x.IN_CONE) AS PARENTS_IN_THE_CONE,
       SUM(x.IS_T2S_AWARD) AS PARENTS_ALREADY_A_T2S_AWARD,
       SUM(x.HAS_P31) AS PARENTS_WITH_THEIR_OWN_P31,
       SUM(x.HAS_P279) AS PARENTS_WITH_A_P279,
       SUM(x.CATEGORIES = 1) AS PARENTS_OF_ONE_CATEGORY,
       SUM(x.CATEGORIES >= 2) AS PARENTS_OF_SEVERAL
FROM (SELECT p.ID_PARENT, COUNT(*) AS CATEGORIES,
             EXISTS (SELECT 1 FROM T_WC_T2S_AWARD_CLASS ac WHERE ac.ID_CLASS = p.ID_PARENT) AS IN_CONE,
             EXISTS (SELECT 1 FROM T_WC_T2S_AWARD a WHERE a.ID_WIKIDATA = p.ID_PARENT AND a.AWARD_SOURCE = 'P166') AS IS_T2S_AWARD,
             EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s1 WHERE s1.ID_WIKIDATA = p.ID_PARENT AND s1.ID_PROPERTY = 'P31') AS HAS_P31,
             EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SUBCLASS sc WHERE sc.ID_CHILD = p.ID_PARENT AND sc.DELETED = 0) AS HAS_P279
      FROM tmp_071_parent p GROUP BY p.ID_PARENT) x;

SELECT '=== D3. the 70 parents with the most categories, with what tells a family from a class ===' AS section;
-- OWN_P31: what the parent is an instance of. OWN_P279: what it is a subclass of.
-- Expected: Academy Awards (Q19020) is an instance of something; "film award" is only a class.

SELECT p.ID_PARENT,
       COALESCE(i.LABEL_EN, '') AS LABEL_EN,
       COUNT(*) AS CATEGORIES,
       SUM(p.WIN_ROWS) AS WIN_ROWS,
       EXISTS (SELECT 1 FROM T_WC_T2S_AWARD_CLASS ac WHERE ac.ID_CLASS = p.ID_PARENT) AS IN_CONE,
       EXISTS (SELECT 1 FROM T_WC_T2S_AWARD a WHERE a.ID_WIKIDATA = p.ID_PARENT AND a.AWARD_SOURCE = 'P166') AS IS_T2S_AWARD,
       (SELECT GROUP_CONCAT(DISTINCT v1.ID_ITEM ORDER BY v1.ID_ITEM SEPARATOR ' ')
        FROM T_WC_WIKIDATA_STATEMENT s1 JOIN T_WC_WIKIDATA_ITEM_VALUE v1 ON v1.ID_STATEMENT = s1.ID_STATEMENT
        WHERE s1.ID_WIKIDATA = p.ID_PARENT AND s1.ID_PROPERTY = 'P31') AS OWN_P31,
       (SELECT GROUP_CONCAT(DISTINCT sc.ID_PARENT ORDER BY sc.ID_PARENT SEPARATOR ' ')
        FROM T_WC_WIKIDATA_SUBCLASS sc WHERE sc.ID_CHILD = p.ID_PARENT AND sc.DELETED = 0) AS OWN_P279
FROM tmp_071_parent p
LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = p.ID_PARENT
GROUP BY p.ID_PARENT, i.LABEL_EN
ORDER BY CATEGORIES DESC, p.ID_PARENT
LIMIT 70;

SELECT '=== D4. labels of the classes the top parents are instances of ===' AS section;

SELECT v1.ID_ITEM AS ID_CLASS, COALESCE(i.LABEL_EN, '') AS LABEL_EN,
       COUNT(DISTINCT x.ID_PARENT) AS PARENTS_INSTANCE_OF_IT,
       EXISTS (SELECT 1 FROM T_WC_T2S_AWARD_CLASS ac WHERE ac.ID_CLASS = v1.ID_ITEM) AS IN_CONE
FROM (SELECT DISTINCT ID_PARENT FROM tmp_071_parent) x
JOIN T_WC_WIKIDATA_STATEMENT s1 ON s1.ID_WIKIDATA = x.ID_PARENT AND s1.ID_PROPERTY = 'P31'
JOIN T_WC_WIKIDATA_ITEM_VALUE v1 ON v1.ID_STATEMENT = s1.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = v1.ID_ITEM
GROUP BY v1.ID_ITEM, i.LABEL_EN
ORDER BY PARENTS_INSTANCE_OF_IT DESC
LIMIT 30;

SELECT '=== D5. the reference families: Academy Awards, Cesar, Golden Globe, BAFTA, Emmy, Palme ===' AS section;

SELECT p.ID_PARENT, COALESCE(i.LABEL_EN, '') AS LABEL_EN, COUNT(*) AS CATEGORIES, SUM(p.WIN_ROWS) AS WIN_ROWS
FROM tmp_071_parent p
LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = p.ID_PARENT
WHERE p.ID_PARENT IN ('Q19020', 'Q174389', 'Q1011547', 'Q732997', 'Q123737', 'Q179191', 'Q28444913')
GROUP BY p.ID_PARENT, i.LABEL_EN
ORDER BY CATEGORIES DESC;

SELECT '=== D6. the 25 categories with the most wins and no P31 parent in the cone ===' AS section;

SELECT c.ID_CATEGORY, COALESCE(a.AWARD_NAME, '') AS AWARD_NAME, c.WIN_ROWS,
       (SELECT GROUP_CONCAT(p.ID_PARENT ORDER BY p.ID_PARENT SEPARATOR ' ') FROM tmp_071_parent p WHERE p.ID_CATEGORY = c.ID_CATEGORY) AS P31_PARENTS
FROM tmp_071_category c
LEFT JOIN T_WC_T2S_AWARD a ON a.ID_WIKIDATA = c.ID_CATEGORY AND a.AWARD_SOURCE = 'P166'
WHERE NOT EXISTS (SELECT 1 FROM tmp_071_parent p JOIN T_WC_T2S_AWARD_CLASS ac ON ac.ID_CLASS = p.ID_PARENT
                  WHERE p.ID_CATEGORY = c.ID_CATEGORY)
ORDER BY c.WIN_ROWS DESC
LIMIT 25;

-- ---------------------------------------------------------------------------------------
-- E. The acceptance cases, through this preview
-- ---------------------------------------------------------------------------------------
SELECT '=== E1. Katharine Hepburn (Q56016): her win rows ===' AS section;

SELECT a.AWARD_NAME, q.YEAR_MIN AS AWARD_YEAR, q.WORK_MIN AS ID_WORK_WIKIDATA, m.MOVIE_TITLE AS MOVIE_FOR, q.CEREMONY_MIN AS ID_CEREMONY
FROM tmp_071_win w
JOIN T_WC_T2S_AWARD a ON a.ID_AWARD = w.ID_AWARD
LEFT JOIN tmp_071_qual q ON q.ID_STATEMENT = w.ID_STATEMENT
LEFT JOIN T_WC_T2S_MOVIE m ON m.ID_WIKIDATA = q.WORK_MIN AND q.WORK_MIN <> ''
WHERE w.ID_SUBJECT = 'Q56016' AND w.ENTITY_KIND = 'person'
ORDER BY q.YEAR_MIN, a.AWARD_NAME;

SELECT '=== E2. Oscars through the family Q19020: Hepburn (expected 4), Walt Disney (Q8704) ===' AS section;

SELECT w.ID_SUBJECT, pe.PERSON_NAME, COUNT(*) AS OSCAR_ROWS, COUNT(DISTINCT w.ID_AWARD) AS OSCAR_CATEGORIES
FROM tmp_071_win w
JOIN T_WC_T2S_PERSON pe ON pe.ID_PERSON = w.ID_ENTITY
WHERE w.ENTITY_KIND = 'person' AND w.ID_SUBJECT IN ('Q56016', 'Q8704') AND w.ID_AWARD IS NOT NULL
  AND EXISTS (SELECT 1 FROM tmp_071_parent p WHERE p.ID_CATEGORY = w.ID_CATEGORY AND p.ID_PARENT = 'Q19020')
GROUP BY w.ID_SUBJECT, pe.PERSON_NAME;

SELECT '=== E3. Cesar for best film (Q645595) by year, from T2S movies ===' AS section;

SELECT q.YEAR_MIN AS AWARD_YEAR, m.MOVIE_TITLE, m.ID_MOVIE
FROM tmp_071_win w
JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = w.ID_ENTITY
LEFT JOIN tmp_071_qual q ON q.ID_STATEMENT = w.ID_STATEMENT
WHERE w.ENTITY_KIND = 'movie' AND w.ID_CATEGORY = 'Q645595'
ORDER BY q.YEAR_MIN DESC, m.MOVIE_TITLE
LIMIT 60;

-- ---------------------------------------------------------------------------------------
-- G. Second pass (2026-11-10), after the first reading of A to F
--    The first run showed that a P31 parent is most often a class (award, film award,
--    class of award). What Wikidata calls a family is an instance of Q107655869 "group of
--    awards": Academy Awards, Cesar Award, Grammy Awards, Filmfare Awards all carry it.
--    This pass measures that rule, the two other routes from a category to its family
--    (P361 part of, P279 subclass of), and the statements carrying several years.
-- ---------------------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS tmp_071_group;
CREATE TEMPORARY TABLE tmp_071_group (PRIMARY KEY (ID_GROUP))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
AS
SELECT gs.ID_WIKIDATA AS ID_GROUP,
       MAX(gv.ID_ITEM = 'Q107655869') AS IS_GROUP,
       MAX(gv.ID_ITEM = 'Q107467117') AS IS_TYPE
FROM T_WC_WIKIDATA_ITEM_VALUE gv
JOIN T_WC_WIKIDATA_STATEMENT gs ON gs.ID_STATEMENT = gv.ID_STATEMENT
WHERE gv.ID_ITEM IN ('Q107655869', 'Q107467117') AND gs.ID_PROPERTY = 'P31'
  AND (gs.`RANK` IS NULL OR gs.`RANK` <> 'deprecated')
GROUP BY gs.ID_WIKIDATA;

SELECT '=== G1. instances of group of awards (Q107655869) and of type of award (Q107467117) ===' AS section;

SELECT SUM(IS_GROUP) AS GROUPS_OF_AWARDS, SUM(IS_TYPE) AS TYPES_OF_AWARD, SUM(IS_GROUP AND IS_TYPE) AS BOTH_AT_ONCE
FROM tmp_071_group;

-- One row per (category, group, route). A group that is also a type of award is kept apart.
DROP TEMPORARY TABLE IF EXISTS tmp_071_link;
CREATE TEMPORARY TABLE tmp_071_link (
  ID_CATEGORY varchar(50) NOT NULL,
  ID_GROUP varchar(50) NOT NULL,
  ROUTE varchar(5) NOT NULL,
  IS_TYPE tinyint(4) NOT NULL,
  KEY ID_CATEGORY (ID_CATEGORY),
  KEY ID_GROUP (ID_GROUP)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_071_link
SELECT DISTINCT p.ID_CATEGORY, p.ID_PARENT, 'P31', g.IS_TYPE
FROM tmp_071_parent p
JOIN tmp_071_group g ON g.ID_GROUP = p.ID_PARENT AND g.IS_GROUP = 1;

INSERT INTO tmp_071_link
SELECT DISTINCT c.ID_CATEGORY, pv.ID_ITEM, 'P361', g.IS_TYPE
FROM tmp_071_category c
JOIN T_WC_WIKIDATA_STATEMENT ps ON ps.ID_WIKIDATA = c.ID_CATEGORY AND ps.ID_PROPERTY = 'P361'
JOIN T_WC_WIKIDATA_ITEM_VALUE pv ON pv.ID_STATEMENT = ps.ID_STATEMENT
JOIN tmp_071_group g ON g.ID_GROUP = pv.ID_ITEM AND g.IS_GROUP = 1
WHERE (ps.`RANK` IS NULL OR ps.`RANK` <> 'deprecated');

INSERT INTO tmp_071_link
SELECT DISTINCT c.ID_CATEGORY, sc.ID_PARENT, 'P279', g.IS_TYPE
FROM tmp_071_category c
JOIN T_WC_WIKIDATA_SUBCLASS sc ON sc.ID_CHILD = c.ID_CATEGORY AND sc.DELETED = 0
JOIN tmp_071_group g ON g.ID_GROUP = sc.ID_PARENT AND g.IS_GROUP = 1;

DROP TEMPORARY TABLE IF EXISTS tmp_071_catfam;
CREATE TEMPORARY TABLE tmp_071_catfam (PRIMARY KEY (ID_CATEGORY))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
AS
SELECT ID_CATEGORY,
       COUNT(DISTINCT CASE WHEN ROUTE = 'P31' AND IS_TYPE = 0 THEN ID_GROUP END) AS N_P31,
       COUNT(DISTINCT CASE WHEN ROUTE = 'P31' THEN ID_GROUP END) AS N_P31_WITH_TYPES,
       COUNT(DISTINCT CASE WHEN ROUTE = 'P361' AND IS_TYPE = 0 THEN ID_GROUP END) AS N_P361,
       COUNT(DISTINCT CASE WHEN ROUTE = 'P279' AND IS_TYPE = 0 THEN ID_GROUP END) AS N_P279,
       COUNT(DISTINCT CASE WHEN IS_TYPE = 0 THEN ID_GROUP END) AS N_ANY
FROM tmp_071_link
GROUP BY ID_CATEGORY;

-- Wins per family, every route together, computed once (a temporary table cannot be read
-- twice by the same statement).
DROP TEMPORARY TABLE IF EXISTS tmp_071_groupwins;
CREATE TEMPORARY TABLE tmp_071_groupwins (PRIMARY KEY (ID_GROUP))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
AS
SELECT x.ID_GROUP, COUNT(*) AS CATEGORIES, SUM(c.WIN_ROWS) AS WIN_ROWS
FROM (SELECT DISTINCT ID_GROUP, ID_CATEGORY FROM tmp_071_link) x
JOIN tmp_071_category c ON c.ID_CATEGORY = x.ID_CATEGORY
GROUP BY x.ID_GROUP;

SELECT '=== G2. categories and wins that reach a family, by route (types of award left out) ===' AS section;

SELECT COUNT(*) AS CATEGORIES,
       SUM(c.WIN_ROWS) AS WIN_ROWS,
       SUM(COALESCE(f.N_P31, 0) > 0) AS CAT_BY_P31,
       SUM(CASE WHEN COALESCE(f.N_P31, 0) > 0 THEN c.WIN_ROWS ELSE 0 END) AS WINS_BY_P31,
       SUM(COALESCE(f.N_P31, 0) = 0 AND COALESCE(f.N_P361, 0) > 0) AS CAT_ADDED_BY_P361,
       SUM(CASE WHEN COALESCE(f.N_P31, 0) = 0 AND COALESCE(f.N_P361, 0) > 0 THEN c.WIN_ROWS ELSE 0 END) AS WINS_ADDED_BY_P361,
       SUM(COALESCE(f.N_P31, 0) = 0 AND COALESCE(f.N_P361, 0) = 0 AND COALESCE(f.N_P279, 0) > 0) AS CAT_ADDED_BY_P279,
       SUM(CASE WHEN COALESCE(f.N_P31, 0) = 0 AND COALESCE(f.N_P361, 0) = 0 AND COALESCE(f.N_P279, 0) > 0 THEN c.WIN_ROWS ELSE 0 END) AS WINS_ADDED_BY_P279,
       SUM(COALESCE(f.N_ANY, 0) = 0) AS CAT_WITHOUT_FAMILY,
       SUM(CASE WHEN COALESCE(f.N_ANY, 0) = 0 THEN c.WIN_ROWS ELSE 0 END) AS WINS_WITHOUT_FAMILY,
       SUM(COALESCE(f.N_P31, 0) = 0 AND COALESCE(f.N_P31_WITH_TYPES, 0) > 0) AS CAT_ONLY_A_TYPE_BY_P31
FROM tmp_071_category c
LEFT JOIN tmp_071_catfam f ON f.ID_CATEGORY = c.ID_CATEGORY;

SELECT '=== G3a. how many families a category reaches by P31 alone ===' AS section;

SELECT COALESCE(f.N_P31, 0) AS FAMILIES, COUNT(*) AS CATEGORIES, SUM(c.WIN_ROWS) AS WIN_ROWS
FROM tmp_071_category c LEFT JOIN tmp_071_catfam f ON f.ID_CATEGORY = c.ID_CATEGORY
GROUP BY COALESCE(f.N_P31, 0);

SELECT '=== G3b. how many families a category reaches by P31, P361 or P279 ===' AS section;

SELECT COALESCE(f.N_ANY, 0) AS FAMILIES, COUNT(*) AS CATEGORIES, SUM(c.WIN_ROWS) AS WIN_ROWS
FROM tmp_071_category c LEFT JOIN tmp_071_catfam f ON f.ID_CATEGORY = c.ID_CATEGORY
GROUP BY COALESCE(f.N_ANY, 0);

SELECT '=== G4. the 80 families with the most wins, with the categories each route gives ===' AS section;

SELECT l.ID_GROUP,
       COALESCE(i.LABEL_EN, '') AS LABEL_EN,
       MAX(l.IS_TYPE) AS ALSO_A_TYPE,
       EXISTS (SELECT 1 FROM T_WC_T2S_AWARD a WHERE a.ID_WIKIDATA = l.ID_GROUP AND a.AWARD_SOURCE = 'P166') AS IS_T2S_AWARD,
       gw.CATEGORIES,
       COUNT(DISTINCT CASE WHEN l.ROUTE = 'P31' THEN l.ID_CATEGORY END) AS CAT_P31,
       COUNT(DISTINCT CASE WHEN l.ROUTE = 'P361' THEN l.ID_CATEGORY END) AS CAT_P361,
       COUNT(DISTINCT CASE WHEN l.ROUTE = 'P279' THEN l.ID_CATEGORY END) AS CAT_P279,
       gw.WIN_ROWS
FROM tmp_071_link l
JOIN tmp_071_groupwins gw ON gw.ID_GROUP = l.ID_GROUP
LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = l.ID_GROUP
GROUP BY l.ID_GROUP, i.LABEL_EN, gw.CATEGORIES, gw.WIN_ROWS
ORDER BY gw.WIN_ROWS DESC, l.ID_GROUP
LIMIT 80;

SELECT '=== G5. the reference families, by route, and what they are instances of ===' AS section;

SELECT r.ID_GROUP,
       COALESCE(i.LABEL_EN, '') AS LABEL_EN,
       (SELECT GROUP_CONCAT(DISTINCT v1.ID_ITEM ORDER BY v1.ID_ITEM SEPARATOR ' ')
        FROM T_WC_WIKIDATA_STATEMENT s1 JOIN T_WC_WIKIDATA_ITEM_VALUE v1 ON v1.ID_STATEMENT = s1.ID_STATEMENT
        WHERE s1.ID_WIKIDATA = r.ID_GROUP AND s1.ID_PROPERTY = 'P31') AS OWN_P31,
       COALESCE(x.CAT_P31, 0) AS CAT_P31, COALESCE(x.CAT_P361, 0) AS CAT_P361, COALESCE(x.CAT_P279, 0) AS CAT_P279,
       COALESCE(gw.CATEGORIES, 0) AS CATEGORIES, COALESCE(gw.WIN_ROWS, 0) AS WIN_ROWS
FROM (SELECT 'Q19020' AS ID_GROUP UNION ALL SELECT 'Q174389' UNION ALL SELECT 'Q1011547' UNION ALL SELECT 'Q732997'
      UNION ALL SELECT 'Q123737' UNION ALL SELECT 'Q1044427' UNION ALL SELECT 'Q28444913' UNION ALL SELECT 'Q179191'
      UNION ALL SELECT 'Q41254' UNION ALL SELECT 'Q37049' UNION ALL SELECT 'Q7191') r
LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = r.ID_GROUP
LEFT JOIN (SELECT ID_GROUP,
                  COUNT(DISTINCT CASE WHEN ROUTE = 'P31' THEN ID_CATEGORY END) AS CAT_P31,
                  COUNT(DISTINCT CASE WHEN ROUTE = 'P361' THEN ID_CATEGORY END) AS CAT_P361,
                  COUNT(DISTINCT CASE WHEN ROUTE = 'P279' THEN ID_CATEGORY END) AS CAT_P279
           FROM tmp_071_link GROUP BY ID_GROUP) x ON x.ID_GROUP = r.ID_GROUP
LEFT JOIN tmp_071_groupwins gw ON gw.ID_GROUP = r.ID_GROUP;

SELECT '=== G6. the 25 categories with the most wins that reach several families by P31 ===' AS section;

SELECT c.ID_CATEGORY, COALESCE(a.AWARD_NAME, '') AS AWARD_NAME, c.WIN_ROWS,
       (SELECT GROUP_CONCAT(DISTINCT CONCAT(l.ID_GROUP, ' ', COALESCE(i.LABEL_EN, '')) ORDER BY l.ID_GROUP SEPARATOR ' / ')
        FROM tmp_071_link l LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = l.ID_GROUP
        WHERE l.ID_CATEGORY = c.ID_CATEGORY AND l.ROUTE = 'P31' AND l.IS_TYPE = 0) AS FAMILIES
FROM tmp_071_category c
JOIN tmp_071_catfam f ON f.ID_CATEGORY = c.ID_CATEGORY AND f.N_P31 > 1
LEFT JOIN T_WC_T2S_AWARD a ON a.ID_WIKIDATA = c.ID_CATEGORY AND a.AWARD_SOURCE = 'P166'
ORDER BY c.WIN_ROWS DESC
LIMIT 25;

SELECT '=== G7. the 40 categories with the most wins by T2S movies or series and no family by any route ===' AS section;

SELECT x.ID_CATEGORY, COALESCE(a.AWARD_NAME, '') AS AWARD_NAME, x.SCREEN_WINS,
       (SELECT GROUP_CONCAT(p.ID_PARENT ORDER BY p.ID_PARENT SEPARATOR ' ') FROM tmp_071_parent p WHERE p.ID_CATEGORY = x.ID_CATEGORY) AS P31_PARENTS,
       (SELECT GROUP_CONCAT(DISTINCT pv.ID_ITEM ORDER BY pv.ID_ITEM SEPARATOR ' ')
        FROM T_WC_WIKIDATA_STATEMENT ps JOIN T_WC_WIKIDATA_ITEM_VALUE pv ON pv.ID_STATEMENT = ps.ID_STATEMENT
        WHERE ps.ID_WIKIDATA = x.ID_CATEGORY AND ps.ID_PROPERTY = 'P361') AS P361_PARENTS
FROM (SELECT w.ID_CATEGORY, COUNT(*) AS SCREEN_WINS
      FROM tmp_071_win w
      WHERE w.ID_AWARD IS NOT NULL AND w.ENTITY_KIND IN ('movie', 'serie')
      GROUP BY w.ID_CATEGORY) x
LEFT JOIN tmp_071_catfam f ON f.ID_CATEGORY = x.ID_CATEGORY
LEFT JOIN T_WC_T2S_AWARD a ON a.ID_WIKIDATA = x.ID_CATEGORY AND a.AWARD_SOURCE = 'P166'
WHERE COALESCE(f.N_ANY, 0) = 0
ORDER BY x.SCREEN_WINS DESC
LIMIT 40;

-- Statements carrying several distinct years: one statement that records several wins.
DROP TEMPORARY TABLE IF EXISTS tmp_071_years;
CREATE TEMPORARY TABLE tmp_071_years (PRIMARY KEY (ID_STATEMENT))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
AS
SELECT s.ID_STATEMENT,
       COUNT(DISTINCT qt.YEAR_VALUE) AS N_YEARS,
       GROUP_CONCAT(DISTINCT qt.YEAR_VALUE ORDER BY qt.YEAR_VALUE SEPARATOR ' ') AS YEARS
FROM tmp_071_stmt s
JOIN T_WC_WIKIDATA_STATEMENT_QUALIFIER sq ON sq.ID_STATEMENT = s.ID_STATEMENT AND sq.ID_QUALIFIER_PROPERTY = 'P585'
JOIN T_WC_WIKIDATA_QUALIFIER_TIME_VALUE qt ON qt.ID_STATEMENT_QUALIFIER = sq.ID_STATEMENT_QUALIFIER
GROUP BY s.ID_STATEMENT
HAVING COUNT(DISTINCT qt.YEAR_VALUE) > 1;

SELECT '=== G8. statements with several distinct years: rows one row per (statement, year) would add ===' AS section;

SELECT w.ENTITY_KIND,
       COUNT(*) AS STATEMENTS_WITH_SEVERAL_YEARS,
       SUM(y.N_YEARS) - COUNT(*) AS ROWS_ADDED_IF_SPLIT_BY_YEAR,
       MAX(y.N_YEARS) AS MOST_YEARS_ON_ONE,
       SUM(COALESCE(q.N_P1686, 0) = 0) AS WITHOUT_WORK,
       SUM(COALESCE(q.N_P1686, 0) = 1) AS WITH_ONE_WORK,
       SUM(COALESCE(q.N_P1686, 0) > 1) AS WITH_SEVERAL_WORKS,
       SUM(COALESCE(q.N_P1686, 0) > 1 AND q.N_P1686 = y.N_YEARS) AS AS_MANY_WORKS_AS_YEARS
FROM tmp_071_win w
JOIN tmp_071_years y ON y.ID_STATEMENT = w.ID_STATEMENT
LEFT JOIN tmp_071_qual q ON q.ID_STATEMENT = w.ID_STATEMENT
WHERE w.ID_AWARD IS NOT NULL
GROUP BY w.ENTITY_KIND;

SELECT '=== G9. the 25 best known persons with a statement carrying several years ===' AS section;

SELECT pe.PERSON_NAME, a.AWARD_NAME, y.YEARS, COALESCE(q.N_P1686, 0) AS WORKS
FROM tmp_071_win w
JOIN tmp_071_years y ON y.ID_STATEMENT = w.ID_STATEMENT
JOIN T_WC_T2S_PERSON pe ON pe.ID_PERSON = w.ID_ENTITY
JOIN T_WC_T2S_AWARD a ON a.ID_AWARD = w.ID_AWARD
LEFT JOIN tmp_071_qual q ON q.ID_STATEMENT = w.ID_STATEMENT
WHERE w.ENTITY_KIND = 'person'
ORDER BY pe.POPULARITY DESC
LIMIT 25;

SELECT '=== G10. statements with several works (P1686) and at most one year: 15 examples ===' AS section;

SELECT pe.PERSON_NAME, a.AWARD_NAME, q.YEAR_MIN AS AWARD_YEAR, q.N_P1686 AS WORKS
FROM tmp_071_win w
JOIN tmp_071_qual q ON q.ID_STATEMENT = w.ID_STATEMENT AND q.N_P1686 > 1
JOIN T_WC_T2S_PERSON pe ON pe.ID_PERSON = w.ID_ENTITY
JOIN T_WC_T2S_AWARD a ON a.ID_AWARD = w.ID_AWARD
WHERE w.ENTITY_KIND = 'person' AND NOT EXISTS (SELECT 1 FROM tmp_071_years y WHERE y.ID_STATEMENT = w.ID_STATEMENT)
ORDER BY pe.POPULARITY DESC
LIMIT 15;

-- ---------------------------------------------------------------------------------------
-- F. Key stability
-- ---------------------------------------------------------------------------------------
SELECT '=== F1. STATEMENT_GUID on the win statements ===' AS section;

SELECT COUNT(*) AS WIN_ROWS,
       SUM(STATEMENT_GUID IS NULL OR STATEMENT_GUID = '') AS WITHOUT_GUID,
       MAX(CHAR_LENGTH(STATEMENT_GUID)) AS GUID_MAX_LENGTH,
       MIN(ID_STATEMENT) AS ID_STATEMENT_MIN, MAX(ID_STATEMENT) AS ID_STATEMENT_MAX
FROM tmp_071_win;

SELECT '=== F2. one statement on several T2S entities of the same kind (shared Wikidata id) ===' AS section;

SELECT ENTITY_KIND, COUNT(*) AS STATEMENTS_ON_SEVERAL_ENTITIES
FROM (SELECT ENTITY_KIND, ID_STATEMENT FROM tmp_071_win GROUP BY ENTITY_KIND, ID_STATEMENT HAVING COUNT(*) > 1) x
GROUP BY ENTITY_KIND;

SELECT '=== F3. the same reading for nominations (P1411), in one line per kind ===' AS section;
-- Only the size of the same change on process 47, to decide whether it follows.

SELECT 'person' AS ENTITY_KIND, COUNT(*) AS NOMINATION_STATEMENTS, COUNT(DISTINCT v.ID_ITEM, p.ID_PERSON) AS DISTINCT_PAIRS
FROM T_WC_WIKIDATA_STATEMENT s
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
JOIN T_WC_T2S_PERSON p ON p.ID_WIKIDATA = s.ID_WIKIDATA
WHERE s.ID_PROPERTY = 'P1411' AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated') AND p.ID_WIKIDATA <> ''
UNION ALL
SELECT 'movie', COUNT(*), COUNT(DISTINCT v.ID_ITEM, m.ID_MOVIE)
FROM T_WC_WIKIDATA_STATEMENT s
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
JOIN T_WC_T2S_MOVIE m ON m.ID_WIKIDATA = s.ID_WIKIDATA
WHERE s.ID_PROPERTY = 'P1411' AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated') AND m.ID_WIKIDATA <> ''
UNION ALL
SELECT 'serie', COUNT(*), COUNT(DISTINCT v.ID_ITEM, t.ID_SERIE)
FROM T_WC_WIKIDATA_STATEMENT s
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
JOIN T_WC_T2S_SERIE t ON t.ID_WIKIDATA = s.ID_WIKIDATA
WHERE s.ID_PROPERTY = 'P1411' AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated') AND t.ID_WIKIDATA <> '';

-- ---------------------------------------------------------------------------------------
-- H. Third pass (2026-11-10): what the family rule misses, and why
--    G7 lists "Academy Award for Best Picture" (Q102427) and "for Best Director" (Q103360)
--    without a family. On Wikidata both declare the Academy Awards by P361 (part of), not
--    by P31. A category is a cached entity for wikidata-crawler, which loads only a short
--    list of properties for it (P31, P279, not P361). This pass measures the hole.
--    The name filters below are a yardstick for this measure only, never a build rule.
-- ---------------------------------------------------------------------------------------
SELECT '=== H1. server version ===' AS section;

SELECT VERSION() AS MARIADB_VERSION;

SELECT '=== H2. what the base holds on Q19020, Q102427, Q103360, Q103618: statements by property ===' AS section;

SELECT s.ID_WIKIDATA, s.ID_PROPERTY, COUNT(*) AS STATEMENTS,
       GROUP_CONCAT(DISTINCT v.ID_ITEM ORDER BY v.ID_ITEM SEPARATOR ' ') AS ITEM_VALUES
FROM T_WC_WIKIDATA_STATEMENT s
LEFT JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
WHERE s.ID_WIKIDATA IN ('Q19020', 'Q102427', 'Q103360', 'Q103618')
GROUP BY s.ID_WIKIDATA, s.ID_PROPERTY
ORDER BY s.ID_WIKIDATA, s.ID_PROPERTY;

SELECT '=== H3. properties loaded on the award categories (all 18117), by number of categories ===' AS section;

SELECT s.ID_PROPERTY, COUNT(DISTINCT s.ID_WIKIDATA) AS CATEGORIES, COUNT(*) AS STATEMENTS
FROM tmp_071_category c
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA = c.ID_CATEGORY
GROUP BY s.ID_PROPERTY
ORDER BY CATEGORIES DESC
LIMIT 25;

SELECT '=== H4. the yardstick by name: categories and wins with and without a family today ===' AS section;

SELECT x.NAME_STARTS_WITH,
       COUNT(*) AS CATEGORIES,
       SUM(x.WIN_ROWS) AS WIN_ROWS,
       SUM(x.HAS_FAMILY) AS CAT_WITH_FAMILY,
       SUM(CASE WHEN x.HAS_FAMILY = 1 THEN x.WIN_ROWS ELSE 0 END) AS WINS_WITH_FAMILY,
       SUM(x.HAS_FAMILY = 0) AS CAT_WITHOUT_FAMILY,
       SUM(CASE WHEN x.HAS_FAMILY = 0 THEN x.WIN_ROWS ELSE 0 END) AS WINS_WITHOUT_FAMILY
FROM (SELECT CASE WHEN a.AWARD_NAME LIKE 'Academy Award%' THEN 'Academy Award'
                  WHEN a.AWARD_NAME LIKE 'César%' THEN 'Cesar'
                  WHEN a.AWARD_NAME LIKE 'Golden Globe%' THEN 'Golden Globe'
                  WHEN a.AWARD_NAME LIKE 'BAFTA%' OR a.AWARD_NAME LIKE 'British Academy Film%' THEN 'BAFTA'
                  WHEN a.AWARD_NAME LIKE 'Primetime Emmy%' THEN 'Primetime Emmy'
                  WHEN a.AWARD_NAME LIKE 'Screen Actors Guild%' THEN 'Screen Actors Guild'
                  WHEN a.AWARD_NAME LIKE 'Goya%' THEN 'Goya'
                  WHEN a.AWARD_NAME LIKE 'David di Donatello%' THEN 'David di Donatello'
                  WHEN a.AWARD_NAME LIKE 'Saturn Award%' THEN 'Saturn Award'
                  WHEN a.AWARD_NAME LIKE 'Tony Award%' THEN 'Tony Award'
                  WHEN a.AWARD_NAME LIKE 'Grammy%' THEN 'Grammy'
             END AS NAME_STARTS_WITH,
             c.WIN_ROWS,
             COALESCE(f.N_ANY, 0) > 0 AS HAS_FAMILY
      FROM tmp_071_category c
      JOIN T_WC_T2S_AWARD a ON a.ID_WIKIDATA = c.ID_CATEGORY AND a.AWARD_SOURCE = 'P166'
      LEFT JOIN tmp_071_catfam f ON f.ID_CATEGORY = c.ID_CATEGORY) x
WHERE x.NAME_STARTS_WITH IS NOT NULL
GROUP BY x.NAME_STARTS_WITH
ORDER BY WIN_ROWS DESC;

SELECT '=== H5. the awards named Academy Award... that reach no family today ===' AS section;

SELECT c.ID_CATEGORY, a.AWARD_NAME, c.WIN_ROWS,
       (SELECT GROUP_CONCAT(p.ID_PARENT ORDER BY p.ID_PARENT SEPARATOR ' ') FROM tmp_071_parent p WHERE p.ID_CATEGORY = c.ID_CATEGORY) AS P31_PARENTS
FROM tmp_071_category c
JOIN T_WC_T2S_AWARD a ON a.ID_WIKIDATA = c.ID_CATEGORY AND a.AWARD_SOURCE = 'P166'
LEFT JOIN tmp_071_catfam f ON f.ID_CATEGORY = c.ID_CATEGORY
WHERE a.AWARD_NAME LIKE 'Academy Award%' AND COALESCE(f.N_ANY, 0) = 0
ORDER BY c.WIN_ROWS DESC;

SELECT '=== H6. families that have a family themselves (group of awards under a group of awards) ===' AS section;
-- A temporary table cannot be read twice in one statement: the twin stands for the parent side.

DROP TEMPORARY TABLE IF EXISTS tmp_071_grouptwin;
CREATE TEMPORARY TABLE tmp_071_grouptwin (PRIMARY KEY (ID_GROUP))
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
AS SELECT ID_GROUP, IS_GROUP, IS_TYPE FROM tmp_071_group;

DROP TEMPORARY TABLE IF EXISTS tmp_071_gparent;
CREATE TEMPORARY TABLE tmp_071_gparent (
  ID_CHILD varchar(50) NOT NULL,
  ID_PARENT varchar(50) NOT NULL,
  ROUTE varchar(5) NOT NULL,
  KEY ID_CHILD (ID_CHILD)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_071_gparent
SELECT DISTINCT g.ID_GROUP, v.ID_ITEM, 'P31'
FROM tmp_071_group g
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA = g.ID_GROUP AND s.ID_PROPERTY = 'P31'
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
JOIN tmp_071_grouptwin p ON p.ID_GROUP = v.ID_ITEM AND p.IS_GROUP = 1 AND p.IS_TYPE = 0
WHERE g.IS_GROUP = 1 AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated');

INSERT INTO tmp_071_gparent
SELECT DISTINCT g.ID_GROUP, sc.ID_PARENT, 'P279'
FROM tmp_071_group g
JOIN T_WC_WIKIDATA_SUBCLASS sc ON sc.ID_CHILD = g.ID_GROUP AND sc.DELETED = 0
JOIN tmp_071_grouptwin p ON p.ID_GROUP = sc.ID_PARENT AND p.IS_GROUP = 1 AND p.IS_TYPE = 0
WHERE g.IS_GROUP = 1;

SELECT ROUTE, COUNT(*) AS LINKS, COUNT(DISTINCT ID_CHILD) AS FAMILIES_WITH_A_PARENT_FAMILY FROM tmp_071_gparent GROUP BY ROUTE;

SELECT x.ID_CHILD, COALESCE(ci.LABEL_EN, '') AS FAMILY, x.ROUTE, x.ID_PARENT, COALESCE(pi.LABEL_EN, '') AS PARENT_FAMILY,
       COALESCE(gw.WIN_ROWS, 0) AS WIN_ROWS
FROM tmp_071_gparent x
LEFT JOIN T_WC_WIKIDATA_ITEM ci ON ci.ID_WIKIDATA = x.ID_CHILD
LEFT JOIN T_WC_WIKIDATA_ITEM pi ON pi.ID_WIKIDATA = x.ID_PARENT
LEFT JOIN tmp_071_groupwins gw ON gw.ID_GROUP = x.ID_CHILD
ORDER BY WIN_ROWS DESC, x.ID_CHILD, x.ID_PARENT
LIMIT 40;

SELECT '=== H7. years a build would refuse (before 1000 or after next year) ===' AS section;

SELECT SUM(YEAR_MIN < 1000) AS BEFORE_1000, SUM(YEAR_MIN BETWEEN 1000 AND 1879) AS FROM_1000_TO_1879,
       SUM(YEAR_MAX > YEAR(CURDATE()) + 1) AS AFTER_NEXT_YEAR
FROM tmp_071_qual WHERE N_P585 > 0;

SELECT NOW() AS NOW_;
