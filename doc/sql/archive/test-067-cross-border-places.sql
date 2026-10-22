-- TMDB-MOVIE-PREPROCESS-067: measure the rule before coding it.
--
-- Read-only (SELECT and temporary tables), voie 1. Written 2026-10-21, revised the same day
-- after Philippe's three decisions (see "Decisions" below).
-- The live hierarchy (T_WC_T2S_LOCATION_ANCESTOR, process 72 since -048) is the transitive
-- closure of P131 and P17 at best rank. The rule under test changes the edges only:
--   (a) an edge whose statement carries an end time (qualifier P582) is dropped, and so is
--       an edge whose parent has a dissolution date (P576: Tokugawa shogunate, Nazi Germany,
--       Yugoslavia, the Ottoman Empire) or a P31 that marks a past state:
--       Q3024240 historical country, Q11514315 historical period, Q19953632 former
--       administrative territorial entity, Q50068795 historical Chinese state, Q12857432
--       Chinese dynasty, Q182547 Roman province (Judaea), and Q99541706 historical
--       unrecognized state only at preferred rank: the First Hungarian Republic and the
--       Confederate States carry it preferred, the United States and Indonesia at normal rank
--       (check-067-ended-classes and check-067-unmarked-historical, 20261021); taken at any
--       rank, the US sheet fell from 15 556 movies to 420. Q2577883 occupied territory
--       counts only without Q15239622 disputed territory on the same item: the General
--       Government (Warsaw Ghetto) is ended, the West Bank and Gaza stay living parents
--       (decision 1). Fictional countries (Q1145276) stay: Wakanda holds its fictional cities.
--       Why the P31 half: only 2 of 510 974 edges carry a P582 and the store holds 135 P576
--       in all (check-067-historical-parents-20261021.txt), while Q3024240 sits on 622 of the
--       1 944 P17 parents, the Soviet Union included, so without it Bangkok (Rattanakosin
--       Kingdom), Edo (Tokugawa shogunate) and Sobibor (Nazi Germany) came out cross-border;
--   (b) an item that keeps two P17 values or more after (a) is cross-border: none of its
--       edges goes up, P131 included (the Appalachians reach France through
--       Saint-Pierre-and-Miquelon, a P131). It keeps its depth-0 row.
-- Decisions of Philippe, 2026-10-21:
--   1. a disputed place goes under each claimed country: it is not cross-border. Disputed
--      means (D1) P31 Q15239622 disputed territory or Q2577883 occupied territory (Jerusalem,
--      Crimea, the West Bank, the Golan Heights), or (D2) the same set of P17 values as a T2S
--      place of D1 (Yalta, Sevastopol and the other Crimean towns: Russia and Ukraine; their
--      P131 parents are mostly not in the store, check-067-disputed-chain-20261021.txt), or
--      (D3) exactly two P17 values, one of them a de facto regime (Q1179466) or a disputed
--      territory (Laayoune, Savur-Mohyla). Not Q10711424 state with limited recognition:
--      the People's Republic of China carries it, and Everest and the Gobi came out disputed. Wikidata puts no qualifier on these P17 statements
--      (check-067-disputed-signals-20261021.txt), hence the classes;
--   2. Normandy (Q15878) stays under France: its P17 edges to Guernsey and Jersey are
--      dropped by name, the one editorial exception;
--   3. only the P17 parents the database knows (a T_WC_WIKIDATA_ITEM row) count for (b).
-- This file rebuilds the closure with the new edges, the same breadth-first walk as
-- _f_buildlocationancestor() (12 levels, INSERT IGNORE keeps the smallest depth), and
-- compares it with the live table. "Country" in the reports means LOCATION_TYPE = 'country',
-- for reading only: the rule itself never reads LOCATION_TYPE.

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Edges, best rank as in step 3d-0, with the property and an end-time flag.
-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS tmp_ended;
DROP TEMPORARY TABLE IF EXISTS tmp_e;
CREATE TEMPORARY TABLE tmp_e (
  ID_CHILD VARCHAR(50) NOT NULL, ID_PARENT VARCHAR(50) NOT NULL, PROP VARCHAR(10) NOT NULL, HAS_END TINYINT NOT NULL, PARENT_ENDED TINYINT NOT NULL DEFAULT 0,
  EDITORIAL_DROP TINYINT NOT NULL DEFAULT 0, PARENT_KNOWN TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (ID_CHILD, ID_PARENT, PROP), KEY (ID_PARENT)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_e (ID_CHILD, ID_PARENT, PROP, HAS_END)
SELECT st.ID_WIKIDATA, iv.ID_ITEM, st.ID_PROPERTY,
       MAX(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q
                   WHERE q.ID_STATEMENT = st.ID_STATEMENT AND q.ID_QUALIFIER_PROPERTY = 'P582'
                     AND COALESCE(q.DELETED, 0) = 0))
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
WHERE st.ID_PROPERTY IN ('P131', 'P17')
  AND st.DELETED = 0
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND (st.`RANK` = 'preferred'
       OR NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT p
                      WHERE p.ID_WIKIDATA = st.ID_WIKIDATA AND p.ID_PROPERTY = st.ID_PROPERTY
                        AND p.`RANK` = 'preferred' AND p.DELETED = 0))
  AND iv.ID_ITEM <> st.ID_WIKIDATA
GROUP BY st.ID_WIKIDATA, iv.ID_ITEM, st.ID_PROPERTY;

DROP TEMPORARY TABLE IF EXISTS tmp_ended;
CREATE TEMPORARY TABLE tmp_ended (ID_ITEM VARCHAR(50) NOT NULL PRIMARY KEY)
  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT IGNORE INTO tmp_ended
SELECT DISTINCT st.ID_WIKIDATA FROM T_WC_WIKIDATA_STATEMENT st
WHERE st.ID_PROPERTY = 'P576' AND st.DELETED = 0 AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND st.ID_WIKIDATA IN (SELECT ID_PARENT FROM tmp_e);
INSERT IGNORE INTO tmp_ended
SELECT DISTINCT st.ID_WIKIDATA FROM T_WC_WIKIDATA_STATEMENT st
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
WHERE st.ID_PROPERTY = 'P31' AND st.DELETED = 0 AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND (iv.ID_ITEM IN ('Q3024240', 'Q11514315', 'Q19953632', 'Q50068795', 'Q12857432', 'Q182547')
       OR (iv.ID_ITEM = 'Q99541706' AND st.`RANK` = 'preferred')
       OR (iv.ID_ITEM = 'Q2577883'
           AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s2
                           JOIN T_WC_WIKIDATA_ITEM_VALUE v2 ON v2.ID_STATEMENT = s2.ID_STATEMENT
                           WHERE s2.ID_WIKIDATA = st.ID_WIKIDATA AND s2.ID_PROPERTY = 'P31' AND s2.DELETED = 0
                             AND (s2.`RANK` IS NULL OR s2.`RANK` <> 'deprecated') AND v2.ID_ITEM = 'Q15239622')))
  AND st.ID_WIKIDATA IN (SELECT ID_PARENT FROM tmp_e);
UPDATE tmp_e e JOIN tmp_ended d ON d.ID_ITEM = e.ID_PARENT SET e.PARENT_ENDED = 1;
UPDATE tmp_e e JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = e.ID_PARENT SET e.PARENT_KNOWN = 1;
-- Decision 2: Normandy stays under France.
UPDATE tmp_e SET EDITORIAL_DROP = 1 WHERE ID_CHILD = 'Q15878' AND PROP = 'P17' AND ID_PARENT IN ('Q25230', 'Q785');

-- MIN(HAS_END): an edge stated twice, once closed and once open, stays open.
-- The open P17 set of each item with two values or more (decision 3: known parents only).
DROP TEMPORARY TABLE IF EXISTS tmp_multi;
CREATE TEMPORARY TABLE tmp_multi (ID_CHILD VARCHAR(50) NOT NULL PRIMARY KEY, N_P17 INT NOT NULL, P17_SET VARCHAR(2000) NOT NULL, KEY (P17_SET(200)))
  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO tmp_multi
SELECT ID_CHILD, COUNT(DISTINCT ID_PARENT), GROUP_CONCAT(DISTINCT ID_PARENT ORDER BY ID_PARENT SEPARATOR ' ') FROM tmp_e
WHERE PROP = 'P17' AND HAS_END = 0 AND PARENT_ENDED = 0 AND EDITORIAL_DROP = 0 AND PARENT_KNOWN = 1
GROUP BY ID_CHILD HAVING COUNT(DISTINCT ID_PARENT) >= 2;

-- Decision 1: the disputed ones, by clause.
DROP TEMPORARY TABLE IF EXISTS tmp_disputed;
CREATE TEMPORARY TABLE tmp_disputed (ID_CHILD VARCHAR(50) NOT NULL PRIMARY KEY, CLAUSE VARCHAR(2) NOT NULL)
  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT IGNORE INTO tmp_disputed
SELECT DISTINCT m.ID_CHILD, 'D1' FROM tmp_multi m
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA = m.ID_CHILD AND s.ID_PROPERTY = 'P31' AND s.DELETED = 0
  AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated')
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
WHERE v.ID_ITEM IN ('Q15239622', 'Q2577883');
DROP TEMPORARY TABLE IF EXISTS tmp_dset;
CREATE TEMPORARY TABLE tmp_dset (P17_SET VARCHAR(2000) NOT NULL, KEY (P17_SET(200)))
  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO tmp_dset
SELECT DISTINCT m.P17_SET FROM tmp_disputed d JOIN tmp_multi m ON m.ID_CHILD = d.ID_CHILD
WHERE EXISTS (SELECT 1 FROM T_WC_T2S_LOCATION l WHERE l.ID_WIKIDATA = d.ID_CHILD AND l.DELETED = 0);
INSERT IGNORE INTO tmp_disputed
SELECT m.ID_CHILD, 'D2' FROM tmp_multi m JOIN tmp_dset x ON x.P17_SET = m.P17_SET;
INSERT IGNORE INTO tmp_disputed
SELECT DISTINCT m.ID_CHILD, 'D3' FROM tmp_multi m
JOIN tmp_e e ON e.ID_CHILD = m.ID_CHILD AND e.PROP = 'P17' AND e.HAS_END = 0 AND e.PARENT_ENDED = 0
  AND e.EDITORIAL_DROP = 0 AND e.PARENT_KNOWN = 1
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA = e.ID_PARENT AND s.ID_PROPERTY = 'P31' AND s.DELETED = 0
  AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated')
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
WHERE m.N_P17 = 2 AND v.ID_ITEM IN ('Q1179466', 'Q15239622');

DROP TEMPORARY TABLE IF EXISTS tmp_cross;
CREATE TEMPORARY TABLE tmp_cross (ID_CHILD VARCHAR(50) NOT NULL PRIMARY KEY, N_P17 INT NOT NULL)
  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO tmp_cross
SELECT m.ID_CHILD, m.N_P17 FROM tmp_multi m
WHERE NOT EXISTS (SELECT 1 FROM tmp_disputed d WHERE d.ID_CHILD = m.ID_CHILD);

DROP TEMPORARY TABLE IF EXISTS tmp_cross_noa;
CREATE TEMPORARY TABLE tmp_cross_noa (ID_CHILD VARCHAR(50) NOT NULL PRIMARY KEY, N_P17 INT NOT NULL)
  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO tmp_cross_noa
SELECT ID_CHILD, COUNT(DISTINCT ID_PARENT) FROM tmp_e
WHERE PROP = 'P17'
GROUP BY ID_CHILD HAVING COUNT(DISTINCT ID_PARENT) >= 2;

DROP TEMPORARY TABLE IF EXISTS tmp_arete;
CREATE TEMPORARY TABLE tmp_arete (ID_CHILD VARCHAR(50) NOT NULL, ID_PARENT VARCHAR(50) NOT NULL, PRIMARY KEY (ID_CHILD, ID_PARENT))
  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT IGNORE INTO tmp_arete
SELECT e.ID_CHILD, e.ID_PARENT FROM tmp_e e
WHERE e.HAS_END = 0 AND e.PARENT_ENDED = 0 AND e.EDITORIAL_DROP = 0
  AND NOT EXISTS (SELECT 1 FROM tmp_cross c WHERE c.ID_CHILD = e.ID_CHILD);

SELECT '1. Edges and cross-border items, all Wikidata items and living T2S places' AS SECTION;

SELECT (SELECT COUNT(*) FROM tmp_e) AS EDGES_BEST_RANK,
       (SELECT COUNT(*) FROM tmp_e WHERE HAS_END = 1) AS EDGES_WITH_END,
       (SELECT COUNT(*) FROM tmp_e WHERE PARENT_ENDED = 1) AS EDGES_TO_DISSOLVED,
       (SELECT COUNT(*) FROM tmp_ended) AS DISSOLVED_PARENTS,
       (SELECT COUNT(*) FROM tmp_arete) AS EDGES_KEPT,
       (SELECT COUNT(*) FROM tmp_cross_noa) AS CROSS_ITEMS_WITHOUT_A,
       (SELECT COUNT(*) FROM tmp_cross) AS CROSS_ITEMS_WITH_A,
       (SELECT COUNT(*) FROM tmp_cross c JOIN T_WC_T2S_LOCATION l ON l.ID_WIKIDATA = c.ID_CHILD AND l.DELETED = 0) AS CROSS_T2S_PLACES,
       (SELECT COUNT(*) FROM tmp_disputed) AS DISPUTED_ITEMS,
       (SELECT COUNT(*) FROM tmp_disputed d JOIN T_WC_T2S_LOCATION l ON l.ID_WIKIDATA = d.ID_CHILD AND l.DELETED = 0) AS DISPUTED_T2S_PLACES;

SELECT '1b. Disputed living places (decision 1), by clause, with their open P17 values' AS SECTION;

SELECT d.CLAUSE, l.ID_LOCATION, l.ID_WIKIDATA, l.LOCATION_NAME, l.MOVIE_COUNT + l.SERIE_COUNT AS WORKS,
       (SELECT GROUP_CONCAT(COALESCE(wp.LABEL_EN, e.ID_PARENT) ORDER BY 1 SEPARATOR ', ')
          FROM tmp_e e LEFT JOIN T_WC_WIKIDATA_ITEM wp ON wp.ID_WIKIDATA = e.ID_PARENT
         WHERE e.ID_CHILD = d.ID_CHILD AND e.PROP = 'P17' AND e.HAS_END = 0 AND e.PARENT_ENDED = 0
           AND e.EDITORIAL_DROP = 0 AND e.PARENT_KNOWN = 1) AS P17_OPEN
FROM tmp_disputed d JOIN T_WC_T2S_LOCATION l ON l.ID_WIKIDATA = d.ID_CHILD AND l.DELETED = 0
ORDER BY d.CLAUSE, WORKS DESC;

-- ---------------------------------------------------------------------------
-- Closure with the new edges, from the living places.
-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS tmp_anc;
CREATE TEMPORARY TABLE tmp_anc (ID_LIEU VARCHAR(50) NOT NULL, ID_ANCETRE VARCHAR(50) NOT NULL, DEPTH INT NOT NULL,
  PRIMARY KEY (ID_LIEU, ID_ANCETRE), KEY (DEPTH)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO tmp_anc SELECT ID_WIKIDATA, ID_WIKIDATA, 0 FROM T_WC_T2S_LOCATION
WHERE DELETED = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '';
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 1 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 0;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 2 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 1;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 3 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 2;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 4 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 3;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 5 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 4;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 6 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 5;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 7 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 6;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 8 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 7;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 9 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 8;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 10 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 9;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 11 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 10;
INSERT IGNORE INTO tmp_anc SELECT t.ID_LIEU, a.ID_PARENT, 12 FROM tmp_anc t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 11;

DROP TEMPORARY TABLE IF EXISTS tmp_new;
CREATE TEMPORARY TABLE tmp_new (ID_LOCATION INT NOT NULL, ID_ANCESTOR INT NOT NULL, DEPTH INT NOT NULL,
  PRIMARY KEY (ID_LOCATION, ID_ANCESTOR), KEY (ID_ANCESTOR)) ENGINE=InnoDB;
INSERT INTO tmp_new
SELECT loc.ID_LOCATION, anc.ID_LOCATION, t.DEPTH
FROM tmp_anc t
JOIN T_WC_T2S_LOCATION loc ON loc.ID_WIKIDATA = t.ID_LIEU AND loc.DELETED = 0
JOIN T_WC_T2S_LOCATION anc ON anc.ID_WIKIDATA = t.ID_ANCETRE AND anc.DELETED = 0;

SELECT '2. Pairs: live table against the rule' AS SECTION;

SELECT (SELECT COUNT(*) FROM T_WC_T2S_LOCATION_ANCESTOR) AS LIVE_PAIRS,
       (SELECT COUNT(*) FROM tmp_new) AS NEW_PAIRS,
       (SELECT COUNT(*) FROM T_WC_T2S_LOCATION_ANCESTOR o
         WHERE NOT EXISTS (SELECT 1 FROM tmp_new n WHERE n.ID_LOCATION = o.ID_LOCATION AND n.ID_ANCESTOR = o.ID_ANCESTOR)) AS LOST,
       (SELECT COUNT(*) FROM tmp_new n
         WHERE NOT EXISTS (SELECT 1 FROM T_WC_T2S_LOCATION_ANCESTOR o WHERE o.ID_LOCATION = n.ID_LOCATION AND o.ID_ANCESTOR = n.ID_ANCESTOR)) AS GAINED,
       (SELECT COUNT(*) FROM T_WC_T2S_LOCATION_ANCESTOR o JOIN T_WC_T2S_LOCATION c ON c.ID_LOCATION = o.ID_ANCESTOR AND c.LOCATION_TYPE = 'country'
         WHERE NOT EXISTS (SELECT 1 FROM tmp_new n WHERE n.ID_LOCATION = o.ID_LOCATION AND n.ID_ANCESTOR = o.ID_ANCESTOR)) AS LOST_TOWARD_A_COUNTRY;

SELECT '2b. Living places that had a country ancestor and have none with the rule, by type' AS SECTION;

SELECT l.LOCATION_TYPE, COUNT(*) AS PLACES, SUM(l.MOVIE_COUNT) AS MOVIE_LINKS, SUM(l.SERIE_COUNT) AS SERIE_LINKS
FROM T_WC_T2S_LOCATION l
WHERE l.DELETED = 0
  AND EXISTS (SELECT 1 FROM T_WC_T2S_LOCATION_ANCESTOR o JOIN T_WC_T2S_LOCATION c ON c.ID_LOCATION = o.ID_ANCESTOR
              WHERE o.ID_LOCATION = l.ID_LOCATION AND o.DEPTH > 0 AND c.LOCATION_TYPE = 'country')
  AND NOT EXISTS (SELECT 1 FROM tmp_new n JOIN T_WC_T2S_LOCATION c ON c.ID_LOCATION = n.ID_ANCESTOR
                  WHERE n.ID_LOCATION = l.ID_LOCATION AND n.DEPTH > 0 AND c.LOCATION_TYPE = 'country')
GROUP BY l.LOCATION_TYPE ORDER BY PLACES DESC;

SELECT '3. The cross-border living places, best known first, with their open P17 values' AS SECTION;

SELECT l.ID_LOCATION, l.ID_WIKIDATA, l.LOCATION_NAME, l.LOCATION_TYPE, c.N_P17, l.MOVIE_COUNT, l.SERIE_COUNT,
       (SELECT GROUP_CONCAT(COALESCE(pl.LOCATION_NAME, e.ID_PARENT) ORDER BY 1 SEPARATOR ', ')
          FROM tmp_e e LEFT JOIN T_WC_T2S_LOCATION pl ON pl.ID_WIKIDATA = e.ID_PARENT
         WHERE e.ID_CHILD = l.ID_WIKIDATA AND e.PROP = 'P17' AND e.HAS_END = 0 AND e.PARENT_ENDED = 0
           AND e.EDITORIAL_DROP = 0 AND e.PARENT_KNOWN = 1) AS P17_OPEN
FROM tmp_cross c JOIN T_WC_T2S_LOCATION l ON l.ID_WIKIDATA = c.ID_CHILD AND l.DELETED = 0
ORDER BY l.MOVIE_COUNT + l.SERIE_COUNT DESC
LIMIT 80;

SELECT '4. Witnesses: country ancestors, live then with the rule, and the P17 edges' AS SECTION;

SELECT l.ID_LOCATION, l.ID_WIKIDATA, l.LOCATION_NAME,
       (SELECT GROUP_CONCAT(c.LOCATION_NAME ORDER BY c.LOCATION_NAME SEPARATOR ', ')
          FROM T_WC_T2S_LOCATION_ANCESTOR o JOIN T_WC_T2S_LOCATION c ON c.ID_LOCATION = o.ID_ANCESTOR
         WHERE o.ID_LOCATION = l.ID_LOCATION AND o.DEPTH > 0 AND c.LOCATION_TYPE = 'country') AS COUNTRIES_LIVE,
       (SELECT GROUP_CONCAT(c.LOCATION_NAME ORDER BY c.LOCATION_NAME SEPARATOR ', ')
          FROM tmp_new n JOIN T_WC_T2S_LOCATION c ON c.ID_LOCATION = n.ID_ANCESTOR
         WHERE n.ID_LOCATION = l.ID_LOCATION AND n.DEPTH > 0 AND c.LOCATION_TYPE = 'country') AS COUNTRIES_NEW,
       (SELECT GROUP_CONCAT(CONCAT(COALESCE(pl.LOCATION_NAME, e.ID_PARENT), IF(e.HAS_END = 1 OR e.PARENT_ENDED = 1, ' (ended)', IF(e.EDITORIAL_DROP = 1, ' (dropped)', IF(e.PARENT_KNOWN = 0, ' (unknown)', '')))) ORDER BY 1 SEPARATOR ', ')
          FROM tmp_e e LEFT JOIN T_WC_T2S_LOCATION pl ON pl.ID_WIKIDATA = e.ID_PARENT
         WHERE e.ID_CHILD = l.ID_WIKIDATA AND e.PROP = 'P17') AS P17_EDGES
FROM T_WC_T2S_LOCATION l
WHERE l.ID_WIKIDATA IN ('Q90', 'Q84', 'Q1348', 'Q456', 'Q1524', 'Q215646', 'Q437', 'Q15878', 'Q1218', 'Q93332',
                        'Q4918', 'Q34617', 'Q7835', 'Q18097', 'Q7204', 'Q1530', 'Q1462', 'Q23792', 'Q4093', 'Q1794',
                        'Q1861', 'Q152658', 'Q154607', 'Q56036', 'Q130212', 'Q7525', 'Q22890', 'Q47588',
                        'Q128499', 'Q36678', 'Q83210', 'Q178168', 'Q47837', 'Q167109', 'Q171357', 'Q323813',
                        'Q186312', 'Q6501000', 'Q39760', 'Q644636')
ORDER BY l.MOVIE_COUNT + l.SERIE_COUNT DESC;

SELECT '5. France (126): movies per role, live and with the rule' AS SECTION;

SELECT j.LOCATION_ROLE,
       COUNT(DISTINCT CASE WHEN o.ID_LOCATION IS NOT NULL THEN j.ID_MOVIE END) AS LIVE,
       COUNT(DISTINCT CASE WHEN n.ID_LOCATION IS NOT NULL THEN j.ID_MOVIE END) AS WITH_RULE
FROM T_WC_T2S_MOVIE_LOCATION j
JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = j.ID_MOVIE AND COALESCE(m.DELETED, 0) = 0
LEFT JOIN T_WC_T2S_LOCATION_ANCESTOR o ON o.ID_LOCATION = j.ID_LOCATION AND o.ID_ANCESTOR = 126
LEFT JOIN tmp_new n ON n.ID_LOCATION = j.ID_LOCATION AND n.ID_ANCESTOR = 126
WHERE COALESCE(j.DELETED, 0) = 0 AND (o.ID_LOCATION IS NOT NULL OR n.ID_LOCATION IS NOT NULL)
GROUP BY j.LOCATION_ROLE;

SELECT '5b. France: the places that stop counting, by movies lost' AS SECTION;

SELECT l.ID_LOCATION, l.LOCATION_NAME, l.LOCATION_TYPE, l.MOVIE_COUNT, l.SERIE_COUNT
FROM T_WC_T2S_LOCATION_ANCESTOR o
JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = o.ID_LOCATION
WHERE o.ID_ANCESTOR = 126
  AND NOT EXISTS (SELECT 1 FROM tmp_new n WHERE n.ID_LOCATION = o.ID_LOCATION AND n.ID_ANCESTOR = 126)
ORDER BY l.MOVIE_COUNT + l.SERIE_COUNT DESC
LIMIT 30;

SELECT '6. Countries: distinct movies contained, live and with the rule (top 25 by loss)' AS SECTION;

SELECT x.ID_LOCATION, x.LOCATION_NAME, x.LIVE, x.WITH_RULE, x.LIVE - x.WITH_RULE AS LOST FROM (
  SELECT c.ID_LOCATION, c.LOCATION_NAME,
         (SELECT COUNT(DISTINCT j.ID_MOVIE) FROM T_WC_T2S_LOCATION_ANCESTOR o
            JOIN T_WC_T2S_MOVIE_LOCATION j ON j.ID_LOCATION = o.ID_LOCATION AND COALESCE(j.DELETED, 0) = 0
           WHERE o.ID_ANCESTOR = c.ID_LOCATION) AS LIVE,
         (SELECT COUNT(DISTINCT j.ID_MOVIE) FROM tmp_new n
            JOIN T_WC_T2S_MOVIE_LOCATION j ON j.ID_LOCATION = n.ID_LOCATION AND COALESCE(j.DELETED, 0) = 0
           WHERE n.ID_ANCESTOR = c.ID_LOCATION) AS WITH_RULE
  FROM T_WC_T2S_LOCATION c
  WHERE c.DELETED = 0 AND c.LOCATION_TYPE = 'country') x
ORDER BY x.LIVE - x.WITH_RULE DESC
LIMIT 25;

DROP TEMPORARY TABLE IF EXISTS tmp_new;
DROP TEMPORARY TABLE IF EXISTS tmp_anc;
DROP TEMPORARY TABLE IF EXISTS tmp_arete;
DROP TEMPORARY TABLE IF EXISTS tmp_cross_noa;
DROP TEMPORARY TABLE IF EXISTS tmp_cross;
DROP TEMPORARY TABLE IF EXISTS tmp_dset;
DROP TEMPORARY TABLE IF EXISTS tmp_disputed;
DROP TEMPORARY TABLE IF EXISTS tmp_multi;
DROP TEMPORARY TABLE IF EXISTS tmp_ended;
DROP TEMPORARY TABLE IF EXISTS tmp_e;
