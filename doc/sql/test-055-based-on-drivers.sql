-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-055 : "based on" (Wikidata P144), measure before building
-- ============================================================================
--
-- READ ONLY. Step 1 of the ticket: no table is built until these figures are read.
--
-- WHAT IS ALREADY KNOWN. wikidata-crawler V2 keeps every item-valued statement of the
-- in-scope films and series, with no property allowlist, so the P144 rows already sit in
-- T_WC_WIKIDATA_STATEMENT + T_WC_WIKIDATA_ITEM_VALUE, where P840/P915 are read from.
-- A V1 measurement (an-image-for-everything, volume-study-2) gave 24,737 rows and
-- 16,715 distinct source works. Nothing has been measured on V2 nor on T2S works.
--
-- WHAT THIS FILE DECIDES.
--   1-2. The volume on V2, then restricted to T2S works (same EXISTS logic as
--        STR_LOCATION_DRIVING: a work counts when its Q-id is in T_WC_T2S_MOVIE/_SERIE).
--   3.   Where each target lives: a T2S film/series, a core film/series outside T2S,
--        a cached item (T_WC_WIKIDATA_ITEM), or nowhere (no label to show).
--   4.   For cached targets, the top P31 classes: they decide the P279 cones of
--        SOURCE_WORK_TYPE, and expose the misuses (persons, characters, events).
--   5.   The most adapted sources, a sanity check on labels and kinds.
--   6.   The witnesses named in the ticket.
--   7.   The inverse P4969 (derivative work), for core targets only.
--   8.   TMDb "based on ..." keywords against P144, on T2S films and series.
--
-- PERFORMANCE. Everything goes through two indexed temporary tables built once, so no
-- join runs on a non-indexable condition (lesson of test-014 section 4, killed after
-- 91 minutes). P144 is read through IDX_..._ID_PROPERTY, about 25k rows.
--
-- COLLATION. Run with the runner's default (--force behaviour).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Working tables
-- ---------------------------------------------------------------------------

-- The T2S works, by Q-id. KIND says which T2S table holds it.
DROP TEMPORARY TABLE IF EXISTS TMP_055_T2S_WORK;
CREATE TEMPORARY TABLE TMP_055_T2S_WORK (
  ID_WIKIDATA  VARCHAR(50) NOT NULL,
  KIND         VARCHAR(10) NOT NULL,
  ID_WORK      INT NOT NULL,
  TITLE        VARCHAR(250) NULL,
  PRIMARY KEY (ID_WIKIDATA, KIND),
  KEY IDX_WORK (KIND, ID_WORK)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO TMP_055_T2S_WORK (ID_WIKIDATA, KIND, ID_WORK, TITLE)
SELECT m.ID_WIKIDATA, 'movie', m.ID_MOVIE, m.MOVIE_TITLE
FROM T_WC_T2S_MOVIE m
WHERE m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '';

INSERT IGNORE INTO TMP_055_T2S_WORK (ID_WIKIDATA, KIND, ID_WORK, TITLE)
SELECT s.ID_WIKIDATA, 'serie', s.ID_SERIE, s.SERIE_TITLE
FROM T_WC_T2S_SERIE s
WHERE s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '';

-- Every P144 statement with an item value, whatever its rank or DELETED flag:
-- sections 1 shows how many each filter removes, the others keep the live ones.
DROP TEMPORARY TABLE IF EXISTS TMP_055_P144;
CREATE TEMPORARY TABLE TMP_055_P144 (
  ID_STATEMENT BIGINT NOT NULL,
  ID_SUBJECT   VARCHAR(50) NOT NULL,
  ID_TARGET    VARCHAR(50) NOT NULL,
  IS_LIVE      TINYINT NOT NULL,
  IS_DEPRECATED TINYINT NOT NULL,
  IS_DELETED   TINYINT NOT NULL,
  PRIMARY KEY (ID_STATEMENT),
  KEY IDX_SUBJECT (ID_SUBJECT),
  KEY IDX_TARGET (ID_TARGET)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_055_P144 (ID_STATEMENT, ID_SUBJECT, ID_TARGET, IS_LIVE, IS_DEPRECATED, IS_DELETED)
SELECT st.ID_STATEMENT, st.ID_WIKIDATA, iv.ID_ITEM,
       CASE WHEN (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated') AND COALESCE(st.DELETED, 0) = 0
            THEN 1 ELSE 0 END,
       CASE WHEN st.`RANK` = 'deprecated' THEN 1 ELSE 0 END,
       CASE WHEN COALESCE(st.DELETED, 0) <> 0 THEN 1 ELSE 0 END
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
WHERE st.ID_PROPERTY = 'P144';

-- One row per live (T2S work, target) link, with where the target lives.
-- TARGET_HOME order matters: a target that is a T2S work is reported as such even
-- though it is also a core entity.
DROP TEMPORARY TABLE IF EXISTS TMP_055_LINK;
CREATE TEMPORARY TABLE TMP_055_LINK (
  KIND         VARCHAR(10) NOT NULL,
  ID_WORK      INT NOT NULL,
  ID_SUBJECT   VARCHAR(50) NOT NULL,
  ID_TARGET    VARCHAR(50) NOT NULL,
  TARGET_HOME  VARCHAR(30) NULL,
  PRIMARY KEY (KIND, ID_WORK, ID_TARGET),
  KEY IDX_TARGET (ID_TARGET),
  KEY IDX_HOME (TARGET_HOME)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO TMP_055_LINK (KIND, ID_WORK, ID_SUBJECT, ID_TARGET)
SELECT w.KIND, w.ID_WORK, p.ID_SUBJECT, p.ID_TARGET
FROM TMP_055_P144 p
INNER JOIN TMP_055_T2S_WORK w ON w.ID_WIKIDATA = p.ID_SUBJECT
WHERE p.IS_LIVE = 1;

UPDATE TMP_055_LINK l
SET l.TARGET_HOME = CASE
    WHEN EXISTS (SELECT 1 FROM TMP_055_T2S_WORK w WHERE w.ID_WIKIDATA = l.ID_TARGET AND w.KIND = 'movie') THEN '1 t2s_movie'
    WHEN EXISTS (SELECT 1 FROM TMP_055_T2S_WORK w WHERE w.ID_WIKIDATA = l.ID_TARGET AND w.KIND = 'serie') THEN '2 t2s_serie'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE wm WHERE wm.ID_WIKIDATA = l.ID_TARGET) THEN '3 core_movie_outside_t2s'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE ws WHERE ws.ID_WIKIDATA = l.ID_TARGET) THEN '4 core_serie_outside_t2s'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM wi WHERE wi.ID_WIKIDATA = l.ID_TARGET) THEN '5 cached_item'
    ELSE '6 missing_everywhere' END;

-- ---------------------------------------------------------------------------
-- 1. P144 on V2, all subjects
-- ---------------------------------------------------------------------------
SELECT '1. P144 on V2, all subjects' AS SECTION;

SELECT COUNT(*)                                     AS STATEMENTS,
       SUM(IS_LIVE)                                 AS LIVE,
       SUM(IS_DEPRECATED)                           AS DEPRECATED,
       SUM(IS_DELETED)                              AS DELETED_FLAG,
       COUNT(DISTINCT CASE WHEN IS_LIVE = 1 THEN ID_SUBJECT END) AS SUBJECTS_LIVE,
       COUNT(DISTINCT CASE WHEN IS_LIVE = 1 THEN ID_TARGET END)  AS TARGETS_LIVE
FROM TMP_055_P144;

-- What kind of subject carries P144 (live statements only).
SELECT CASE
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE wm WHERE wm.ID_WIKIDATA = p.ID_SUBJECT) THEN 'core movie'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE ws WHERE ws.ID_WIKIDATA = p.ID_SUBJECT) THEN 'core serie'
         ELSE 'other subject' END                   AS SUBJECT_KIND,
       COUNT(*)                                     AS STATEMENTS,
       COUNT(DISTINCT p.ID_SUBJECT)                 AS SUBJECTS,
       COUNT(DISTINCT p.ID_TARGET)                  AS TARGETS
FROM TMP_055_P144 p
WHERE p.IS_LIVE = 1
GROUP BY SUBJECT_KIND
ORDER BY STATEMENTS DESC;

-- ---------------------------------------------------------------------------
-- 2. Restricted to T2S works: volume and coverage
-- ---------------------------------------------------------------------------
SELECT '2. P144 on T2S works, volume and coverage' AS SECTION;

SELECT w.KIND,
       COUNT(*)                                     AS T2S_WORKS_WITH_QID,
       COUNT(DISTINCT l.ID_WORK)                    AS WORKS_WITH_P144,
       ROUND(100 * COUNT(DISTINCT l.ID_WORK) / COUNT(*), 2) AS PCT_COVERED,
       (SELECT COUNT(*) FROM TMP_055_LINK l2 WHERE l2.KIND = w.KIND)                    AS LINKS,
       (SELECT COUNT(DISTINCT l2.ID_TARGET) FROM TMP_055_LINK l2 WHERE l2.KIND = w.KIND) AS DISTINCT_TARGETS
FROM TMP_055_T2S_WORK w
LEFT JOIN (SELECT DISTINCT KIND, ID_WORK FROM TMP_055_LINK) l ON l.KIND = w.KIND AND l.ID_WORK = w.ID_WORK
GROUP BY w.KIND;

SELECT COUNT(*) AS LINKS_ALL, COUNT(DISTINCT ID_TARGET) AS DISTINCT_TARGETS_ALL
FROM TMP_055_LINK;

-- How many sources per work: several P144 on one film is normal (novel + earlier film).
SELECT N_SOURCES, COUNT(*) AS WORKS
FROM (SELECT KIND, ID_WORK, COUNT(*) AS N_SOURCES FROM TMP_055_LINK GROUP BY KIND, ID_WORK) x
GROUP BY N_SOURCES
ORDER BY N_SOURCES;

-- ---------------------------------------------------------------------------
-- 3. Where the targets live
-- ---------------------------------------------------------------------------
SELECT '3. Where the targets live' AS SECTION;

SELECT TARGET_HOME,
       COUNT(DISTINCT ID_TARGET)                    AS TARGETS,
       COUNT(*)                                     AS LINKS,
       SUM(KIND = 'movie')                          AS LINKS_FROM_MOVIES,
       SUM(KIND = 'serie')                          AS LINKS_FROM_SERIES
FROM TMP_055_LINK
GROUP BY TARGET_HOME
ORDER BY TARGET_HOME;

-- The targets missing everywhere, the most linked first: no label to show for them.
SELECT l.ID_TARGET, COUNT(*) AS LINKS,
       GROUP_CONCAT(DISTINCT w.TITLE ORDER BY w.TITLE SEPARATOR ' | ') AS EXAMPLE_WORKS
FROM TMP_055_LINK l
INNER JOIN TMP_055_T2S_WORK w ON w.KIND = l.KIND AND w.ID_WORK = l.ID_WORK
WHERE l.TARGET_HOME = '6 missing_everywhere'
GROUP BY l.ID_TARGET
ORDER BY LINKS DESC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- 4. Cached targets: the P31 classes that will decide SOURCE_WORK_TYPE
--
--    Expected drivers: literary work, novel, novella, short story, comic, manga,
--    graphic novel, play, musical, video game. Misuses to look for: human, fictional
--    character, real event, fictional universe, book series. A NULL label means the
--    class is not cached; its Q-id is enough to look it up.
-- ---------------------------------------------------------------------------
SELECT '4. Top 50 P31 classes of the cached targets' AS SECTION;

DROP TEMPORARY TABLE IF EXISTS TMP_055_CACHED_TARGET;
CREATE TEMPORARY TABLE TMP_055_CACHED_TARGET (
  ID_TARGET VARCHAR(50) NOT NULL,
  LINKS     INT NOT NULL,
  PRIMARY KEY (ID_TARGET)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_055_CACHED_TARGET (ID_TARGET, LINKS)
SELECT ID_TARGET, COUNT(*) FROM TMP_055_LINK
WHERE TARGET_HOME = '5 cached_item'
GROUP BY ID_TARGET;

SELECT iv.ID_ITEM AS ID_CLASS,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wc.LABELS_JSON, '$.en')),
                NULLIF(wc.LABEL_EN, ''),
                JSON_UNQUOTE(JSON_EXTRACT(wc.LABELS_JSON, '$.fr'))) AS CLASS_LABEL,
       COUNT(DISTINCT ct.ID_TARGET)                 AS TARGETS,
       SUM(ct.LINKS)                                AS LINKS,
       SUBSTRING(GROUP_CONCAT(DISTINCT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wt.LABELS_JSON, '$.en')), NULLIF(wt.LABEL_EN, ''), ct.ID_TARGET)
                 ORDER BY ct.LINKS DESC SEPARATOR ' | '), 1, 200) AS EXAMPLES
FROM TMP_055_CACHED_TARGET ct
INNER JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = ct.ID_TARGET
       AND st.ID_PROPERTY = 'P31'
       AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM wc ON wc.ID_WIKIDATA = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_ITEM wt ON wt.ID_WIKIDATA = ct.ID_TARGET
GROUP BY iv.ID_ITEM, CLASS_LABEL
ORDER BY TARGETS DESC
LIMIT 50;

-- Cached targets with no P31 at all: they would land in 'other' whatever the cones.
SELECT COUNT(*) AS CACHED_TARGETS, SUM(ct.LINKS) AS LINKS,
       SUM(NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                       WHERE st.ID_WIKIDATA = ct.ID_TARGET AND st.ID_PROPERTY = 'P31'
                         AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated'))) AS WITHOUT_P31
FROM TMP_055_CACHED_TARGET ct;

-- ---------------------------------------------------------------------------
-- 5. The 30 most adapted sources (all homes), a check on labels and kinds
-- ---------------------------------------------------------------------------
SELECT '5. The 30 most adapted sources' AS SECTION;

SELECT l.ID_TARGET,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')), NULLIF(wi.LABEL_EN, ''),
                NULLIF(wm.LABEL_EN, ''), NULLIF(ws.LABEL_EN, ''))  AS SOURCE_LABEL,
       l.TARGET_HOME,
       SUM(l.KIND = 'movie')                        AS MOVIES,
       SUM(l.KIND = 'serie')                        AS SERIES,
       SUBSTRING(GROUP_CONCAT(DISTINCT w.TITLE ORDER BY w.TITLE SEPARATOR ' | '), 1, 200) AS ADAPTATIONS
FROM TMP_055_LINK l
INNER JOIN TMP_055_T2S_WORK w ON w.KIND = l.KIND AND w.ID_WORK = l.ID_WORK
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = l.ID_TARGET
LEFT JOIN T_WC_WIKIDATA_MOVIE wm ON wm.ID_WIKIDATA = l.ID_TARGET
LEFT JOIN T_WC_WIKIDATA_SERIE ws ON ws.ID_WIKIDATA = l.ID_TARGET
GROUP BY l.ID_TARGET, SOURCE_LABEL, l.TARGET_HOME
ORDER BY COUNT(*) DESC
LIMIT 30;

-- ---------------------------------------------------------------------------
-- 6. The ticket's witnesses, by TMDb id
--
--    The Shining (movie 694), Scarface 1983 (movie 111), Arrival (movie 329865),
--    The Last of Us (serie 100088). The title column confirms the id; an empty result
--    for a witness means no live P144 on its Q-id.
-- ---------------------------------------------------------------------------
SELECT '6. Witnesses' AS SECTION;

SELECT w.KIND, w.ID_WORK, w.TITLE, w.ID_WIKIDATA,
       l.ID_TARGET,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')), NULLIF(wi.LABEL_EN, ''),
                NULLIF(wm.LABEL_EN, ''), NULLIF(ws.LABEL_EN, ''))  AS SOURCE_LABEL,
       l.TARGET_HOME
FROM TMP_055_T2S_WORK w
LEFT JOIN TMP_055_LINK l ON l.KIND = w.KIND AND l.ID_WORK = w.ID_WORK
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = l.ID_TARGET
LEFT JOIN T_WC_WIKIDATA_MOVIE wm ON wm.ID_WIKIDATA = l.ID_TARGET
LEFT JOIN T_WC_WIKIDATA_SERIE ws ON ws.ID_WIKIDATA = l.ID_TARGET
WHERE (w.KIND = 'movie' AND w.ID_WORK IN (694, 111, 329865))
   OR (w.KIND = 'serie' AND w.ID_WORK IN (100088))
ORDER BY w.KIND, w.ID_WORK;

-- ---------------------------------------------------------------------------
-- 7. The inverse P4969 (derivative work), for core targets only
--
--    Cached items do not keep P4969, so this only covers sources that are themselves
--    films or series. MISSING_P144 counts (source, T2S work) pairs given by P4969 on the
--    source with no P144 the other way: what a union would add.
-- ---------------------------------------------------------------------------
SELECT '7. Inverse P4969 on core sources' AS SECTION;

SELECT COUNT(*)                                     AS P4969_PAIRS_TO_T2S,
       SUM(NOT EXISTS (SELECT 1 FROM TMP_055_P144 p
                       WHERE p.ID_SUBJECT = iv.ID_ITEM AND p.ID_TARGET = st.ID_WIKIDATA
                         AND p.IS_LIVE = 1))       AS MISSING_P144
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
INNER JOIN TMP_055_T2S_WORK w ON w.ID_WIKIDATA = iv.ID_ITEM
WHERE st.ID_PROPERTY = 'P4969'
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND COALESCE(st.DELETED, 0) = 0;

-- ---------------------------------------------------------------------------
-- 8. TMDb "based on ..." keywords against P144
--
--    The keyword gives the type without the source. If many T2S works carry a
--    "based on" keyword but no P144, the keyword is worth a fallback for the type.
--    Keywords are matched by name, not by a guessed id.
-- ---------------------------------------------------------------------------
SELECT '8. TMDb based-on keywords against P144' AS SECTION;

DROP TEMPORARY TABLE IF EXISTS TMP_055_KW;
CREATE TEMPORARY TABLE TMP_055_KW (
  ID_KEYWORD INT NOT NULL,
  NAME       VARCHAR(250) NULL,
  PRIMARY KEY (ID_KEYWORD)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_055_KW (ID_KEYWORD, NAME)
SELECT k.ID_KEYWORD, k.NAME
FROM T_WC_TMDB_KEYWORD k
WHERE k.NAME LIKE 'based on %' AND COALESCE(k.DELETED, 0) = 0;

DROP TEMPORARY TABLE IF EXISTS TMP_055_KW_WORK;
CREATE TEMPORARY TABLE TMP_055_KW_WORK (
  KIND       VARCHAR(10) NOT NULL,
  ID_WORK    INT NOT NULL,
  ID_KEYWORD INT NOT NULL,
  PRIMARY KEY (KIND, ID_WORK, ID_KEYWORD),
  KEY IDX_KW (ID_KEYWORD)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO TMP_055_KW_WORK (KIND, ID_WORK, ID_KEYWORD)
SELECT 'movie', mk.ID_MOVIE, mk.ID_KEYWORD
FROM T_WC_TMDB_MOVIE_KEYWORD mk
INNER JOIN TMP_055_KW kw ON kw.ID_KEYWORD = mk.ID_KEYWORD
INNER JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = mk.ID_MOVIE
WHERE COALESCE(mk.DELETED, 0) = 0;

INSERT IGNORE INTO TMP_055_KW_WORK (KIND, ID_WORK, ID_KEYWORD)
SELECT 'serie', sk.ID_SERIE, sk.ID_KEYWORD
FROM T_WC_TMDB_SERIE_KEYWORD sk
INNER JOIN TMP_055_KW kw ON kw.ID_KEYWORD = sk.ID_KEYWORD
INNER JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = sk.ID_SERIE
WHERE COALESCE(sk.DELETED, 0) = 0;

-- 8a. Each keyword: how many T2S works carry it, and how many of those have a P144.
SELECT kw.ID_KEYWORD, kw.NAME,
       SUM(kwk.KIND = 'movie')                      AS MOVIES,
       SUM(kwk.KIND = 'serie')                      AS SERIES,
       SUM(EXISTS (SELECT 1 FROM TMP_055_LINK l WHERE l.KIND = kwk.KIND AND l.ID_WORK = kwk.ID_WORK)) AS WITH_P144,
       ROUND(100 * SUM(EXISTS (SELECT 1 FROM TMP_055_LINK l WHERE l.KIND = kwk.KIND AND l.ID_WORK = kwk.ID_WORK))
             / COUNT(*), 1)                         AS PCT_WITH_P144
FROM TMP_055_KW kw
INNER JOIN TMP_055_KW_WORK kwk ON kwk.ID_KEYWORD = kw.ID_KEYWORD
GROUP BY kw.ID_KEYWORD, kw.NAME
ORDER BY COUNT(*) DESC
LIMIT 30;

-- 8b. The two-by-two table: keyword yes/no against P144 yes/no, per kind.
SELECT w.KIND,
       SUM(kwx.ID_WORK IS NOT NULL AND lx.ID_WORK IS NOT NULL) AS KEYWORD_AND_P144,
       SUM(kwx.ID_WORK IS NOT NULL AND lx.ID_WORK IS NULL)     AS KEYWORD_ONLY,
       SUM(kwx.ID_WORK IS NULL AND lx.ID_WORK IS NOT NULL)     AS P144_ONLY
FROM TMP_055_T2S_WORK w
LEFT JOIN (SELECT DISTINCT KIND, ID_WORK FROM TMP_055_KW_WORK) kwx ON kwx.KIND = w.KIND AND kwx.ID_WORK = w.ID_WORK
LEFT JOIN (SELECT DISTINCT KIND, ID_WORK FROM TMP_055_LINK) lx    ON lx.KIND = w.KIND AND lx.ID_WORK = w.ID_WORK
GROUP BY w.KIND;

-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS TMP_055_KW_WORK;
DROP TEMPORARY TABLE IF EXISTS TMP_055_KW;
DROP TEMPORARY TABLE IF EXISTS TMP_055_CACHED_TARGET;
DROP TEMPORARY TABLE IF EXISTS TMP_055_LINK;
DROP TEMPORARY TABLE IF EXISTS TMP_055_P144;
DROP TEMPORARY TABLE IF EXISTS TMP_055_T2S_WORK;
