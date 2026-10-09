-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-055 : two defects seen in the acceptance of 2026-10-09
-- ============================================================================
--
-- READ ONLY. Measures before any fix.
--
-- 1. A SOURCE THAT HAS A T2S SHEET BUT IS NOT A FILM. Heart of Darkness (Q129778, the
--    Conrad novel) came out typed 'screen', year 2026: a T2S film carries the novel's
--    Wikidata id, so process 73 found a film at that Q-id and gave it the 'screen'
--    precedence. The error is upstream, in T_WC_T2S_MOVIE.ID_WIKIDATA (TMDb to Wikidata
--    matching), and may touch other works. Expected: a short list of T2S works whose
--    Q-id is a book, a manga or a play according to its own P31 classes.
--    1c widens the question to every T2S film and series: how many carry a Q-id that
--    Wikidata does not know as a film or a series at all.
--
-- 2. THE SOURCES WITHOUT A NAME. 1,743 of 17,464 (10 %), The Last of Us game among them.
--    How many have a label in another language than English (a cheap fallback), how
--    many have an item row with no label at all, how many have no item row.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ===========================================================================
-- 1. Sources with a T2S sheet whose own classes say they are not a film or a series
-- ===========================================================================
DROP TEMPORARY TABLE IF EXISTS TMP_055_HOP_RANK;
CREATE TEMPORARY TABLE TMP_055_HOP_RANK (
  ID_SOURCE_WORK INT NOT NULL PRIMARY KEY,
  BEST_RANK INT NULL,
  P31_CLASSES VARCHAR(500) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_055_HOP_RANK (ID_SOURCE_WORK, BEST_RANK, P31_CLASSES)
SELECT sw.ID_SOURCE_WORK,
       MIN(c.CONE_RANK),
       LEFT(GROUP_CONCAT(DISTINCT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wk.LABELS_JSON, '$.en')), iv.ID_ITEM)
                         ORDER BY iv.ID_ITEM SEPARATOR ' | '), 500)
FROM T_WC_T2S_SOURCE_WORK sw
JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = sw.ID_WIKIDATA
     AND st.ID_PROPERTY = 'P31' AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_T2S_SOURCE_WORK_CLASS c ON c.ID_CLASS = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_ITEM wk ON wk.ID_WIKIDATA = iv.ID_ITEM
WHERE sw.DELETED = 0 AND (sw.ID_MOVIE IS NOT NULL OR sw.ID_SERIE IS NOT NULL)
GROUP BY sw.ID_SOURCE_WORK;

SELECT '1a. Sources with a T2S sheet, by the cone their own P31 falls in (8 = screen)' AS SECTION;

SELECT COALESCE(c.SOURCE_WORK_TYPE, '(no cone)') AS P31_SAYS,
       COALESCE(c.SOURCE_WORK_FORM, '') AS FORM,
       COUNT(*) AS SOURCES,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE wm WHERE wm.ID_WIKIDATA = sw.ID_WIKIDATA)
        OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE ws WHERE ws.ID_WIKIDATA = sw.ID_WIKIDATA)) AS IN_CORE_FILM_OR_SERIE
FROM T_WC_T2S_SOURCE_WORK sw
LEFT JOIN TMP_055_HOP_RANK h ON h.ID_SOURCE_WORK = sw.ID_SOURCE_WORK
LEFT JOIN (SELECT DISTINCT CONE_RANK, SOURCE_WORK_TYPE, SOURCE_WORK_FORM FROM T_WC_T2S_SOURCE_WORK_CLASS) c
       ON c.CONE_RANK = h.BEST_RANK
WHERE sw.DELETED = 0 AND (sw.ID_MOVIE IS NOT NULL OR sw.ID_SERIE IS NOT NULL)
GROUP BY P31_SAYS, FORM
ORDER BY SOURCES DESC;

-- The suspects: a T2S sheet on a Q-id whose classes are not screen ones (and which
-- Wikidata does not hold as a film or a series). Heart of Darkness must be here.
SELECT '1b. The suspects, the 40 most adapted' AS SECTION;

SELECT sw.ID_SOURCE_WORK, sw.ID_WIKIDATA, sw.SOURCE_WORK_NAME,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')), wi.LABEL_EN) AS WIKIDATA_LABEL,
       h.P31_CLASSES,
       sw.ID_MOVIE, m.MOVIE_TITLE, m.RELEASE_YEAR,
       sw.ID_SERIE, s.SERIE_TITLE, s.FIRST_AIR_YEAR,
       sw.MOVIE_COUNT, sw.SERIE_COUNT
FROM T_WC_T2S_SOURCE_WORK sw
JOIN TMP_055_HOP_RANK h ON h.ID_SOURCE_WORK = sw.ID_SOURCE_WORK
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = sw.ID_WIKIDATA
LEFT JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = sw.ID_MOVIE
LEFT JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = sw.ID_SERIE
WHERE sw.DELETED = 0
  AND (h.BEST_RANK IS NULL OR h.BEST_RANK <> 8)
  AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE wm WHERE wm.ID_WIKIDATA = sw.ID_WIKIDATA)
  AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE ws WHERE ws.ID_WIKIDATA = sw.ID_WIKIDATA)
ORDER BY sw.MOVIE_COUNT + sw.SERIE_COUNT DESC
LIMIT 40;

-- The wider question: every T2S film and series whose Q-id is not a Wikidata film or
-- series entity. Not all are errors (a cached item can be a legitimate film missed by
-- the crawler's classification), but the count says how big the matching gap is.
SELECT '1c. T2S works whose Q-id is not a Wikidata film or series' AS SECTION;

SELECT 'movie' AS KIND, COUNT(*) AS T2S_WITH_QID,
       SUM(NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE wm WHERE wm.ID_WIKIDATA = m.ID_WIKIDATA)
           AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE ws WHERE ws.ID_WIKIDATA = m.ID_WIKIDATA)) AS QID_NOT_A_FILM_OR_SERIE,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM wi WHERE wi.ID_WIKIDATA = m.ID_WIKIDATA)) AS QID_IS_A_CACHED_ITEM
FROM T_WC_T2S_MOVIE m WHERE m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> ''
UNION ALL
SELECT 'serie', COUNT(*),
       SUM(NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE wm WHERE wm.ID_WIKIDATA = s.ID_WIKIDATA)
           AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE ws WHERE ws.ID_WIKIDATA = s.ID_WIKIDATA)),
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM wi WHERE wi.ID_WIKIDATA = s.ID_WIKIDATA))
FROM T_WC_T2S_SERIE s WHERE s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '';

-- Of those, the ones whose Q-id is a cached item typed as a written work: the clearest
-- matching errors (a film pointing to its own source novel).
SELECT '1d. T2S films pointing to a literary item, the 30 most popular' AS SECTION;

SELECT m.ID_MOVIE, m.MOVIE_TITLE, m.RELEASE_YEAR, m.ID_WIKIDATA,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')), wi.LABEL_EN) AS WIKIDATA_LABEL,
       MIN(c.CONE_RANK) AS BEST_RANK
FROM T_WC_T2S_MOVIE m
JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = m.ID_WIKIDATA
JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = m.ID_WIKIDATA
     AND st.ID_PROPERTY = 'P31' AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
JOIN T_WC_T2S_SOURCE_WORK_CLASS c ON c.ID_CLASS = iv.ID_ITEM
WHERE NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE wm WHERE wm.ID_WIKIDATA = m.ID_WIKIDATA)
GROUP BY m.ID_MOVIE, m.MOVIE_TITLE, m.RELEASE_YEAR, m.ID_WIKIDATA, WIKIDATA_LABEL
HAVING BEST_RANK <> 8
ORDER BY MAX(m.POPULARITY) DESC
LIMIT 30;

DROP TEMPORARY TABLE IF EXISTS TMP_055_HOP_RANK;

-- ===========================================================================
-- 2. The sources without a name
-- ===========================================================================
SELECT '2a. Sources without a name: what the item row holds' AS SECTION;

SELECT CASE WHEN wi.ID_WIKIDATA IS NULL THEN '1 no item row'
            WHEN wi.LABELS_JSON IS NULL OR JSON_LENGTH(wi.LABELS_JSON) = 0 THEN '2 item row, no label at all'
            ELSE '3 item row, labels in other languages' END AS CASE_,
       COUNT(*) AS SOURCES,
       SUM(sw.MOVIE_COUNT) AS MOVIE_LINKS, SUM(sw.SERIE_COUNT) AS SERIE_LINKS
FROM T_WC_T2S_SOURCE_WORK sw
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = sw.ID_WIKIDATA
WHERE sw.DELETED = 0 AND sw.SOURCE_WORK_NAME IS NULL
GROUP BY CASE_
ORDER BY CASE_;

-- Which language would the fallback take (the first key of LABELS_JSON).
SELECT '2b. First available label language, when there is one' AS SECTION;

SELECT JSON_UNQUOTE(JSON_EXTRACT(JSON_KEYS(wi.LABELS_JSON), '$[0]')) AS FIRST_LANGUAGE, COUNT(*) AS SOURCES
FROM T_WC_T2S_SOURCE_WORK sw
JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = sw.ID_WIKIDATA
WHERE sw.DELETED = 0 AND sw.SOURCE_WORK_NAME IS NULL AND JSON_LENGTH(wi.LABELS_JSON) > 0
GROUP BY FIRST_LANGUAGE
ORDER BY SOURCES DESC
LIMIT 15;

-- The unnamed sources the most adapted, with what a fallback would show. The Last of
-- Us (Q1986744) is expected here.
SELECT '2c. Unnamed sources, the 30 most adapted' AS SECTION;

SELECT sw.ID_SOURCE_WORK, sw.ID_WIKIDATA, sw.SOURCE_WORK_TYPE, sw.MOVIE_COUNT, sw.SERIE_COUNT,
       JSON_LENGTH(wi.LABELS_JSON) AS LABEL_LANGUAGES,
       JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON,
           CONCAT('$.', JSON_UNQUOTE(JSON_EXTRACT(JSON_KEYS(wi.LABELS_JSON), '$[0]'))))) AS FALLBACK_LABEL,
       (SELECT GROUP_CONCAT(DISTINCT m.MOVIE_TITLE SEPARATOR ' | ') FROM T_WC_T2S_MOVIE_SOURCE_WORK l
        JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = l.ID_MOVIE WHERE l.ID_SOURCE_WORK = sw.ID_SOURCE_WORK) AS ADAPTED_MOVIES,
       (SELECT GROUP_CONCAT(DISTINCT s.SERIE_TITLE SEPARATOR ' | ') FROM T_WC_T2S_SERIE_SOURCE_WORK l
        JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = l.ID_SERIE WHERE l.ID_SOURCE_WORK = sw.ID_SOURCE_WORK) AS ADAPTED_SERIES
FROM T_WC_T2S_SOURCE_WORK sw
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = sw.ID_WIKIDATA
WHERE sw.DELETED = 0 AND sw.SOURCE_WORK_NAME IS NULL
ORDER BY sw.MOVIE_COUNT + sw.SERIE_COUNT DESC
LIMIT 30;
