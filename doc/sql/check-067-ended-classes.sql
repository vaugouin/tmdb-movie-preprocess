-- TMDB-MOVIE-PREPROCESS-067: which of the five "ended" P31 classes catch living countries.
--
-- Read-only, voie 1. Written 2026-10-21. The third run of test-067-cross-border-places.sql
-- marked the United States as ended and emptied its sheet (15 556 movies to 420). For each
-- class, the P17 parents that carry it, and among them those that are also a country or a
-- sovereign state (Q6256, Q3624078), the living ones to protect.

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT '1. P31 of the United States (Q30)' AS SECTION;

SELECT iv.ID_ITEM, wi.LABEL_EN, s.`RANK`
FROM T_WC_WIKIDATA_STATEMENT s
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = iv.ID_ITEM
WHERE s.ID_WIKIDATA = 'Q30' AND s.ID_PROPERTY = 'P31' AND s.DELETED = 0;

SELECT '2. Per class: P17 parents carrying it, and those also typed country or sovereign state' AS SECTION;

SELECT c.ID_ITEM AS CLASS, COUNT(DISTINCT s.ID_WIKIDATA) AS PARENTS,
       COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s2
                                         JOIN T_WC_WIKIDATA_ITEM_VALUE i2 ON i2.ID_STATEMENT = s2.ID_STATEMENT
                                        WHERE s2.ID_WIKIDATA = s.ID_WIKIDATA AND s2.ID_PROPERTY = 'P31' AND s2.DELETED = 0
                                          AND i2.ID_ITEM IN ('Q6256', 'Q3624078')) THEN s.ID_WIKIDATA END) AS ALSO_COUNTRY,
       GROUP_CONCAT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s2
                                         JOIN T_WC_WIKIDATA_ITEM_VALUE i2 ON i2.ID_STATEMENT = s2.ID_STATEMENT
                                        WHERE s2.ID_WIKIDATA = s.ID_WIKIDATA AND s2.ID_PROPERTY = 'P31' AND s2.DELETED = 0
                                          AND i2.ID_ITEM IN ('Q6256', 'Q3624078')) THEN wi.LABEL_EN END ORDER BY wi.LABEL_EN SEPARATOR ', ') AS ALSO_COUNTRY_NAMES
FROM (SELECT 'Q3024240' AS ID_ITEM UNION ALL SELECT 'Q11514315' UNION ALL SELECT 'Q2577883'
      UNION ALL SELECT 'Q99541706' UNION ALL SELECT 'Q19953632') c
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_ITEM = c.ID_ITEM
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_STATEMENT = iv.ID_STATEMENT AND s.ID_PROPERTY = 'P31' AND s.DELETED = 0
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = s.ID_WIKIDATA
WHERE s.ID_WIKIDATA IN (SELECT iv3.ID_ITEM FROM T_WC_WIKIDATA_STATEMENT s3
                         JOIN T_WC_WIKIDATA_ITEM_VALUE iv3 ON iv3.ID_STATEMENT = s3.ID_STATEMENT
                        WHERE s3.ID_PROPERTY = 'P17' AND s3.DELETED = 0)
GROUP BY c.ID_ITEM;
