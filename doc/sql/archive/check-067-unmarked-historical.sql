-- TMDB-MOVIE-PREPROCESS-067: P17 parents that are past states but carry none of the ended
-- classes of test-067-cross-border-places.sql, so their children still come out
-- cross-border (check-067-disputed-signals-20261021.txt): Eger, Szekesfehervar and Kecskemet
-- under the First Hungarian Republic and its provisional government, Jamestown under the
-- Confederate States, Masada under Judaea, Chang'an under six dynasties. Their item rows
-- exist, so "count only the known parents" does not help. What do they carry?
-- Also the disputed side: the P31 of the parents met on disputed places (Donetsk People's
-- Republic, Sahrawi Republic, Northern Cyprus, Kosovo, Abkhazia).
--
-- Read-only, voie 1. Written 2026-10-21.

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT '1. Parents: label, P31 classes, and the properties they hold' AS SECTION;

SELECT wi.ID_WIKIDATA, wi.LABEL_EN,
       (SELECT GROUP_CONCAT(DISTINCT CONCAT(v.ID_ITEM, ' ', COALESCE(c.LABEL_EN, '?'), ' (', COALESCE(s.`RANK`, '-'), ')') ORDER BY v.ID_ITEM SEPARATOR ' | ')
          FROM T_WC_WIKIDATA_STATEMENT s JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
          LEFT JOIN T_WC_WIKIDATA_ITEM c ON c.ID_WIKIDATA = v.ID_ITEM
         WHERE s.ID_WIKIDATA = wi.ID_WIKIDATA AND s.ID_PROPERTY = 'P31' AND s.DELETED = 0) AS P31,
       (SELECT GROUP_CONCAT(DISTINCT s.ID_PROPERTY ORDER BY s.ID_PROPERTY SEPARATOR ' ')
          FROM T_WC_WIKIDATA_STATEMENT s WHERE s.ID_WIKIDATA = wi.ID_WIKIDATA AND s.DELETED = 0) AS PROPERTIES
FROM T_WC_WIKIDATA_ITEM wi
WHERE wi.ID_WIKIDATA IN (
  SELECT DISTINCT iv.ID_ITEM FROM T_WC_WIKIDATA_STATEMENT st
  JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
  WHERE st.ID_PROPERTY = 'P17' AND st.DELETED = 0
    AND st.ID_WIKIDATA IN ('Q167109', 'Q130212', 'Q171357', 'Q323813', 'Q186312', 'Q6501000', 'Q457242',
                           'Q4409143', 'Q47837', 'Q644636', 'Q178168', 'Q211318', 'Q1093762'))
ORDER BY wi.LABEL_EN;

SELECT '2. The same classes on living countries: how many of the 30 best-known countries carry each' AS SECTION;

SELECT v.ID_ITEM AS P31_CLASS, c.LABEL_EN, COUNT(DISTINCT l.ID_WIKIDATA) AS LIVING_COUNTRIES,
       GROUP_CONCAT(DISTINCT l.LOCATION_NAME ORDER BY l.LOCATION_NAME SEPARATOR ', ') AS WHICH
FROM (SELECT ID_WIKIDATA, LOCATION_NAME FROM T_WC_T2S_LOCATION
      WHERE DELETED = 0 AND LOCATION_TYPE = 'country' ORDER BY MOVIE_COUNT DESC LIMIT 30) l
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA = l.ID_WIKIDATA AND s.ID_PROPERTY = 'P31' AND s.DELETED = 0
JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM c ON c.ID_WIKIDATA = v.ID_ITEM
GROUP BY v.ID_ITEM, c.LABEL_EN
ORDER BY LIVING_COUNTRIES DESC;
