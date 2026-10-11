-- The Muppets: point custom list 92 at the existing TMDb collection instead of creating a second one
--
-- custom-collection-muppets-20261011.txt showed that TMDb has "The Muppets Collection" (T2S 1188, 2 films),
-- which check-326 had missed (it looked for a collection holding The Muppet Movie, and this one does not).
-- List 92 was inserted without TMDB_TARGET_RECORD, so the next process 41 run would create a second collection
-- "The Muppets film series" beside it. With the target set, the eight films of the Wikidata film series
-- (P179 Q108273055) are ADDED to collection 1188, like the Blade Runner list 91.
--
-- Run on the VPS before the next nightly run:
--   cd ~/docker/tmdb-movie-preprocess && git pull
--   ~/docker/tools/runsqlvaugouindb.sh ~/docker/tmdb-movie-preprocess/doc/sql/custom-collection-muppets-target.sql

SELECT ID_T2S_COLLECTION, ID_RECORD, COLLECTION_SOURCE, COLLECTION_NAME, MOVIE_COUNT
FROM T_WC_T2S_COLLECTION WHERE ID_T2S_COLLECTION = 1188;

SELECT mc.DISPLAY_ORDER, m.ID_MOVIE, m.TITLE, m.DAT_RELEASE
FROM T_WC_T2S_MOVIE_COLLECTION mc JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = mc.ID_MOVIE
WHERE mc.ID_T2S_COLLECTION = 1188 ORDER BY mc.DISPLAY_ORDER;

UPDATE T_WC_CUSTOM_LIST l
JOIN T_WC_T2S_COLLECTION c ON c.ID_T2S_COLLECTION = 1188 AND c.COLLECTION_SOURCE = 'collection'
SET l.TMDB_TARGET_RECORD = CONCAT('ID_COLLECTION = ', c.ID_RECORD),
    l.LIST_NAME = 'The Muppets film series (additions to the TMDb collection)',
    l.LIST_NAME_FR = 'Les films des Muppets (ajouts à la collection TMDb)',
    l.TIM_UPDATED = NOW()
WHERE l.ID_CUSTOM_LIST = 92 AND l.WIKIDATA_PROPERTIES = 'P179 Q108273055';

-- A custom collection created by an earlier run, if any (must be empty: the nightly run has not happened yet)
SELECT ID_T2S_COLLECTION, COLLECTION_SOURCE, COLLECTION_NAME, ID_RECORD
FROM T_WC_T2S_COLLECTION WHERE COLLECTION_SOURCE = 'custom' AND ID_RECORD = 92;

SELECT ID_CUSTOM_LIST, LIST_NAME, WIKIDATA_PROPERTIES, TMDB_TARGET_RECORD, SORT_BY, TARGET_TABLE
FROM T_WC_CUSTOM_LIST WHERE ID_CUSTOM_LIST = 92;
