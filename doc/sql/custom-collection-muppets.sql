-- The Muppets: a T2S collection for the Muppet film series, which TMDb does not have (custom list, TARGET_TABLE = 2)
--
-- Why. On 2026-10-11 (shoot 11-21-11) "the movie adaptation of this show" found nothing for The Muppet Show.
-- archive/check-326-muppets-adaptations-20261011.txt settles where the films are:
--   * the source-work tables link only Muppet Treasure Island (1996) and the 2026 special to the series
--     (Wikidata P144 "based on");
--   * no T2S collection holds The Muppet Movie (part 4 of the check is empty);
--   * the V2 cache holds eight films with P179 "part of the series" = The Muppets (Q108273055), from
--     The Muppet Movie (1979) to Muppets Most Wanted (2014).
-- So the eight films become a collection through mechanism 2 of custom collections (process 41, sub-process 5):
-- WIKIDATA_PROPERTIES = 'P179 Q108273055'. No TMDB_TARGET_RECORD: there is no TMDb collection to complete, so a new
-- T2S collection is created (COLLECTION_SOURCE = 'custom'), and its ID_WIKIDATA becomes Q108273055.
-- SORT_BY = 5: release date ascending. Future Muppet films join alone once Wikidata carries the statement.
--
-- Run on the VPS:
--   cd ~/docker/tmdb-movie-preprocess && git pull
--   ~/docker/tools/runsqlvaugouindb.sh ~/docker/tmdb-movie-preprocess/doc/sql/custom-collection-muppets.sql
-- then the next process 41 run builds the collection; part 3 checks it (re-run with -f the day after).

-- Part 1. Before: is a Muppets collection already there?
SELECT ID_T2S_COLLECTION, COLLECTION_SOURCE, COLLECTION_NAME, ID_WIKIDATA, MOVIE_COUNT
FROM T_WC_T2S_COLLECTION
WHERE COLLECTION_NAME LIKE '%Muppet%' OR ID_WIKIDATA = 'Q108273055';

-- Part 2. The custom list, inserted once
INSERT INTO T_WC_CUSTOM_LIST
  (LIST_NAME, LIST_NAME_FR, OVERVIEW, WIKIDATA_PROPERTIES, ID_IMDB_LIST, TMDB_ELEMENTS, TMDB_TARGET_RECORD,
   DELETED, DISPLAY_ORDER, ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
   POSTER_PATH, WIKIPEDIA_IMAGE_PATH, SORT_BY, TARGET_TABLE)
SELECT
  'The Muppets film series',
  'Les films des Muppets',
  'The feature films of the Muppets, from The Muppet Movie (1979) onwards, the film series born of The Muppet Show.',
  'P179 Q108273055',
  '',
  '',
  NULL,
  0, NULL, 1, CURDATE(), 1, NOW(), 1,
  NULL, NULL, 5, 2
FROM DUAL
WHERE NOT EXISTS (SELECT 1 FROM T_WC_CUSTOM_LIST l
                  WHERE l.TARGET_TABLE = 2 AND l.DELETED = 0 AND l.WIKIDATA_PROPERTIES = 'P179 Q108273055');

SELECT ID_CUSTOM_LIST, LIST_NAME, WIKIDATA_PROPERTIES, TMDB_TARGET_RECORD, SORT_BY, TARGET_TABLE, DELETED
FROM T_WC_CUSTOM_LIST
WHERE TARGET_TABLE = 2 AND WIKIDATA_PROPERTIES = 'P179 Q108273055';

-- Part 3. After the next process 41 run
SELECT c.ID_T2S_COLLECTION, c.COLLECTION_NAME, mc.DISPLAY_ORDER, m.ID_MOVIE, m.TITLE, m.DAT_RELEASE
FROM T_WC_T2S_COLLECTION c
JOIN T_WC_T2S_MOVIE_COLLECTION mc ON mc.ID_T2S_COLLECTION = c.ID_T2S_COLLECTION
JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = mc.ID_MOVIE
WHERE c.COLLECTION_SOURCE = 'custom' AND c.ID_WIKIDATA = 'Q108273055'
ORDER BY mc.DISPLAY_ORDER;
