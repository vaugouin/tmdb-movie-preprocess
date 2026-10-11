-- Blade Runner: complete the TMDb collection with the rest of the franchise (custom list, TARGET_TABLE = 2)
--
-- Why. The T2S collection 1777 (https://www.vaugouin.com/tmdb/t2scollection.php?collectionid=1777) is the
-- TMDb "Blade Runner Collection": two films. Asked "the other movies in this franchise" on 2026-10-11
-- (shoot 11-38-10), the voice agent answered with those two, while the franchise also counts three 2017
-- short films and two series (FASTAPI-TEXT2SQL-327).
--
-- How. A T_WC_CUSTOM_LIST row with TARGET_TABLE = 2 is read by tmdb-movie-preprocess process 41 (sub-process 5).
-- With TMDB_TARGET_RECORD = 'ID_COLLECTION = <TMDb collection id>' its members are ADDED to that existing
-- T2S collection instead of creating a new one (tmdb-movie-preprocess.py, around lines 1735-1760 and 1850).
-- Two membership mechanisms, cumulative:
--   * mechanism 2, WIKIDATA_PROPERTIES = 'P8345 Q48724847': every work whose Wikidata "media franchise" is
--     Blade Runner (checked on Wikidata 2026-10-11: the 1982 film, 2049, 2036: Nexus Dawn, 2048: Nowhere to Run,
--     Black Out 2022, Black Lotus and 2099 all carry it), read in the V2 statements, so future works join alone;
--   * mechanism 1, ID_IMDB_LIST: the same seven works by IMDb id, as a belt for any work whose V2 statements
--     are not cached yet.
-- Both mechanisms keep only works present in T2S, non-adult, with an ID_WIKIDATA on the TMDb side.
-- Caveat: with a Q in WIKIDATA_PROPERTIES the custom row would normally give the collection its ID_WIKIDATA;
-- with a target record the TMDb collection row is reused and keeps its own fields.
--
-- Run on the VPS:
--   cd ~/docker/tmdb-movie-preprocess && git pull
--   ~/docker/tools/runsqlvaugouindb.sh ~/docker/tmdb-movie-preprocess/doc/sql/custom-collection-blade-runner.sql
-- then the next process 41 run (or a run limited to it) attaches the members; part 3 checks the result.

-- ---------------------------------------------------------------------------------------------------------
-- Part 1. Before: the target collection and the candidate works as the base knows them
-- ---------------------------------------------------------------------------------------------------------

SELECT ID_T2S_COLLECTION, ID_RECORD, COLLECTION_SOURCE, COLLECTION_NAME, MOVIE_COUNT, SERIE_COUNT
FROM T_WC_T2S_COLLECTION
WHERE ID_T2S_COLLECTION = 1777;

-- Each candidate: present in TMDb? with a Wikidata id? in T2S? already in collection 1777?
SELECT 'movie' AS KIND, c.ID_IMDB, m.ID_MOVIE AS ID_TMDB, m.TITLE, m.DAT_RELEASE AS DAT, m.ID_WIKIDATA, m.ADULT,
       (SELECT COUNT(*) FROM T_WC_T2S_MOVIE t WHERE t.ID_MOVIE = m.ID_MOVIE) AS IN_T2S,
       (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_COLLECTION mc WHERE mc.ID_MOVIE = m.ID_MOVIE AND mc.ID_T2S_COLLECTION = 1777) AS IN_1777
FROM (SELECT 'tt0083658' AS ID_IMDB UNION ALL SELECT 'tt1856101' UNION ALL SELECT 'tt7326248'
      UNION ALL SELECT 'tt7387408' UNION ALL SELECT 'tt7428594') c
LEFT JOIN T_WC_TMDB_MOVIE m ON m.ID_IMDB = c.ID_IMDB
UNION ALL
SELECT 'serie', c.ID_IMDB, s.ID_SERIE, s.TITLE, s.DAT_FIRST_AIR, s.ID_WIKIDATA, s.ADULT,
       (SELECT COUNT(*) FROM T_WC_T2S_SERIE t WHERE t.ID_SERIE = s.ID_SERIE),
       (SELECT COUNT(*) FROM T_WC_T2S_SERIE_COLLECTION sc WHERE sc.ID_SERIE = s.ID_SERIE AND sc.ID_T2S_COLLECTION = 1777)
FROM (SELECT 'tt9359796' AS ID_IMDB UNION ALL SELECT 'tt18224594') c
LEFT JOIN T_WC_TMDB_SERIE s ON s.ID_IMDB = c.ID_IMDB;

-- Does the V2 cache hold the "media franchise = Blade Runner" statements (mechanism 2)?
SELECT w.ID_WIKIDATA, w.ID_PROPERTY, wv.ID_ITEM, w.`RANK`
FROM T_WC_WIKIDATA_ITEM_VALUE wv
JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT
WHERE w.ID_PROPERTY = 'P8345' AND wv.ID_ITEM = 'Q48724847';

-- ---------------------------------------------------------------------------------------------------------
-- Part 2. The custom list, inserted once (re-running the file does not duplicate it)
-- ---------------------------------------------------------------------------------------------------------

INSERT INTO T_WC_CUSTOM_LIST
  (LIST_NAME, LIST_NAME_FR, OVERVIEW, WIKIDATA_PROPERTIES, ID_IMDB_LIST, TMDB_ELEMENTS, TMDB_TARGET_RECORD,
   DELETED, DISPLAY_ORDER, ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
   POSTER_PATH, WIKIPEDIA_IMAGE_PATH, SORT_BY, TARGET_TABLE)
SELECT
  'Blade Runner franchise (additions to the TMDb collection)',
  'Univers Blade Runner (ajouts à la collection TMDb)',
  'The whole Blade Runner franchise: the two films, the three 2017 short films that bridge them (2036: Nexus Dawn, 2048: Nowhere to Run, Black Out 2022) and the series Blade Runner: Black Lotus and Blade Runner 2099.',
  'P8345 Q48724847',
  'https://www.imdb.com/title/tt0083658/\r\nhttps://www.imdb.com/title/tt1856101/\r\nhttps://www.imdb.com/title/tt7326248/\r\nhttps://www.imdb.com/title/tt7387408/\r\nhttps://www.imdb.com/title/tt7428594/\r\nhttps://www.imdb.com/title/tt9359796/\r\nhttps://www.imdb.com/title/tt18224594/\r\n',
  '',
  CONCAT('ID_COLLECTION = ', c.ID_RECORD),
  0, NULL, 1, CURDATE(), 1, NOW(), 1,
  NULL, NULL, 5, 2
FROM T_WC_T2S_COLLECTION c
WHERE c.ID_T2S_COLLECTION = 1777
  AND c.COLLECTION_SOURCE = 'collection'
  AND NOT EXISTS (SELECT 1 FROM T_WC_CUSTOM_LIST l
                  WHERE l.TARGET_TABLE = 2 AND l.DELETED = 0
                    AND l.TMDB_TARGET_RECORD = CONCAT('ID_COLLECTION = ', c.ID_RECORD));

SELECT ID_CUSTOM_LIST, LIST_NAME, WIKIDATA_PROPERTIES, TMDB_TARGET_RECORD, SORT_BY, TARGET_TABLE, DELETED
FROM T_WC_CUSTOM_LIST
WHERE TARGET_TABLE = 2 AND LIST_NAME LIKE 'Blade Runner franchise%';

-- ---------------------------------------------------------------------------------------------------------
-- Part 3. After the next process 41 run (re-run this file with -f the day after: part 2 is then a no-op)
-- ---------------------------------------------------------------------------------------------------------

SELECT 'movie' AS KIND, mc.DISPLAY_ORDER, m.ID_MOVIE AS ID_TMDB, m.TITLE, m.DAT_RELEASE AS DAT
FROM T_WC_T2S_MOVIE_COLLECTION mc JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = mc.ID_MOVIE
WHERE mc.ID_T2S_COLLECTION = 1777
UNION ALL
SELECT 'serie', sc.DISPLAY_ORDER, s.ID_SERIE, s.TITLE, s.DAT_FIRST_AIR
FROM T_WC_T2S_SERIE_COLLECTION sc JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = sc.ID_SERIE
WHERE sc.ID_T2S_COLLECTION = 1777
ORDER BY KIND, DISPLAY_ORDER;
