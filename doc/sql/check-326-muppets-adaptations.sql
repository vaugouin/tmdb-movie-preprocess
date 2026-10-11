-- FASTAPI-TEXT2SQL-326: can the base answer "the movie adaptations of The Muppet Show"?
--
-- Wikidata, read 2026-10-11:
--   * only TWO works carry P144 "based on" = The Muppet Show (Q2120540): Muppet Treasure Island (Q1788520, 1996 film)
--     and Pigs in Space (Q105637148); plus the 2026 TV special The Muppet Show (Q137993434).
--   * The Muppet Movie (Q1518589) carries no P144; it is P179 "part of the series" = The Muppets (Q108273055),
--     the film series. So the source-work tables can only ever give Muppet Treasure Island; the films as a
--     whole are a collection question.
-- This file checks what the T2S tables actually hold for both paths.
--
-- Run on the VPS:
--   cd ~/docker/tmdb-movie-preprocess && git pull
--   ~/docker/tools/runsqlvaugouindb.sh ~/docker/tmdb-movie-preprocess/doc/sql/check-326-muppets-adaptations.sql

-- 1. The series in T2S (TMDb id 4459 in the 2026-10-11 log)
SELECT s.ID_SERIE, s.TITLE, s.DAT_FIRST_AIR, s.ID_WIKIDATA
FROM T_WC_TMDB_SERIE s WHERE s.ID_SERIE = 4459;

-- 2. Is The Muppet Show a source work, and is it linked back to its series row?
SELECT sw.*
FROM T_WC_T2S_SOURCE_WORK sw
WHERE sw.ID_WIKIDATA = 'Q2120540' OR sw.ID_SERIE = 4459;

-- 3. Works adapted from it, through the source-work junctions
SELECT 'movie' AS KIND, m.ID_MOVIE AS ID_TMDB, m.TITLE, m.DAT_RELEASE AS DAT
FROM T_WC_T2S_SOURCE_WORK sw
JOIN T_WC_T2S_MOVIE_SOURCE_WORK msw ON msw.ID_SOURCE_WORK = sw.ID_SOURCE_WORK AND (msw.DELETED = 0 OR msw.DELETED IS NULL)
JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = msw.ID_MOVIE
WHERE sw.ID_WIKIDATA = 'Q2120540'
UNION ALL
SELECT 'serie', s.ID_SERIE, s.TITLE, s.DAT_FIRST_AIR
FROM T_WC_T2S_SOURCE_WORK sw
JOIN T_WC_T2S_SERIE_SOURCE_WORK ssw ON ssw.ID_SOURCE_WORK = sw.ID_SOURCE_WORK AND (ssw.DELETED = 0 OR ssw.DELETED IS NULL)
JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = ssw.ID_SERIE
WHERE sw.ID_WIKIDATA = 'Q2120540';

-- 4. The collection path: T2S collections holding The Muppet Movie (Q1518589), and their members
SELECT c.ID_T2S_COLLECTION, c.COLLECTION_SOURCE, c.COLLECTION_NAME, c.MOVIE_COUNT, c.SERIE_COUNT
FROM T_WC_TMDB_MOVIE m
JOIN T_WC_T2S_MOVIE_COLLECTION mc ON mc.ID_MOVIE = m.ID_MOVIE
JOIN T_WC_T2S_COLLECTION c ON c.ID_T2S_COLLECTION = mc.ID_T2S_COLLECTION
WHERE m.ID_WIKIDATA = 'Q1518589';

-- 5. The Wikidata film series The Muppets (Q108273055) as the V2 cache sees it: works carrying P179 = Q108273055
SELECT w.ID_WIKIDATA, m.ID_MOVIE, m.TITLE, m.DAT_RELEASE
FROM T_WC_WIKIDATA_ITEM_VALUE wv
JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT
LEFT JOIN T_WC_TMDB_MOVIE m ON m.ID_WIKIDATA = w.ID_WIKIDATA
WHERE w.ID_PROPERTY = 'P179' AND wv.ID_ITEM = 'Q108273055'
ORDER BY m.DAT_RELEASE;
