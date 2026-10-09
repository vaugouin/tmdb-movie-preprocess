-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-055 : acceptance after a pass of Process 73 (source works)
-- ============================================================================
--
-- READ ONLY. Run after tmdb-movie-preprocess-source-works.sh (or a main pass).
-- Expectations are written BEFORE the run, next to each section, so the result is
-- judged against them and not read into them.
--
--   A. Volumes: about 17,450 live sources, about 17,600 movie links and 7,600 serie
--      links (measurement of 2026-10-07: 25,222 links). DELETED = 1 is 0 on the first
--      pass.
--   B. Integrity: every count of this section must be 0.
--   C. The closed vocabularies: no value outside the lists.
--   D. Distribution by type and form, to compare with run 2 of 2026-10-07
--      (literary 10,142 + light_novel 443, screen 3,239, comic 1,588 manga + 477,
--      game 350, stage 216 musical + 152 play + 16, folklore 87, other ~742).
--   E. The witnesses of the ticket.
--   F. The ten evaluation questions of FASTAPI-TEXT2SQL-320, as plain SQL: each must
--      return rows (except question 10, which must return none for Pulp Fiction).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
SELECT 'A. Volumes' AS SECTION;

SELECT DELETED, COUNT(*) AS SOURCES,
       SUM(SOURCE_WORK_NAME IS NULL) AS WITHOUT_NAME,
       SUM(SOURCE_WORK_NAME_FR = SOURCE_WORK_NAME) AS FR_EQUALS_EN,
       SUM(ID_MOVIE IS NOT NULL) AS SOURCE_IS_T2S_MOVIE,
       SUM(ID_SERIE IS NOT NULL) AS SOURCE_IS_T2S_SERIE,
       SUM(SOURCE_WORK_YEAR IS NOT NULL) AS WITH_YEAR,
       MIN(ID_SOURCE_WORK) AS MIN_ID, MAX(ID_SOURCE_WORK) AS MAX_ID
FROM T_WC_T2S_SOURCE_WORK
GROUP BY DELETED;

SELECT (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_SOURCE_WORK) AS MOVIE_LINKS,
       (SELECT COUNT(DISTINCT ID_MOVIE) FROM T_WC_T2S_MOVIE_SOURCE_WORK) AS MOVIES_WITH_A_SOURCE,
       (SELECT COUNT(*) FROM T_WC_T2S_SERIE_SOURCE_WORK) AS SERIE_LINKS,
       (SELECT COUNT(DISTINCT ID_SERIE) FROM T_WC_T2S_SERIE_SOURCE_WORK) AS SERIES_WITH_A_SOURCE;

-- ---------------------------------------------------------------------------
SELECT 'B. Integrity, every count must be 0' AS SECTION;

SELECT
  -- a link pointing to a source that does not exist, or that is deleted
  (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_SOURCE_WORK l
   LEFT JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
   WHERE sw.ID_SOURCE_WORK IS NULL OR sw.DELETED = 1) AS MOVIE_LINK_ORPHAN_SOURCE,
  (SELECT COUNT(*) FROM T_WC_T2S_SERIE_SOURCE_WORK l
   LEFT JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
   WHERE sw.ID_SOURCE_WORK IS NULL OR sw.DELETED = 1) AS SERIE_LINK_ORPHAN_SOURCE,
  -- a link pointing to a work that does not exist
  (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_SOURCE_WORK l
   LEFT JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = l.ID_MOVIE WHERE m.ID_MOVIE IS NULL) AS MOVIE_LINK_ORPHAN_WORK,
  (SELECT COUNT(*) FROM T_WC_T2S_SERIE_SOURCE_WORK l
   LEFT JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = l.ID_SERIE WHERE s.ID_SERIE IS NULL) AS SERIE_LINK_ORPHAN_WORK,
  -- a source whose T2S hop does not point to the same Wikidata item
  (SELECT COUNT(*) FROM T_WC_T2S_SOURCE_WORK sw
   JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = sw.ID_MOVIE
   WHERE m.ID_WIKIDATA <> sw.ID_WIKIDATA) AS WRONG_MOVIE_HOP,
  (SELECT COUNT(*) FROM T_WC_T2S_SOURCE_WORK sw
   JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = sw.ID_SERIE
   WHERE s.ID_WIKIDATA <> sw.ID_WIKIDATA) AS WRONG_SERIE_HOP,
  -- a work linked to itself
  (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_SOURCE_WORK l
   JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
   WHERE sw.ID_MOVIE = l.ID_MOVIE) AS MOVIE_SELF_LINK,
  -- a live source with no adaptation left
  (SELECT COUNT(*) FROM T_WC_T2S_SOURCE_WORK
   WHERE DELETED = 0 AND COALESCE(MOVIE_COUNT, 0) + COALESCE(SERIE_COUNT, 0) = 0) AS LIVE_SOURCE_WITHOUT_LINK;

-- The anti-join of the ticket: a T2S film carrying a live P144 to a known source has
-- its link row. Must be 0 (self-links excluded on purpose).
SELECT COUNT(*) AS MOVIE_P144_WITHOUT_LINK
FROM T_WC_WIKIDATA_STATEMENT st
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
JOIN T_WC_T2S_MOVIE m ON m.ID_WIKIDATA = st.ID_WIKIDATA
JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_WIKIDATA = iv.ID_ITEM AND sw.DELETED = 0
WHERE st.ID_PROPERTY = 'P144'
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND COALESCE(st.DELETED, 0) = 0
  AND NOT (sw.ID_MOVIE <=> m.ID_MOVIE)
  AND NOT EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE_SOURCE_WORK l
                  WHERE l.ID_MOVIE = m.ID_MOVIE AND l.ID_SOURCE_WORK = sw.ID_SOURCE_WORK);

-- ---------------------------------------------------------------------------
SELECT 'C. Closed vocabularies, every count must be 0' AS SECTION;

SELECT SUM(SOURCE_WORK_TYPE NOT IN ('literary','comic','stage','game','screen','folklore','other')
           OR SOURCE_WORK_TYPE IS NULL) AS TYPE_OUT_OF_LIST,
       SUM(SOURCE_WORK_FORM IS NOT NULL
           AND SOURCE_WORK_FORM NOT IN ('manga','light_novel','play','musical','novel','short_story')) AS FORM_OUT_OF_LIST,
       SUM(SOURCE_WORK_FORM = 'manga' AND SOURCE_WORK_TYPE <> 'comic')
     + SUM(SOURCE_WORK_FORM IN ('play','musical') AND SOURCE_WORK_TYPE <> 'stage')
     + SUM(SOURCE_WORK_FORM IN ('light_novel','novel','short_story') AND SOURCE_WORK_TYPE <> 'literary') AS FORM_TYPE_MISMATCH
FROM T_WC_T2S_SOURCE_WORK WHERE DELETED = 0;

-- ---------------------------------------------------------------------------
SELECT 'D. Distribution by type and form' AS SECTION;

SELECT SOURCE_WORK_TYPE, COALESCE(SOURCE_WORK_FORM, '') AS SOURCE_WORK_FORM,
       COUNT(*) AS SOURCES, SUM(MOVIE_COUNT) AS MOVIE_LINKS, SUM(SERIE_COUNT) AS SERIE_LINKS
FROM T_WC_T2S_SOURCE_WORK WHERE DELETED = 0
GROUP BY SOURCE_WORK_TYPE, SOURCE_WORK_FORM
ORDER BY SOURCES DESC;

-- The cone sizes, rank by rank: a cone in the hundreds of thousands is a top class.
SELECT CONE_RANK, SOURCE_WORK_TYPE, COALESCE(SOURCE_WORK_FORM, '') AS SOURCE_WORK_FORM, COUNT(*) AS CLASSES
FROM T_WC_T2S_SOURCE_WORK_CLASS
GROUP BY CONE_RANK, SOURCE_WORK_TYPE, SOURCE_WORK_FORM
ORDER BY CONE_RANK;

-- ---------------------------------------------------------------------------
SELECT 'E. Witnesses' AS SECTION;

-- The Shining (694) -> the novel, literary; Arrival (329865) -> Story of Your Life,
-- literary; Scarface 1983 (111) -> the novel AND the 1932 film (screen, with ID_MOVIE);
-- The Last of Us (series 100088) -> the video game, game.
SELECT 'movie' AS KIND, l.ID_MOVIE AS ID_WORK, m.MOVIE_TITLE AS TITLE,
       sw.ID_SOURCE_WORK, sw.SOURCE_WORK_NAME, sw.SOURCE_WORK_NAME_FR, sw.SOURCE_WORK_TYPE,
       sw.SOURCE_WORK_FORM, sw.SOURCE_WORK_YEAR, sw.ID_MOVIE AS SOURCE_ID_MOVIE, sw.ID_SERIE AS SOURCE_ID_SERIE
FROM T_WC_T2S_MOVIE_SOURCE_WORK l
JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = l.ID_MOVIE
JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
WHERE l.ID_MOVIE IN (694, 329865, 111)
UNION ALL
SELECT 'serie', l.ID_SERIE, s.SERIE_TITLE,
       sw.ID_SOURCE_WORK, sw.SOURCE_WORK_NAME, sw.SOURCE_WORK_NAME_FR, sw.SOURCE_WORK_TYPE,
       sw.SOURCE_WORK_FORM, sw.SOURCE_WORK_YEAR, sw.ID_MOVIE, sw.ID_SERIE
FROM T_WC_T2S_SERIE_SOURCE_WORK l
JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = l.ID_SERIE
JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
WHERE l.ID_SERIE IN (100088);

-- The three book covers of 2026-10-07 (FASTAPI-TEXT2SQL-318, VIDEO-SHOWCASE-085):
-- Dracula (Q41542, 30 films and 3 series measured), Heart of Darkness (French name
-- "Au coeur des tenebres"), Nineteen Eighty-Four. Each must be found by its name.
SELECT sw.ID_SOURCE_WORK, sw.ID_WIKIDATA, sw.SOURCE_WORK_NAME, sw.SOURCE_WORK_NAME_FR,
       sw.SOURCE_WORK_TYPE, sw.SOURCE_WORK_FORM, sw.SOURCE_WORK_YEAR, sw.MOVIE_COUNT, sw.SERIE_COUNT
FROM T_WC_T2S_SOURCE_WORK sw
WHERE sw.DELETED = 0
  AND (sw.ID_WIKIDATA = 'Q41542'
       OR sw.SOURCE_WORK_NAME IN ('Heart of Darkness', 'Nineteen Eighty-Four')
       OR sw.SOURCE_WORK_NAME_FR LIKE 'Au c%ur des t%n%bres')
ORDER BY sw.MOVIE_COUNT DESC;

-- ---------------------------------------------------------------------------
SELECT 'F. The ten evaluation questions of FASTAPI-TEXT2SQL-320, as plain SQL' AS SECTION;

-- 1. Remakes of Scarface (1932): films whose source is the 1932 film.
SELECT 'Q1 remakes of Scarface 1932' AS Q, m.ID_MOVIE, m.MOVIE_TITLE, m.RELEASE_YEAR
FROM T_WC_T2S_MOVIE_SOURCE_WORK l
JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
JOIN T_WC_T2S_MOVIE src ON src.ID_MOVIE = sw.ID_MOVIE
JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = l.ID_MOVIE
WHERE src.MOVIE_TITLE = 'Scarface' AND src.RELEASE_YEAR = 1932;

-- 3. Series adapted from a movie, the 15 most popular.
SELECT 'Q3 series from a movie' AS Q, s.ID_SERIE, s.SERIE_TITLE, sw.SOURCE_WORK_NAME
FROM T_WC_T2S_SERIE_SOURCE_WORK l
JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = l.ID_SERIE
WHERE sw.ID_MOVIE IS NOT NULL
ORDER BY s.POPULARITY DESC LIMIT 15;

-- 4. Movies adapted from a series rated higher than the series.
SELECT 'Q4 movie rated above its source series' AS Q, m.MOVIE_TITLE, m.IMDB_RATING, src.SERIE_TITLE, src.IMDB_RATING AS SOURCE_RATING
FROM T_WC_T2S_MOVIE_SOURCE_WORK l
JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
JOIN T_WC_T2S_SERIE src ON src.ID_SERIE = sw.ID_SERIE
JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = l.ID_MOVIE
WHERE m.IMDB_RATING > src.IMDB_RATING AND src.IMDB_RATING > 0
ORDER BY m.IMDB_RATING DESC LIMIT 15;

-- 5. The most adapted works, all kinds together.
SELECT 'Q5 most adapted' AS Q, sw.SOURCE_WORK_NAME, sw.SOURCE_WORK_TYPE, sw.MOVIE_COUNT, sw.SERIE_COUNT,
       sw.MOVIE_COUNT + sw.SERIE_COUNT AS ADAPTATIONS
FROM T_WC_T2S_SOURCE_WORK sw WHERE sw.DELETED = 0
ORDER BY ADAPTATIONS DESC LIMIT 15;

-- 6. Movies and series based on video games (counts).
SELECT 'Q6 based on video games' AS Q,
       (SELECT COUNT(DISTINCT l.ID_MOVIE) FROM T_WC_T2S_MOVIE_SOURCE_WORK l
        JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK WHERE sw.SOURCE_WORK_TYPE = 'game') AS MOVIES,
       (SELECT COUNT(DISTINCT l.ID_SERIE) FROM T_WC_T2S_SERIE_SOURCE_WORK l
        JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK WHERE sw.SOURCE_WORK_TYPE = 'game') AS SERIES;

-- 7. Movies adapted from a musical, the 15 most popular.
SELECT 'Q7 based on a musical' AS Q, m.MOVIE_TITLE, m.RELEASE_YEAR, sw.SOURCE_WORK_NAME
FROM T_WC_T2S_MOVIE_SOURCE_WORK l
JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = l.ID_MOVIE
WHERE sw.SOURCE_WORK_FORM = 'musical'
ORDER BY m.POPULARITY DESC LIMIT 15;

-- 8. Movies adapted from "Au coeur des tenebres" (found by its French name).
SELECT 'Q8 adapted from Au coeur des tenebres' AS Q, m.MOVIE_TITLE, m.RELEASE_YEAR
FROM T_WC_T2S_MOVIE_SOURCE_WORK l
JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = l.ID_MOVIE
WHERE sw.SOURCE_WORK_NAME_FR LIKE 'Au c%ur des t%n%bres';

-- 10. Pulp Fiction (680) must have no source: 0 rows expected.
SELECT 'Q10 Pulp Fiction source, expected empty' AS Q, sw.SOURCE_WORK_NAME
FROM T_WC_T2S_MOVIE_SOURCE_WORK l
JOIN T_WC_T2S_SOURCE_WORK sw ON sw.ID_SOURCE_WORK = l.ID_SOURCE_WORK
WHERE l.ID_MOVIE = 680;
