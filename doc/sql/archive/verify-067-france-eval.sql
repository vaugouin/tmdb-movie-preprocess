-- TMDB-MOVIE-PREPROCESS-067 acceptance: after the nightly run of process 72 with 1.1.23, do the
-- three movies that evaluation 2544 ("Movies shot in France", EVALUATIONS-024) expects still
-- reach France (126) through a filming place? Inception (27205), Saving Private Ryan (857) and
-- The Intouchables (77338) are tied to Paris or Calvados, never to France itself.
--
-- Read-only, voie 1. Written 2026-10-22.

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT '1. Filming places of the three movies, and whether each reaches France' AS SECTION;

SELECT j.ID_MOVIE, l.ID_LOCATION, l.LOCATION_NAME, l.LOCATION_TYPE,
       EXISTS (SELECT 1 FROM T_WC_T2S_LOCATION_ANCESTOR o
                WHERE o.ID_LOCATION = l.ID_LOCATION AND o.ID_ANCESTOR = 126) AS REACHES_FRANCE
FROM T_WC_T2S_MOVIE_LOCATION j
JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = j.ID_LOCATION AND l.DELETED = 0
WHERE j.ID_MOVIE IN (27205, 857, 77338) AND j.LOCATION_ROLE = 'filming' AND COALESCE(j.DELETED, 0) = 0
ORDER BY j.ID_MOVIE, REACHES_FRANCE DESC, l.LOCATION_NAME;

SELECT '2. Per movie: filmed in France through the hierarchy' AS SECTION;

SELECT m.ID_MOVIE,
       EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE_LOCATION j
                 JOIN T_WC_T2S_LOCATION_ANCESTOR o ON o.ID_LOCATION = j.ID_LOCATION AND o.ID_ANCESTOR = 126
                WHERE j.ID_MOVIE = m.ID_MOVIE AND j.LOCATION_ROLE = 'filming' AND COALESCE(j.DELETED, 0) = 0) AS FILMED_IN_FRANCE
FROM (SELECT 27205 AS ID_MOVIE UNION ALL SELECT 857 UNION ALL SELECT 77338) m;
