-- ============================================================================
-- FASTAPI-TEXT2SQL-179 prerequisite: are the T2S season / episode tables
-- complete and current enough to become the row source of /seasons and /episodes?
-- ============================================================================
--
-- READ ONLY. Checks the output of Processes 27, 28, 29, 31, 32, 33, 34 and 35
-- against their T_WC_TMDB_* sources, under the gates the processes apply:
--   season          : parent serie in T_WC_T2S_SERIE
--   episode         : parent serie in T2S_SERIE and parent season in T2S_SEASON
--   person_season   : person in T2S_PERSON, serie in T2S_SERIE, season in T2S_SEASON
--   person_episode  : the same plus episode in T2S_EPISODE
--   images / videos : parent season (or episode) in T2S
--
-- Reading the result:
--   A  what the swap would stop serving (scope, not a defect)
--   B  rows that pass the gate but are missing from T2S        -> must be 0
--   C  rows present in T2S whose content differs from source   -> must be 0
--   D  rows in T2S that the gate would not admit (orphans)     -> must be 0
--   E  composite-key duplicates, the keys the endpoints use    -> must match source
--   F  credits dropped by the T2S_PERSON gate (information: the endpoints
--      already JOIN T_WC_T2S_PERSON, so the swap loses nothing here)
--
-- In B and C, rows whose source TIM_UPDATED is AFTER the last successful run
-- are latency, not defects: the next run picks them up. Only the "before" column
-- counts. The watermarks are printed first.
--
-- Run on the VPS with ~/docker/tools/runsqlvaugouindb.sh (see ~/Code/tools/AGENTS.md).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;

SELECT NOW() AS measured_at;

SELECT VAR_NAME, VAR_VALUE
FROM T_WC_SERVER_VARIABLE
WHERE VAR_NAME IN ('strtmdbmoviepreprocesst2sseasonlastrun',
                   'strtmdbmoviepreprocesst2sepisodelastrun',
                   'strtmdbmoviepreprocessscope',
                   'strtmdbmoviepreprocesscurrentsubprocess')
ORDER BY VAR_NAME;

SET @wm_season  = (SELECT CAST(VAR_VALUE AS DATETIME) FROM T_WC_SERVER_VARIABLE
                   WHERE VAR_NAME = 'strtmdbmoviepreprocesst2sseasonlastrun' LIMIT 1);
SET @wm_episode = (SELECT CAST(VAR_VALUE AS DATETIME) FROM T_WC_SERVER_VARIABLE
                   WHERE VAR_NAME = 'strtmdbmoviepreprocesst2sepisodelastrun' LIMIT 1);


-- ############################################################################
-- ### A . SCOPE: what the swap stops serving                                ###
-- ############################################################################

SELECT '=== A . scope of the swap ===' AS section;

SELECT 'seasons' AS entity,
       COUNT(*) AS tmdb_total,
       SUM(se.ID_SERIE IS NOT NULL) AS parent_serie_in_t2s,
       SUM(se.ID_SERIE IS NULL) AS no_longer_served
FROM T_WC_TMDB_SEASON s
LEFT JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = s.ID_SERIE
UNION ALL
SELECT 'episodes',
       COUNT(*),
       SUM(se.ID_SERIE IS NOT NULL AND ss.ID_SEASON IS NOT NULL),
       SUM(se.ID_SERIE IS NULL OR ss.ID_SEASON IS NULL)
FROM T_WC_TMDB_EPISODE e
LEFT JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = e.ID_SERIE
LEFT JOIN T_WC_T2S_SEASON ss ON ss.ID_SEASON = e.ID_SEASON;

-- Episodes of a T2S series whose season is absent from T_WC_TMDB_SEASON itself:
-- a crawler hole, which the swap would turn into a missing episode.
SELECT 'episodes of a T2S serie whose ID_SEASON is not in T_WC_TMDB_SEASON' AS check_name,
       COUNT(*) AS n
FROM T_WC_TMDB_EPISODE e
JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = e.ID_SERIE
LEFT JOIN T_WC_TMDB_SEASON s ON s.ID_SEASON = e.ID_SEASON
WHERE s.ID_SEASON IS NULL;


-- ############################################################################
-- ### B . MISSING: passes the gate, absent from T2S                         ###
-- ############################################################################

SELECT '=== B . missing rows ===' AS section;

SELECT 'T2S_SEASON' AS t2s_table,
       SUM(s.TIM_UPDATED <  @wm_season OR @wm_season IS NULL) AS missing_before_last_run,
       SUM(s.TIM_UPDATED >= @wm_season) AS missing_since_last_run
FROM T_WC_TMDB_SEASON s
JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = s.ID_SERIE
LEFT JOIN T_WC_T2S_SEASON t ON t.ID_SEASON = s.ID_SEASON
WHERE t.ID_SEASON IS NULL
UNION ALL
SELECT 'T2S_EPISODE',
       SUM(e.TIM_UPDATED <  @wm_episode OR @wm_episode IS NULL),
       SUM(e.TIM_UPDATED >= @wm_episode)
FROM T_WC_TMDB_EPISODE e
JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = e.ID_SERIE
JOIN T_WC_T2S_SEASON ss ON ss.ID_SEASON = e.ID_SEASON
LEFT JOIN T_WC_T2S_EPISODE t ON t.ID_EPISODE = e.ID_EPISODE
WHERE t.ID_EPISODE IS NULL;

-- Credits, images and videos have no watermark (full pass every run): any
-- missing row is either a defect or a row created since the last run.
SELECT 'T2S_PERSON_SEASON' AS t2s_table, COUNT(*) AS missing
FROM T_WC_TMDB_PERSON_SEASON ps
JOIN T_WC_T2S_PERSON p   ON p.ID_PERSON = ps.ID_PERSON
JOIN T_WC_T2S_SERIE se   ON se.ID_SERIE = ps.ID_SERIE
JOIN T_WC_T2S_SEASON ss  ON ss.ID_SEASON = ps.ID_SEASON
LEFT JOIN T_WC_T2S_PERSON_SEASON t ON t.ID_T2S_PERSON_SEASON = ps.ID_TMDB_PERSON_SEASON
WHERE t.ID_T2S_PERSON_SEASON IS NULL
UNION ALL
SELECT 'T2S_PERSON_EPISODE', COUNT(*)
FROM T_WC_TMDB_PERSON_EPISODE pe
JOIN T_WC_T2S_PERSON p   ON p.ID_PERSON = pe.ID_PERSON
JOIN T_WC_T2S_SERIE se   ON se.ID_SERIE = pe.ID_SERIE
JOIN T_WC_T2S_SEASON ss  ON ss.ID_SEASON = pe.ID_SEASON
JOIN T_WC_T2S_EPISODE ee ON ee.ID_EPISODE = pe.ID_EPISODE
LEFT JOIN T_WC_T2S_PERSON_EPISODE t ON t.ID_T2S_PERSON_EPISODE = pe.ID_TMDB_PERSON_EPISODE
WHERE t.ID_T2S_PERSON_EPISODE IS NULL
UNION ALL
SELECT 'T2S_SEASON_IMAGE', COUNT(*)
FROM T_WC_TMDB_SEASON_IMAGE i
JOIN T_WC_T2S_SEASON ss ON ss.ID_SEASON = i.ID_SEASON
LEFT JOIN T_WC_T2S_SEASON_IMAGE t ON t.ID_ROW = i.ID_ROW
WHERE t.ID_ROW IS NULL
UNION ALL
SELECT 'T2S_EPISODE_IMAGE', COUNT(*)
FROM T_WC_TMDB_EPISODE_IMAGE i
JOIN T_WC_T2S_EPISODE ee ON ee.ID_EPISODE = i.ID_EPISODE
LEFT JOIN T_WC_T2S_EPISODE_IMAGE t ON t.ID_ROW = i.ID_ROW
WHERE t.ID_ROW IS NULL
UNION ALL
SELECT 'T2S_SEASON_VIDEO', COUNT(*)
FROM T_WC_TMDB_SEASON_VIDEO v
JOIN T_WC_T2S_SEASON ss ON ss.ID_SEASON = v.ID_SEASON
LEFT JOIN T_WC_T2S_SEASON_VIDEO t ON t.ID_ROW = v.ID_ROW
WHERE t.ID_ROW IS NULL
UNION ALL
SELECT 'T2S_EPISODE_VIDEO', COUNT(*)
FROM T_WC_TMDB_EPISODE_VIDEO v
JOIN T_WC_T2S_EPISODE ee ON ee.ID_EPISODE = v.ID_EPISODE
LEFT JOIN T_WC_T2S_EPISODE_VIDEO t ON t.ID_ROW = v.ID_ROW
WHERE t.ID_ROW IS NULL;

-- Per series: the 20 T2S series with the most missing seasons or episodes,
-- so a gap concentrated on a few shows is told apart from a diffuse one.
SELECT se.ID_SERIE, se.SERIE_TITLE,
       COUNT(DISTINCT CASE WHEN ts.ID_SEASON IS NULL THEN s.ID_SEASON END) AS missing_seasons,
       COUNT(DISTINCT CASE WHEN ts.ID_SEASON IS NOT NULL AND te.ID_EPISODE IS NULL
                           THEN e.ID_EPISODE END) AS missing_episodes
FROM T_WC_T2S_SERIE se
JOIN T_WC_TMDB_SEASON s ON s.ID_SERIE = se.ID_SERIE
LEFT JOIN T_WC_T2S_SEASON ts ON ts.ID_SEASON = s.ID_SEASON
LEFT JOIN T_WC_TMDB_EPISODE e ON e.ID_SEASON = s.ID_SEASON
LEFT JOIN T_WC_T2S_EPISODE te ON te.ID_EPISODE = e.ID_EPISODE
GROUP BY se.ID_SERIE, se.SERIE_TITLE
HAVING missing_seasons > 0 OR missing_episodes > 0
ORDER BY missing_seasons DESC, missing_episodes DESC
LIMIT 20;


-- ############################################################################
-- ### C . DRIFT: present in T2S, content differs from source                ###
-- ############################################################################

SELECT '=== C . content drift ===' AS section;

SELECT 'T2S_SEASON' AS t2s_table,
       SUM(s.TIM_UPDATED <  @wm_season OR @wm_season IS NULL) AS drift_before_last_run,
       SUM(s.TIM_UPDATED >= @wm_season) AS drift_since_last_run
FROM T_WC_TMDB_SEASON s
JOIN T_WC_T2S_SEASON t ON t.ID_SEASON = s.ID_SEASON
WHERE NOT (t.ID_SERIE <=> s.ID_SERIE AND t.SEASON_NUMBER <=> s.SEASON_NUMBER
       AND t.SEASON_TITLE <=> s.TITLE AND t.OVERVIEW <=> s.OVERVIEW
       AND t.DAT_AIR <=> s.DAT_AIR AND t.POSTER_PATH <=> s.POSTER_PATH
       AND t.EPISODE_COUNT <=> s.EPISODE_COUNT AND t.VOTE_AVERAGE <=> s.VOTE_AVERAGE
       AND t.ID_IMDB <=> s.ID_IMDB AND t.ID_WIKIDATA <=> s.ID_WIKIDATA
       AND t.DELETED <=> s.DELETED)
UNION ALL
SELECT 'T2S_EPISODE',
       SUM(e.TIM_UPDATED <  @wm_episode OR @wm_episode IS NULL),
       SUM(e.TIM_UPDATED >= @wm_episode)
FROM T_WC_TMDB_EPISODE e
JOIN T_WC_T2S_EPISODE t ON t.ID_EPISODE = e.ID_EPISODE
WHERE NOT (t.ID_SERIE <=> e.ID_SERIE AND t.ID_SEASON <=> e.ID_SEASON
       AND t.SEASON_NUMBER <=> e.SEASON_NUMBER AND t.EPISODE_NUMBER <=> e.EPISODE_NUMBER
       AND t.EPISODE_TITLE <=> e.TITLE AND t.OVERVIEW <=> e.OVERVIEW
       AND t.DAT_AIR <=> e.DAT_AIR AND t.RUNTIME <=> e.RUNTIME
       AND t.EPISODE_TYPE <=> e.EPISODE_TYPE AND t.STILL_PATH <=> e.STILL_PATH
       AND t.VOTE_AVERAGE <=> e.VOTE_AVERAGE AND t.VOTE_COUNT <=> e.VOTE_COUNT
       AND t.ID_IMDB <=> e.ID_IMDB AND t.DELETED <=> e.DELETED);

SELECT 'T2S_PERSON_SEASON' AS t2s_table, COUNT(*) AS drift
FROM T_WC_TMDB_PERSON_SEASON ps
JOIN T_WC_T2S_PERSON_SEASON t ON t.ID_T2S_PERSON_SEASON = ps.ID_TMDB_PERSON_SEASON
WHERE NOT (t.ID_PERSON <=> ps.ID_PERSON AND t.ID_SEASON <=> ps.ID_SEASON
       AND t.CREDIT_TYPE <=> ps.CREDIT_TYPE AND t.CAST_CHARACTER <=> ps.CAST_CHARACTER
       AND t.CREW_DEPARTMENT <=> ps.CREW_DEPARTMENT AND t.CREW_JOB <=> ps.CREW_JOB
       AND t.TOTAL_EPISODE_COUNT <=> ps.TOTAL_EPISODE_COUNT
       AND t.DISPLAY_ORDER <=> ps.DISPLAY_ORDER)
UNION ALL
SELECT 'T2S_PERSON_EPISODE', COUNT(*)
FROM T_WC_TMDB_PERSON_EPISODE pe
JOIN T_WC_T2S_PERSON_EPISODE t ON t.ID_T2S_PERSON_EPISODE = pe.ID_TMDB_PERSON_EPISODE
WHERE NOT (t.ID_PERSON <=> pe.ID_PERSON AND t.ID_EPISODE <=> pe.ID_EPISODE
       AND t.CREDIT_TYPE <=> pe.CREDIT_TYPE AND t.CAST_CHARACTER <=> pe.CAST_CHARACTER
       AND t.CREW_DEPARTMENT <=> pe.CREW_DEPARTMENT AND t.CREW_JOB <=> pe.CREW_JOB
       AND t.DISPLAY_ORDER <=> pe.DISPLAY_ORDER);


-- ############################################################################
-- ### D . ORPHANS: in T2S, but the gate would not admit them                ###
-- ############################################################################

SELECT '=== D . orphans ===' AS section;

SELECT 'T2S_SEASON' AS t2s_table, COUNT(*) AS orphans
FROM T_WC_T2S_SEASON t
LEFT JOIN T_WC_TMDB_SEASON s ON s.ID_SEASON = t.ID_SEASON
LEFT JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = t.ID_SERIE
WHERE s.ID_SEASON IS NULL OR se.ID_SERIE IS NULL
UNION ALL
SELECT 'T2S_EPISODE', COUNT(*)
FROM T_WC_T2S_EPISODE t
LEFT JOIN T_WC_TMDB_EPISODE e ON e.ID_EPISODE = t.ID_EPISODE
LEFT JOIN T_WC_T2S_SEASON ss ON ss.ID_SEASON = t.ID_SEASON
WHERE e.ID_EPISODE IS NULL OR ss.ID_SEASON IS NULL
UNION ALL
SELECT 'T2S_PERSON_SEASON', COUNT(*)
FROM T_WC_T2S_PERSON_SEASON t
LEFT JOIN T_WC_TMDB_PERSON_SEASON ps ON ps.ID_TMDB_PERSON_SEASON = t.ID_T2S_PERSON_SEASON
LEFT JOIN T_WC_T2S_PERSON p ON p.ID_PERSON = t.ID_PERSON
LEFT JOIN T_WC_T2S_SEASON ss ON ss.ID_SEASON = t.ID_SEASON
WHERE ps.ID_TMDB_PERSON_SEASON IS NULL OR p.ID_PERSON IS NULL OR ss.ID_SEASON IS NULL
UNION ALL
SELECT 'T2S_PERSON_EPISODE', COUNT(*)
FROM T_WC_T2S_PERSON_EPISODE t
LEFT JOIN T_WC_TMDB_PERSON_EPISODE pe ON pe.ID_TMDB_PERSON_EPISODE = t.ID_T2S_PERSON_EPISODE
LEFT JOIN T_WC_T2S_PERSON p ON p.ID_PERSON = t.ID_PERSON
LEFT JOIN T_WC_T2S_EPISODE ee ON ee.ID_EPISODE = t.ID_EPISODE
WHERE pe.ID_TMDB_PERSON_EPISODE IS NULL OR p.ID_PERSON IS NULL OR ee.ID_EPISODE IS NULL;


-- ############################################################################
-- ### E . COMPOSITE KEYS the endpoints resolve on                           ###
-- ############################################################################

SELECT '=== E . composite-key duplicates ===' AS section;
-- /seasons resolves (ID_SERIE, SEASON_NUMBER) and /episodes resolves
-- (ID_SERIE, SEASON_NUMBER, EPISODE_NUMBER). A duplicate in T2S that is not
-- in the source would make the swap pick a different row.

SELECT 'T2S_SEASON (ID_SERIE, SEASON_NUMBER)' AS key_name, COUNT(*) AS duplicated_keys
FROM (SELECT ID_SERIE, SEASON_NUMBER FROM T_WC_T2S_SEASON
      GROUP BY ID_SERIE, SEASON_NUMBER HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'TMDB_SEASON, same key, T2S series only', COUNT(*)
FROM (SELECT s.ID_SERIE, s.SEASON_NUMBER FROM T_WC_TMDB_SEASON s
      JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = s.ID_SERIE
      GROUP BY s.ID_SERIE, s.SEASON_NUMBER HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'T2S_EPISODE (ID_SERIE, SEASON_NUMBER, EPISODE_NUMBER)', COUNT(*)
FROM (SELECT ID_SERIE, SEASON_NUMBER, EPISODE_NUMBER FROM T_WC_T2S_EPISODE
      GROUP BY ID_SERIE, SEASON_NUMBER, EPISODE_NUMBER HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'TMDB_EPISODE, same key, T2S series only', COUNT(*)
FROM (SELECT e.ID_SERIE, e.SEASON_NUMBER, e.EPISODE_NUMBER FROM T_WC_TMDB_EPISODE e
      JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = e.ID_SERIE
      GROUP BY e.ID_SERIE, e.SEASON_NUMBER, e.EPISODE_NUMBER HAVING COUNT(*) > 1) d;


-- ############################################################################
-- ### F . CREDITS dropped by the T2S_PERSON gate (information)              ###
-- ############################################################################

SELECT '=== F . credits whose person is not in T2S_PERSON ===' AS section;
-- Today's endpoints already JOIN T_WC_T2S_PERSON, so these credits are not
-- served now either. Listed so the number is known, not because it blocks.

SELECT 'PERSON_SEASON' AS source_table, ps.CREDIT_TYPE, COUNT(*) AS dropped
FROM T_WC_TMDB_PERSON_SEASON ps
JOIN T_WC_T2S_SEASON ss ON ss.ID_SEASON = ps.ID_SEASON
LEFT JOIN T_WC_T2S_PERSON p ON p.ID_PERSON = ps.ID_PERSON
WHERE p.ID_PERSON IS NULL
GROUP BY ps.CREDIT_TYPE
UNION ALL
SELECT 'PERSON_EPISODE', pe.CREDIT_TYPE, COUNT(*)
FROM T_WC_TMDB_PERSON_EPISODE pe
JOIN T_WC_T2S_EPISODE ee ON ee.ID_EPISODE = pe.ID_EPISODE
LEFT JOIN T_WC_T2S_PERSON p ON p.ID_PERSON = pe.ID_PERSON
WHERE p.ID_PERSON IS NULL
GROUP BY pe.CREDIT_TYPE;
