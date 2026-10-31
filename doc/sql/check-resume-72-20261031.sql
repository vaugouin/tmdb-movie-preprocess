-- Resumed run ./tmdb-movie-preprocess.sh 72 on 2026-10-31, after wikidata-crawler finished. READ-ONLY.
-- 1. run position: resume point, processes executed, start, runtime;
-- 2. skip and error variables of 72, 73, 74, 75 and groupwork (all expected empty);
-- 3. wikidata-crawler status (expected SUCCESS).

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT '1. Run position' AS SECTION;
SELECT VAR_NAME, LEFT(VAR_VALUE, 300) AS VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE DELETED = 0 AND VAR_NAME IN (
    'strtmdbmoviepreprocessfrom',
    'strtmdbmoviepreprocessscope',
    'strtmdbmoviepreprocessprocessesexecuted',
    'strtmdbmoviepreprocesscurrentprocess',
    'strtmdbmoviepreprocesscurrentsubprocess',
    'strtmdbmoviepreprocessstartdatetime',
    'strtmdbmoviepreprocessenddatetime',
    'strtmdbmoviepreprocesstotalruntime')
ORDER BY VAR_NAME;

SELECT '2. Skip and error variables' AS SECTION;
SELECT VAR_NAME, LEFT(VAR_VALUE, 300) AS VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE DELETED = 0 AND VAR_NAME LIKE 'strtmdbmoviepreprocess%'
  AND (VAR_NAME LIKE '%skipped' OR VAR_NAME LIKE '%error')
ORDER BY VAR_NAME;

SELECT '3. wikidata-crawler' AS SECTION;
SELECT VAR_NAME, LEFT(VAR_VALUE, 200) AS VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE DELETED = 0 AND VAR_NAME IN ('strwikidatacrawlerstatus', 'strwikidatacrawlercurrentprocess')
ORDER BY VAR_NAME;

SELECT '4. Variables written in the last 3 hours' AS SECTION;
SELECT VAR_NAME, LEFT(VAR_VALUE, 120) AS VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE DELETED = 0 AND VAR_NAME LIKE 'strtmdbmoviepreprocess%'
  AND TIM_UPDATED >= NOW() - INTERVAL 3 HOUR
ORDER BY TIM_UPDATED;
