-- 1205 in process 4 (T2S_MOVIE) on 2026-10-31, after the base-copy loop. READ-ONLY.
-- 1. where process 4 stopped (sub process, chunk) and what the run had executed;
-- 2. what wikidata-crawler was doing (status, step, start);
-- 3. transactions still open on the server, longest first.

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT '1. tmdb-movie-preprocess position' AS SECTION;
SELECT VAR_NAME, LEFT(VAR_VALUE, 200) AS VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE DELETED = 0 AND VAR_NAME IN (
    'strtmdbmoviepreprocesscurrentprocess',
    'strtmdbmoviepreprocesscurrentsubprocess',
    'strtmdbmoviepreprocesscurrentmovieid',
    'strtmdbmoviepreprocessprocessesexecuted',
    'strtmdbmoviepreprocessfrom',
    'strtmdbmoviepreprocessstartdatetime',
    'strtmdbmoviepreprocesstotalruntime',
    'strtmdbmoviepreprocesst2smovielastrun')
ORDER BY VAR_NAME;

SELECT '2. wikidata-crawler position' AS SECTION;
SELECT VAR_NAME, LEFT(VAR_VALUE, 200) AS VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE DELETED = 0 AND VAR_NAME LIKE 'strwikidatacrawler%'
  AND (VAR_NAME IN ('strwikidatacrawlerstatus', 'strwikidatacrawlercurrentprocess')
       OR VAR_NAME LIKE 'strwikidatacrawlerstep11%startedat'
       OR VAR_NAME LIKE 'strwikidatacrawlerbulkload%')
ORDER BY VAR_NAME;

SELECT '3. Open transactions' AS SECTION;
SELECT t.trx_id, t.trx_state, t.trx_started,
       TIMESTAMPDIFF(SECOND, t.trx_started, NOW()) AS AGE_S,
       t.trx_rows_locked, t.trx_rows_modified, t.trx_mysql_thread_id,
       p.USER, p.DB, p.COMMAND, p.TIME, LEFT(p.INFO, 160) AS QUERY
FROM information_schema.INNODB_TRX t
LEFT JOIN information_schema.PROCESSLIST p ON p.ID = t.trx_mysql_thread_id
ORDER BY t.trx_started;
