-- Processes 72, 73, 75 ran in ~0 s in the 2026-10-31 14:49 rerun. READ-ONLY.
-- Their skip and error variables, plus where wikidata-crawler stands.

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT VAR_NAME, LEFT(VAR_VALUE, 200) AS VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE DELETED = 0 AND VAR_NAME IN (
    'strtmdbmoviepreprocesslocationskipped',
    'strtmdbmoviepreprocesslocationerror',
    'strtmdbmoviepreprocesssourceworkskipped',
    'strtmdbmoviepreprocesssourceworkerror',
    'strtmdbmoviepreprocessworkfinanceskipped',
    'strtmdbmoviepreprocessworkfinanceerror',
    'strtmdbmoviepreprocessgroupworkerror',
    'strwikidatacrawlerstatus',
    'strwikidatacrawlercurrentprocess',
    'strwikidatacrawlerbulkloadlaststatement')
ORDER BY VAR_NAME;
