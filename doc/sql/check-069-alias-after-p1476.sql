-- TMDB-MOVIE-PREPROCESS-069 / WIKIDATA-CRAWLER-031 / FASTAPI-TEXT2SQL-360 step 3: read-only.
-- After the first Wikidata dump loaded by wikidata-crawler 1.1.14+ and the next nightly
-- process 52, checks that P1476 is loaded and that the served title alias tables now hold
-- ALIAS_SOURCE = 'title' rows, then looks up the title forms behind the lexicon misses of the
-- 1.1.20 campaign (text2sql-eval/claude/ANALYSIS_v1_1_20_lexicon.md) and of -366.
-- Run (SELECT only):
--   ssh vps-agent run tmdb-movie-preprocess/doc/sql/check-069-alias-after-p1476.sql

-- A. P1476 in the V2 statements
SELECT 'A. P1476 statements' AS STEP, COUNT(*) AS N,
       SUM(COALESCE(DELETED, 0) = 0) AS LIVE, MAX(TIM_UPDATED) AS LAST_UPDATE
FROM T_WC_WIKIDATA_STATEMENT WHERE ID_PROPERTY = 'P1476';

-- B. Served alias tables, by source
SELECT 'B. movie alias' AS STEP, ALIAS_SOURCE, COUNT(*) AS N, SUM(COALESCE(DELETED, 0) = 0) AS LIVE,
       MAX(TIM_UPDATED) AS LAST_UPDATE
FROM T_WC_T2S_MOVIE_TITLE_ALIAS GROUP BY ALIAS_SOURCE ORDER BY ALIAS_SOURCE;

SELECT 'B. serie alias' AS STEP, ALIAS_SOURCE, COUNT(*) AS N, SUM(COALESCE(DELETED, 0) = 0) AS LIVE,
       MAX(TIM_UPDATED) AS LAST_UPDATE
FROM T_WC_T2S_SERIE_TITLE_ALIAS GROUP BY ALIAS_SOURCE ORDER BY ALIAS_SOURCE;

-- C. 'title' rows by language (top 10)
SELECT 'C. movie title by lang' AS STEP, LANG_CODE, COUNT(*) AS N
FROM T_WC_T2S_MOVIE_TITLE_ALIAS WHERE ALIAS_SOURCE = 'title'
GROUP BY LANG_CODE ORDER BY N DESC LIMIT 10;

-- D. Aliases of the films behind the misses and -366 (Star Wars 11, Les Visiteurs 2 10353)
SELECT 'D. aliases of known films' AS STEP, a.ID_MOVIE, m.MOVIE_TITLE, a.ALIAS_SOURCE, a.LANG_CODE, a.TITLE
FROM T_WC_T2S_MOVIE_TITLE_ALIAS a JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = a.ID_MOVIE
WHERE a.ID_MOVIE IN (11, 10353) AND COALESCE(a.DELETED, 0) = 0
ORDER BY a.ID_MOVIE, a.ALIAS_SOURCE, a.LANG_CODE;

-- E. The ten title forms that fell to ChromaDB on 1.1.20: any alias row now?
SELECT 'E. miss forms in alias table' AS STEP, q.FORM, a.ID_MOVIE, m.MOVIE_TITLE, a.ALIAS_SOURCE, a.LANG_CODE
FROM (
  SELECT 'detective dee sea dragon' AS FORM UNION ALL
  SELECT 'Young Detective Dee: Rise of the Sea Dragon' UNION ALL
  SELECT 'Le Faucon contre-attaque' UNION ALL
  SELECT 'Vingt-quatre heures pour tuer' UNION ALL
  SELECT 'Les tambours de la rivière Mohawk' UNION ALL
  SELECT 'Trois petites bières' UNION ALL
  SELECT 'La Veuve insatiable' UNION ALL
  SELECT 'Évaluez tous les risques' UNION ALL
  SELECT 'Au-delà de la vallée des poupées' UNION ALL
  SELECT 'Star Wars: Episode IV - A New Hope' UNION ALL
  SELECT 'Star Wars : épisode IV - Un nouvel espoir'
) q
LEFT JOIN T_WC_T2S_MOVIE_TITLE_ALIAS a ON a.TITLE = q.FORM COLLATE utf8mb4_unicode_ci AND COALESCE(a.DELETED, 0) = 0
LEFT JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = a.ID_MOVIE
ORDER BY q.FORM;
