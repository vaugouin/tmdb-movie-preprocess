-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-061 : recette apres le premier passage du processus 72 en 1.1.7
-- ============================================================================
--
-- LECTURE SEULE. A lancer apres un passage de la portee main (72 puis 70 et 71).
--
-- ATTENDU :
--   section 1 : les variables du processus 72 datees de la nuit, sans erreur ;
--   section 2 : sur les lieux deja connus avant aujourd'hui, les colonnes que 72
--               remettait a NULL (POSTER_PATH, WIKIPEDIA_IMAGE_PATH, DISPLAY_ORDER,
--               TIM_WIKIDATA_COMPLETED, proprietaires) gardent leurs valeurs ; les
--               lieux nouveaux du jour servent de temoin ;
--   section 3 : les trois echantillons EVALUATIONS-023 (2428, 2429, 2435) rafraichis
--               cette nuit par le processus 70, avec un resultat non vide.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT '1. Variables du processus 72' AS SECTION;

SELECT VAR_NAME, LEFT(VAR_VALUE, 60) AS VAR_VALUE, TIM_UPDATED
FROM T_WC_SERVER_VARIABLE
WHERE DELETED = 0 AND VAR_NAME LIKE 'strtmdbmoviepreprocesslocation%'
ORDER BY VAR_NAME;

SELECT '2. Colonnes recopiees, lieux vivants : anciens contre nouveaux du jour' AS SECTION;

SELECT IF(DAT_CREAT < CURDATE(), 'anciens', 'nouveaux du jour') AS LIEUX,
       COUNT(*)                                 AS N,
       SUM(POSTER_PATH IS NOT NULL)             AS POSTER_PATH,
       SUM(WIKIPEDIA_IMAGE_PATH IS NOT NULL)    AS WIKIPEDIA_IMAGE_PATH,
       SUM(WIKIPEDIA_MAIN_IMAGE_URL IS NOT NULL) AS MAIN_IMAGE_URL,
       SUM(WIKIPEDIA_MAIN_IMAGE_URL_FR IS NOT NULL) AS MAIN_IMAGE_URL_FR,
       SUM(DISPLAY_ORDER IS NOT NULL)           AS DISPLAY_ORDER,
       SUM(TIM_WIKIDATA_COMPLETED IS NOT NULL)  AS TIM_WIKIDATA_COMPLETED,
       MIN(TIM_WIKIDATA_COMPLETED)              AS WIKIDATA_COMPLETED_MIN,
       SUM(ID_CREATOR IS NOT NULL)              AS ID_CREATOR,
       SUM(ID_OWNER IS NOT NULL)                AS ID_OWNER
FROM T_WC_T2S_LOCATION
WHERE DELETED = 0
GROUP BY 1;

SELECT '3. Echantillons 2428, 2429, 2435 apres le processus 70' AS SECTION;

SELECT ID_T2S_EVALUATION, ASSERTION_REFRESH_LAST,
       LEFT(ASSERTIONS_QUERY_RESULT, 200) AS ASSERTIONS_QUERY_RESULT
FROM T_WC_T2S_EVALUATION
WHERE ID_T2S_EVALUATION IN (2428, 2429, 2435);
