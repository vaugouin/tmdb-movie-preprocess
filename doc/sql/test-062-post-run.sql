-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-062 : recette apres le premier passage avec les episodes
-- ============================================================================
--
-- LECTURE SEULE. A lancer apres migration-062-episode-location.sql ET un passage du
-- processus 72 en 1.1.8 (le journal doit dire « episodes inclus »).
--
-- ATTENDU, d'apres la mesure du 2026-10-17 (test-062-episode-locations-20261017.txt) :
--   section 1 : environ 3 378 liens, de l'ordre de 3 370 episodes, deux roles ;
--   section 2 : environ 440 lieux vivants avec EPISODE_COUNT > 0 et ni film ni serie ;
--   section 3 : aucun lien orphelin ;
--   section 4 : les ID_LOCATION des lieux deja connus n'ont pas bouge (comparer avec
--               test-014-location-id-stability) ;
--   section 5 : The West Wing (688) porte des lieux propres a ses episodes.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT '1. Liens episode-lieu par role' AS SECTION;

SELECT LOCATION_ROLE, COUNT(*) AS LIENS,
       COUNT(DISTINCT ID_EPISODE) AS EPISODES, COUNT(DISTINCT ID_LOCATION) AS LIEUX
FROM T_WC_T2S_EPISODE_LOCATION
GROUP BY LOCATION_ROLE;

SELECT '2. Lieux vivants par origine' AS SECTION;

SELECT COUNT(*)                                                         AS LIEUX_VIVANTS,
       SUM(COALESCE(EPISODE_COUNT, 0) > 0)                              AS AVEC_EPISODE,
       SUM(COALESCE(EPISODE_COUNT, 0) > 0 AND COALESCE(MOVIE_COUNT, 0) = 0
           AND COALESCE(SERIE_COUNT, 0) = 0)                            AS EPISODES_SEULEMENT,
       SUM(COALESCE(MOVIE_COUNT, 0) = 0 AND COALESCE(SERIE_COUNT, 0) = 0
           AND COALESCE(EPISODE_COUNT, 0) = 0)                          AS SANS_AUCUNE_OEUVRE
FROM T_WC_T2S_LOCATION
WHERE DELETED = 0;

SELECT '3. Liens orphelins (doit etre 0, 0)' AS SECTION;

SELECT SUM(l.ID_LOCATION IS NULL) AS SANS_LIEU,
       SUM(e.ID_EPISODE IS NULL)  AS SANS_EPISODE
FROM T_WC_T2S_EPISODE_LOCATION el
LEFT JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = el.ID_LOCATION AND l.DELETED = 0
LEFT JOIN T_WC_T2S_EPISODE  e ON e.ID_EPISODE  = el.ID_EPISODE;

SELECT '4. Identifiants : lieux crees aujourd hui' AS SECTION;

SELECT COUNT(*) AS LIEUX_CREES_AUJOURD_HUI, MIN(ID_LOCATION) AS PREMIER, MAX(ID_LOCATION) AS DERNIER
FROM T_WC_T2S_LOCATION
WHERE DAT_CREAT = CURDATE();

SELECT '5. Exemple : The West Wing, lieux des episodes que la serie ne porte pas' AS SECTION;

SELECT e.SEASON_NUMBER, e.EPISODE_NUMBER, e.EPISODE_TITLE, el.LOCATION_ROLE, l.LOCATION_NAME
FROM T_WC_T2S_EPISODE_LOCATION el
JOIN T_WC_T2S_EPISODE  e ON e.ID_EPISODE  = el.ID_EPISODE
JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = el.ID_LOCATION
WHERE e.ID_SERIE = 688
  AND NOT EXISTS (SELECT 1 FROM T_WC_T2S_SERIE_LOCATION sl
                  WHERE sl.ID_SERIE = 688 AND sl.ID_LOCATION = el.ID_LOCATION
                    AND sl.LOCATION_ROLE = el.LOCATION_ROLE)
ORDER BY e.SEASON_NUMBER, e.EPISODE_NUMBER
LIMIT 25;
