-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-075 : rattrapage de LATITUDE et LONGITUDE sur T_WC_T2S_LOCATION
-- ============================================================================
--
-- Ecriture sur T_WC_T2S_LOCATION : voie 2, a lancer par Philippe, APRES
-- migration-075-location-coordinates.sql et une fois le code 1.1.59 deploye. Une seule
-- fois : il donne les coordonnees aux lieux deja servis sans attendre la nuit ; ensuite
-- l'etape 4d du processus 72 les recalcule a chaque reconstruction, avec la meme regle.
-- Rejouable sans dommage (meme resultat).
--
-- La regle, identique a l'etape 4d de f_buildlocationtables() :
--   - P625 dans T_WC_WIKIDATA_STATEMENT, valeur dans T_WC_WIKIDATA_COORDINATE_VALUE ;
--   - rang 'deprecated' ecarte, declaration DELETED ecartee ;
--   - globe terrestre seulement (Q2, ou NULL) ;
--   - latitude dans [-90, 90], longitude dans [-180, 180] ;
--   - plusieurs valeurs : 'preferred' d'abord, puis le plus petit ID_STATEMENT ;
--   - les deux colonnes viennent de la meme declaration.
-- Une table temporaire porte le choix : MariaDB refuse de lire dans une table derivee la
-- table qu'il met a jour (1093).
--
-- Attendu (controle check-075-location-coordinates du 2026-11-12) : environ 11 700 lieux
-- vivants sur 13 075 recoivent une coordonnee ; la fiction et les lieux sans type en
-- restent presque tous prives.
--
-- ⚠ Ne pas lancer pendant le processus 72 (sa bascule remplacerait la table) ni pendant
-- un chargement de wikidata-crawler.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 1. Avant.
SELECT '1. Avant' AS SECTION;
SELECT COUNT(*) AS PLACES, SUM(DELETED = 0) AS LIVING,
       SUM(LATITUDE IS NOT NULL) AS WITH_COORDINATES,
       SUM(DELETED = 0 AND LATITUDE IS NOT NULL) AS LIVING_WITH_COORDINATES
FROM T_WC_T2S_LOCATION;

-- 2. Le choix d'une coordonnee par lieu.
DROP TEMPORARY TABLE IF EXISTS tmp_lieu_coordonnees;
CREATE TEMPORARY TABLE tmp_lieu_coordonnees (
  ID_WIKIDATA VARCHAR(20) NOT NULL PRIMARY KEY,
  LATITUDE DECIMAL(9,6) NOT NULL,
  LONGITUDE DECIMAL(9,6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_lieu_coordonnees (ID_WIKIDATA, LATITUDE, LONGITUDE)
SELECT r.ID_WIKIDATA, ROUND(r.LATITUDE, 6), ROUND(r.LONGITUDE, 6)
FROM (
  SELECT st.ID_WIKIDATA, cv.LATITUDE, cv.LONGITUDE,
         ROW_NUMBER() OVER (PARTITION BY st.ID_WIKIDATA
                            ORDER BY (st.`RANK` = 'preferred') DESC, st.ID_STATEMENT ASC) AS RN
  FROM T_WC_T2S_LOCATION l
  JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = l.ID_WIKIDATA AND st.ID_PROPERTY = 'P625'
       AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated') AND COALESCE(st.DELETED, 0) = 0
  JOIN T_WC_WIKIDATA_COORDINATE_VALUE cv ON cv.ID_STATEMENT = st.ID_STATEMENT
       AND COALESCE(cv.GLOBE_ID_WIKIDATA, 'Q2') = 'Q2'
       AND cv.LATITUDE BETWEEN -90 AND 90 AND cv.LONGITUDE BETWEEN -180 AND 180) r
WHERE r.RN = 1;

SELECT '2. Coordonnees retenues' AS SECTION;
SELECT COUNT(*) AS CHOSEN FROM tmp_lieu_coordonnees;

-- 3. L'ecriture. Toutes les lignes : un lieu sans coordonnee retenue repasse a NULL.
-- TIM_UPDATED n'est pas touche : la nuit suivante le reecrit de toute facon.
UPDATE T_WC_T2S_LOCATION loc
LEFT JOIN tmp_lieu_coordonnees c ON c.ID_WIKIDATA = loc.ID_WIKIDATA
SET loc.LATITUDE = c.LATITUDE, loc.LONGITUDE = c.LONGITUDE;

DROP TEMPORARY TABLE IF EXISTS tmp_lieu_coordonnees;

-- 4. Apres.
SELECT '4. Apres' AS SECTION;
SELECT COUNT(*) AS PLACES, SUM(DELETED = 0) AS LIVING,
       SUM(LATITUDE IS NOT NULL) AS WITH_COORDINATES,
       SUM(DELETED = 0 AND LATITUDE IS NOT NULL) AS LIVING_WITH_COORDINATES,
       SUM((LATITUDE IS NULL) <> (LONGITUDE IS NULL)) AS HALF_FILLED_MUST_BE_0
FROM T_WC_T2S_LOCATION;

SELECT '4b. Apres, lieux vivants par type' AS SECTION;
SELECT COALESCE(LOCATION_TYPE, '(NULL)') AS LOCATION_TYPE, COUNT(*) AS PLACES,
       SUM(LATITUDE IS NOT NULL) AS WITH_COORDINATES
FROM T_WC_T2S_LOCATION WHERE DELETED = 0
GROUP BY LOCATION_TYPE ORDER BY PLACES DESC;

-- 5. Relecture de trois lieux connus : Giverny (49.08, 1.53), Wadi Rum (29.6, 35.4),
-- Tokyo (35.69, 139.69).
SELECT '5. Giverny, Wadi Rum, Tokyo' AS SECTION;
SELECT ID_LOCATION, ID_WIKIDATA, LOCATION_NAME, LOCATION_TYPE, LATITUDE, LONGITUDE
FROM T_WC_T2S_LOCATION
WHERE LOCATION_NAME IN ('Giverny', 'Wadi Rum', 'Tokyo') AND DELETED = 0
ORDER BY LOCATION_NAME;
