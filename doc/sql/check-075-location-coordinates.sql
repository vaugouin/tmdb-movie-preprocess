-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-075 : coordonnees des lieux (LATITUDE, LONGITUDE), controle
-- ============================================================================
--
-- Lecture seule (voie 1, compte agent). A passer avant la migration pour mesurer ce que
-- la regle de choix va ecrire, puis apres le rattrapage et apres la premiere nuit du
-- processus 72 pour verifier ce qui a ete ecrit (section 6, qui sait si les colonnes
-- existent deja).
--
-- La regle de choix, la meme que l'etape 4d de f_buildlocationtables() et que
-- backfill-075-location-coordinates.sql :
--   - P625 (coordinate location) dans T_WC_WIKIDATA_STATEMENT, valeur dans
--     T_WC_WIKIDATA_COORDINATE_VALUE ;
--   - rang 'deprecated' ecarte, declaration DELETED ecartee ;
--   - globe terrestre seulement (GLOBE_ID_WIKIDATA = 'Q2', NULL compte pour la Terre) ;
--   - latitude dans [-90, 90], longitude dans [-180, 180] ;
--   - plusieurs valeurs : 'preferred' d'abord, puis le plus petit ID_STATEMENT (ordre
--     des claims dans le dump), comme f_wikidatabestvaluesql().
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 1. Les globes des P625 portes par les lieux T2S.
SELECT '1. Globes des P625 des lieux' AS SECTION;
SELECT COALESCE(cv.GLOBE_ID_WIKIDATA, '(NULL)') AS GLOBE, COUNT(*) AS STATEMENTS,
       COUNT(DISTINCT l.ID_LOCATION) AS PLACES
FROM T_WC_T2S_LOCATION l
JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = l.ID_WIKIDATA AND st.ID_PROPERTY = 'P625'
JOIN T_WC_WIKIDATA_COORDINATE_VALUE cv ON cv.ID_STATEMENT = st.ID_STATEMENT
GROUP BY cv.GLOBE_ID_WIKIDATA ORDER BY STATEMENTS DESC;

-- 2. Rangs, declarations supprimees, valeurs hors bornes.
SELECT '2. Rangs, DELETED, bornes' AS SECTION;
SELECT COALESCE(st.`RANK`, '(NULL)') AS STATEMENT_RANK, COALESCE(st.DELETED, 0) AS DELETED,
       SUM(cv.LATITUDE IS NULL OR cv.LONGITUDE IS NULL) AS NULL_VALUES,
       SUM(cv.LATITUDE NOT BETWEEN -90 AND 90 OR cv.LONGITUDE NOT BETWEEN -180 AND 180) AS OUT_OF_RANGE,
       COUNT(*) AS STATEMENTS
FROM T_WC_T2S_LOCATION l
JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = l.ID_WIKIDATA AND st.ID_PROPERTY = 'P625'
JOIN T_WC_WIKIDATA_COORDINATE_VALUE cv ON cv.ID_STATEMENT = st.ID_STATEMENT
GROUP BY st.`RANK`, COALESCE(st.DELETED, 0);

-- 3. Lieux a plusieurs P625 retenables, et ecart entre les valeurs.
SELECT '3. Lieux a plusieurs P625' AS SECTION;
SELECT COUNT(*) AS PLACES_WITH_SEVERAL, SUM(N_PREFERRED > 0) AS WITH_A_PREFERRED,
       SUM(SPREAD_DEG > 1) AS SPREAD_OVER_1_DEGREE
FROM (
  SELECT l.ID_LOCATION, COUNT(*) AS N, SUM(st.`RANK` = 'preferred') AS N_PREFERRED,
         GREATEST(MAX(cv.LATITUDE) - MIN(cv.LATITUDE), MAX(cv.LONGITUDE) - MIN(cv.LONGITUDE)) AS SPREAD_DEG
  FROM T_WC_T2S_LOCATION l
  JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = l.ID_WIKIDATA AND st.ID_PROPERTY = 'P625'
       AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated') AND COALESCE(st.DELETED, 0) = 0
  JOIN T_WC_WIKIDATA_COORDINATE_VALUE cv ON cv.ID_STATEMENT = st.ID_STATEMENT
       AND COALESCE(cv.GLOBE_ID_WIKIDATA, 'Q2') = 'Q2'
  GROUP BY l.ID_LOCATION HAVING COUNT(*) > 1) t;

SELECT '3b. Les dix plus grands ecarts' AS SECTION;
SELECT l.ID_LOCATION, l.ID_WIKIDATA, l.LOCATION_NAME, l.LOCATION_TYPE, COUNT(*) AS N,
       SUM(st.`RANK` = 'preferred') AS N_PREFERRED,
       ROUND(GREATEST(MAX(cv.LATITUDE) - MIN(cv.LATITUDE), MAX(cv.LONGITUDE) - MIN(cv.LONGITUDE)), 3) AS SPREAD_DEG
FROM T_WC_T2S_LOCATION l
JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = l.ID_WIKIDATA AND st.ID_PROPERTY = 'P625'
     AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated') AND COALESCE(st.DELETED, 0) = 0
JOIN T_WC_WIKIDATA_COORDINATE_VALUE cv ON cv.ID_STATEMENT = st.ID_STATEMENT
     AND COALESCE(cv.GLOBE_ID_WIKIDATA, 'Q2') = 'Q2'
WHERE l.DELETED = 0
GROUP BY l.ID_LOCATION, l.ID_WIKIDATA, l.LOCATION_NAME, l.LOCATION_TYPE
HAVING COUNT(*) > 1
ORDER BY SPREAD_DEG DESC LIMIT 10;

-- 4. Ce que la regle de choix ecrira : lieux vivants avec une coordonnee, par type.
SELECT '4. Couverture prevue, lieux vivants, par type' AS SECTION;
DROP TEMPORARY TABLE IF EXISTS tmp_check_075;
CREATE TEMPORARY TABLE tmp_check_075 (
  ID_WIKIDATA VARCHAR(20) NOT NULL PRIMARY KEY,
  LATITUDE DECIMAL(9,6) NOT NULL,
  LONGITUDE DECIMAL(9,6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO tmp_check_075 (ID_WIKIDATA, LATITUDE, LONGITUDE)
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

SELECT COALESCE(l.LOCATION_TYPE, '(NULL)') AS LOCATION_TYPE, COUNT(*) AS PLACES,
       SUM(c.ID_WIKIDATA IS NOT NULL) AS WITH_COORDINATES,
       ROUND(100 * SUM(c.ID_WIKIDATA IS NOT NULL) / COUNT(*), 1) AS PCT
FROM T_WC_T2S_LOCATION l
LEFT JOIN tmp_check_075 c ON c.ID_WIKIDATA = l.ID_WIKIDATA
WHERE l.DELETED = 0
GROUP BY l.LOCATION_TYPE
WITH ROLLUP;

SELECT '4b. Lieux supprimes (DELETED = 1) qui en auront une aussi' AS SECTION;
SELECT COUNT(*) AS DELETED_PLACES, SUM(c.ID_WIKIDATA IS NOT NULL) AS WITH_COORDINATES
FROM T_WC_T2S_LOCATION l
LEFT JOIN tmp_check_075 c ON c.ID_WIKIDATA = l.ID_WIKIDATA
WHERE l.DELETED = 1;

-- 5. Trois lieux connus.
SELECT '5. Giverny, Wadi Rum, Tokyo' AS SECTION;
SELECT l.ID_LOCATION, l.ID_WIKIDATA, l.LOCATION_NAME, l.LOCATION_TYPE, c.LATITUDE, c.LONGITUDE
FROM T_WC_T2S_LOCATION l
LEFT JOIN tmp_check_075 c ON c.ID_WIKIDATA = l.ID_WIKIDATA
WHERE l.LOCATION_NAME IN ('Giverny', 'Wadi Rum', 'Tokyo') AND l.DELETED = 0
ORDER BY l.LOCATION_NAME;

-- 6. Une fois les colonnes creees : ce qui est ecrit, et l'accord avec la regle.
SELECT '6. Colonnes presentes ?' AS SECTION;
SELECT COLUMN_NAME, COLUMN_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'T_WC_T2S_LOCATION'
  AND COLUMN_NAME IN ('LATITUDE', 'LONGITUDE');

DROP TEMPORARY TABLE IF EXISTS tmp_check_075;
