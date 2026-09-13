-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-014 : recette de la stabilite d'ID_LOCATION (correctif du 2026-09-13)
-- ============================================================================
--
-- LECTURE SEULE. A lancer DEUX FOIS, avant un passage du processus 72 et apres, en datant
-- les deux sorties, puis comparer. Le bloc A doit etre identique d'une sortie a l'autre.
--
-- LES ATTENTES, posees AVANT le premier passage corrige, pour que la recette puisse echouer :
--   A. les dix temoins gardent leur ID_LOCATION d'un passage a l'autre ;
--   B. MAX(ID_LOCATION) ne diminue jamais, SUPPRIMES ne diminue jamais, et
--      MAX(ID_LOCATION) >= LIGNES (un numero libere n'est jamais reattribue) ;
--   C. aucun ID_WIKIDATA en double (la cle unique le garantit, le bloc le prouve) ;
--   D. aucune association ne pointe sur un lieu DELETED = 1 (attendu 0 et 0) ;
--   E. la liste des sortis du perimetre, pour lecture.
--
-- Avant le correctif, un passage renumerotait tout : le bloc A du 2026-09-12 donnait
-- New York = 2, Los Angeles = 4, Londres = 21, Paris = 38, Californie = 5, Rome = 3,
-- Madrid = 70, Pinewood = 4473, et le passage du 2026-09-13 (240 lieux de plus) les a
-- redistribues. Les numeros du passage qui precede le correctif sont ceux qui restent.
--
-- ⚠ COLLATION. Lancer avec --force.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- A. Les temoins et leur ID_LOCATION
-- ---------------------------------------------------------------------------
SELECT 'A. Les temoins et leur ID_LOCATION' AS SECTION;
SELECT ID_LOCATION, ID_WIKIDATA, LOCATION_NAME, LOCATION_TYPE, DELETED, DAT_CREAT
FROM T_WC_T2S_LOCATION
WHERE ID_WIKIDATA IN ('Q60', 'Q65', 'Q84', 'Q90', 'Q99', 'Q220', 'Q2807', 'Q661896', 'Q334', 'Q2276331')
ORDER BY ID_WIKIDATA;

-- ---------------------------------------------------------------------------
-- B. Le compteur et les supprimes
-- ---------------------------------------------------------------------------
SELECT 'B. Le compteur et les supprimes' AS SECTION;
SELECT COUNT(*)                        AS LIGNES,
       SUM(DELETED = 0)                AS VIVANTS,
       SUM(DELETED = 1)                AS SUPPRIMES,
       MAX(ID_LOCATION)                AS MAX_ID_LOCATION,
       MAX(ID_LOCATION) - COUNT(*)     AS NUMEROS_JAMAIS_ATTRIBUES
FROM T_WC_T2S_LOCATION;

-- ---------------------------------------------------------------------------
-- C. Unicite de la cle metier
-- ---------------------------------------------------------------------------
SELECT 'C. Unicite de la cle metier (attendu 0)' AS SECTION;
SELECT COUNT(*) - COUNT(DISTINCT ID_WIKIDATA) AS ID_WIKIDATA_EN_DOUBLE
FROM T_WC_T2S_LOCATION;

-- ---------------------------------------------------------------------------
-- D. Les associations ne pointent que sur des lieux vivants
-- ---------------------------------------------------------------------------
SELECT 'D. Associations vers un lieu supprime (attendu 0 et 0)' AS SECTION;
SELECT (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_LOCATION ml
        INNER JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = ml.ID_LOCATION
        WHERE l.DELETED = 1) AS FILMS_VERS_SUPPRIME,
       (SELECT COUNT(*) FROM T_WC_T2S_SERIE_LOCATION sl
        INNER JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = sl.ID_LOCATION
        WHERE l.DELETED = 1) AS SERIES_VERS_SUPPRIME;

-- ---------------------------------------------------------------------------
-- E. Les sortis du perimetre, les vingt derniers
-- ---------------------------------------------------------------------------
SELECT 'E. Les sortis du perimetre, les vingt derniers' AS SECTION;
SELECT ID_LOCATION, ID_WIKIDATA, LOCATION_NAME, LOCATION_TYPE, TIM_UPDATED
FROM T_WC_T2S_LOCATION
WHERE DELETED = 1
ORDER BY TIM_UPDATED DESC, ID_LOCATION DESC
LIMIT 20;
