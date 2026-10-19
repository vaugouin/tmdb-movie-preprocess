-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-048 : la hierarchie des lieux, mesuree avant d'etre construite
-- ============================================================================
--
-- LECTURE SEULE (tables temporaires seulement). A passer avant de coder
-- T_WC_T2S_LOCATION_ANCESTOR.
--
-- LES QUESTIONS.
--   1. Les lieux de reference (Paris Q90, Ile-de-France Q13917, France Q142, Lyon Q456,
--      Marseille Q23482, la Lune Q405) sont-ils des lignes de T_WC_T2S_LOCATION ?
--   2. Ce que Paris porte en P131 et P17, avec le rang : le ticket dit six P131 et sept
--      P17, il faut voir ce que le meilleur rang en garde.
--   3. Combien de lieux vivants portent au moins un P131 ou un P17.
--   4. La chaine tient-elle au-dela d'un saut ? WIKIDATA-CRAWLER-021 donne P131 et P17
--      aux items EN CACHE, c'est-a-dire cites par une entite du perimetre. Un parent qui
--      n'est cite que par un autre item en cache (Ile-de-France, cite par Paris) peut
--      n'avoir aucune declaration, et la remontee s'arreterait a lui.
--   5. La fermeture simulee : lignes, profondeurs, combien de lieux atteignent un ancetre
--      qui est lui-meme un lieu T2S, et ce que rendraient « en France » et « a Paris ».
--
-- LA REGLE DU MEILLEUR RANG, appliquee par (item, propriete) : si l'item porte au moins
-- une declaration 'preferred' pour la propriete, seules celles-la comptent ; sinon toutes
-- les non 'deprecated'. C'est la lecture « truthy » de Wikidata.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 1. Les lieux de reference.
SELECT '1. Lieux de reference' AS SECTION;

SELECT q.QID, q.ATTENDU, l.ID_LOCATION, l.LOCATION_NAME, l.LOCATION_TYPE, l.DELETED,
       l.MOVIE_COUNT, l.SERIE_COUNT, l.EPISODE_COUNT
FROM (SELECT 'Q90' AS QID, 'Paris' AS ATTENDU UNION ALL SELECT 'Q13917', 'Ile-de-France'
      UNION ALL SELECT 'Q142', 'France' UNION ALL SELECT 'Q456', 'Lyon'
      UNION ALL SELECT 'Q23482', 'Marseille' UNION ALL SELECT 'Q405', 'Moon'
      UNION ALL SELECT 'Q183', 'Germany' UNION ALL SELECT 'Q30', 'United States'
      UNION ALL SELECT 'Q60', 'New York City' UNION ALL SELECT 'Q99', 'California') q
LEFT JOIN T_WC_T2S_LOCATION l ON l.ID_WIKIDATA = q.QID;

-- 2. Paris : ses P131 et P17, rang compris.
SELECT '2. Paris (Q90), P131 et P17' AS SECTION;

SELECT st.ID_PROPERTY, iv.ID_ITEM, wi.LABEL_EN, st.`RANK`, st.DELETED
FROM T_WC_WIKIDATA_STATEMENT st
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = iv.ID_ITEM
WHERE st.ID_WIKIDATA = 'Q90' AND st.ID_PROPERTY IN ('P131', 'P17')
ORDER BY st.ID_PROPERTY, st.`RANK`, iv.ID_ITEM;

-- Les aretes au meilleur rang, sur tout V2 (toutes les declarations P131 / P17).
DROP TEMPORARY TABLE IF EXISTS tmp_arete;
CREATE TEMPORARY TABLE tmp_arete (
  ID_CHILD  VARCHAR(50) NOT NULL,
  ID_PARENT VARCHAR(50) NOT NULL,
  PRIMARY KEY (ID_CHILD, ID_PARENT),
  KEY (ID_PARENT)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO tmp_arete (ID_CHILD, ID_PARENT)
SELECT st.ID_WIKIDATA, iv.ID_ITEM
FROM T_WC_WIKIDATA_STATEMENT st
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
WHERE st.ID_PROPERTY IN ('P131', 'P17')
  AND st.DELETED = 0
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND (st.`RANK` = 'preferred'
       OR NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT p
                      WHERE p.ID_WIKIDATA = st.ID_WIKIDATA AND p.ID_PROPERTY = st.ID_PROPERTY
                        AND p.`RANK` = 'preferred' AND p.DELETED = 0))
  AND iv.ID_ITEM <> st.ID_WIKIDATA;

SELECT '2b. Aretes au meilleur rang' AS SECTION;

SELECT COUNT(*) AS ARETES, COUNT(DISTINCT ID_CHILD) AS ENFANTS, COUNT(DISTINCT ID_PARENT) AS PARENTS
FROM tmp_arete;

SELECT a.ID_PARENT, wi.LABEL_EN FROM tmp_arete a
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = a.ID_PARENT
WHERE a.ID_CHILD = 'Q90';

-- 3. Les lieux vivants qui ont au moins un parent.
SELECT '3. Lieux vivants avec au moins un parent' AS SECTION;

SELECT COUNT(*) AS LIEUX_VIVANTS,
       SUM(EXISTS (SELECT 1 FROM tmp_arete a WHERE a.ID_CHILD = l.ID_WIKIDATA)) AS AVEC_PARENT,
       SUM(l.LOCATION_TYPE = 'country') AS PAYS
FROM T_WC_T2S_LOCATION l WHERE l.DELETED = 0;

SELECT COALESCE(l.LOCATION_TYPE, '(null)') AS LOCATION_TYPE, COUNT(*) AS LIEUX,
       SUM(EXISTS (SELECT 1 FROM tmp_arete a WHERE a.ID_CHILD = l.ID_WIKIDATA)) AS AVEC_PARENT
FROM T_WC_T2S_LOCATION l WHERE l.DELETED = 0
GROUP BY l.LOCATION_TYPE ORDER BY LIEUX DESC;

-- 4. Au-dela d'un saut : les parents directs ont-ils eux-memes un parent ?
SELECT '4. Parents directs des lieux, et leurs propres parents' AS SECTION;

SELECT COUNT(DISTINCT a.ID_PARENT) AS PARENTS_DIRECTS,
       COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM tmp_arete a2 WHERE a2.ID_CHILD = a.ID_PARENT)
                           THEN a.ID_PARENT END) AS AYANT_UN_PARENT,
       COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM T_WC_T2S_LOCATION l2 WHERE l2.ID_WIKIDATA = a.ID_PARENT AND l2.DELETED = 0)
                           THEN a.ID_PARENT END) AS QUI_SONT_DES_LIEUX_T2S
FROM tmp_arete a
JOIN T_WC_T2S_LOCATION l ON l.ID_WIKIDATA = a.ID_CHILD AND l.DELETED = 0;

-- 5. La fermeture simulee, sur les QID, profondeur bornee a 10.
SELECT '5. Fermeture simulee' AS SECTION;

DROP TEMPORARY TABLE IF EXISTS tmp_ancetre;
CREATE TEMPORARY TABLE tmp_ancetre (
  ID_LIEU    VARCHAR(50) NOT NULL,
  ID_ANCETRE VARCHAR(50) NOT NULL,
  DEPTH      INT NOT NULL,
  PRIMARY KEY (ID_LIEU, ID_ANCETRE),
  KEY (DEPTH, ID_ANCETRE)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_ancetre SELECT ID_WIKIDATA, ID_WIKIDATA, 0 FROM T_WC_T2S_LOCATION WHERE DELETED = 0;
INSERT IGNORE INTO tmp_ancetre SELECT t.ID_LIEU, a.ID_PARENT, 1 FROM tmp_ancetre t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 0;
INSERT IGNORE INTO tmp_ancetre SELECT t.ID_LIEU, a.ID_PARENT, 2 FROM tmp_ancetre t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 1;
INSERT IGNORE INTO tmp_ancetre SELECT t.ID_LIEU, a.ID_PARENT, 3 FROM tmp_ancetre t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 2;
INSERT IGNORE INTO tmp_ancetre SELECT t.ID_LIEU, a.ID_PARENT, 4 FROM tmp_ancetre t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 3;
INSERT IGNORE INTO tmp_ancetre SELECT t.ID_LIEU, a.ID_PARENT, 5 FROM tmp_ancetre t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 4;
INSERT IGNORE INTO tmp_ancetre SELECT t.ID_LIEU, a.ID_PARENT, 6 FROM tmp_ancetre t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 5;
INSERT IGNORE INTO tmp_ancetre SELECT t.ID_LIEU, a.ID_PARENT, 7 FROM tmp_ancetre t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 6;
INSERT IGNORE INTO tmp_ancetre SELECT t.ID_LIEU, a.ID_PARENT, 8 FROM tmp_ancetre t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 7;
INSERT IGNORE INTO tmp_ancetre SELECT t.ID_LIEU, a.ID_PARENT, 9 FROM tmp_ancetre t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 8;
INSERT IGNORE INTO tmp_ancetre SELECT t.ID_LIEU, a.ID_PARENT, 10 FROM tmp_ancetre t JOIN tmp_arete a ON a.ID_CHILD = t.ID_ANCETRE WHERE t.DEPTH = 9;

-- Lignes ajoutees a chaque profondeur ; une profondeur 10 non vide signale une chaine
-- anormalement longue ou un cycle.
SELECT DEPTH, COUNT(*) AS LIGNES, COUNT(DISTINCT ID_LIEU) AS LIEUX
FROM tmp_ancetre GROUP BY DEPTH ORDER BY DEPTH;

-- Ce que garderait la table : les deux bouts sont des lieux T2S vivants.
SELECT COUNT(*) AS LIGNES_GARDEES, COUNT(DISTINCT t.ID_LIEU) AS LIEUX,
       SUM(t.DEPTH > 0) AS LIGNES_AU_DESSUS_DE_0,
       COUNT(DISTINCT CASE WHEN t.DEPTH > 0 THEN t.ID_LIEU END) AS LIEUX_AVEC_ANCETRE_T2S
FROM tmp_ancetre t
JOIN T_WC_T2S_LOCATION la ON la.ID_WIKIDATA = t.ID_ANCETRE AND la.DELETED = 0;

-- Les ancetres de Paris, tous puis ceux qui sont des lieux T2S.
SELECT t.ID_ANCETRE, t.DEPTH, wi.LABEL_EN, la.ID_LOCATION
FROM tmp_ancetre t
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = t.ID_ANCETRE
LEFT JOIN T_WC_T2S_LOCATION la ON la.ID_WIKIDATA = t.ID_ANCETRE AND la.DELETED = 0
WHERE t.ID_LIEU = 'Q90' ORDER BY t.DEPTH, t.ID_ANCETRE;

-- Les ancetres T2S les plus peuples, pour reperer une racine absurde (un continent, la Terre).
SELECT t.ID_ANCETRE, la.LOCATION_NAME, la.LOCATION_TYPE, COUNT(*) AS DESCENDANTS, MIN(t.DEPTH) AS DMIN, MAX(t.DEPTH) AS DMAX
FROM tmp_ancetre t
JOIN T_WC_T2S_LOCATION la ON la.ID_WIKIDATA = t.ID_ANCETRE AND la.DELETED = 0
WHERE t.DEPTH > 0
GROUP BY t.ID_ANCETRE, la.LOCATION_NAME, la.LOCATION_TYPE
ORDER BY DESCENDANTS DESC LIMIT 25;

-- 6. Les questions de recette, en avant-premiere.
SELECT '6. Recette simulee' AS SECTION;

-- Films tournes en France : rattachement direct, puis par la hierarchie.
SELECT 'filming / France' AS CAS,
       COUNT(DISTINCT CASE WHEN l.ID_WIKIDATA = 'Q142' THEN ml.ID_MOVIE END) AS FILMS_DIRECTS,
       COUNT(DISTINCT ml.ID_MOVIE) AS FILMS_PAR_HIERARCHIE
FROM T_WC_T2S_MOVIE_LOCATION ml
JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = ml.ID_LOCATION
JOIN tmp_ancetre t ON t.ID_LIEU = l.ID_WIKIDATA AND t.ID_ANCETRE = 'Q142'
WHERE ml.LOCATION_ROLE = 'filming' AND COALESCE(ml.DELETED, 0) = 0;

SELECT 'narrative / France' AS CAS,
       COUNT(DISTINCT CASE WHEN l.ID_WIKIDATA = 'Q142' THEN ml.ID_MOVIE END) AS FILMS_DIRECTS,
       COUNT(DISTINCT ml.ID_MOVIE) AS FILMS_PAR_HIERARCHIE,
       COUNT(DISTINCT CASE WHEN l.ID_WIKIDATA = 'Q90' THEN ml.ID_MOVIE END) AS DONT_PARIS,
       COUNT(DISTINCT CASE WHEN l.ID_WIKIDATA = 'Q23482' THEN ml.ID_MOVIE END) AS DONT_MARSEILLE
FROM T_WC_T2S_MOVIE_LOCATION ml
JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = ml.ID_LOCATION
JOIN tmp_ancetre t ON t.ID_LIEU = l.ID_WIKIDATA AND t.ID_ANCETRE = 'Q142'
WHERE ml.LOCATION_ROLE = 'narrative' AND COALESCE(ml.DELETED, 0) = 0;

-- Films tournes a Paris : aucun ne doit l'etre seulement a Lyon.
SELECT 'filming / Paris' AS CAS, COUNT(DISTINCT ml.ID_MOVIE) AS FILMS_PAR_HIERARCHIE,
       SUM(l.ID_WIKIDATA = 'Q456') AS LIGNES_LYON
FROM T_WC_T2S_MOVIE_LOCATION ml
JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = ml.ID_LOCATION
JOIN tmp_ancetre t ON t.ID_LIEU = l.ID_WIKIDATA AND t.ID_ANCETRE = 'Q90'
WHERE ml.LOCATION_ROLE = 'filming' AND COALESCE(ml.DELETED, 0) = 0;

-- Les lieux contenus dans Paris qui portent des films (les plus cites).
SELECT l.ID_LOCATION, l.LOCATION_NAME, l.LOCATION_TYPE, t.DEPTH, l.MOVIE_COUNT
FROM tmp_ancetre t
JOIN T_WC_T2S_LOCATION l ON l.ID_WIKIDATA = t.ID_LIEU AND l.DELETED = 0
WHERE t.ID_ANCETRE = 'Q90' AND t.DEPTH > 0
ORDER BY l.MOVIE_COUNT DESC LIMIT 15;

-- La Lune reste une feuille isolee.
SELECT t.ID_LIEU, t.ID_ANCETRE, t.DEPTH FROM tmp_ancetre t WHERE t.ID_LIEU = 'Q405';

DROP TEMPORARY TABLE IF EXISTS tmp_ancetre;
DROP TEMPORARY TABLE IF EXISTS tmp_arete;
