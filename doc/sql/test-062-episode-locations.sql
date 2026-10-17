-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-062 : ce que les episodes apporteraient aux lieux
-- ============================================================================
--
-- LECTURE SEULE. A passer avant de coder la table T_WC_T2S_EPISODE_LOCATION.
--
-- LA QUESTION. La mesure du 2026-09-11 (test-014-location-subjects-20260911.txt) a
-- compte 1 636 lieux et 3 889 citations portes seulement par des episodes, mais elle
-- partait de T_WC_WIKIDATA_EPISODE, la table V2. Le processus 72 rapproche les sujets
-- par ID_WIKIDATA sur les tables T2S. Ce qui compte est donc : combien de ces episodes
-- Wikidata T2S connait-il, par T_WC_T2S_EPISODE.ID_WIKIDATA ?
--
-- PIEGE CONNU. Du 2026-07-12 au correctif de wikidata-crawler, les episodes etaient
-- classes comme series en V2. Le rapprochement par ID_WIKIDATA sur T2S ne depend pas de
-- la table V2 de rangement, c'est pourquoi la section 2 part des statements, pas de
-- T_WC_WIKIDATA_EPISODE.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 1. Les episodes T2S qui portent un QID.
SELECT '1. Episodes T2S avec ID_WIKIDATA' AS SECTION;

SELECT COUNT(*)                                   AS EPISODES,
       SUM(ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '') AS AVEC_QID,
       COUNT(DISTINCT NULLIF(ID_WIKIDATA, ''))    AS QID_DISTINCTS,
       SUM(COALESCE(DELETED, 0) = 1)              AS SUPPRIMES
FROM T_WC_T2S_EPISODE;

-- 2. Les citations P840 / P915 dont le sujet est un episode T2S, par role.
SELECT '2. Citations portees par un episode T2S' AS SECTION;

SELECT st.ID_PROPERTY,
       COUNT(*)                        AS CITATIONS,
       COUNT(DISTINCT e.ID_EPISODE)    AS EPISODES,
       COUNT(DISTINCT e.ID_SERIE)      AS SERIES,
       COUNT(DISTINCT iv.ID_ITEM)      AS LIEUX
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
INNER JOIN T_WC_T2S_EPISODE e ON e.ID_WIKIDATA = st.ID_WIKIDATA
WHERE st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
GROUP BY st.ID_PROPERTY;

-- 3. Les lieux NOUVEAUX : cites par un episode T2S et absents de T_WC_T2S_LOCATION vivant.
SELECT '3. Lieux nouveaux apportes par les episodes' AS SECTION;

SELECT COUNT(DISTINCT iv.ID_ITEM) AS LIEUX_NOUVEAUX
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
INNER JOIN T_WC_T2S_EPISODE e ON e.ID_WIKIDATA = st.ID_WIKIDATA
WHERE st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND NOT EXISTS (SELECT 1 FROM T_WC_T2S_LOCATION l
                  WHERE l.ID_WIKIDATA = iv.ID_ITEM AND l.DELETED = 0);

-- 4. Les episodes Wikidata qui citent un lieu mais que T2S ne connait pas : le
--    rapprochement manquant. Sujets classes episode en V2, sans ligne T2S.
SELECT '4. Episodes V2 a lieux sans episode T2S' AS SECTION;

SELECT COUNT(DISTINCT st.ID_WIKIDATA) AS EPISODES_V2_A_LIEUX,
       COUNT(DISTINCT CASE WHEN e.ID_EPISODE IS NULL THEN st.ID_WIKIDATA END) AS SANS_EPISODE_T2S
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_EPISODE we ON we.ID_WIKIDATA = st.ID_WIKIDATA
LEFT JOIN T_WC_T2S_EPISODE e ON e.ID_WIKIDATA = st.ID_WIKIDATA
WHERE st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated');

-- 5. Redondance avec la serie : part des liens episode-lieu-role que la serie porte deja.
SELECT '5. Liens deja portes par la serie' AS SECTION;

SELECT COUNT(*) AS LIENS_EPISODE,
       SUM(EXISTS (SELECT 1 FROM T_WC_T2S_SERIE_LOCATION sl
                   WHERE sl.ID_SERIE = x.ID_SERIE AND sl.ID_LOCATION = x.ID_LOCATION
                     AND sl.LOCATION_ROLE = x.LOCATION_ROLE)) AS DEJA_SUR_LA_SERIE
FROM (
  SELECT DISTINCT e.ID_EPISODE, e.ID_SERIE, l.ID_LOCATION,
         CASE st.ID_PROPERTY WHEN 'P915' THEN 'filming' ELSE 'narrative' END AS LOCATION_ROLE
  FROM T_WC_WIKIDATA_STATEMENT st
  INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
  INNER JOIN T_WC_T2S_EPISODE e ON e.ID_WIKIDATA = st.ID_WIKIDATA
  INNER JOIN T_WC_T2S_LOCATION l ON l.ID_WIKIDATA = iv.ID_ITEM AND l.DELETED = 0
  WHERE st.ID_PROPERTY IN ('P840', 'P915')
    AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
) x;

-- 6. Les series les plus couvertes, pour choisir l'exemple de l'acceptation.
SELECT '6. Series dont les episodes portent le plus de lieux' AS SECTION;

SELECT s.ID_SERIE, s.SERIE_TITLE,
       COUNT(DISTINCT e.ID_EPISODE) AS EPISODES,
       COUNT(DISTINCT iv.ID_ITEM)   AS LIEUX,
       COUNT(*)                     AS CITATIONS
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
INNER JOIN T_WC_T2S_EPISODE e ON e.ID_WIKIDATA = st.ID_WIKIDATA
INNER JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = e.ID_SERIE
WHERE st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
GROUP BY s.ID_SERIE, s.SERIE_TITLE
ORDER BY CITATIONS DESC
LIMIT 15;

-- 7. Les index de T_WC_T2S_EPISODE : ID_WIKIDATA doit etre indexe, le perimetre du
--    processus 72 le lit par EXISTS sur chaque statement.
SELECT '7. Index de T_WC_T2S_EPISODE' AS SECTION;

SHOW INDEX FROM T_WC_T2S_EPISODE WHERE Column_name = 'ID_WIKIDATA';
