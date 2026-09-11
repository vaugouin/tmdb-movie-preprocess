-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-014 : choisir les racines de LOCATION_TYPE sur la donnee
-- ============================================================================
--
-- LECTURE SEULE. A lancer avant d'ecrire la classification, jamais apres.
--
-- POURQUOI CE FICHIER EXISTE. LOCATION_TYPE se derive du cone P279, comme le filtre
-- des recompenses de -036 et -039, et le mecanisme est deja ecrit et eprouve :
-- f_buildawardconetable() dans tmdb_preprocess_helpers.py construit la fermeture
-- transitive sous une racine, en 14 260 classes pour Q618779.
--
-- Ce qui manque, ce sont les RACINES. Les poser de memoire serait la meme faute que
-- celle du 2026-08-31, ou un cas temoin pointait un QID reconstruit de tete et rendait
-- zero ligne en se lisant comme un succes. Cette requete les fait choisir sur les
-- classes que les 11 922 lieux portent REELLEMENT.
--
-- CE QU'IL FAUT EN TIRER, dans l'ordre :
--   1. les classes les plus frequentes couvrent-elles l'essentiel, ou la queue est-elle
--      longue ? Si dix classes couvrent 80 % des lieux, cinq cones suffisent ;
--   2. quelles classes sont AMBIGUES, c'est-a-dire appartiendraient a deux cones ?
--      Singapour est a la fois une ville et un pays. L'ordre de classification devra
--      trancher, et il se decide en voyant les cas, pas en les imaginant ;
--   3. combien de lieux n'ont AUCUN P31 ? Ceux-la resteront sans type, et il faut savoir
--      combien pour juger si le manque est tolerable.
--
-- ⚠ COLLATION. Lancer avec --force.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- 1. Les classes portees par les lieux, par frequence.
--
--    Le libelle vient de T_WC_WIKIDATA_ITEM : sans lui la liste serait une colonne
--    de Q-numeros, illisible, et c'est justement la lisibilite qui permet de choisir.
-- ---------------------------------------------------------------------------
SELECT '1. Classes P31 des lieux, par frequence' AS SECTION;

SELECT cls.ID_ITEM                                          AS ID_CLASSE,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')),
                NULLIF(wi.LABEL_EN, ''))                     AS CLASSE,
       COUNT(DISTINCT lieux.ID_ITEM)                         AS LIEUX
FROM (
  SELECT DISTINCT iv.ID_ITEM
  FROM T_WC_WIKIDATA_STATEMENT st
  INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
  WHERE st.ID_PROPERTY IN ('P840', 'P915')
    AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
) lieux
INNER JOIN T_WC_WIKIDATA_STATEMENT sc ON sc.ID_WIKIDATA = lieux.ID_ITEM
       AND sc.ID_PROPERTY = 'P31'
       AND (sc.`RANK` IS NULL OR sc.`RANK` <> 'deprecated')
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE cls ON cls.ID_STATEMENT = sc.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = cls.ID_ITEM
GROUP BY cls.ID_ITEM, CLASSE
ORDER BY LIEUX DESC
LIMIT 40;

-- ---------------------------------------------------------------------------
-- 2. Combien de lieux n'ont aucune classe, et resteront donc sans type.
--
--    Le cas n'est pas theorique : le filtre des recompenses a du garder les 552 lignes
--    sans P31, ou se trouvaient de vraies recompenses absentes de V2. Ici la question
--    est plus simple, on ne perd rien, on ne classe pas. Mais le chiffre decide si
--    LOCATION_TYPE est utilisable comme filtre ou seulement comme indication.
-- ---------------------------------------------------------------------------
SELECT '2. Lieux sans P31' AS SECTION;

SELECT COUNT(*)                                                        AS LIEUX_TOTAL,
       SUM(CASE WHEN a.ID_ITEM IS NULL THEN 1 ELSE 0 END)              AS SANS_P31,
       ROUND(100 * SUM(CASE WHEN a.ID_ITEM IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS PCT_SANS_P31
FROM (
  SELECT DISTINCT iv.ID_ITEM
  FROM T_WC_WIKIDATA_STATEMENT st
  INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
  WHERE st.ID_PROPERTY IN ('P840', 'P915')
    AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
) lieux
LEFT JOIN (
  SELECT DISTINCT st.ID_WIKIDATA AS ID_ITEM
  FROM T_WC_WIKIDATA_STATEMENT st
  WHERE st.ID_PROPERTY = 'P31'
    AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
) a ON a.ID_ITEM = lieux.ID_ITEM;

-- ---------------------------------------------------------------------------
-- 3. Les lieux qui portent PLUSIEURS classes, qui sont les cas d'ambiguite.
--
--    C'est ici que se lit l'ordre de classification a poser. Un lieu a la fois ville
--    et pays, a la fois batiment et lieu de fiction, doit tomber dans un seul type, et
--    lequel n'est pas evident depuis un bureau.
-- ---------------------------------------------------------------------------
SELECT '3. Lieux a classes multiples, les vingt plus charges' AS SECTION;

SELECT lieux.ID_ITEM,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wl.LABELS_JSON, '$.en')),
                NULLIF(wl.LABEL_EN, ''))                     AS LIEU,
       COUNT(DISTINCT cls.ID_ITEM)                           AS NB_CLASSES,
       GROUP_CONCAT(DISTINCT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wc.LABELS_JSON, '$.en')),
                                      NULLIF(wc.LABEL_EN, ''), cls.ID_ITEM)
                    ORDER BY 1 SEPARATOR ' | ')              AS CLASSES
FROM (
  SELECT DISTINCT iv.ID_ITEM
  FROM T_WC_WIKIDATA_STATEMENT st
  INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
  WHERE st.ID_PROPERTY IN ('P840', 'P915')
    AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
) lieux
INNER JOIN T_WC_WIKIDATA_STATEMENT sc ON sc.ID_WIKIDATA = lieux.ID_ITEM
       AND sc.ID_PROPERTY = 'P31'
       AND (sc.`RANK` IS NULL OR sc.`RANK` <> 'deprecated')
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE cls ON cls.ID_STATEMENT = sc.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM wc ON wc.ID_WIKIDATA = cls.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_ITEM wl ON wl.ID_WIKIDATA = lieux.ID_ITEM
GROUP BY lieux.ID_ITEM, LIEU
HAVING NB_CLASSES > 1
ORDER BY NB_CLASSES DESC, LIEU ASC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- 4. Les lieux les plus cites, qui sont ceux que les questions nommeront.
--
--    Utile pour deux choses : verifier que les libelles sont propres, et voir de quel
--    type sont les lieux qui comptent vraiment. Un LOCATION_TYPE juste sur la longue
--    traine et faux sur New York ne vaudrait rien.
-- ---------------------------------------------------------------------------
SELECT '4. Les vingt lieux les plus cites' AS SECTION;

SELECT iv.ID_ITEM,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')),
                NULLIF(wi.LABEL_EN, ''))                     AS LIEU_EN,
       JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.fr'))     AS LIEU_FR,
       SUM(CASE WHEN st.ID_PROPERTY = 'P915' THEN 1 ELSE 0 END) AS TOURNAGE,
       SUM(CASE WHEN st.ID_PROPERTY = 'P840' THEN 1 ELSE 0 END) AS ACTION,
       COUNT(*)                                              AS CITATIONS
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = iv.ID_ITEM
WHERE st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
GROUP BY iv.ID_ITEM, LIEU_EN, LIEU_FR
ORDER BY CITATIONS DESC
LIMIT 20;
