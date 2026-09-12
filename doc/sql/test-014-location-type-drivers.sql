-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-014 : quelles classes decident du type, et ou ca derape
-- ============================================================================
--
-- LECTURE SEULE. Deuxieme iteration sur LOCATION_TYPE, apres la recette du 2026-09-12.
--
-- CE QUE LA RECETTE A MONTRE. La conception tient : les huit temoins de priorite sont
-- justes, Paris et Berlin en 'city' malgre leurs classes administratives, Singapour en
-- 'country', Bikini Bottom et Springfield en 'fiction'. L'ORDRE des cones est donc bon.
-- Ce sont les RACINES qui sont incompletes, et de trois facons.
--
--   1. 'region' domine a 35,7 % (4 261 lieux) devant 'city' (4 030), et le classement
--      des vingt lieux les plus lies donne le symptome : MADRID est rangee en 'region'.
--      Madrid est une ville. Le cone region compte 19 482 classes, sept fois celui des
--      villes, et il attrape des entites peuplees que le cone city ne couvre pas.
--      Hypothese a verifier ici : les classes administratives nationales, du type
--      "commune de France" (Q484170, 721 lieux) ou l'equivalent americain (Q1093829,
--      543 lieux), ne descendent PAS de Q486972 "human settlement" mais d'une entite
--      territoriale administrative, donc tombent en region.
--
--   2. Le cone 'fiction' est trop etroit, 53 classes. La section B2 liste, parmi les
--      lieux SANS type : fictional country (45), fictional town (41), fictional island
--      (25), fictional spacecraft (16). Ce sont des fictions, elles doivent l'etre.
--
--   3. Il manque des racines entieres. PINEWOOD STUDIOS sort sans type alors qu'il est
--      le 15e lieu le plus lie du corpus, 668 oeuvres : "film studio" (Q375336) n'est
--      dans aucun cone. Idem prison (22) et architectural structure (18), que j'avais
--      envisagee puis ecartee a tort. Et une famille manque tout entiere, le relief :
--      mountain (47) et beach (25) ne sont ni des structures ni des regions.
--
-- ⚠ CE FICHIER NE CORRIGE RIEN, il mesure ce qui decide. Ajouter des racines sur une
-- hypothese serait refaire la faute que la premiere iteration a evitee : les sept
-- racines actuelles avaient ete relevees, pas devinees, et c'est pour cela que l'ordre
-- est juste du premier coup. La deuxieme iteration merite la meme discipline.
--
-- ⚠ COLLATION. Lancer avec --force.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- 1. LA QUESTION CENTRALE : quelles classes font qu'un lieu est dit 'region' ?
--
--    Pour chaque lieu classe 'region', on retrouve les classes qui l'ont amene la,
--    c'est-a-dire ses P31 presents dans le cone region. Les plus frequentes sont les
--    racines a deplacer. Si "commune de France" apparait en tete, l'hypothese 1 est
--    confirmee et le correctif est d'elargir le cone city, pas de retrecir region.
-- ---------------------------------------------------------------------------
SELECT '1. Les classes qui produisent le verdict region' AS SECTION;

SELECT iv.ID_ITEM AS ID_CLASSE,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')),
                JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.fr')),
                NULLIF(wi.LABEL_EN, ''))          AS CLASSE,
       COUNT(DISTINCT loc.ID_LOCATION)            AS LIEUX,
       GROUP_CONCAT(DISTINCT loc.LOCATION_NAME ORDER BY loc.MOVIE_COUNT DESC SEPARATOR ' | ') AS EXEMPLES
FROM T_WC_T2S_LOCATION loc
INNER JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = loc.ID_WIKIDATA
       AND st.ID_PROPERTY = 'P31'
       AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
INNER JOIN T_WC_T2S_LOCATION_CLASS lc ON lc.ID_CLASS = iv.ID_ITEM AND lc.LOCATION_TYPE = 'region'
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = iv.ID_ITEM
WHERE loc.LOCATION_TYPE = 'region'
GROUP BY iv.ID_ITEM, CLASSE
ORDER BY LIEUX DESC
LIMIT 25;

-- ---------------------------------------------------------------------------
-- 2. Le temoin nomme : pourquoi Madrid n'est pas une ville.
--
--    Toutes ses classes, avec le type que chacune porte aujourd'hui. Si aucune n'est
--    dans le cone city, le diagnostic est clos et le correctif evident.
-- ---------------------------------------------------------------------------
SELECT '2. Madrid, toutes ses classes et leur cone' AS SECTION;

SELECT iv.ID_ITEM AS ID_CLASSE,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')),
                JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.fr')),
                NULLIF(wi.LABEL_EN, ''))          AS CLASSE,
       COALESCE(lc.LOCATION_TYPE, '(dans aucun cone)') AS CONE
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_T2S_LOCATION_CLASS lc ON lc.ID_CLASS = iv.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = iv.ID_ITEM
WHERE st.ID_WIKIDATA = 'Q2807'
  AND st.ID_PROPERTY = 'P31'
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
ORDER BY CONE, CLASSE;

-- ---------------------------------------------------------------------------
-- 3. Les deux classes muettes les plus lourdes, Q484170 et Q1093829.
--
--    Elles pesaient 721 et 543 lieux dans la mesure des classes, sans libelle en base.
--    Leur cone actuel dit ou elles tombent, et leurs parents P279 disent ce qu'elles
--    sont vraiment, ce que le libelle manquant empeche de lire.
-- ---------------------------------------------------------------------------
SELECT '3. Les classes muettes les plus lourdes' AS SECTION;

SELECT c.ID_CLASS AS ID_CLASSE,
       COALESCE(lc.LOCATION_TYPE, '(dans aucun cone)') AS CONE,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wp.LABELS_JSON, '$.en')),
                JSON_UNQUOTE(JSON_EXTRACT(wp.LABELS_JSON, '$.fr')),
                sc.ID_PARENT)                     AS PARENT_P279
FROM (SELECT 'Q484170' AS ID_CLASS UNION ALL SELECT 'Q1093829'
      UNION ALL SELECT 'Q62049'   UNION ALL SELECT 'Q2074737'
      UNION ALL SELECT 'Q747074'  UNION ALL SELECT 'Q5951278'
      UNION ALL SELECT 'Q42744322' UNION ALL SELECT 'Q856076') c
LEFT JOIN T_WC_T2S_LOCATION_CLASS lc ON lc.ID_CLASS = c.ID_CLASS
LEFT JOIN T_WC_WIKIDATA_SUBCLASS sc ON sc.ID_CHILD = c.ID_CLASS AND sc.DELETED = 0
LEFT JOIN T_WC_WIKIDATA_ITEM wp ON wp.ID_WIKIDATA = sc.ID_PARENT
ORDER BY c.ID_CLASS, PARENT_P279;

-- ---------------------------------------------------------------------------
-- 4. Les racines candidates, chiffrees avant d'etre ajoutees.
--
--    Combien de lieux SANS TYPE chacune recupererait, et combien elle en volerait a un
--    type deja attribue. La seconde colonne est celle qui doit rester petite : une
--    racine qui reclasse massivement des lieux deja classes n'est pas un ajout, c'est
--    un changement de conception.
-- ---------------------------------------------------------------------------
SELECT '4. Ce que chaque racine candidate rapporterait' AS SECTION;

SELECT cand.ID_CLASS                                                          AS RACINE,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')),
                NULLIF(wi.LABEL_EN, ''))                                      AS CLASSE,
       cand.TYPE_PROPOSE,
       COUNT(DISTINCT CASE WHEN loc.LOCATION_TYPE IS NULL THEN loc.ID_LOCATION END) AS GAGNE_SANS_TYPE,
       COUNT(DISTINCT CASE WHEN loc.LOCATION_TYPE IS NOT NULL THEN loc.ID_LOCATION END) AS DEJA_CLASSES
FROM (
  SELECT 'Q375336'    AS ID_CLASS, 'structure' AS TYPE_PROPOSE UNION ALL  -- film studio
  SELECT 'Q40357',    'structure' UNION ALL                               -- prison
  SELECT 'Q811979',   'structure' UNION ALL                               -- architectural structure
  SELECT 'Q1145276',  'fiction'   UNION ALL                               -- fictional country
  SELECT 'Q106921111','fiction'   UNION ALL                               -- fictional town
  SELECT 'Q6619693',  'fiction'   UNION ALL                               -- fictional island
  SELECT 'Q14637321', 'fiction'   UNION ALL                               -- fictional spacecraft
  SELECT 'Q8502',     'nature'    UNION ALL                               -- mountain
  SELECT 'Q40080',    'nature'                                            -- beach
) cand
LEFT JOIN T_WC_WIKIDATA_SUBCLASS sc ON sc.ID_PARENT = cand.ID_CLASS AND sc.DELETED = 0
LEFT JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_ITEM IN (cand.ID_CLASS, sc.ID_CHILD)
LEFT JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_STATEMENT = iv.ID_STATEMENT
       AND st.ID_PROPERTY = 'P31'
       AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
LEFT JOIN T_WC_T2S_LOCATION loc ON loc.ID_WIKIDATA = st.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = cand.ID_CLASS
GROUP BY cand.ID_CLASS, CLASSE, cand.TYPE_PROPOSE
ORDER BY GAGNE_SANS_TYPE DESC;
