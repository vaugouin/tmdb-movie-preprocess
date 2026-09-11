-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-014 : recette du processus 72, apres execution
-- ============================================================================
--
-- LECTURE SEULE. A lancer apres le premier passage du processus 72.
--
-- LES PREDICTIONS, posees le 2026-09-11 AVANT d'ecrire le code, pour que la recette
-- puisse echouer. Une recette dont on decouvre les attentes apres coup ne verifie rien.
--
--   T_WC_T2S_LOCATION ............... 11 922 lignes
--   T_WC_T2S_MOVIE_LOCATION ......... 84 567  (32 888 filming + 51 679 narrative)
--   T_WC_T2S_SERIE_LOCATION ......... 11 199  (4 371 filming + 6 828 narrative)
--
-- ⚠ CES DEUX DERNIERS CHIFFRES SONT DES BORNES HAUTES, pas des egalites attendues. La
-- volumetrie les a comptes sur l'ensemble des statements, alors que la table porte une
-- cle unique sur (oeuvre, lieu, role) : une oeuvre citant deux fois le meme lieu dans le
-- meme role, ce que Wikidata autorise, ne produira qu'une ligne. Un ecart VERS LE BAS
-- est donc normal et mesure la duplication dans la source ; un ecart vers le haut serait
-- un defaut.
--
-- ⚠ COLLATION. Lancer avec --force.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- A. Les volumes, contre les predictions
-- ---------------------------------------------------------------------------
SELECT 'A. Volumes' AS SECTION;

SELECT 'T_WC_T2S_LOCATION' AS TABLE_NAME, COUNT(*) AS LIGNES, 11922 AS ATTENDU,
       COUNT(*) - 11922 AS ECART
FROM T_WC_T2S_LOCATION
UNION ALL
SELECT 'T_WC_T2S_MOVIE_LOCATION', COUNT(*), 84567, COUNT(*) - 84567 FROM T_WC_T2S_MOVIE_LOCATION
UNION ALL
SELECT 'T_WC_T2S_SERIE_LOCATION', COUNT(*), 11199, COUNT(*) - 11199 FROM T_WC_T2S_SERIE_LOCATION;

SELECT LOCATION_ROLE, COUNT(*) AS FILMS FROM T_WC_T2S_MOVIE_LOCATION GROUP BY LOCATION_ROLE ORDER BY LOCATION_ROLE;
SELECT LOCATION_ROLE, COUNT(*) AS SERIES FROM T_WC_T2S_SERIE_LOCATION GROUP BY LOCATION_ROLE ORDER BY LOCATION_ROLE;

-- ---------------------------------------------------------------------------
-- B. Le type, sa couverture et sa repartition
--
--    Attendu : 0,5 % sans P31 du tout (85 sur 16 299 mesures avant restriction), plus
--    les lieux dont aucune classe ne tombe dans un cone. Le second nombre n'a PAS ete
--    predit, il se decouvre ici : c'est lui qui dira si les sept racines suffisent ou
--    s'il en manque une famille entiere.
-- ---------------------------------------------------------------------------
SELECT 'B. Repartition de LOCATION_TYPE' AS SECTION;

SELECT COALESCE(LOCATION_TYPE, '(aucun)') AS LOCATION_TYPE,
       COUNT(*)                            AS LIEUX,
       ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM T_WC_T2S_LOCATION), 1) AS PCT
FROM T_WC_T2S_LOCATION
GROUP BY COALESCE(LOCATION_TYPE, '(aucun)')
ORDER BY LIEUX DESC;

-- Les classes des lieux restes sans type, par frequence : si une classe tres frequente
-- apparait ici, c'est une racine oubliee et non une longue traine.
SELECT 'B2. Classes des lieux sans type, les quinze premieres' AS SECTION;

SELECT iv.ID_ITEM AS ID_CLASSE,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')), NULLIF(wi.LABEL_EN, '')) AS CLASSE,
       COUNT(DISTINCT loc.ID_LOCATION) AS LIEUX_SANS_TYPE
FROM T_WC_T2S_LOCATION loc
INNER JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = loc.ID_WIKIDATA
       AND st.ID_PROPERTY = 'P31'
       AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = iv.ID_ITEM
WHERE loc.LOCATION_TYPE IS NULL
GROUP BY iv.ID_ITEM, CLASSE
ORDER BY LIEUX_SANS_TYPE DESC
LIMIT 15;

-- ---------------------------------------------------------------------------
-- C. LES TEMOINS DE PRIORITE, et c'est la section qui juge la conception.
--
--    L'ordre des cones a ete choisi sur trois cas precis, donc trois cas doivent le
--    verifier. Si l'un d'eux tombe a cote, ce n'est pas un detail a corriger a la main,
--    c'est l'ordre de LOCATION_TYPE_CONES qui est faux.
-- ---------------------------------------------------------------------------
SELECT 'C. Temoins de priorite' AS SECTION;

SELECT loc.ID_WIKIDATA, loc.LOCATION_NAME, loc.LOCATION_NAME_FR, loc.LOCATION_TYPE,
       CASE loc.ID_WIKIDATA
         WHEN 'Q90'       THEN 'city     (porte 15 classes dont une collectivite territoriale)'
         WHEN 'Q64'       THEN 'city     (porte "federated state of Germany")'
         WHEN 'Q334'      THEN 'country  (porte big city ET country)'
         WHEN 'Q2276331'  THEN 'fiction  (Bikini Bottom, ne doit pas se ranger avec les villes)'
         WHEN 'Q142'      THEN 'country'
         WHEN 'Q60'       THEN 'city'
         WHEN 'Q252'      THEN 'country  (l anomalie indonesienne)'
         WHEN 'Q151076'   THEN 'fiction  (Springfield)'
       END AS ATTENDU
FROM T_WC_T2S_LOCATION loc
WHERE loc.ID_WIKIDATA IN ('Q90','Q64','Q334','Q2276331','Q142','Q60','Q252','Q151076')
ORDER BY loc.ID_WIKIDATA;

-- ---------------------------------------------------------------------------
-- D. Les libelles et les descriptions, qui conditionnent l'embedding
--
--    OVERVIEW n'est pas decoratif : c'est lui qui desambiguise. "Paris, capital and
--    largest city of France" et "Paris, city in Texas" se separent par le vecteur, la ou
--    "Paris" et "Paris" produisent deux documents identiques. Un lieu sans description
--    restera donc indistinguable de son homonyme.
-- ---------------------------------------------------------------------------
SELECT 'D. Couverture des textes' AS SECTION;

SELECT COUNT(*)                                                              AS LIEUX,
       SUM(CASE WHEN LOCATION_NAME IS NOT NULL THEN 1 ELSE 0 END)            AS AVEC_NOM_EN,
       SUM(CASE WHEN LOCATION_NAME_FR IS NOT NULL THEN 1 ELSE 0 END)         AS AVEC_NOM_FR,
       SUM(CASE WHEN OVERVIEW IS NOT NULL AND OVERVIEW <> '' THEN 1 ELSE 0 END) AS AVEC_DESCRIPTION,
       SUM(CASE WHEN LOCATION_NAME IS NULL THEN 1 ELSE 0 END)                AS SANS_NOM_DU_TOUT
FROM T_WC_T2S_LOCATION;

-- Les homonymes, qui sont le cas que la description doit sauver.
SELECT 'D2. Les noms portes par plusieurs lieux' AS SECTION;

SELECT LOCATION_NAME, COUNT(*) AS LIEUX,
       SUM(CASE WHEN OVERVIEW IS NULL OR OVERVIEW = '' THEN 1 ELSE 0 END) AS DONT_SANS_DESCRIPTION
FROM T_WC_T2S_LOCATION
WHERE LOCATION_NAME IS NOT NULL
GROUP BY LOCATION_NAME
HAVING COUNT(*) > 1
ORDER BY LIEUX DESC, LOCATION_NAME ASC
LIMIT 15;

-- ---------------------------------------------------------------------------
-- E. Les agregats, et le piege du zero sentinelle
--
--    Un lieu sans oeuvre notee doit porter NULL et non zero. Un zero se trierait comme
--    une mauvaise note alors qu'il signifie l'absence de mesure : c'est le piege paye
--    trois fois dans cette migration, le spine Criterion, le IS NOT NULL qui comptait
--    les zeros, et le NULL <> 'Q123' de selenium. Cette section doit rendre ZERO ligne.
-- ---------------------------------------------------------------------------
SELECT 'E. Zeros sentinelles, doit rendre zero ligne' AS SECTION;

SELECT ID_LOCATION, LOCATION_NAME, IMDB_RATING, IMDB_RATING_WEIGHTED, POPULARITY,
       MOVIE_COUNT, SERIE_COUNT
FROM T_WC_T2S_LOCATION
WHERE IMDB_RATING = 0 OR IMDB_RATING_WEIGHTED = 0 OR POPULARITY = 0
LIMIT 10;

-- ---------------------------------------------------------------------------
-- F. Le classement, a comparer avec celui mesure sur Wikidata
--
--    L'Indonesie etait premiere avec 6 541 citations, devant New York. Apres la
--    restriction aux oeuvres T2S elle devrait retomber vers 2 956, et le classement
--    devrait se rapprocher de ce qu'un amateur de cinema attend.
-- ---------------------------------------------------------------------------
SELECT 'F. Les vingt lieux les plus lies' AS SECTION;

SELECT loc.ID_LOCATION, loc.ID_WIKIDATA, loc.LOCATION_NAME, loc.LOCATION_TYPE,
       loc.MOVIE_COUNT, loc.SERIE_COUNT,
       loc.MOVIE_COUNT + loc.SERIE_COUNT AS TOTAL,
       ROUND(loc.IMDB_RATING_WEIGHTED, 2) AS NOTE
FROM T_WC_T2S_LOCATION loc
ORDER BY TOTAL DESC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- G. L'integrite des liens : aucune association ne doit pointer dans le vide
-- ---------------------------------------------------------------------------
SELECT 'G. Liens orphelins, doit rendre zero partout' AS SECTION;

SELECT 'movie -> location' AS LIEN, COUNT(*) AS ORPHELINS
FROM T_WC_T2S_MOVIE_LOCATION ml
LEFT JOIN T_WC_T2S_LOCATION loc ON loc.ID_LOCATION = ml.ID_LOCATION
WHERE loc.ID_LOCATION IS NULL
UNION ALL
SELECT 'serie -> location', COUNT(*)
FROM T_WC_T2S_SERIE_LOCATION sl
LEFT JOIN T_WC_T2S_LOCATION loc ON loc.ID_LOCATION = sl.ID_LOCATION
WHERE loc.ID_LOCATION IS NULL
UNION ALL
SELECT 'movie_location -> movie', COUNT(*)
FROM T_WC_T2S_MOVIE_LOCATION ml
LEFT JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = ml.ID_MOVIE
WHERE m.ID_MOVIE IS NULL
UNION ALL
SELECT 'serie_location -> serie', COUNT(*)
FROM T_WC_T2S_SERIE_LOCATION sl
LEFT JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = sl.ID_SERIE
WHERE s.ID_SERIE IS NULL;

-- ---------------------------------------------------------------------------
-- H. Les cones eux-memes, pour juger si une racine est trop large ou trop etroite
-- ---------------------------------------------------------------------------
SELECT 'H. Taille des cones de classes' AS SECTION;

SELECT LOCATION_TYPE, COUNT(*) AS CLASSES
FROM T_WC_T2S_LOCATION_CLASS
GROUP BY LOCATION_TYPE
ORDER BY CLASSES DESC;
