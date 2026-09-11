-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-014 : qui cite les lieux, et pourquoi 4 377 d'entre eux
-- ne seront pas crees
-- ============================================================================
--
-- LECTURE SEULE.
--
-- LA QUESTION. La mesure du 2026-09-11 a compte 16 299 items cites en valeur de P840 ou
-- P915, mais seulement 11 922 a creer, l'ecart tenant aux lieux cites uniquement par des
-- sujets absents de T2S. Philippe : « je pense qu'il ne s'agit ni de films ni de series,
-- correct ? »
--
-- DEUX INDICES LE SUGGERENT DEJA, dans la section 4 du fichier des classes. Springfield
-- est cite 814 fois en lieu d'action et ZERO en tournage ; Bikini Bottom 780 fois, meme
-- profil. Ce sont les Simpson et Bob l'eponge, donc des EPISODES, qui vivent dans
-- T_WC_WIKIDATA_EPISODE et non dans T2S_MOVIE ni T2S_SERIE. P840 s'applique a toute
-- oeuvre de fiction, pas seulement au cinema : romans, jeux video, bandes dessinees et
-- episodes en portent.
--
-- Mais deux noms ne font pas une mesure, et il existe une seconde cause possible : des
-- films et series que Wikidata connait et que TMDb ignore, ou dont le lien QID manque.
-- Les deux causes n'appellent pas la meme reaction. La premiere est normale et
-- definitive, la seconde serait une perte a instruire.
--
-- ⚠ COLLATION. Lancer avec --force.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- 1. De quelle nature sont les sujets qui citent un lieu.
--
--    Un sujet est classe par la table d'entite V2 ou il vit. L'ordre du CASE est une
--    priorite : une entite peut figurer dans plusieurs tables, et la premiere qui
--    correspond gagne. « hors perimetre V2 » designe un sujet qui n'est dans aucune
--    table d'entite, ce qui arrive pour les oeuvres que le crawl ne classe pas.
-- ---------------------------------------------------------------------------
SELECT '1. Nature des sujets citant un lieu' AS SECTION;

SELECT CASE
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE   x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '1. film (V2)'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE   x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '2. serie (V2)'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '3. episode (V2)'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SEASON  x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '4. saison (V2)'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON  x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '5. personne (V2)'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM    x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '6. item generique (V2)'
         ELSE '7. hors perimetre V2'
       END                                  AS NATURE_DU_SUJET,
       COUNT(DISTINCT st.ID_WIKIDATA)       AS SUJETS,
       COUNT(DISTINCT iv.ID_ITEM)           AS LIEUX_CITES,
       COUNT(*)                             AS CITATIONS
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
WHERE st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
GROUP BY NATURE_DU_SUJET
ORDER BY NATURE_DU_SUJET;

-- ---------------------------------------------------------------------------
-- 2. Les films et series que Wikidata connait mais que T2S ignore.
--
--    C'est la seconde cause, celle qui compte vraiment : un sujet que V2 range comme
--    film ou serie, et qui n'a pas de correspondance dans le read-model. Si ce nombre
--    est eleve, ce n'est pas la nature des oeuvres qui explique l'ecart, c'est une perte
--    de rapprochement, et elle merite son propre ticket.
-- ---------------------------------------------------------------------------
SELECT '2. Films et series V2 absents de T2S' AS SECTION;

SELECT CASE WHEN wm.ID_WIKIDATA IS NOT NULL THEN 'film' ELSE 'serie' END AS TYPE_V2,
       COUNT(DISTINCT st.ID_WIKIDATA)  AS SUJETS_SANS_EQUIVALENT_T2S,
       COUNT(DISTINCT iv.ID_ITEM)      AS LIEUX_QU_ILS_CITENT
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_MOVIE wm ON wm.ID_WIKIDATA = st.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_SERIE ws ON ws.ID_WIKIDATA = st.ID_WIKIDATA
WHERE st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND (wm.ID_WIKIDATA IS NOT NULL OR ws.ID_WIKIDATA IS NOT NULL)
  AND NOT EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE m WHERE m.ID_WIKIDATA = st.ID_WIKIDATA)
  AND NOT EXISTS (SELECT 1 FROM T_WC_T2S_SERIE s WHERE s.ID_WIKIDATA = st.ID_WIKIDATA)
GROUP BY TYPE_V2
ORDER BY TYPE_V2;

-- ---------------------------------------------------------------------------
-- 3. Les 4 377 lieux perdus : par quoi sont-ils cites, exclusivement ?
--
--    Un lieu n'est perdu que si AUCUN de ses citants n'est un film ou une serie T2S.
--    Cette section dit qui sont ces citants exclusifs, et donc si la perte est une
--    consequence normale du perimetre ou le symptome d'autre chose.
-- ---------------------------------------------------------------------------
SELECT '3. Les lieux perdus, et leurs citants exclusifs' AS SECTION;

SELECT CASE
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '1. episode'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE   x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '2. film V2 hors T2S'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE   x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '3. serie V2 hors T2S'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM    x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '4. autre oeuvre (roman, jeu, BD)'
         ELSE '5. hors perimetre V2'
       END                              AS CITANT,
       COUNT(DISTINCT perdus.ID_ITEM)   AS LIEUX_CONCERNES,
       COUNT(*)                         AS CITATIONS
FROM (
  SELECT DISTINCT iv.ID_ITEM
  FROM T_WC_WIKIDATA_STATEMENT st
  INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
  WHERE st.ID_PROPERTY IN ('P840', 'P915')
    AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
    AND NOT EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE m WHERE m.ID_WIKIDATA = st.ID_WIKIDATA)
    AND NOT EXISTS (SELECT 1 FROM T_WC_T2S_SERIE s WHERE s.ID_WIKIDATA = st.ID_WIKIDATA)
    AND NOT EXISTS (
      SELECT 1
      FROM T_WC_WIKIDATA_STATEMENT st2
      INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv2 ON iv2.ID_STATEMENT = st2.ID_STATEMENT
      WHERE iv2.ID_ITEM = iv.ID_ITEM
        AND st2.ID_PROPERTY IN ('P840', 'P915')
        AND (st2.`RANK` IS NULL OR st2.`RANK` <> 'deprecated')
        AND (EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE m2 WHERE m2.ID_WIKIDATA = st2.ID_WIKIDATA)
          OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE s2 WHERE s2.ID_WIKIDATA = st2.ID_WIKIDATA))
    )
) perdus
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_ITEM = perdus.ID_ITEM
INNER JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_STATEMENT = iv.ID_STATEMENT
       AND st.ID_PROPERTY IN ('P840', 'P915')
       AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
GROUP BY CITANT
ORDER BY CITANT;

-- ---------------------------------------------------------------------------
-- 4. L'anomalie indonesienne.
--
--    Q252 arrive PREMIER avec 6 541 citations, 3 354 en tournage et 3 187 en action,
--    devant New York (5 500) et Los Angeles (3 940). Un pays qui aurait plus de lieux de
--    tournage que Los Angeles ne decrit pas le cinema, il decrit une campagne d'edition
--    de masse dans Wikidata. Cette section identifie les citants pour trancher : si les
--    sujets sont des milliers de films indonesiens ajoutes en lot, c'est un artefact
--    connu et sans gravite ; si ce sont des oeuvres heterogenes, c'est autre chose.
--
--    L'enjeu est concret : ce lieu pesera 6 541 associations a lui seul, soit 7 % du
--    total, et il remontera en tete de toute question sur les lieux de tournage.
-- ---------------------------------------------------------------------------
SELECT '4. Qui cite l Indonesie' AS SECTION;

SELECT CASE
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE   x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '1. film (V2)'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE   x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '2. serie (V2)'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_EPISODE x WHERE x.ID_WIKIDATA = st.ID_WIKIDATA) THEN '3. episode (V2)'
         ELSE '4. autre'
       END                             AS NATURE,
       st.ID_PROPERTY,
       COUNT(*)                        AS CITATIONS,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE m WHERE m.ID_WIKIDATA = st.ID_WIKIDATA)
                  OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE s WHERE s.ID_WIKIDATA = st.ID_WIKIDATA)
                THEN 1 ELSE 0 END)     AS DONT_DANS_T2S
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
WHERE iv.ID_ITEM = 'Q252'
  AND st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
GROUP BY NATURE, st.ID_PROPERTY
ORDER BY NATURE, st.ID_PROPERTY;

-- Un echantillon nomme, pour voir de quelles oeuvres il s'agit.
SELECT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')),
                NULLIF(wi.LABEL_EN, ''))  AS OEUVRE,
       st.ID_WIKIDATA,
       st.ID_PROPERTY
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = st.ID_WIKIDATA
WHERE iv.ID_ITEM = 'Q252'
  AND st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
ORDER BY st.ID_WIKIDATA ASC
LIMIT 15;
