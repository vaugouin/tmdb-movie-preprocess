-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-039 : recette des ensembles pilotes, prix et nominations
-- ============================================================================
--
-- CE QUI CHANGE, ET POURQUOI CE N'EST PAS UN DEBRANCHEMENT. Les processus 44 et 47
-- decidaient ce qu'est une recompense en lisant T_WC_WIKIDATA_ITEM_PROPERTY, qui
-- APLATIT la valeur principale et les valeurs de tous les qualificatifs sous le meme
-- identifiant de propriete : la requete SPARQL de V1 laissait ?ps non contraint
-- (sparql-crawler.py:316-318). Sous P166 cohabitaient donc la recompense, la
-- CEREMONIE qui l'a remise (P805) et l'OEUVRE pour laquelle elle l'a ete (P1686),
-- toutes trois rangees comme des prix. Cas temoin verifie sur wikidata.org : Cord
-- Jefferson y porte « 96th Academy Awards » et « American Fiction » comme des P166.
--
-- ⚠ CE FICHIER NE VERIFIE PAS QUE RIEN NE CHANGE, il verifie que TOUT CE QUI CHANGE
-- S'EXPLIQUE. C'est la difference avec la recette de -043. La population va baisser,
-- c'est le but ; la question est de savoir si chaque item qui sort est bien une
-- ceremonie, une oeuvre, ou un item dont plus aucun laureat n'est suivi. Un item qui
-- sort sans entrer dans l'une de ces cases est une VRAIE recompense perdue, et c'est
-- le seul chiffre qui peut bloquer la bascule.
--
-- A LANCER DEUX FOIS, avant puis apres le passage de nuit. Les sections A a D lisent
-- les sources et predisent ; la section E lit le resultat et confirme.
--
-- ⚠ COLLATION. Lancer avec --force. Sans la premiere ligne, une comparaison passant
-- par un CAST rend ERROR 1267.
--
-- Proprietes : P166 recompense recue, P1411 nomme pour, P805 la ceremonie, P1686
-- l'oeuvre, P31 instance de, P279 sous-classe de (le cone sous Q618779).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ============================================================================
-- A. TAILLE DES DEUX ENSEMBLES PILOTES, V1 contre V2
-- ============================================================================
--
-- Meme pre-filtre des deux cotes (au moins une entite T2S liee) et meme cone, pour
-- que la difference ne tienne qu'a la source. Une baisse est attendue et voulue.
-- ============================================================================

SELECT 'A1. Ensemble pilote V1 (ce que la nuit produisait)' AS SECTION;

SELECT ip.ID_PROPERTY                          AS PROPRIETE,
       COUNT(DISTINCT ip.ID_ITEM)              AS ITEMS_PILOTES
FROM T_WC_WIKIDATA_ITEM_PROPERTY ip
WHERE ip.ID_PROPERTY IN ('P166','P1411')
  AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = ip.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
     OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = ip.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
     OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> '') )
GROUP BY ip.ID_PROPERTY;

SELECT 'A2. Ensemble pilote V2 (ce que la nuit produira)' AS SECTION;

SELECT sa.ID_PROPERTY                          AS PROPRIETE,
       COUNT(DISTINCT av.ID_ITEM)              AS ITEMS_PILOTES
FROM T_WC_WIKIDATA_STATEMENT sa
JOIN T_WC_WIKIDATA_ITEM_VALUE av ON av.ID_STATEMENT = sa.ID_STATEMENT
WHERE sa.ID_PROPERTY IN ('P166','P1411')
  AND (sa.`RANK` IS NULL OR sa.`RANK` <> 'deprecated')
  AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = sa.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
     OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = sa.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
     OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = sa.ID_WIKIDATA AND pe.ID_WIKIDATA <> '') )
GROUP BY sa.ID_PROPERTY;


-- ============================================================================
-- B. CE QUI SORT, ET POURQUOI. La section qui decide.
-- ============================================================================
--
-- Chaque item present dans le pilote V1 et absent du pilote V2, range dans une cause.
-- Les trois premieres sont attendues et souhaitees ; la quatrieme ne l'est pas.
--
-- CEREMONIE        l'item apparait ailleurs comme valeur de P805. C'etait une
--                  ceremonie aplatie sous P166, pas un prix.
-- OEUVRE           l'item apparait ailleurs comme valeur de P1686. C'etait l'oeuvre
--                  pour laquelle le prix a ete remis.
-- HORS PERIMETRE   l'item n'a aucun statement en V2 : il n'a pas ete importe.
--                  -> WIKIDATA-CRAWLER-011, tranche le 2026-08-28, perte acceptee.
-- ⚠ INEXPLIQUE     aucune des trois. A regarder une par une avant de basculer.
-- ============================================================================

SELECT 'B1. Les items qui quittent le pilote P166, par cause' AS SECTION;

SELECT CASE
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER sq
                      JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qv
                        ON qv.ID_STATEMENT_QUALIFIER = sq.ID_STATEMENT_QUALIFIER
                      WHERE qv.ID_ITEM = d.ID_ITEM AND sq.ID_QUALIFIER_PROPERTY = 'P805')
           THEN 'CEREMONIE'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER sq
                      JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qv
                        ON qv.ID_STATEMENT_QUALIFIER = sq.ID_STATEMENT_QUALIFIER
                      WHERE qv.ID_ITEM = d.ID_ITEM AND sq.ID_QUALIFIER_PROPERTY = 'P1686')
           THEN 'OEUVRE'
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s2
                          WHERE s2.ID_WIKIDATA = d.ID_ITEM)
           THEN 'HORS PERIMETRE'
         ELSE 'INEXPLIQUE'
       END AS CAUSE,
       COUNT(*) AS ITEMS
FROM ( SELECT DISTINCT ip.ID_ITEM
       FROM T_WC_WIKIDATA_ITEM_PROPERTY ip
       WHERE ip.ID_PROPERTY = 'P166'
         AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = ip.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
            OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = ip.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
            OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> '') )
         AND NOT EXISTS (
               SELECT 1
               FROM T_WC_WIKIDATA_STATEMENT sa
               JOIN T_WC_WIKIDATA_ITEM_VALUE av ON av.ID_STATEMENT = sa.ID_STATEMENT
               WHERE sa.ID_PROPERTY = 'P166'
                 AND av.ID_ITEM = ip.ID_ITEM
                 AND (sa.`RANK` IS NULL OR sa.`RANK` <> 'deprecated')
                 AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m2  WHERE m2.ID_WIKIDATA  = sa.ID_WIKIDATA AND m2.ID_WIKIDATA  <> '')
                    OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s2  WHERE s2.ID_WIKIDATA  = sa.ID_WIKIDATA AND s2.ID_WIKIDATA  <> '')
                    OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON p2 WHERE p2.ID_WIKIDATA = sa.ID_WIKIDATA AND p2.ID_WIKIDATA <> '') ) ) ) d
GROUP BY CAUSE
ORDER BY ITEMS DESC;

SELECT 'B2. Vingt items inexpliques a regarder un par un (attendu : peu, ou zero)' AS SECTION;

-- Le libelle est cherche dans TROIS tables, et ce n'est pas de la prudence excessive :
-- une ceremonie retransmise vit dans T_WC_WIKIDATA_SERIE depuis la reintegration de
-- Q15416 dans SERIES_ROOTS. Q85314819 « 96th Academy Awards » y est, et pas dans le
-- cache d'items. Ne jamais presupposer la table ou vit un QID.
SELECT d.ID_ITEM,
       COALESCE(i.LABEL_EN, se.LABEL_EN, mo.LABEL_EN) AS LIBELLE,
       CASE WHEN i.ID_WIKIDATA IS NOT NULL THEN 'item'
            WHEN se.ID_WIKIDATA IS NOT NULL THEN 'serie'
            WHEN mo.ID_WIKIDATA IS NOT NULL THEN 'film'
            ELSE 'inconnu' END AS OU
FROM ( SELECT DISTINCT ip.ID_ITEM
       FROM T_WC_WIKIDATA_ITEM_PROPERTY ip
       WHERE ip.ID_PROPERTY = 'P166'
         AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = ip.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
            OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = ip.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
            OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> '') )
         AND NOT EXISTS (
               SELECT 1
               FROM T_WC_WIKIDATA_STATEMENT sa
               JOIN T_WC_WIKIDATA_ITEM_VALUE av ON av.ID_STATEMENT = sa.ID_STATEMENT
               WHERE sa.ID_PROPERTY = 'P166' AND av.ID_ITEM = ip.ID_ITEM
                 AND (sa.`RANK` IS NULL OR sa.`RANK` <> 'deprecated')
                 AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m2  WHERE m2.ID_WIKIDATA  = sa.ID_WIKIDATA AND m2.ID_WIKIDATA  <> '')
                    OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s2  WHERE s2.ID_WIKIDATA  = sa.ID_WIKIDATA AND s2.ID_WIKIDATA  <> '')
                    OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON p2 WHERE p2.ID_WIKIDATA = sa.ID_WIKIDATA AND p2.ID_WIKIDATA <> '') ) )
         AND EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s3 WHERE s3.ID_WIKIDATA = ip.ID_ITEM)
         AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER sq
                         JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qv
                           ON qv.ID_STATEMENT_QUALIFIER = sq.ID_STATEMENT_QUALIFIER
                         WHERE qv.ID_ITEM = ip.ID_ITEM
                           AND sq.ID_QUALIFIER_PROPERTY IN ('P805','P1686')) ) d
LEFT JOIN T_WC_WIKIDATA_ITEM  i  ON i.ID_WIKIDATA  = d.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_SERIE se  ON se.ID_WIKIDATA = d.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_MOVIE mo  ON mo.ID_WIKIDATA = d.ID_ITEM
LIMIT 20;


-- ============================================================================
-- C. LE GARDE-FOU DE -036 SERT-IL ENCORE ?
-- ============================================================================
--
-- f_awardconeguard() et son filtre de cone ont ete poses le 2026-08-21 sur une
-- source structurellement fausse : ils ecartaient ce qui n'etait pas une recompense.
-- En V2 la confusion n'existe plus, et la question est franche : le cone exclut-il
-- encore quelque chose ? S'il rend zero, c'est desormais du code mort, a retirer
-- explicitement plutot qu'a laisser en decoration. S'il rend un chiffre, c'est un
-- filtre de qualite qui garde sa valeur propre.
-- ============================================================================

SELECT 'C. Items du pilote V2 que le cone ecarterait' AS SECTION;

SELECT sa.ID_PROPERTY AS PROPRIETE, COUNT(DISTINCT av.ID_ITEM) AS ECARTES_PAR_LE_CONE
FROM T_WC_WIKIDATA_STATEMENT sa
JOIN T_WC_WIKIDATA_ITEM_VALUE av ON av.ID_STATEMENT = sa.ID_STATEMENT
WHERE sa.ID_PROPERTY IN ('P166','P1411')
  AND (sa.`RANK` IS NULL OR sa.`RANK` <> 'deprecated')
  AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = sa.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
     OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = sa.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
     OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = sa.ID_WIKIDATA AND pe.ID_WIKIDATA <> '') )
  AND NOT ( EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                    JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                    JOIN T_WC_T2S_AWARD_CLASS ac ON ac.ID_CLASS = iv.ID_ITEM
                    WHERE st.ID_WIKIDATA = av.ID_ITEM AND st.ID_PROPERTY = 'P31')
            OR NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                           WHERE st.ID_WIKIDATA = av.ID_ITEM AND st.ID_PROPERTY = 'P31') )
GROUP BY sa.ID_PROPERTY;


-- ============================================================================
-- D. CE QUE LA SEPARATION REND DISPONIBLE, et qui n'existait pas en V1
-- ============================================================================
--
-- L'annee et l'oeuvre etaient noyees dans ITEM_PROPERTY au meme rang que le prix.
-- Separees, elles deviennent des colonnes exploitables. Ce n'est pas une verification,
-- c'est la mesure du benefice : de quoi decider s'il vaut un ticket d'exploitation.
-- Repose sur WIKIDATA-CRAWLER-019, sans quoi les qualificatifs seraient effondres.
-- ============================================================================

SELECT 'D. Qualificatifs disponibles sur les statements de prix' AS SECTION;

SELECT sa.ID_PROPERTY                                     AS PROPRIETE,
       COUNT(*)                                           AS STATEMENTS,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER sq
                   WHERE sq.ID_STATEMENT = sa.ID_STATEMENT AND sq.ID_QUALIFIER_PROPERTY = 'P585')) AS AVEC_ANNEE,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER sq
                   WHERE sq.ID_STATEMENT = sa.ID_STATEMENT AND sq.ID_QUALIFIER_PROPERTY = 'P1686')) AS AVEC_OEUVRE,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER sq
                   WHERE sq.ID_STATEMENT = sa.ID_STATEMENT AND sq.ID_QUALIFIER_PROPERTY = 'P805')) AS AVEC_CEREMONIE
FROM T_WC_WIKIDATA_STATEMENT sa
WHERE sa.ID_PROPERTY IN ('P166','P1411')
  AND (sa.`RANK` IS NULL OR sa.`RANK` <> 'deprecated')
GROUP BY sa.ID_PROPERTY;


-- ============================================================================
-- E. APRES LE PASSAGE : ce que les deux tables T2S contiennent
-- ============================================================================
--
-- A ne lancer qu'APRES les processus 44 et 47. Les compteurs doivent rejoindre la
-- section A2, aux items sans lien pres que la purge retire.
--
-- Reference du 2026-08-21, apres -036 : T_WC_T2S_AWARD 16 164 lignes et
-- T_WC_T2S_NOMINATION 2 910, contre 44 084 et 30 602 avant le garde-fou.
-- ============================================================================

SELECT 'E1. Volumes produits' AS SECTION;

SELECT 'T_WC_T2S_AWARD' AS TABLE_T2S, COUNT(*) AS LIGNES,
       COUNT(DISTINCT AWARD_SOURCE) AS SOURCES
FROM T_WC_T2S_AWARD
UNION ALL
SELECT 'T_WC_T2S_NOMINATION', COUNT(*), COUNT(DISTINCT AWARD_SOURCE)
FROM T_WC_T2S_NOMINATION;

SELECT 'E2. Lignes que le pilote V2 ne produirait plus (attendu : 0, la purge les enleve)' AS SECTION;

SELECT 'T_WC_T2S_AWARD' AS TABLE_T2S, COUNT(*) AS ORPHELINES
FROM T_WC_T2S_AWARD a
WHERE NOT EXISTS (
    SELECT 1
    FROM T_WC_WIKIDATA_STATEMENT w
    JOIN T_WC_WIKIDATA_ITEM_VALUE wv ON wv.ID_STATEMENT = w.ID_STATEMENT
    WHERE w.ID_PROPERTY = a.AWARD_SOURCE
      AND wv.ID_ITEM = a.ID_WIKIDATA
      AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated')
      AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = w.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
         OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = w.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
         OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = w.ID_WIKIDATA AND pe.ID_WIKIDATA <> '') ) );

SELECT 'E3. Le cas temoin : Cord Jefferson ne doit plus porter de ceremonie ni d oeuvre' AS SECTION;

-- Q120175513 Cord Jefferson. En V1, ITEM_PROPERTY lui donnait « 96th Academy Awards »
-- et « American Fiction » comme des P166. En V2 il ne doit rester que des categories
-- de prix, la ceremonie et l'oeuvre ayant rejoint leurs propres colonnes.
SELECT av.ID_ITEM,
       COALESCE(i.LABEL_EN, se.LABEL_EN, mo.LABEL_EN) AS LIBELLE
FROM T_WC_WIKIDATA_STATEMENT sa
JOIN T_WC_WIKIDATA_ITEM_VALUE av ON av.ID_STATEMENT = sa.ID_STATEMENT
LEFT JOIN T_WC_WIKIDATA_ITEM  i  ON i.ID_WIKIDATA  = av.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_SERIE se ON se.ID_WIKIDATA = av.ID_ITEM
LEFT JOIN T_WC_WIKIDATA_MOVIE mo ON mo.ID_WIKIDATA = av.ID_ITEM
WHERE sa.ID_WIKIDATA = 'Q120175513' AND sa.ID_PROPERTY = 'P166';
