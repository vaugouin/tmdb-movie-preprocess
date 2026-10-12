-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-040 et -041 : recette des six processus restants
-- ============================================================================
--
-- CE QUI A BASCULE. Les processus 43 (groupes, P463 P108 P54), 46 (deces, P509
-- P1196), 61 (liaison, P179) et le mecanisme 2 des processus 41, 42, 45 lisent les
-- statements V2 au lieu de T_WC_WIKIDATA_ITEM_PROPERTY. La porte
-- T_WC_WIKIDATA_PERSON_V1 devient T_WC_WIKIDATA_PERSON.
--
-- ⚠ -040 PARTAIT D'UNE LECTURE FAUSSE, ET LA RECETTE EN TIENT COMPTE. Le ticket
-- decrivait PERSON_V1 comme un « pont TMDb vers Wikidata » et demandait de mesurer
-- la couverture de P4985 avant de basculer. C'est sans objet : T_WC_TMDB_PERSON
-- porte deja ID_WIKIDATA, le pont existe sans elle. La jointure est une PORTE, elle
-- exige que Wikidata connaisse l'entite comme une personne. Ce qu'il faut mesurer
-- est donc la couverture de la table d'entite V2, section B.
--
-- A LANCER DEUX FOIS, avant puis apres le passage de nuit.
--
-- ⚠ COLLATION. Lancer avec --force.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ============================================================================
-- B. LA PORTE PERSONNE : V2 connait-elle autant de personnes que V1 ?
-- ============================================================================
--
-- C'est le seul chiffre qui puisse bloquer -040. Une personne connue de V1 et pas de
-- V2 sort des groupes et des deces, et si elle etait la deuxieme d'un couple, elle
-- emporte le groupe entier avec elle : le seuil est a deux.
-- ============================================================================

SELECT 'B. La porte personne, sur les personnes TMDb suivies' AS SECTION;

SELECT COUNT(*)                                   AS PERSONNES_TMDB_AVEC_QID,
       SUM(v1.ID_WIKIDATA IS NOT NULL)            AS CONNUES_DE_V1,
       SUM(v2.ID_WIKIDATA IS NOT NULL)            AS CONNUES_DE_V2,
       SUM(v1.ID_WIKIDATA IS NOT NULL AND v2.ID_WIKIDATA IS NULL) AS PERTE,
       SUM(v1.ID_WIKIDATA IS NULL AND v2.ID_WIKIDATA IS NOT NULL) AS GAIN
FROM T_WC_TMDB_PERSON p
LEFT JOIN T_WC_WIKIDATA_PERSON_V1 v1 ON v1.ID_WIKIDATA = p.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_PERSON    v2 ON v2.ID_WIKIDATA = p.ID_WIKIDATA
WHERE p.ID_WIKIDATA IS NOT NULL AND p.ID_WIKIDATA <> '';

SELECT 'B2. Dix personnes que V2 ne connait pas (attendu : peu)' AS SECTION;

SELECT p.ID_PERSON, p.NAME, p.ID_WIKIDATA, p.POPULARITY
FROM T_WC_TMDB_PERSON p
INNER JOIN T_WC_WIKIDATA_PERSON_V1 v1 ON v1.ID_WIKIDATA = p.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_PERSON    v2 ON v2.ID_WIKIDATA = p.ID_WIKIDATA
WHERE v2.ID_WIKIDATA IS NULL
ORDER BY p.POPULARITY DESC
LIMIT 10;


-- ============================================================================
-- A. TAILLE DES ENSEMBLES PILOTES, V1 contre V2
-- ============================================================================
--
-- Meme seuil de deux personnes des deux cotes, meme porte a son etage : la
-- difference ne doit tenir qu'a la source.
-- ============================================================================

SELECT 'A1. Groupes et deces, ensemble pilote V1' AS SECTION;

SELECT ip.ID_PROPERTY AS PROPRIETE, COUNT(*) AS ITEMS_PILOTES
FROM ( SELECT ip.ID_PROPERTY, ip.ID_ITEM
       FROM T_WC_WIKIDATA_ITEM_PROPERTY ip
       INNER JOIN T_WC_TMDB_PERSON tp ON tp.ID_WIKIDATA = ip.ID_WIKIDATA
       INNER JOIN T_WC_WIKIDATA_PERSON_V1 wp ON tp.ID_WIKIDATA = wp.ID_WIKIDATA
       WHERE ip.ID_PROPERTY IN ('P463','P108','P54','P509','P1196')
       GROUP BY ip.ID_PROPERTY, ip.ID_ITEM
       HAVING COUNT(DISTINCT tp.ID_PERSON) >= 2 ) ip
GROUP BY ip.ID_PROPERTY;

SELECT 'A2. Groupes et deces, ensemble pilote V2' AS SECTION;

SELECT sp.ID_PROPERTY AS PROPRIETE, COUNT(*) AS ITEMS_PILOTES
FROM ( SELECT sp.ID_PROPERTY, pv.ID_ITEM
       FROM T_WC_WIKIDATA_STATEMENT sp
       JOIN T_WC_WIKIDATA_ITEM_VALUE pv ON pv.ID_STATEMENT = sp.ID_STATEMENT
       INNER JOIN T_WC_TMDB_PERSON tp ON tp.ID_WIKIDATA = sp.ID_WIKIDATA
       INNER JOIN T_WC_WIKIDATA_PERSON wp ON tp.ID_WIKIDATA = wp.ID_WIKIDATA
       WHERE sp.ID_PROPERTY IN ('P463','P108','P54','P509','P1196')
         AND (sp.`RANK` IS NULL OR sp.`RANK` <> 'deprecated')
       GROUP BY sp.ID_PROPERTY, pv.ID_ITEM
       HAVING COUNT(DISTINCT tp.ID_PERSON) >= 2 ) sp
GROUP BY sp.ID_PROPERTY;


-- ============================================================================
-- C. CE QUI SORT DU PILOTE DES GROUPES, ET POURQUOI
-- ============================================================================
--
-- Mêmes causes qu'en -039, et pour la meme raison : V1 aplatissait la valeur
-- principale et les qualificatifs sous un seul identifiant de propriete.
--
-- QUALIFICATIF     l'item est ailleurs valeur d'un qualificatif. Aplatissement.
-- PORTE PERSONNE   les personnes qui le portaient ne passent plus la porte V2.
-- HORS PERIMETRE   l'item n'a aucun statement en V2. WIKIDATA-CRAWLER-011, tranche.
-- ⚠ INEXPLIQUE     aucune des trois. Le seul chiffre qui puisse bloquer.
-- ============================================================================

SELECT 'C. Items quittant le pilote des groupes (P463), par cause' AS SECTION;

SELECT CASE
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER sq
                      JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qv
                        ON qv.ID_STATEMENT_QUALIFIER = sq.ID_STATEMENT_QUALIFIER
                      WHERE qv.ID_ITEM = d.ID_ITEM)
           THEN 'QUALIFICATIF'
         WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s2
                          WHERE s2.ID_WIKIDATA = d.ID_ITEM)
           THEN 'HORS PERIMETRE'
         WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s3
                      JOIN T_WC_WIKIDATA_ITEM_VALUE v3 ON v3.ID_STATEMENT = s3.ID_STATEMENT
                      INNER JOIN T_WC_TMDB_PERSON tp3 ON tp3.ID_WIKIDATA = s3.ID_WIKIDATA
                      WHERE v3.ID_ITEM = d.ID_ITEM AND s3.ID_PROPERTY = 'P463')
           THEN 'PORTE PERSONNE'
         ELSE 'INEXPLIQUE'
       END AS CAUSE,
       COUNT(*) AS ITEMS
FROM ( SELECT ip.ID_ITEM
       FROM T_WC_WIKIDATA_ITEM_PROPERTY ip
       INNER JOIN T_WC_TMDB_PERSON tp ON tp.ID_WIKIDATA = ip.ID_WIKIDATA
       INNER JOIN T_WC_WIKIDATA_PERSON_V1 wp ON tp.ID_WIKIDATA = wp.ID_WIKIDATA
       WHERE ip.ID_PROPERTY = 'P463'
       GROUP BY ip.ID_ITEM
       HAVING COUNT(DISTINCT tp.ID_PERSON) >= 2 ) d
WHERE NOT EXISTS (
        SELECT 1
        FROM T_WC_WIKIDATA_STATEMENT sp
        JOIN T_WC_WIKIDATA_ITEM_VALUE pv ON pv.ID_STATEMENT = sp.ID_STATEMENT
        INNER JOIN T_WC_TMDB_PERSON tp2 ON tp2.ID_WIKIDATA = sp.ID_WIKIDATA
        INNER JOIN T_WC_WIKIDATA_PERSON wp2 ON tp2.ID_WIKIDATA = wp2.ID_WIKIDATA
        WHERE sp.ID_PROPERTY = 'P463' AND pv.ID_ITEM = d.ID_ITEM
          AND (sp.`RANK` IS NULL OR sp.`RANK` <> 'deprecated')
        GROUP BY pv.ID_ITEM
        HAVING COUNT(DISTINCT tp2.ID_PERSON) >= 2 )
GROUP BY CAUSE
ORDER BY ITEMS DESC;


-- ============================================================================
-- D. LE PROCESSUS 61 : les collections dont TOUS les films partagent une serie
-- ============================================================================
--
-- Sa requete compte deux fois, une fois pour la collection courante et une fois pour
-- l'item entier, et les deux compteurs doivent rester d'accord. Une divergence entre
-- V1 et V2 se verrait ici avant de se voir dans la table produite.
-- ============================================================================

SELECT 'D. P179, items partages par au moins deux entites' AS SECTION;

SELECT 'V1' AS SOURCE, COUNT(*) AS ITEMS
FROM ( SELECT ip.ID_ITEM FROM T_WC_WIKIDATA_ITEM_PROPERTY ip
       WHERE ip.ID_PROPERTY = 'P179'
       GROUP BY ip.ID_ITEM HAVING COUNT(DISTINCT ip.ID_WIKIDATA) >= 2 ) a
UNION ALL
SELECT 'V2', COUNT(*)
FROM ( SELECT pv.ID_ITEM
       FROM T_WC_WIKIDATA_STATEMENT sp
       JOIN T_WC_WIKIDATA_ITEM_VALUE pv ON pv.ID_STATEMENT = sp.ID_STATEMENT
       WHERE sp.ID_PROPERTY = 'P179'
         AND (sp.`RANK` IS NULL OR sp.`RANK` <> 'deprecated')
       GROUP BY pv.ID_ITEM HAVING COUNT(DISTINCT sp.ID_WIKIDATA) >= 2 ) b;


-- ============================================================================
-- E. APRES LE PASSAGE
-- ============================================================================
--
-- Reference d'avant bascule, a relever au premier passage pour pouvoir comparer.
-- ============================================================================

SELECT 'E1. Volumes produits' AS SECTION;

SELECT 'T_WC_T2S_GROUP' AS TABLE_T2S, COUNT(*) AS LIGNES FROM T_WC_T2S_GROUP
UNION ALL SELECT 'T_WC_T2S_DEATH',      COUNT(*) FROM T_WC_T2S_DEATH
UNION ALL SELECT 'T_WC_T2S_COLLECTION', COUNT(*) FROM T_WC_T2S_COLLECTION
UNION ALL SELECT 'T_WC_T2S_LIST',       COUNT(*) FROM T_WC_T2S_LIST
UNION ALL SELECT 'T_WC_T2S_MOVEMENT',   COUNT(*) FROM T_WC_T2S_MOVEMENT;

SELECT 'E2. Lignes que le pilote V2 ne produirait plus (attendu : 0 apres purge)' AS SECTION;

SELECT 'T_WC_T2S_GROUP' AS TABLE_T2S, COUNT(*) AS ORPHELINES
FROM T_WC_T2S_GROUP g
WHERE g.GROUP_SOURCE <> 'custom'
  AND ( SELECT COUNT(DISTINCT p.ID_PERSON)
        FROM T_WC_WIKIDATA_ITEM_VALUE wv
        JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT
        INNER JOIN T_WC_TMDB_PERSON p ON p.ID_WIKIDATA = w.ID_WIKIDATA
        INNER JOIN T_WC_WIKIDATA_PERSON wp ON p.ID_WIKIDATA = wp.ID_WIKIDATA
        WHERE w.ID_PROPERTY = g.GROUP_SOURCE AND wv.ID_ITEM = g.ID_WIKIDATA
          AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated') ) < 2

UNION ALL

SELECT 'T_WC_T2S_DEATH', COUNT(*)
FROM T_WC_T2S_DEATH dh
WHERE ( SELECT COUNT(DISTINCT p.ID_PERSON)
        FROM T_WC_WIKIDATA_ITEM_VALUE wv
        JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT
        INNER JOIN T_WC_TMDB_PERSON p ON p.ID_WIKIDATA = w.ID_WIKIDATA
        INNER JOIN T_WC_WIKIDATA_PERSON wp ON p.ID_WIKIDATA = wp.ID_WIKIDATA
        WHERE w.ID_PROPERTY = dh.DEATH_SOURCE AND wv.ID_ITEM = dh.ID_WIKIDATA
          AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated') ) < 2;
