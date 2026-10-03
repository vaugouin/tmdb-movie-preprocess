-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-052 : la correction faite sur Wikidata est-elle arrivee ?
-- ============================================================================
--
-- LE CONTEXTE. Le 2026-10-03, Philippe a corrige sur Wikidata les items qui
-- portaient « award received = Hollywood Walk of Fame (Q71719) », le lieu, au
-- lieu de « star on Hollywood Walk of Fame (Q17985761) ». Cinq items corriges :
-- Kristen Bell, Jim Henson, Anna Magnani (doublon supprime), Kelly Clarkson,
-- Nipsey Hussle (valeur remplacee). Deux restes en l'etat, items proteges :
-- Victor Young et The Beatles. Etat verifie le meme jour par SPARQL.
--
-- QUAND LE PASSER. Apres le premier run de wikidata-crawler sur un dump GENERE
-- APRES le 2026-10-03 (un dump commence avant ne contient pas les corrections) :
-- P1 et P2 disent ce qu'a recu V2. P3 et P4 ne bougent qu'apres le passage
-- suivant de tmdb-movie-preprocess, qui reconstruit T_WC_T2S_AWARD.
--
-- CE QU'ON ATTEND.
--   P1 . les cinq items corriges : sur_lieu = 0 et sur_etoile = 1 ;
--        Victor Young et The Beatles : sur_lieu = 1 tant qu'ils ne sont pas corriges.
--   P2 . le total vers Q71719 : 2 (les deux items proteges), 0 une fois corriges.
--   P3 . la ligne 27042 : 1 personne (Victor Young) tant qu'il n'est pas corrige,
--        puis plus aucun lien. Tant qu'elle a un lien, la resolution d'entite peut
--        encore la choisir.
--   P4 . la ligne 9550 : environ 2 177 + 2 (Kelly Clarkson et Nipsey Hussle).
--
-- LECTURE SEULE.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;


SELECT '=== P1 . les sept items, statement par statement ===' AS section;

SELECT q.ID_WIKIDATA,
       q.nom,
       q.attendu,
       COALESCE(SUM(iv.ID_ITEM = 'Q71719'    AND (st.DELETED IS NULL OR st.DELETED = 0)), 0) AS sur_lieu,
       COALESCE(SUM(iv.ID_ITEM = 'Q17985761' AND (st.DELETED IS NULL OR st.DELETED = 0)), 0) AS sur_etoile,
       MAX(st.IMPORT_BATCH_ID)                                                               AS dernier_batch
FROM  (SELECT 'Q178882'  AS ID_WIKIDATA, 'Kristen Bell'   AS nom, 'corrige'  AS attendu UNION ALL
       SELECT 'Q191037',  'Jim Henson',     'corrige'  UNION ALL
       SELECT 'Q56011',   'Anna Magnani',   'corrige'  UNION ALL
       SELECT 'Q483507',  'Kelly Clarkson', 'corrige'  UNION ALL
       SELECT 'Q2073970', 'Nipsey Hussle',  'corrige'  UNION ALL
       SELECT 'Q365199',  'Victor Young',   'protege'  UNION ALL
       SELECT 'Q1299',    'The Beatles',    'protege') AS q
LEFT   JOIN T_WC_WIKIDATA_STATEMENT st  ON st.ID_WIKIDATA = q.ID_WIKIDATA COLLATE utf8mb4_unicode_ci
                                       AND st.ID_PROPERTY = 'P166'
LEFT   JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                                       AND iv.ID_ITEM IN ('Q71719', 'Q17985761')
GROUP  BY q.ID_WIKIDATA, q.nom, q.attendu
ORDER  BY q.attendu, q.nom;

-- Une ligne a 0 / 0 partout : l'item n'a aucun statement P166 vers l'une ou
-- l'autre valeur dans V2, ce qui pour un item corrige veut dire que le dump n'a
-- pas encore ete importe, ou que l'item est hors perimetre.


SELECT '=== P2 . tous les statements P166 qui pointent encore vers le lieu ===' AS section;

SELECT st.ID_WIKIDATA,
       st.IMPORT_BATCH_ID,
       st.DELETED
FROM   T_WC_WIKIDATA_ITEM_VALUE iv
JOIN   T_WC_WIKIDATA_STATEMENT st ON st.ID_STATEMENT = iv.ID_STATEMENT
WHERE  iv.ID_ITEM = 'Q71719'
  AND  st.ID_PROPERTY = 'P166'
ORDER  BY st.ID_WIKIDATA;


SELECT '=== P3 . P4 . les deux lignes de T_WC_T2S_AWARD (apres tmdb-movie-preprocess) ===' AS section;

SELECT a.ID_AWARD,
       a.ID_WIKIDATA,
       a.AWARD_NAME,
       a.DELETED,
       (SELECT COUNT(*) FROM T_WC_T2S_PERSON_AWARD pa
         WHERE pa.ID_AWARD = a.ID_AWARD AND (pa.DELETED IS NULL OR pa.DELETED = 0)) AS liens_personnes,
       (SELECT GROUP_CONCAT(p.PERSON_NAME ORDER BY p.PERSON_NAME SEPARATOR ', ')
          FROM T_WC_T2S_PERSON_AWARD pa
          JOIN T_WC_T2S_PERSON p ON p.ID_PERSON = pa.ID_PERSON
         WHERE pa.ID_AWARD = a.ID_AWARD AND a.ID_AWARD = 27042
           AND (pa.DELETED IS NULL OR pa.DELETED = 0))                            AS personnes_sur_le_lieu
FROM   T_WC_T2S_AWARD a
WHERE  a.ID_WIKIDATA IN ('Q71719', 'Q17985761')
ORDER  BY a.ID_AWARD;
