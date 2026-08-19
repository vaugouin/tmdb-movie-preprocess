-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-036 : ce qu'une suppression couterait vraiment
-- ============================================================================
--
-- LA DECISION QUE CE FICHIER ECLAIRE. Depuis WIKIDATA-CRAWLER-020, on sait trier
-- T_WC_T2S_AWARD par la classe Wikidata de chaque ligne : 15 612 lignes sur
-- 44 084 ont un P31 dans le cone P279 sous Q618779 (award), 27 920 n'en ont
-- aucun, 552 n'ont pas de P31 du tout. Le filtre garderait donc un tiers de la
-- table et en jetterait deux tiers.
--
-- Ce chiffre ne suffit pas a decider. Une ligne que rien ne reference ne coute
-- rien a supprimer ; une ligne affichee aujourd'hui sur une fiche film, serie ou
-- personne est une regression en attente. La question n'est pas « combien de
-- lignes » mais « combien de liens », et surtout « lesquels ».
--
-- CE QUE CHAQUE BLOC TRANCHE.
--   I1 . le cout global : lignes et liens, par verdict
--   I2 . les 30 lignes condamnees les plus referencees, a regarder une par une
--   I3 . les 552 sans classe : oubli de cache ou vraie pollution ?
--   I4 . le meme calcul sur T_WC_T2S_NOMINATION, meme mecanisme, meme cone
--   I5 . le contre-test : que reste-t-il d'affichable apres le filtre ?
--
-- COMMENT LIRE I1. Si les lignes gardees concentrent la quasi-totalite des liens,
-- la suppression est indolore et peut se faire. Si les lignes condamnees en
-- portent une part importante, la conclusion n'est pas « ne rien faire » mais
-- « l'application affiche aujourd'hui majoritairement de la pollution », ce qui
-- est un constat plus grave, pas un feu vert plus faible.
--
-- LECTURE SEULE. Aucun DELETE ici, et il ne faut pas en ajouter : ce fichier
-- mesure, un autre supprimera. Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;


-- ############################################################################
-- I1 . LE COUT GLOBAL : LIGNES ET LIENS, PAR VERDICT
-- ############################################################################
-- Le CAST dans l'ancre du CTE est obligatoire : sans lui MariaDB type la colonne
-- sur la longueur du litteral et refuse ses propres Q-ids (ERROR 1406).
--
-- « au moins un P31 dans le cone » et non « tous » : une categorie de prix porte
-- souvent plusieurs P31 sur des axes differents (Q103618 pointe a la fois la
-- famille Q19020 et l'archetype Q96474676), et 575 lignes sont des deux cotes.

SELECT '=== I1 . cout global d une suppression, par verdict ===' AS section;

WITH RECURSIVE cone_award (qid) AS (
    SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    FROM   (SELECT 'Q618779' AS qid) AS r
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   cone_award c ON c.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
),
verdict_award AS (
    SELECT a.ID_AWARD,
           MAX(CASE WHEN piv.ID_ITEM IN (SELECT qid FROM cone_award) THEN 1 ELSE 0 END) AS dans_le_cone,
           COUNT(piv.ID_ITEM) AS nb_p31
    FROM   T_WC_T2S_AWARD a
    LEFT   JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = a.ID_WIKIDATA
                                          AND st.ID_PROPERTY = 'P31'
    LEFT   JOIN T_WC_WIKIDATA_ITEM_VALUE piv ON piv.ID_STATEMENT = st.ID_STATEMENT
    WHERE  a.DELETED IS NULL OR a.DELETED = 0
    GROUP  BY a.ID_AWARD
),
liens_movie  AS (SELECT ID_AWARD, COUNT(*) AS n FROM T_WC_T2S_MOVIE_AWARD  WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_AWARD),
liens_serie  AS (SELECT ID_AWARD, COUNT(*) AS n FROM T_WC_T2S_SERIE_AWARD  WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_AWARD),
liens_person AS (SELECT ID_AWARD, COUNT(*) AS n FROM T_WC_T2S_PERSON_AWARD WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_AWARD)
SELECT CASE WHEN v.dans_le_cone = 1 THEN 'A gardee   (P31 dans le cone)'
            WHEN v.nb_p31       = 0 THEN 'C sans classe (a examiner)'
            ELSE                         'B condamnee (P31 hors cone)' END AS verdict,
       COUNT(*)                                                      AS lignes_award,
       SUM(COALESCE(lm.n,0) + COALESCE(ls.n,0) + COALESCE(lp.n,0) > 0) AS lignes_referencees,
       SUM(COALESCE(lm.n,0))                                         AS liens_films,
       SUM(COALESCE(ls.n,0))                                         AS liens_series,
       SUM(COALESCE(lp.n,0))                                         AS liens_personnes,
       SUM(COALESCE(lm.n,0) + COALESCE(ls.n,0) + COALESCE(lp.n,0))   AS liens_total
FROM   verdict_award v
LEFT   JOIN liens_movie  lm ON lm.ID_AWARD = v.ID_AWARD
LEFT   JOIN liens_serie  ls ON ls.ID_AWARD = v.ID_AWARD
LEFT   JOIN liens_person lp ON lp.ID_AWARD = v.ID_AWARD
GROUP  BY verdict
ORDER  BY verdict;


-- ############################################################################
-- I2 . LES LIGNES CONDAMNEES LES PLUS REFERENCEES
-- ############################################################################
-- Le chiffre global ne dit pas si les liens perdus sont dilues sur des milliers
-- de lignes anecdotiques ou concentres sur quelques entrees tres vues. Ces trente
-- lignes se regardent une par une : c'est la que se decide un eventuel
-- traitement particulier plutot qu'une suppression uniforme.

SELECT '=== I2 . les 30 lignes condamnees qui coutent le plus ===' AS section;

WITH RECURSIVE cone_award (qid) AS (
    SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    FROM   (SELECT 'Q618779' AS qid) AS r
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   cone_award c ON c.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
),
verdict_award AS (
    SELECT a.ID_AWARD,
           MAX(CASE WHEN piv.ID_ITEM IN (SELECT qid FROM cone_award) THEN 1 ELSE 0 END) AS dans_le_cone,
           COUNT(piv.ID_ITEM) AS nb_p31
    FROM   T_WC_T2S_AWARD a
    LEFT   JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = a.ID_WIKIDATA
                                          AND st.ID_PROPERTY = 'P31'
    LEFT   JOIN T_WC_WIKIDATA_ITEM_VALUE piv ON piv.ID_STATEMENT = st.ID_STATEMENT
    WHERE  a.DELETED IS NULL OR a.DELETED = 0
    GROUP  BY a.ID_AWARD
),
liens_movie  AS (SELECT ID_AWARD, COUNT(*) AS n FROM T_WC_T2S_MOVIE_AWARD  WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_AWARD),
liens_serie  AS (SELECT ID_AWARD, COUNT(*) AS n FROM T_WC_T2S_SERIE_AWARD  WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_AWARD),
liens_person AS (SELECT ID_AWARD, COUNT(*) AS n FROM T_WC_T2S_PERSON_AWARD WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_AWARD)
SELECT a.ID_AWARD,
       a.ID_WIKIDATA,
       COALESCE(NULLIF(a.AWARD_NAME,''), '(nom vide)') AS nom,
       a.AWARD_SOURCE                                  AS source,
       COALESCE(GROUP_CONCAT(DISTINCT cl.LABEL_EN ORDER BY cl.LABEL_EN SEPARATOR ' | '),
                '(classe non cachee)')                 AS classes_p31,
       COALESCE(lm.n,0) AS liens_films,
       COALESCE(ls.n,0) AS liens_series,
       COALESCE(lp.n,0) AS liens_personnes,
       COALESCE(lm.n,0) + COALESCE(ls.n,0) + COALESCE(lp.n,0) AS liens_total
FROM   verdict_award v
JOIN   T_WC_T2S_AWARD a ON a.ID_AWARD = v.ID_AWARD
LEFT   JOIN liens_movie  lm ON lm.ID_AWARD = v.ID_AWARD
LEFT   JOIN liens_serie  ls ON ls.ID_AWARD = v.ID_AWARD
LEFT   JOIN liens_person lp ON lp.ID_AWARD = v.ID_AWARD
LEFT   JOIN T_WC_WIKIDATA_STATEMENT st31 ON st31.ID_WIKIDATA = a.ID_WIKIDATA
                                        AND st31.ID_PROPERTY = 'P31'
LEFT   JOIN T_WC_WIKIDATA_ITEM_VALUE iv31 ON iv31.ID_STATEMENT = st31.ID_STATEMENT
LEFT   JOIN T_WC_WIKIDATA_ITEM cl ON cl.ID_WIKIDATA = iv31.ID_ITEM
WHERE  v.dans_le_cone = 0
  AND  v.nb_p31 > 0
GROUP  BY a.ID_AWARD, a.ID_WIKIDATA, nom, a.AWARD_SOURCE, classe_p31,
          lm.n, ls.n, lp.n
ORDER  BY liens_total DESC
LIMIT  30;


-- ############################################################################
-- I3 . LES LIGNES SANS CLASSE : OUBLI DE CACHE OU VRAIE POLLUTION ?
-- ############################################################################
-- 552 lignes n'ont aucun P31 lisible en V2. Deux causes possibles, opposees : la
-- classe existe chez Wikidata mais n'a pas ete mise en cache (la ligne est une
-- vraie recompense qu'il ne faut pas jeter), ou l'entite n'est pas une entite de
-- prix du tout. Le nom et la source departagent a l'oeil.

SELECT '=== I3 . les lignes sans P31, echantillon de 40 ===' AS section;

WITH liens AS (
    SELECT ID_AWARD, SUM(n) AS liens_total FROM (
        SELECT ID_AWARD, COUNT(*) AS n FROM T_WC_T2S_MOVIE_AWARD  WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_AWARD
        UNION ALL
        SELECT ID_AWARD, COUNT(*)        FROM T_WC_T2S_SERIE_AWARD  WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_AWARD
        UNION ALL
        SELECT ID_AWARD, COUNT(*)        FROM T_WC_T2S_PERSON_AWARD WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_AWARD
    ) u GROUP BY ID_AWARD
)
SELECT a.ID_AWARD,
       a.ID_WIKIDATA,
       COALESCE(NULLIF(a.AWARD_NAME,''), '(nom vide)') AS nom,
       a.AWARD_SOURCE                                  AS source,
       COALESCE(l.liens_total, 0)                      AS liens_total,
       CASE WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM   i WHERE i.ID_WIKIDATA = a.ID_WIKIDATA) THEN 'ITEM'
            WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  m WHERE m.ID_WIKIDATA = a.ID_WIKIDATA) THEN 'MOVIE'
            WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE  s WHERE s.ID_WIKIDATA = a.ID_WIKIDATA) THEN 'SERIE'
            WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON p WHERE p.ID_WIKIDATA = a.ID_WIKIDATA) THEN 'PERSON'
            ELSE 'ABSENTE DE V2' END                   AS ou_vit_l_entite
FROM   T_WC_T2S_AWARD a
LEFT   JOIN liens l ON l.ID_AWARD = a.ID_AWARD
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s
                   WHERE s.ID_WIKIDATA = a.ID_WIKIDATA AND s.ID_PROPERTY = 'P31')
ORDER  BY liens_total DESC
LIMIT  40;


-- ############################################################################
-- I4 . LE MEME CALCUL SUR LES NOMINATIONS
-- ############################################################################
-- Meme mecanisme amont (P1411 au lieu de P166, co-nommes P1706 aplatis au lieu
-- des ceremonies), donc meme pollution attendue et meme cone pour la trancher.
-- Le ticket couvre les deux tables ; il serait incoherent de n'en mesurer qu'une.

SELECT '=== I4 . cout global sur T_WC_T2S_NOMINATION ===' AS section;

WITH RECURSIVE cone_award (qid) AS (
    SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    FROM   (SELECT 'Q618779' AS qid) AS r
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   cone_award c ON c.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
),
verdict_nom AS (
    SELECT n.ID_NOMINATION,
           MAX(CASE WHEN piv.ID_ITEM IN (SELECT qid FROM cone_award) THEN 1 ELSE 0 END) AS dans_le_cone,
           COUNT(piv.ID_ITEM) AS nb_p31
    FROM   T_WC_T2S_NOMINATION n
    LEFT   JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = n.ID_WIKIDATA
                                          AND st.ID_PROPERTY = 'P31'
    LEFT   JOIN T_WC_WIKIDATA_ITEM_VALUE piv ON piv.ID_STATEMENT = st.ID_STATEMENT
    WHERE  n.DELETED IS NULL OR n.DELETED = 0
    GROUP  BY n.ID_NOMINATION
),
liens_movie  AS (SELECT ID_NOMINATION, COUNT(*) AS n FROM T_WC_T2S_MOVIE_NOMINATION  WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_NOMINATION),
liens_serie  AS (SELECT ID_NOMINATION, COUNT(*) AS n FROM T_WC_T2S_SERIE_NOMINATION  WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_NOMINATION),
liens_person AS (SELECT ID_NOMINATION, COUNT(*) AS n FROM T_WC_T2S_PERSON_NOMINATION WHERE DELETED IS NULL OR DELETED = 0 GROUP BY ID_NOMINATION)
SELECT CASE WHEN v.dans_le_cone = 1 THEN 'A gardee   (P31 dans le cone)'
            WHEN v.nb_p31       = 0 THEN 'C sans classe (a examiner)'
            ELSE                         'B condamnee (P31 hors cone)' END AS verdict,
       COUNT(*)                                                      AS lignes_nomination,
       SUM(COALESCE(lm.n,0) + COALESCE(ls.n,0) + COALESCE(lp.n,0) > 0) AS lignes_referencees,
       SUM(COALESCE(lm.n,0) + COALESCE(ls.n,0) + COALESCE(lp.n,0))   AS liens_total
FROM   verdict_nom v
LEFT   JOIN liens_movie  lm ON lm.ID_NOMINATION = v.ID_NOMINATION
LEFT   JOIN liens_serie  ls ON ls.ID_NOMINATION = v.ID_NOMINATION
LEFT   JOIN liens_person lp ON lp.ID_NOMINATION = v.ID_NOMINATION
GROUP  BY verdict
ORDER  BY verdict;


-- ############################################################################
-- I5 . LE CONTRE-TEST : QUE RESTE-T-IL D AFFICHABLE APRES LE FILTRE ?
-- ############################################################################
-- Mesurer ce qu'on perd ne suffit pas, il faut savoir ce qu'on garde. Une entite
-- qui perdrait TOUS ses prix est un appauvrissement visible de la fiche ; une
-- entite qui en garde l'essentiel ne verra rien. Ce bloc compte les personnes,
-- films et series qui passeraient de « au moins un prix » a « aucun ».
--
-- C'est le bon indicateur de regression, meilleur que le nombre de liens perdus :
-- l'utilisateur ne voit pas des liens, il voit des fiches qui se vident.

SELECT '=== I5 . entites qui perdraient TOUS leurs prix ===' AS section;

WITH RECURSIVE cone_award (qid) AS (
    SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    FROM   (SELECT 'Q618779' AS qid) AS r
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   cone_award c ON c.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
),
award_garde AS (
    SELECT a.ID_AWARD
    FROM   T_WC_T2S_AWARD a
    JOIN   T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = a.ID_WIKIDATA
                                     AND st.ID_PROPERTY = 'P31'
    JOIN   T_WC_WIKIDATA_ITEM_VALUE piv ON piv.ID_STATEMENT = st.ID_STATEMENT
    WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
      AND  piv.ID_ITEM IN (SELECT qid FROM cone_award)
    GROUP  BY a.ID_AWARD
)
SELECT 'personnes' AS entite,
       COUNT(*)    AS avec_prix_aujourd_hui,
       SUM(gardes = 0) AS perdraient_tout,
       ROUND(100 * SUM(gardes = 0) / COUNT(*), 1) AS pct
FROM   (SELECT pa.ID_PERSON,
               SUM(pa.ID_AWARD IN (SELECT ID_AWARD FROM award_garde)) AS gardes
        FROM   T_WC_T2S_PERSON_AWARD pa
        WHERE  pa.DELETED IS NULL OR pa.DELETED = 0
        GROUP  BY pa.ID_PERSON) x
UNION ALL
SELECT 'films',
       COUNT(*),
       SUM(gardes = 0),
       ROUND(100 * SUM(gardes = 0) / COUNT(*), 1)
FROM   (SELECT ma.ID_MOVIE,
               SUM(ma.ID_AWARD IN (SELECT ID_AWARD FROM award_garde)) AS gardes
        FROM   T_WC_T2S_MOVIE_AWARD ma
        WHERE  ma.DELETED IS NULL OR ma.DELETED = 0
        GROUP  BY ma.ID_MOVIE) y
UNION ALL
SELECT 'series',
       COUNT(*),
       SUM(gardes = 0),
       ROUND(100 * SUM(gardes = 0) / COUNT(*), 1)
FROM   (SELECT sa.ID_SERIE,
               SUM(sa.ID_AWARD IN (SELECT ID_AWARD FROM award_garde)) AS gardes
        FROM   T_WC_T2S_SERIE_AWARD sa
        WHERE  sa.DELETED IS NULL OR sa.DELETED = 0
        GROUP  BY sa.ID_SERIE) z;
