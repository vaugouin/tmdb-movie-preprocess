-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-036 : preparer le filtre, et le mesurer avant de coder
-- ============================================================================
--
-- CE QU ON A APPRIS EN LISANT LE CODE, ET QUI SIMPLIFIE TOUT. Le processus 44
-- purge deja : `tmdb-movie-preprocess.py:2933` execute un DELETE qui ramene
-- T_WC_T2S_AWARD a ce que produit sa requete pilote, et les lignes suivantes
-- nettoient les jonctions orphelines. Il n'y a donc AUCUN script de suppression a
-- ecrire, et il ne faut surtout pas en ecrire un : une suppression manuelle serait
-- defaite au passage suivant, qui recreerait les memes lignes depuis la meme
-- source. Le correctif tient dans la requete pilote, et le retour arriere est un
-- git revert suivi d'un run.
--
-- LA CAUSE, EN UNE PHRASE. La requete pilote (`tmdb-movie-preprocess.py:2729-2737`)
-- filtre sur le SUJET, celui qui recoit le prix, et prend la VALEUR, le prix
-- lui-meme, sans aucun filtre. Personne ne verifie que ce qu'on range comme
-- recompense en est une.
--
-- LA REGLE PROPOSEE, mesuree le 2026-08-19. Garder une valeur si sa classe P31 est
-- dans le cone P279 sous Q618779 (award), OU si elle n'a aucun P31. Le second
-- terme n'est pas un relachement : c'est la protection des 552 lignes sans classe,
-- parmi lesquelles se trouvent de vraies recompenses absentes de V2 (Waldo Salt
-- Screenwriting Award, prix Feneon, Gaudi Awards) qu'un filtre strict detruirait.
--
-- CE QUE CHAQUE BLOC FAIT.
--   P1 . construit la table d'appoint du cone (SEUL BLOC QUI ECRIT)
--   P2 . mesure a blanc l'effet exact de la regle sur T_WC_T2S_AWARD
--   P3 . la requete pilote corrigee, telle qu'elle ira dans le code
--   P4 . ce que la purge de la ligne 2933 supprimerait, jonctions comprises
--   P5 . le controle de non-destruction : les 552 sans classe survivent-elles ?
--
-- P1 EST LE SEUL BLOC QUI MODIFIE LA BASE, et il ne touche a aucune donnee
-- metier : il cree une table d'appoint, il peut etre rejoue sans consequence, et
-- la supprimer ne casse rien tant que le code n'a pas ete change. Tout le reste
-- est en lecture seule.
--
-- Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;


-- ############################################################################
-- P1 . LA TABLE D APPOINT DU CONE (seul bloc qui ecrit)
-- ############################################################################
-- Materialiser plutot que porter un CTE recursif a chaque iteration du processus.
-- 14 260 classes le 2026-08-19, donc une table minuscule, reconstruite en tete de
-- processus 44 une fois le code change.

DROP TABLE IF EXISTS T_WC_T2S_AWARD_CLASS;

CREATE TABLE T_WC_T2S_AWARD_CLASS (
    ID_CLASS    VARCHAR(50) NOT NULL,
    DAT_CREAT   DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ID_CLASS)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO T_WC_T2S_AWARD_CLASS (ID_CLASS)
WITH RECURSIVE cone_award (qid) AS (
    SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid
    FROM   (SELECT 'Q618779' AS qid) AS r
    UNION
    SELECT sc.ID_CHILD
    FROM   T_WC_WIKIDATA_SUBCLASS sc
    JOIN   cone_award c ON c.qid = sc.ID_PARENT
    WHERE  sc.DELETED = 0
)
SELECT qid FROM cone_award;

SELECT '=== P1 . classes du cone chargees ===' AS section;

SELECT COUNT(*) AS classes_du_cone, 14260 AS repere_20260819
FROM   T_WC_T2S_AWARD_CLASS;


-- ############################################################################
-- P2 . L EFFET EXACT DE LA REGLE, A BLANC
-- ############################################################################
-- Reperes du 2026-08-19 : 15 612 dans le cone, 552 sans classe, 27 920 hors cone.
-- La regle gardant les deux premieres populations, on attend 16 164 gardees.

SELECT '=== P2 . ce que la regle garderait et jetterait ===' AS section;

SELECT CASE WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                         JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                         JOIN T_WC_T2S_AWARD_CLASS ac ON ac.ID_CLASS = iv.ID_ITEM
                         WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
            THEN 'A gardee, classe dans le cone'
            WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                             WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
            THEN 'B gardee, aucune classe (protection)'
            ELSE 'C supprimee, classe hors cone' END AS verdict,
       COUNT(*) AS lignes_award
FROM   T_WC_T2S_AWARD a
WHERE  a.DELETED IS NULL OR a.DELETED = 0
GROUP  BY verdict
ORDER  BY verdict;


-- ############################################################################
-- P3 . LA REQUETE PILOTE CORRIGEE
-- ############################################################################
-- Forme exacte destinee a remplacer `tmdb-movie-preprocess.py:2729-2737`. La
-- clause sur le sujet est conservee telle quelle ; seule la clause sur ID_ITEM est
-- nouvelle. La lancer ici valide qu'elle rend bien le jeu attendu avant de la
-- porter dans le code.
--
-- Attendu : un compte proche des 16 164 de P2, aux entites non suivies pres.

SELECT '=== P3 . taille du jeu pilote apres correction ===' AS section;

SELECT COUNT(*) AS valeurs_retenues
FROM ( SELECT DISTINCT ip.ID_ITEM
       FROM   T_WC_WIKIDATA_ITEM_PROPERTY ip
       WHERE  ip.ID_PROPERTY = 'P166'
         AND  ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = ip.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
             OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = ip.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
             OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = ip.ID_WIKIDATA AND pe.ID_WIKIDATA <> '') )
         AND  ( EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                        JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                        JOIN T_WC_T2S_AWARD_CLASS ac ON ac.ID_CLASS = iv.ID_ITEM
                        WHERE st.ID_WIKIDATA = ip.ID_ITEM AND st.ID_PROPERTY = 'P31')
             OR NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                            WHERE st.ID_WIKIDATA = ip.ID_ITEM AND st.ID_PROPERTY = 'P31') ) ) AS pilote;


-- ############################################################################
-- P4 . CE QUE LA PURGE SUPPRIMERAIT, JONCTIONS COMPRISES
-- ############################################################################
-- La ligne 2933 supprime les lignes award ; les lignes 3009 a 3019 suppriment
-- ensuite les jonctions dont l'ID_AWARD a disparu. Ce bloc chiffre les deux, pour
-- que l'ordre de grandeur soit connu avant le run et non decouvert apres.

SELECT '=== P4 . volumes supprimes au prochain passage ===' AS section;

SELECT COUNT(DISTINCT a.ID_AWARD) AS lignes_award_supprimees,
       27920 AS repere_20260819
FROM   T_WC_T2S_AWARD a
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
               WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
  AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                   JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                   JOIN T_WC_T2S_AWARD_CLASS ac ON ac.ID_CLASS = iv.ID_ITEM
                   WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31');

SELECT '=== P4-bis . les jonctions qui tomberaient avec elles ===' AS section;

WITH condamnees AS (
    SELECT a.ID_AWARD
    FROM   T_WC_T2S_AWARD a
    WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
      AND  EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                   WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
      AND  NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                       JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                       JOIN T_WC_T2S_AWARD_CLASS ac ON ac.ID_CLASS = iv.ID_ITEM
                       WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
)
SELECT (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_AWARD  x JOIN condamnees c ON c.ID_AWARD = x.ID_AWARD) AS jonctions_films,
       (SELECT COUNT(*) FROM T_WC_T2S_SERIE_AWARD  x JOIN condamnees c ON c.ID_AWARD = x.ID_AWARD) AS jonctions_series,
       (SELECT COUNT(*) FROM T_WC_T2S_PERSON_AWARD x JOIN condamnees c ON c.ID_AWARD = x.ID_AWARD) AS jonctions_personnes;


-- ############################################################################
-- P5 . LE CONTROLE DE NON-DESTRUCTION
-- ############################################################################
-- La regle protege les lignes sans P31 : ce bloc verifie que les recompenses
-- reelles reperees par I3 en font bien partie et survivraient au filtre. Si l'une
-- d'elles apparaissait comme supprimee, la regle serait a revoir avant d'ecrire
-- la moindre ligne de code.

SELECT '=== P5 . les temoins d I3 survivent-ils ? ===' AS section;

SELECT a.ID_WIKIDATA,
       COALESCE(NULLIF(a.AWARD_NAME,''), '(vide)') AS nom,
       CASE WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                         JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                         JOIN T_WC_T2S_AWARD_CLASS ac ON ac.ID_CLASS = iv.ID_ITEM
                         WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
            THEN 'gardee, classe dans le cone'
            WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                             WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
            THEN 'gardee, aucune classe'
            ELSE 'SUPPRIMEE, a expliquer' END AS verdict
FROM   T_WC_T2S_AWARD a
WHERE  a.ID_WIKIDATA IN ('Q124458350','Q117100866','Q61478580','Q3117505',
                         'Q383301','Q642539','Q74681122','Q134384292')
ORDER  BY verdict, a.ID_WIKIDATA;
