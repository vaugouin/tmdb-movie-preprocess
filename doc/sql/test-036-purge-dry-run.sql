-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-036 : la clause de purge, jouee en SELECT avant la nuit
-- ============================================================================
--
-- POURQUOI CE FICHIER EXISTE. P3 a valide la requete PILOTE en conditions reelles,
-- elle a rendu 16 164. La clause de PURGE, elle, n'a jamais ete executee : c'est la
-- meme regle ecrite pour un autre alias (w.ID_ITEM au lieu de
-- T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM), et une erreur de syntaxe SQL n'y serait pas
-- rattrapee par le garde-fou du cone, qui protege d'un graphe incomplet et non d'un
-- SQL mal forme. Une exception dans le processus 44 couterait les cinquante
-- processus suivants, la boucle n'ayant pas de try.
--
-- Ce fichier rejoue donc la clause telle quelle, en SELECT COUNT au lieu de DELETE.
-- Il ne modifie rien. S'il rend un nombre, la syntaxe est bonne et la nuit peut
-- passer. S'il rend une erreur, il faut corriger avant le run.
--
-- ATTENDU : 27 920, le meme chiffre que P2, P4 et I1. Un ecart signifierait que la
-- purge et la construction ne disent pas la meme chose, ce qui ferait osciller le
-- processus, recreant chaque nuit ce qu'il vient d'effacer.
--
-- PREALABLE : T_WC_T2S_AWARD_CLASS doit exister. La creer avec le bloc P1 de
-- award-cone-filter-preparation.sql, ou laisser le processus la construire.
--
-- LECTURE SEULE. Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

SELECT '=== la clause de purge, en SELECT ===' AS section;

SELECT COUNT(*) AS lignes_que_la_purge_supprimerait, 27920 AS repere
FROM T_WC_T2S_AWARD
WHERE NOT EXISTS (
    SELECT 1
    FROM T_WC_WIKIDATA_ITEM_PROPERTY w
    WHERE w.ID_PROPERTY = T_WC_T2S_AWARD.AWARD_SOURCE
      AND w.ID_ITEM = T_WC_T2S_AWARD.ID_WIKIDATA
      AND (
            EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = w.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')
         OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = w.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')
         OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = w.ID_WIKIDATA AND pe.ID_WIKIDATA <> '')
      )
      AND ( EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st2 JOIN T_WC_WIKIDATA_ITEM_VALUE iv2 ON iv2.ID_STATEMENT = st2.ID_STATEMENT JOIN T_WC_T2S_AWARD_CLASS ac2 ON ac2.ID_CLASS = iv2.ID_ITEM WHERE st2.ID_WIKIDATA = w.ID_ITEM AND st2.ID_PROPERTY = 'P31') OR NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st2 WHERE st2.ID_WIKIDATA = w.ID_ITEM AND st2.ID_PROPERTY = 'P31') ) 
);
