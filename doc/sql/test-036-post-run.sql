-- ============================================================================
-- Recette du passage de nuit qui a execute -036 pour la premiere fois
-- ============================================================================
--
-- CE QUE CETTE NUIT DEVAIT FAIRE. Les processus 44 et 47 filtrent desormais leur
-- requete pilote sur le cone P279 sous Q618779, et leur purge applique la meme
-- regle. Attendu : T_WC_T2S_AWARD passe de 44 084 a ~16 164 lignes,
-- T_WC_T2S_NOMINATION de 30 602 a ~2 910, et ~57 078 lignes de jonction tombent
-- avec elles.
--
-- ⚠ LE PIEGE DE CETTE RECETTE, ET IL FAUT LE LIRE AVANT LES CHIFFRES. La recette
-- de -017 (test-017-controle-avant-apres-nuit.sql) compte les libelles francais
-- NON VIDES et exige qu'ils ne baissent pas. Elle est desormais inutilisable telle
-- quelle sur les prix et les nominations : -036 a supprime 63 % des lignes, donc
-- AWARD_NAME_FR va s'effondrer en valeur absolue, et c'est le resultat VOULU.
-- Sur ces deux tables il faut lire un TAUX, pas un compte. Le bloc N4 s'en charge.
-- Les quatre autres colonnes (*_ITEM, *_GROUP, *_DEATH, *_MOVEMENT) restent
-- comparables a l'ancienne reference, elles ne sont pas touchees par -036.
--
-- CE QUE CHAQUE BLOC TRANCHE.
--   N0 . le processus a-t-il seulement tourne, ou le garde-fou a-t-il saute ?
--   N1 . les volumes, tables et jonctions, contre les valeurs predites
--   N2 . la suppression a-t-elle frappe la bonne population, et elle seule ?
--   N3 . la regression visible : combien de fiches se sont vraiment videes ?
--   N4 . les colonnes de -017, relues en taux puisque le denominateur a change
--   N5 . la contamination de -017 a-t-elle disparu par ricochet ?
--
-- LECTURE SEULE. Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;


-- ############################################################################
-- N0 . LE PROCESSUS A-T-IL TOURNE ?
-- ############################################################################
-- A lire en premier et sans sauter. Si awardconeskipstreak vaut 1 ou plus, le
-- garde-fou a saute le traitement : rien n'a ete supprime, les tables sont celles
-- d'hier, et tout le reste de ce fichier ne fait que le confirmer. Ce n'est pas un
-- incident, c'est le comportement voulu quand T_WC_WIKIDATA_SUBCLASS est
-- incomplete ; awardconeskipreason dit pourquoi.
--
-- Attendus : status SUCCESS, coneclasses ~14 260, skipstreak 0,
-- awarddeletedcount ~27 920, nominationdeletedcount ~27 692.

SELECT '=== N0 . identite du run et telemetrie prix / nominations ===' AS section;

SELECT VAR_NAME AS variable, VAR_VALUE AS valeur, TIM_UPDATED
FROM   T_WC_SERVER_VARIABLE
WHERE  VAR_NAME IN ('strtmdbmoviepreprocessstartdatetime',
                    'strtmdbmoviepreprocessenddatetime',
                    'strtmdbmoviepreprocesstotalruntime',
                    'strtmdbmoviepreprocessawardconeclasses',
                    'strtmdbmoviepreprocessawardconeskipstreak',
                    'strtmdbmoviepreprocessawardconeskipreason',
                    'strtmdbmoviepreprocessawardcreatedcount',
                    'strtmdbmoviepreprocessawarddeletedcount',
                    'strtmdbmoviepreprocessawardprocessedcount',
                    'strtmdbmoviepreprocessawardprocessedseconds',
                    'strtmdbmoviepreprocessnominationcreatedcount',
                    'strtmdbmoviepreprocessnominationdeletedcount',
                    'strtmdbmoviepreprocessnominationprocessedcount',
                    'strtmdbmoviepreprocessnominationprocessedseconds')
ORDER  BY VAR_NAME;


-- ############################################################################
-- N1 . LES VOLUMES
-- ############################################################################
-- Comptages reels, jamais information_schema : la regle du dossier, apprise le
-- 2026-08-16 quand une estimation avait fait conclure a la disparition d'un film
-- sur cinq qui n'avait jamais eu lieu.

SELECT '=== N1a . les deux tables, avant et apres ===' AS section;

SELECT 'T_WC_T2S_AWARD' AS t2s_table,
       COUNT(*) AS lignes, 44084 AS avant_le_run, 16164 AS attendu_apres
FROM   T_WC_T2S_AWARD WHERE DELETED IS NULL OR DELETED = 0
UNION ALL
SELECT 'T_WC_T2S_NOMINATION',
       COUNT(*), 30602, 2910
FROM   T_WC_T2S_NOMINATION WHERE DELETED IS NULL OR DELETED = 0;

SELECT '=== N1b . les six tables de jonction ===' AS section;
-- Reperes du 2026-08-19, total des liens portes par les lignes condamnees :
-- 9 934 films + 1 002 series + 46 142 personnes = 57 078 cote prix.

SELECT 'T_WC_T2S_MOVIE_AWARD'       AS jonction, COUNT(*) AS lignes, 9934  AS liens_condamnes_le_19 FROM T_WC_T2S_MOVIE_AWARD       WHERE DELETED IS NULL OR DELETED = 0
UNION ALL SELECT 'T_WC_T2S_SERIE_AWARD',        COUNT(*), 1002  FROM T_WC_T2S_SERIE_AWARD        WHERE DELETED IS NULL OR DELETED = 0
UNION ALL SELECT 'T_WC_T2S_PERSON_AWARD',       COUNT(*), 46142 FROM T_WC_T2S_PERSON_AWARD       WHERE DELETED IS NULL OR DELETED = 0
UNION ALL SELECT 'T_WC_T2S_MOVIE_NOMINATION',   COUNT(*), NULL  FROM T_WC_T2S_MOVIE_NOMINATION   WHERE DELETED IS NULL OR DELETED = 0
UNION ALL SELECT 'T_WC_T2S_SERIE_NOMINATION',   COUNT(*), NULL  FROM T_WC_T2S_SERIE_NOMINATION   WHERE DELETED IS NULL OR DELETED = 0
UNION ALL SELECT 'T_WC_T2S_PERSON_NOMINATION',  COUNT(*), NULL  FROM T_WC_T2S_PERSON_NOMINATION  WHERE DELETED IS NULL OR DELETED = 0;

SELECT '=== N1c . reste-t-il des jonctions orphelines ? ===' AS section;
-- Les lignes 3009 a 3019 du processus les nettoient. Attendu : 0 partout. Une
-- jonction orpheline pointerait un ID_AWARD supprime, et les pages afficheraient
-- une recompense sans nom.

SELECT (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_AWARD      x WHERE NOT EXISTS (SELECT 1 FROM T_WC_T2S_AWARD      a WHERE a.ID_AWARD      = x.ID_AWARD))      AS orphelines_film_prix,
       (SELECT COUNT(*) FROM T_WC_T2S_SERIE_AWARD      x WHERE NOT EXISTS (SELECT 1 FROM T_WC_T2S_AWARD      a WHERE a.ID_AWARD      = x.ID_AWARD))      AS orphelines_serie_prix,
       (SELECT COUNT(*) FROM T_WC_T2S_PERSON_AWARD     x WHERE NOT EXISTS (SELECT 1 FROM T_WC_T2S_AWARD      a WHERE a.ID_AWARD      = x.ID_AWARD))      AS orphelines_personne_prix,
       (SELECT COUNT(*) FROM T_WC_T2S_MOVIE_NOMINATION x WHERE NOT EXISTS (SELECT 1 FROM T_WC_T2S_NOMINATION n WHERE n.ID_NOMINATION = x.ID_NOMINATION)) AS orphelines_film_nom,
       (SELECT COUNT(*) FROM T_WC_T2S_PERSON_NOMINATION x WHERE NOT EXISTS (SELECT 1 FROM T_WC_T2S_NOMINATION n WHERE n.ID_NOMINATION = x.ID_NOMINATION)) AS orphelines_personne_nom;


-- ############################################################################
-- N2 . LA SUPPRESSION A-T-ELLE FRAPPE LA BONNE POPULATION, ET ELLE SEULE ?
-- ############################################################################
-- Deux erreurs opposees sont possibles et il faut les mesurer separement. Trop
-- supprime : les lignes protegees, celles sans P31, auraient disparu. Pas assez :
-- des lignes hors cone auraient survecu. Les deux verdicts doivent etre a zero.

SELECT '=== N2a . la regle est-elle respectee par ce qui reste ? ===' AS section;

SELECT COUNT(*) AS lignes_restantes,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                             JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                             JOIN T_WC_T2S_AWARD_CLASS ac ON ac.ID_CLASS = iv.ID_ITEM
                             WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
                 THEN 1 ELSE 0 END) AS classe_dans_le_cone,
       SUM(CASE WHEN NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                                 WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
                 THEN 1 ELSE 0 END) AS sans_classe_protegees,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                             WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
                 AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st
                                 JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
                                 JOIN T_WC_T2S_AWARD_CLASS ac ON ac.ID_CLASS = iv.ID_ITEM
                                 WHERE st.ID_WIKIDATA = a.ID_WIKIDATA AND st.ID_PROPERTY = 'P31')
                 THEN 1 ELSE 0 END) AS SURVIVANTES_HORS_CONE_A_ZERO
FROM   T_WC_T2S_AWARD a
WHERE  a.DELETED IS NULL OR a.DELETED = 0;

SELECT '=== N2b . les huit temoins d I3 sont-ils toujours la ? ===' AS section;
-- De vraies recompenses absentes de V2, que seule la clause de protection sauve.
-- Attendu : huit lignes. Une absence signifierait que le second terme de la regle
-- n'a pas ete applique, et que du bon a ete detruit avec le mauvais.

SELECT a.ID_WIKIDATA, COALESCE(NULLIF(a.AWARD_NAME,''), '(vide)') AS nom
FROM   T_WC_T2S_AWARD a
WHERE  a.ID_WIKIDATA IN ('Q124458350','Q117100866','Q61478580','Q3117505',
                         'Q383301','Q642539','Q74681122','Q134384292')
ORDER  BY a.ID_WIKIDATA;

SELECT '=== N2c . et les populations polluantes ont-elles disparu ? ===' AS section;
-- Reperes du 2026-08-19 : 9 855 humains, 7 028 films, 1 387 ceremonies, 930
-- series, 777 albums. Attendu : proche de zero partout. « Proche » et non « zero »
-- parce qu'une entite peut porter plusieurs P31, dont un dans le cone.

SELECT COUNT(*) AS lignes_restantes,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE  m WHERE m.ID_WIKIDATA = a.ID_WIKIDATA)) AS dont_films,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON p WHERE p.ID_WIKIDATA = a.ID_WIKIDATA)) AS dont_personnes,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE  s WHERE s.ID_WIKIDATA = a.ID_WIKIDATA)) AS dont_series,
       7028 AS films_le_19, 9855 AS personnes_le_19
FROM   T_WC_T2S_AWARD a
WHERE  a.DELETED IS NULL OR a.DELETED = 0;


-- ############################################################################
-- N3 . LA REGRESSION VISIBLE : COMBIEN DE FICHES SE SONT VIDEES ?
-- ############################################################################
-- Le seul chiffre que l'utilisateur peut voir. I5 et I6 l'avaient predit sur la
-- base d'avant : 693 personnes, 348 films, 52 series perdraient tous leurs prix ;
-- 102 personnes, 37 films, 15 series toutes leurs nominations. Ici on mesure ce
-- qui reste, donc le complement : le nombre d'entites encore pourvues.
--
-- Attendus, en partant des chiffres d'avant : personnes avec prix 51 203 - 693 =
-- 50 510 ; films 7 943 - 348 = 7 595 ; series 787 - 52 = 735. Cote nominations,
-- personnes 14 339 - 102 = 14 237 ; films 10 823 - 37 = 10 786 ; series 353 - 15 = 338.

SELECT '=== N3 . entites encore pourvues, prix et nominations ===' AS section;

SELECT 'personnes avec au moins un prix' AS mesure, COUNT(DISTINCT ID_PERSON) AS entites, 50510 AS attendu FROM T_WC_T2S_PERSON_AWARD      WHERE DELETED IS NULL OR DELETED = 0
UNION ALL SELECT 'films avec au moins un prix',      COUNT(DISTINCT ID_MOVIE),  7595  FROM T_WC_T2S_MOVIE_AWARD       WHERE DELETED IS NULL OR DELETED = 0
UNION ALL SELECT 'series avec au moins un prix',     COUNT(DISTINCT ID_SERIE),  735   FROM T_WC_T2S_SERIE_AWARD       WHERE DELETED IS NULL OR DELETED = 0
UNION ALL SELECT 'personnes avec une nomination',    COUNT(DISTINCT ID_PERSON), 14237 FROM T_WC_T2S_PERSON_NOMINATION WHERE DELETED IS NULL OR DELETED = 0
UNION ALL SELECT 'films avec une nomination',        COUNT(DISTINCT ID_MOVIE),  10786 FROM T_WC_T2S_MOVIE_NOMINATION  WHERE DELETED IS NULL OR DELETED = 0
UNION ALL SELECT 'series avec une nomination',       COUNT(DISTINCT ID_SERIE),  338   FROM T_WC_T2S_SERIE_NOMINATION  WHERE DELETED IS NULL OR DELETED = 0;


-- ############################################################################
-- N4 . LES COLONNES DE -017, RELUES EN TAUX
-- ############################################################################
-- Le denominateur a change sur deux des six colonnes, donc le compte brut ne dit
-- plus rien. Ce qui doit tenir, c'est le TAUX de remplissage : la bascule V1 vers
-- V2 n'a aucune raison de moins bien servir une table devenue plus petite. Si le
-- taux des prix chutait, ce serait que la suppression a emporte majoritairement
-- des lignes NOMMEES, ce qui contredirait tout ce qu'on a mesure.
--
-- Reperes du 2026-08-19, apres -017 et avant -036 :
--   ITEM_LABEL_FR      657 385 / 695 741  94,5 %   (non touche par -036)
--   AWARD_NAME_FR       26 507 /  44 084  60,1 %   -> denominateur ~16 164
--   NOMINATION_NAME_FR  10 852 /  30 602  35,5 %   -> denominateur ~2 910
--   GROUP_NAME_FR        8 147 /   8 184  99,5 %   (non touche)
--   DEATH_NAME_FR          451 /     451 100,0 %   (non touche)
--   MOVEMENT_NAME_FR         8 /      21  38,1 %   (non touche)

SELECT '=== N4 . taux de remplissage des six colonnes FR ===' AS section;

SELECT 'T2S_AWARD' AS t2s_table, 'AWARD_NAME_FR' AS colonne,
       SUM(AWARD_NAME_FR IS NOT NULL AND AWARD_NAME_FR <> '') AS rempli,
       COUNT(*) AS total,
       ROUND(100 * SUM(AWARD_NAME_FR IS NOT NULL AND AWARD_NAME_FR <> '') / COUNT(*), 1) AS pct,
       60.1 AS pct_le_19
FROM   T_WC_T2S_AWARD WHERE DELETED IS NULL OR DELETED = 0
UNION ALL
SELECT 'T2S_NOMINATION', 'NOMINATION_NAME_FR',
       SUM(NOMINATION_NAME_FR IS NOT NULL AND NOMINATION_NAME_FR <> ''), COUNT(*),
       ROUND(100 * SUM(NOMINATION_NAME_FR IS NOT NULL AND NOMINATION_NAME_FR <> '') / COUNT(*), 1), 35.5
FROM   T_WC_T2S_NOMINATION WHERE DELETED IS NULL OR DELETED = 0
UNION ALL
SELECT 'T2S_ITEM', 'ITEM_LABEL_FR',
       SUM(ITEM_LABEL_FR IS NOT NULL AND ITEM_LABEL_FR <> ''), COUNT(*),
       ROUND(100 * SUM(ITEM_LABEL_FR IS NOT NULL AND ITEM_LABEL_FR <> '') / COUNT(*), 1), 94.5
FROM   T_WC_T2S_ITEM WHERE DELETED IS NULL OR DELETED = 0
UNION ALL
SELECT 'T2S_GROUP', 'GROUP_NAME_FR',
       SUM(GROUP_NAME_FR IS NOT NULL AND GROUP_NAME_FR <> ''), COUNT(*),
       ROUND(100 * SUM(GROUP_NAME_FR IS NOT NULL AND GROUP_NAME_FR <> '') / COUNT(*), 1), 99.5
FROM   T_WC_T2S_GROUP WHERE DELETED IS NULL OR DELETED = 0
UNION ALL
SELECT 'T2S_DEATH', 'DEATH_NAME_FR',
       SUM(DEATH_NAME_FR IS NOT NULL AND DEATH_NAME_FR <> ''), COUNT(*),
       ROUND(100 * SUM(DEATH_NAME_FR IS NOT NULL AND DEATH_NAME_FR <> '') / COUNT(*), 1), 100.0
FROM   T_WC_T2S_DEATH WHERE DELETED IS NULL OR DELETED = 0
UNION ALL
SELECT 'T2S_MOVEMENT', 'MOVEMENT_NAME_FR',
       SUM(MOVEMENT_NAME_FR IS NOT NULL AND MOVEMENT_NAME_FR <> ''), COUNT(*),
       ROUND(100 * SUM(MOVEMENT_NAME_FR IS NOT NULL AND MOVEMENT_NAME_FR <> '') / COUNT(*), 1), 38.1
FROM   T_WC_T2S_MOVEMENT WHERE DELETED IS NULL OR DELETED = 0;


-- ############################################################################
-- N5 . LA CONTAMINATION DE -017 A-T-ELLE DISPARU PAR RICOCHET ?
-- ############################################################################
-- Les 155 lignes affichant un titre de film comme nom de prix, et les 315 affichant
-- un titre de serie, appartenaient toutes a la population condamnee. -036 devrait
-- donc les avoir emportees sans qu'on ecrive une ligne pour elles.
--
-- C'est la verification qui ferme la boucle de la journee : un defaut constate en
-- lisant les libelles se resout a la derivation, parce que la faute n'etait pas
-- dans le libellé mais dans la ligne. Attendu : zero des deux cotes.

SELECT '=== N5 . titres d oeuvres encore affiches comme noms de prix ===' AS section;

SELECT 'film'  AS type_entite,
       COUNT(*) AS lignes_avec_nom_fr,
       SUM(a.AWARD_NAME_FR = m.LABEL_EN) AS nom_fr_egale_le_titre,
       155 AS le_20_avant_036
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_MOVIE m ON m.ID_WIKIDATA = a.ID_WIKIDATA
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> ''
UNION ALL
SELECT 'serie',
       COUNT(*),
       SUM(a.AWARD_NAME_FR = s.LABEL_EN),
       315
FROM   T_WC_T2S_AWARD a
JOIN   T_WC_WIKIDATA_SERIE s ON s.ID_WIKIDATA = a.ID_WIKIDATA
WHERE  (a.DELETED IS NULL OR a.DELETED = 0)
  AND  a.AWARD_NAME_FR IS NOT NULL AND a.AWARD_NAME_FR <> '';
