-- ============================================================================
-- WIKIDATA-CRAWLER-017 : controle avant / apres le passage de nuit
-- ============================================================================
--
-- LECTURE SEULE. A lancer DEUX FOIS : une fois avant que tmdb-movie-preprocess ne
-- tourne, une fois apres, puis comparer les deux sorties ligne a ligne.
--
-- CE QU ON CHERCHE, et ce n est pas une erreur SQL. Une requete cassee se verrait
-- dans le journal du process. Ce qui ne se verrait nulle part, c est un libelle
-- francais devenu vide : la colonne resterait, la page s afficherait, et seul le
-- texte manquerait. Le bloc A compte donc les valeurs non vides, table par table.
-- Elles ne doivent pas baisser. Une hausse est possible et bienvenue, V2 apportant
-- des libelles que V1 n avait pas (1 239 mesures le 2026-08-17).
--
-- Le bloc B teste une hypothese qui, si elle se verifie, retire un ticket du
-- backlog plutot qu elle n en ajoute un. Voir son en-tete.
--
-- Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;

SELECT NOW() AS horodatage_de_cette_mesure,
       'noter AVANT ou APRES le passage de nuit' AS a_reporter;


-- ############################################################################
-- ### A . LES SIX COLONNES *_FR ALIMENTEES PAR WIKIDATA                    ###
-- ############################################################################

SELECT '=== A . libelles FR non vides, par table ===' AS section;
-- Les six colonnes que LOCALIZATION-004 identifie comme venant de ITEM_V1. Le
-- rapport rempli/total importe autant que le compte brut : une table dont le
-- total bouge (nouvelles entites) sans que le rempli suive signale un probleme
-- que le compte seul masquerait.

SELECT 'T2S_AWARD' AS t2s_table, 'AWARD_NAME_FR' AS colonne,
       SUM(COALESCE(AWARD_NAME_FR,'') <> '') AS rempli, COUNT(*) AS total,
       ROUND(100 * SUM(COALESCE(AWARD_NAME_FR,'') <> '') / NULLIF(COUNT(*),0), 1) AS pct
FROM   T_WC_T2S_AWARD
UNION ALL
SELECT 'T2S_GROUP', 'GROUP_NAME_FR',
       SUM(COALESCE(GROUP_NAME_FR,'') <> ''), COUNT(*),
       ROUND(100 * SUM(COALESCE(GROUP_NAME_FR,'') <> '') / NULLIF(COUNT(*),0), 1)
FROM   T_WC_T2S_GROUP
UNION ALL
SELECT 'T2S_MOVEMENT', 'MOVEMENT_NAME_FR',
       SUM(COALESCE(MOVEMENT_NAME_FR,'') <> ''), COUNT(*),
       ROUND(100 * SUM(COALESCE(MOVEMENT_NAME_FR,'') <> '') / NULLIF(COUNT(*),0), 1)
FROM   T_WC_T2S_MOVEMENT
UNION ALL
SELECT 'T2S_DEATH', 'DEATH_NAME_FR',
       SUM(COALESCE(DEATH_NAME_FR,'') <> ''), COUNT(*),
       ROUND(100 * SUM(COALESCE(DEATH_NAME_FR,'') <> '') / NULLIF(COUNT(*),0), 1)
FROM   T_WC_T2S_DEATH
UNION ALL
SELECT 'T2S_NOMINATION', 'NOMINATION_NAME_FR',
       SUM(COALESCE(NOMINATION_NAME_FR,'') <> ''), COUNT(*),
       ROUND(100 * SUM(COALESCE(NOMINATION_NAME_FR,'') <> '') / NULLIF(COUNT(*),0), 1)
FROM   T_WC_T2S_NOMINATION
UNION ALL
SELECT 'T2S_ITEM', 'ITEM_LABEL_FR',
       SUM(COALESCE(ITEM_LABEL_FR,'') <> ''), COUNT(*),
       ROUND(100 * SUM(COALESCE(ITEM_LABEL_FR,'') <> '') / NULLIF(COUNT(*),0), 1)
FROM   T_WC_T2S_ITEM;


SELECT '=== A-bis . les images, meme controle ===' AS section;
-- Le process 71 ecrit ces colonnes, la nuit les reecrira. Repere du 2026-08-17 :
-- 933 606 lignes servies au total, dont MOVIE 141 424 en et 42 821 fr.

SELECT 'T2S_MOVIE' AS t2s_table,
       SUM(COALESCE(WIKIPEDIA_MAIN_IMAGE_URL,'')    <> '') AS image_en,
       SUM(COALESCE(WIKIPEDIA_MAIN_IMAGE_URL_FR,'') <> '') AS image_fr
FROM   T_WC_T2S_MOVIE
UNION ALL SELECT 'T2S_PERSON', SUM(COALESCE(WIKIPEDIA_MAIN_IMAGE_URL,'')<>''), SUM(COALESCE(WIKIPEDIA_MAIN_IMAGE_URL_FR,'')<>'') FROM T_WC_T2S_PERSON
UNION ALL SELECT 'T2S_ITEM',   SUM(COALESCE(WIKIPEDIA_MAIN_IMAGE_URL,'')<>''), SUM(COALESCE(WIKIPEDIA_MAIN_IMAGE_URL_FR,'')<>'') FROM T_WC_T2S_ITEM
UNION ALL SELECT 'T2S_AWARD',  SUM(COALESCE(WIKIPEDIA_MAIN_IMAGE_URL,'')<>''), SUM(COALESCE(WIKIPEDIA_MAIN_IMAGE_URL_FR,'')<>'') FROM T_WC_T2S_AWARD;


-- ############################################################################
-- ### B . V1 INVENTAIT-IL DES LIBELLES FRANCAIS ?                          ###
-- ############################################################################

SELECT '=== B . les 100 271 sans cle fr en V2 : vrai francais ou anglais deguise ? ===' AS section;
-- L HYPOTHESE. La mesure du 2026-08-17 a trouve 351 771 lignes servies par le repli
-- V1, dont 100 271 pour des entites que V2 CONNAIT mais dont le LABELS_JSON n a pas
-- de cle 'fr'. Lu naivement, c est un defaut de collecte de V2.
--
-- Mais V1 interrogeait Wikidata en SPARQL, et le service de libelles de Wikidata
-- applique un REPLI DE LANGUE AUTOMATIQUE : quand le francais manque, il rend
-- l anglais, sans le signaler. Une part de ces 100 271 serait donc de l anglais
-- range dans une ligne LANG='fr'.
--
-- Si la premiere colonne domine, il n y a pas de libelle francais perdu : c est V1
-- qui en inventait, et V2 a raison de ne rien avoir. Le sujet sort du backlog.
-- Si la seconde domine, V2 perd de vrais libelles francais et cela merite un ticket.

SELECT SUM(fr.LABEL =  en.LABEL) AS fr_egal_en_donc_repli_sparql,
       SUM(fr.LABEL <> en.LABEL) AS vrai_libelle_francais,
       COUNT(*)                  AS total_compare
FROM       T_WC_WIKIDATA_ITEM_V1 fr
INNER JOIN T_WC_WIKIDATA_ITEM_V1 en ON en.ID_WIKIDATA = fr.ID_WIKIDATA AND en.LANG = 'en'
INNER JOIN T_WC_WIKIDATA_ITEM    v2 ON v2.ID_WIKIDATA = fr.ID_WIKIDATA
WHERE  fr.LANG = 'fr'
  AND  NULLIF(fr.LABEL,'') IS NOT NULL
  AND  NULLIF(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.fr')),'') IS NULL;


SELECT '=== B-bis . vingt cas ou V1 dit la meme chose en fr et en en ===' AS section;
-- A relire a l oeil. Si ce sont des noms propres (un titre de film anglais reste
-- anglais en francais), l egalite est legitime et ne prouve rien. Si ce sont des
-- noms communs anglais la ou le francais existe, l hypothese du repli est confirmee.

SELECT fr.ID_WIKIDATA, fr.LABEL AS libelle_dit_francais, en.LABEL AS libelle_anglais
FROM       T_WC_WIKIDATA_ITEM_V1 fr
INNER JOIN T_WC_WIKIDATA_ITEM_V1 en ON en.ID_WIKIDATA = fr.ID_WIKIDATA AND en.LANG = 'en'
INNER JOIN T_WC_WIKIDATA_ITEM    v2 ON v2.ID_WIKIDATA = fr.ID_WIKIDATA
WHERE  fr.LANG = 'fr'
  AND  fr.LABEL = en.LABEL
  AND  NULLIF(fr.LABEL,'') IS NOT NULL
  AND  NULLIF(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.fr')),'') IS NULL
LIMIT  20;

SELECT '========== FIN ==========' AS section;
