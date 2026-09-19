-- ============================================================================
-- WIKIDATA-CRAWLER-017 : taux de repli V1 des libelles francais, rejouable
-- ============================================================================
--
-- LECTURE SEULE. A rejouer apres chaque run wikidata-crawler / tmdb-movie-preprocess
-- pour actualiser le seul chiffre qui bloque encore la decommission de V1.
--
-- CE QUE MESURE CE SCRIPT. Parmi les libelles francais que V1 detient
-- (T_WC_WIKIDATA_ITEM_V1, LANG='fr'), quelle part est REELLEMENT servie par le repli
-- V1 une fois pris en compte le repli de langue de f_getwikidatalabel, qui lit d'abord
-- $.fr de V2, puis $.en de V2, et ne descend sur V1 qu'en dernier recours.
--
-- POURQUOI DEUX CHIFFRES, ET LEQUEL COMPTE.
--   * servis_par_repli_v1 seul = les entites ou V2 n'a NI $.fr NI $.en : c'est le
--     ~36 % mesure le 2026-08-18, le vrai reliquat de dependance a V1 apres la mise
--     en place du repli de langue. C'EST LE CHIFFRE QUI BLOQUE LA COUPURE (etape 3
--     du sequencement, voir WIKIDATA-CRAWLER-022).
--   * servis_par_repli_langue_v2_en + servis_par_repli_v1 = le ~51 % brut mesure la
--     veille, avant que f_getwikidatalabel ne serve l'anglais de V2 : conserve ici
--     pour lire l'ecart et verifier que le repli de langue tient toujours son gain.
--
-- Reference : ../../../Nestor/projets/t2s-backlog/topics/wikidata-v1-v2-migration.md
-- section 3 (la ventilation datee) et section 8 (le point de non-retour).
-- Requete soeur, plus complete (divergences a l'oeil) : test-017-labels-avant-apres.sql.
--
-- Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;


SELECT '=== taux de repli V1 des libelles FR, apres repli de langue ===' AS section;
-- pct_repli_v1 est le chiffre a reporter dans le document de migration.

SELECT
  SUM(v2fr IS NOT NULL)                                    AS servis_par_v2_fr,
  SUM(v2fr IS NULL AND v2en IS NOT NULL)                   AS servis_par_repli_langue_v2_en,
  SUM(v2fr IS NULL AND v2en IS NULL AND v1fr IS NOT NULL)  AS servis_par_repli_v1,
  COUNT(*)                                                 AS lignes_v1_fr,
  ROUND(100 * SUM(v2fr IS NULL AND v2en IS NULL AND v1fr IS NOT NULL)
            / COUNT(*), 1)                                 AS pct_repli_v1,
  ROUND(100 * SUM(v2fr IS NULL AND v1fr IS NOT NULL)
            / COUNT(*), 1)                                 AS pct_repli_brut_avant_repli_langue
FROM (
  SELECT NULLIF(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.fr')),'') AS v2fr,
         COALESCE(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.en')),''),
                  NULLIF(v2.LABEL_EN,''))                             AS v2en,
         NULLIF(v1.LABEL,'')                                          AS v1fr
  FROM   T_WC_WIKIDATA_ITEM_V1 v1
  LEFT JOIN T_WC_WIKIDATA_ITEM v2 ON v2.ID_WIKIDATA = v1.ID_WIKIDATA
  WHERE  v1.LANG = 'fr'
) t;

SELECT '========== FIN ==========' AS section;
