-- ============================================================================
-- WIKIDATA-CRAWLER-017 : ce que la nuit prochaine va changer, mesure avant/apres
-- ============================================================================
--
-- LECTURE SEULE. A lancer AVANT que tmdb-movie-preprocess ne tourne.
--
-- POURQUOI CE TEST N'EST PAS CELUI QU'ON CROIT. La bascule ne peut pas PERDRE de
-- libelle : chaque lecture migree retombe sur V1 quand V2 n'a rien
-- (COALESCE(JSON_EXTRACT(v2.LABELS_JSON,'$.xx'), ..., v1.LABEL)). Zero perte par
-- construction, il est donc inutile de la mesurer.
--
-- Le vrai risque est ailleurs, et il est silencieux : LA DIVERGENCE DE VALEURS. Quand
-- V2 ET V1 portent tous deux un libelle mais DIFFERENT, la nuit remplacera l'ancien
-- par le nouveau, sur des ecrans en production, sans qu'aucun compteur ne bouge. V2
-- est plus frais (re-run hebdomadaire) la ou V1 est backfill-only, un libelle deja
-- present n'y etant jamais rafraichi : les divergences sont donc attendues, et le plus
-- souvent en faveur de V2. Il faut quand meme les compter et en regarder quelques-unes
-- avant, pas apres.
--
-- Executer avec --force -t.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;


SELECT '=== T1 . libelle FR : ce que la bascule change ===' AS section;
-- gagnes_par_v2          : V2 sait, V1 ne savait pas -> nouveau libelle a l ecran
-- servis_par_le_repli_v1 : V2 ne sait pas -> le repli travaille, rien ne bouge
-- divergents             : les deux savent, mais differemment -> LA valeur change
-- identiques             : les deux savent la meme chose -> rien ne bouge

SELECT SUM(v2fr IS NOT NULL AND v1fr IS NULL)                  AS gagnes_par_v2,
       SUM(v2fr IS NULL     AND v1fr IS NOT NULL)              AS servis_par_le_repli_v1,
       SUM(v2fr IS NOT NULL AND v1fr IS NOT NULL AND v2fr <> v1fr) AS divergents,
       SUM(v2fr IS NOT NULL AND v1fr IS NOT NULL AND v2fr =  v1fr) AS identiques,
       COUNT(*)                                                AS lignes_v1_fr
FROM (
    SELECT NULLIF(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON, '$.fr')), '') AS v2fr,
           NULLIF(v1.LABEL, '')                                           AS v1fr
    FROM   T_WC_WIKIDATA_ITEM_V1 v1
    LEFT JOIN T_WC_WIKIDATA_ITEM v2 ON v2.ID_WIKIDATA = v1.ID_WIKIDATA
    WHERE  v1.LANG = 'fr'
) t;


SELECT '=== T2 . libelle EN : idem ===' AS section;

SELECT SUM(v2en IS NOT NULL AND v1en IS NULL)                  AS gagnes_par_v2,
       SUM(v2en IS NULL     AND v1en IS NOT NULL)              AS servis_par_le_repli_v1,
       SUM(v2en IS NOT NULL AND v1en IS NOT NULL AND v2en <> v1en) AS divergents,
       SUM(v2en IS NOT NULL AND v1en IS NOT NULL AND v2en =  v1en) AS identiques,
       COUNT(*)                                                AS lignes_v1_en
FROM (
    SELECT COALESCE(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON, '$.en')), ''),
                    NULLIF(v2.LABEL_EN, ''))                            AS v2en,
           NULLIF(v1.LABEL, '')                                         AS v1en
    FROM   T_WC_WIKIDATA_ITEM_V1 v1
    LEFT JOIN T_WC_WIKIDATA_ITEM v2 ON v2.ID_WIKIDATA = v1.ID_WIKIDATA
    WHERE  v1.LANG = 'en'
) t;


SELECT '=== T3 . vingt divergences FR, a regarder a l oeil ===' AS section;
-- Le seul controle qui ne se delegue pas. Si la colonne V2 est manifestement meilleure
-- (accents, casse, desambiguisation), la bascule est un gain. Si elle est plus pauvre
-- ou dans une autre langue, il faut s arreter avant la nuit.

SELECT v1.ID_WIKIDATA,
       v1.LABEL                                                  AS avant_v1,
       JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON, '$.fr'))        AS apres_v2
FROM   T_WC_WIKIDATA_ITEM_V1 v1
INNER JOIN T_WC_WIKIDATA_ITEM v2 ON v2.ID_WIKIDATA = v1.ID_WIKIDATA
WHERE  v1.LANG = 'fr'
  AND  NULLIF(v1.LABEL,'') IS NOT NULL
  AND  NULLIF(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.fr')),'') IS NOT NULL
  AND  JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.fr')) <> v1.LABEL
LIMIT  20;


SELECT '=== T4 . la requete de detail, telle que le code la lance ===' AS section;
-- Meme forme exacte que les quatre blocs migres : depart d une table derivee d une
-- ligne, puis trois jointures gauches. Remplacer le Q-id par un cas connu. Doit rendre
-- UNE ligne, jamais zero, et trois valeurs non NULL (chaine vide au pire).

SELECT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.en')),
                NULLIF(v2.LABEL_EN,''), v1.LABEL, '')            AS LABEL,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.DESCRIPTIONS_JSON,'$.en')),
                NULLIF(v2.DESCRIPTION_EN,''), v1.DESCRIPTION, '') AS DESCRIPTION,
       COALESCE(pl.MAIN_IMAGE_URL, v1.WIKIPEDIA_IMAGE_PATH, '')  AS WIKIPEDIA_IMAGE_PATH
FROM      (SELECT 'Q103618' AS ID_WIKIDATA) k
LEFT JOIN T_WC_WIKIDATA_ITEM      v2 ON v2.ID_WIKIDATA = k.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_ITEM_V1   v1 ON v1.ID_WIKIDATA = k.ID_WIKIDATA AND v1.LANG = 'en'
LEFT JOIN T_WC_WIKIPEDIA_PAGE_LANG pl ON pl.ID_WIKIDATA = k.ID_WIKIDATA AND pl.LANG = 'en'
                                     AND COALESCE(pl.MAIN_IMAGE_URL,'') <> '';

SELECT '========== FIN ==========' AS section;
