-- Sonde de cadrage du vocabulaire controle (RECONCILIATION-001, dimension country).
-- Lecture seule, voie 1. Mesure l'ecart entre la DEMANDE (pays cites en P495 par le
-- corpus) et l'OFFRE locale (pays ayant deja un code ISO2 P297 en base), pour chiffrer
-- l'etage override (entites sous-nationales / historiques sans ISO propre) avant de
-- recolter sur Wikidata.
--
-- P495 (pays d'origine)  -> valeur item  -> T_WC_WIKIDATA_ITEM_VALUE.ID_ITEM (le Q pays)
-- P297 (code ISO 3166-1 alpha-2) -> valeur external_id -> T_WC_WIKIDATA_EXTERNAL_ID_VALUE
-- P498 (code ISO 4217 devise)    -> idem (mesure annexe)

-- Demande : les pays distincts cites en P495, avec leur frequence d'usage.
CREATE TEMPORARY TABLE tmp_demand AS
SELECT iv.ID_ITEM AS COUNTRY_Q, COUNT(*) AS N_USES
  FROM T_WC_WIKIDATA_STATEMENT s
  JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = s.ID_STATEMENT
 WHERE s.ID_PROPERTY = 'P495'
   AND s.DELETED = 0
 GROUP BY iv.ID_ITEM;

-- Offre : les pays ayant un code ISO2 (P297) present en base.
CREATE TEMPORARY TABLE tmp_iso AS
SELECT s.ID_WIKIDATA AS COUNTRY_Q, MIN(e.VALUE_EXTERNAL_ID) AS ISO2
  FROM T_WC_WIKIDATA_STATEMENT s
  JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE e ON e.ID_STATEMENT = s.ID_STATEMENT
 WHERE s.ID_PROPERTY = 'P297'
   AND s.DELETED = 0
 GROUP BY s.ID_WIKIDATA;

-- VUE 1 : couverture globale. Combien de pays distincts, combien couverts par un ISO2
-- local, combien d'usages (pondere par frequence) tombent dans le trou.
SELECT
    COUNT(*)                                             AS N_PAYS_DISTINCTS,
    SUM(iso.ISO2 IS NOT NULL)                            AS N_AVEC_ISO2_LOCAL,
    SUM(iso.ISO2 IS NULL)                                AS N_SANS_ISO2_LOCAL,
    SUM(d.N_USES)                                        AS N_USAGES_TOTAL,
    SUM(CASE WHEN iso.ISO2 IS NULL THEN d.N_USES ELSE 0 END) AS N_USAGES_NON_COUVERTS
  FROM tmp_demand d
  LEFT JOIN tmp_iso iso ON iso.COUNTRY_Q = d.COUNTRY_Q;

-- VUE 2 : l'etage OVERRIDE. Les pays cites par le corpus SANS ISO2 local, tries par
-- frequence. C'est la liste finie a mapper a la main (ou via P17 pays-parent).
SELECT d.COUNTRY_Q, i.LABEL_EN, d.N_USES
  FROM tmp_demand d
  LEFT JOIN tmp_iso iso ON iso.COUNTRY_Q = d.COUNTRY_Q
  LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = d.COUNTRY_Q
 WHERE iso.ISO2 IS NULL
 ORDER BY d.N_USES DESC
 LIMIT 100;

-- VUE 3 : controle de l'offre locale. Les pays les plus cites QUI ONT un ISO2 local,
-- pour verifier que le code local est exploitable (Q142 -> FR, Q30 -> US ...).
SELECT d.COUNTRY_Q, i.LABEL_EN, iso.ISO2, d.N_USES
  FROM tmp_demand d
  JOIN tmp_iso iso ON iso.COUNTRY_Q = d.COUNTRY_Q
  LEFT JOIN T_WC_WIKIDATA_ITEM i ON i.ID_WIKIDATA = d.COUNTRY_Q
 ORDER BY d.N_USES DESC
 LIMIT 20;

-- VUE 4 : mesure annexe de l'offre devise (P498 -> ISO 4217), pour savoir si la dimension
-- currency peut se sourcer en local ou devra passer par SPARQL.
SELECT COUNT(*) AS N_DEVISES_AVEC_ISO4217_LOCAL
  FROM T_WC_WIKIDATA_STATEMENT s
  JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE e ON e.ID_STATEMENT = s.ID_STATEMENT
 WHERE s.ID_PROPERTY = 'P498'
   AND s.DELETED = 0;
