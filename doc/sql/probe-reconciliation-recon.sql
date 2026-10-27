-- =====================================================================
-- Reconnaissance avant l'extension de la sonde de reconciliation. LECTURE SEULE.
-- But : lever les incertitudes du rapport d'exploration avant d'ecrire la
-- sonde etendue (duree, realisateur par Q, pays). Rien n'est modifie.
--   A. colonnes reelles des tables incertaines (information_schema)
--   B. presence des proprietes Wikidata P2047/P57/P495 et unite de duree
--   C. cohortes candidates (films muets, multidevises, 3 sources, series)
-- =====================================================================

-- --- A. Colonnes reelles (confirme mapping pays, IS_SILENT, statements) ---
SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE
FROM   information_schema.COLUMNS
WHERE  TABLE_SCHEMA = DATABASE()
  AND (
        TABLE_NAME = 'T_WC_T2S_LOCATION'
     OR TABLE_NAME = 'T_WC_WIKIDATA_STATEMENT'
     OR TABLE_NAME = 'T_WC_WIKIDATA_QUANTITY_VALUE'
     OR TABLE_NAME = 'T_WC_WIKIDATA_ITEM_VALUE'
     OR TABLE_NAME = 'T_WC_TMDB_PERSON_MOVIE'
     OR (TABLE_NAME = 'T_WC_TMDB_PERSON' AND COLUMN_NAME LIKE '%WIKIDATA%')
     OR (TABLE_NAME = 'T_WC_TMDB_MOVIE' AND
          (COLUMN_NAME LIKE '%SILENT%' OR COLUMN_NAME LIKE '%COUNTR%'
           OR COLUMN_NAME IN ('RUNTIME','RELEASE_YEAR','ID_MOVIE','ID_IMDB','ID_WIKIDATA','BUDGET','REVENUE')))
      )
ORDER  BY TABLE_NAME, ORDINAL_POSITION;

-- --- B1. Les trois proprietes existent-elles dans les statements ? ---
SELECT s.ID_PROPERTY, COUNT(*) AS N_STATEMENTS, COUNT(DISTINCT s.ID_WIKIDATA) AS N_SUBJECTS
FROM   T_WC_WIKIDATA_STATEMENT s
WHERE  s.ID_PROPERTY IN ('P2047','P57','P495')
GROUP  BY s.ID_PROPERTY;

-- --- B2. Unite de la duree P2047 (minute attendue = Q7727) ---
SELECT q.UNIT_ID_WIKIDATA, COUNT(*) AS N
FROM   T_WC_WIKIDATA_STATEMENT s
JOIN   T_WC_WIKIDATA_QUANTITY_VALUE q ON q.ID_STATEMENT = s.ID_STATEMENT
WHERE  s.ID_PROPERTY = 'P2047'
GROUP  BY q.UNIT_ID_WIKIDATA
ORDER  BY N DESC
LIMIT  10;

-- --- C-a. Films anciens (< 1930), candidats muets. ---
SELECT m.ID_MOVIE, m.ID_WIKIDATA, m.ID_IMDB, m.TITLE, m.RELEASE_YEAR, m.RUNTIME
FROM   T_WC_TMDB_MOVIE m
WHERE  m.ID_WIKIDATA IS NOT NULL AND m.RELEASE_YEAR < 1930 AND m.RELEASE_YEAR > 0
ORDER  BY m.RELEASE_YEAR
LIMIT  20;

-- --- C-b. Cas multidevises : au moins une ligne finance en devise != USD. ---
SELECT m.ID_MOVIE, m.ID_WIKIDATA, m.TITLE, m.RELEASE_YEAR,
       GROUP_CONCAT(DISTINCT COALESCE(f.CURRENCY_CODE, f.ID_CURRENCY)) AS CURRENCIES,
       COUNT(*) AS N_FINANCE
FROM   T_WC_TMDB_MOVIE m
JOIN   T_WC_T2S_WORK_FINANCE f ON f.ITEM_TYPE='movie' AND f.ID_ITEM = m.ID_MOVIE
WHERE  (f.CURRENCY_CODE IS NULL OR f.CURRENCY_CODE <> 'USD')
GROUP  BY m.ID_MOVIE, m.ID_WIKIDATA, m.TITLE, m.RELEASE_YEAR
ORDER  BY N_FINANCE DESC
LIMIT  15;

-- --- C-c. Films bien remplis sur les trois sources argent. ---
SELECT m.ID_MOVIE, m.ID_WIKIDATA, m.TITLE, m.RELEASE_YEAR,
       m.BUDGET, m.REVENUE, o.BOX_OFFICE_USD
FROM   T_WC_TMDB_MOVIE m
JOIN   T_WC_OMDB_TITLE o ON o.ID_IMDB = m.ID_IMDB
WHERE  m.BUDGET > 0 AND m.REVENUE > 0 AND o.BOX_OFFICE_USD > 0 AND m.ID_WIKIDATA IS NOT NULL
ORDER  BY m.REVENUE DESC
LIMIT  20;

-- --- C-d. Films ou Wikidata porte duree + realisateur + pays (exerce les 3 nouveaux attributs). ---
SELECT m.ID_MOVIE, m.ID_WIKIDATA, m.TITLE, m.RELEASE_YEAR,
       SUM(s.ID_PROPERTY='P2047') AS N_DUR,
       SUM(s.ID_PROPERTY='P57')   AS N_DIR,
       SUM(s.ID_PROPERTY='P495')  AS N_CTY
FROM   T_WC_TMDB_MOVIE m
JOIN   T_WC_WIKIDATA_STATEMENT s
       ON s.ID_WIKIDATA = m.ID_WIKIDATA AND s.ID_PROPERTY IN ('P2047','P57','P495')
      AND (s.RANK IS NULL OR s.RANK <> 'deprecated')
WHERE  m.ID_WIKIDATA IS NOT NULL
GROUP  BY m.ID_MOVIE, m.ID_WIKIDATA, m.TITLE, m.RELEASE_YEAR
HAVING N_DUR>0 AND N_DIR>0 AND N_CTY>0
ORDER  BY m.REVENUE DESC
LIMIT  20;

-- --- C-e. Series dans le pivot, avec/sans finance (ITEM_TYPE='serie'). ---
SELECT se.ID_SERIE, se.ID_WIKIDATA, se.ID_IMDB, se.TITLE, se.FIRST_AIR_YEAR,
       COUNT(f.ID_STATEMENT) AS N_FINANCE, GROUP_CONCAT(DISTINCT f.FINANCE_KIND) AS KINDS
FROM   T_WC_TMDB_SERIE se
LEFT   JOIN T_WC_T2S_WORK_FINANCE f ON f.ITEM_TYPE='serie' AND f.ID_ITEM = se.ID_SERIE
WHERE  se.ID_WIKIDATA IS NOT NULL
GROUP  BY se.ID_SERIE, se.ID_WIKIDATA, se.ID_IMDB, se.TITLE, se.FIRST_AIR_YEAR
ORDER  BY N_FINANCE DESC, se.FIRST_AIR_YEAR
LIMIT  15;
