-- =====================================================================
-- Sonde de reconciliation ETENDUE. LECTURE SEULE.
-- Quatre vues, cohorte figee (IDs reels tires de la reconnaissance) :
--   1. Numerique  : budget, box office (worldwide/US), DUREE. TMDb vs Wikidata vs OMDb.
--   2. Realisateur: comparaison d'ENSEMBLES de Q (P57) TMDb vs Wikidata.
--   3. Pays       : code ISO TMDb vs Q Wikidata (P495) mappe en ISO (vocabulaire en dur).
--   4. Series     : finance seule (ITEM_TYPE='serie'), TMDb muet par construction.
--
-- Sources reelles (rappel) : seule la finance est sourcee Wikidata ; duree/
-- realisateur/pays Wikidata = statements bruts T_WC_WIKIDATA_STATEMENT + valeurs.
-- Unite duree normalisee en minutes : Q7727=min, Q11574=seconde, Q25235=heure.
-- =====================================================================

-- ---------- Cohortes figees ----------
CREATE TEMPORARY TABLE tmp_movies AS
SELECT m.ID_MOVIE, m.ID_WIKIDATA, m.ID_IMDB, m.TITLE, m.RELEASE_YEAR, m.IS_SILENT,
       m.BUDGET, m.REVENUE, m.RUNTIME
FROM   T_WC_TMDB_MOVIE m
WHERE  m.ID_WIKIDATA IN (
  -- blockbusters bien remplis (accord attendu, multi-realisateurs, cuts multiples)
  'Q23781155','Q24871','Q44578','Q6074','Q23780914','Q68934496','Q3512046',
  'Q27044293','Q182218','Q31202708','Q55436290','Q3604746','Q24733929',
  'Q116764712','Q113877606','Q29580929','Q113244935','Q131547207',
  -- multidevises
  'Q3414948','Q23758755','Q160071','Q300356','Q1077160','Q18517638',
  'Q48758615','Q19544495','Q1757747',
  -- films muets / tres anciens (box office muet, runtime minuscule)
  'Q267176','Q913078','Q1854297','Q1210171','Q1920204','Q20992221',
  'Q1989298','Q2913387','Q1983441'
);

CREATE TEMPORARY TABLE tmp_series AS
SELECT se.ID_SERIE, se.ID_WIKIDATA, se.ID_IMDB, se.TITLE, se.FIRST_AIR_YEAR
FROM   T_WC_TMDB_SERIE se
WHERE  se.ID_WIKIDATA IN (
  'Q19329909','Q454398','Q504716','Q2667119','Q7903650','Q22905755'
);

-- Petit vocabulaire controle Q -> ISO2 pour les pays du panel (le reste reste en Q).
CREATE TEMPORARY TABLE tmp_country_iso (
  ID_WIKIDATA VARCHAR(20) COLLATE utf8mb4_unicode_ci PRIMARY KEY,
  ISO2 VARCHAR(2) COLLATE utf8mb4_unicode_ci);
INSERT INTO tmp_country_iso VALUES
 ('Q30','US'),('Q145','GB'),('Q142','FR'),('Q183','DE'),('Q38','IT'),
 ('Q29','ES'),('Q408','AU'),('Q159','RU'),('Q148','CN'),('Q8646','HK'),
 ('Q17','JP'),('Q16','CA'),('Q884','KR'),('Q668','IN'),('Q55','NL'),
 ('Q31','BE'),('Q34','SE'),('Q35','DK'),('Q20','NO'),('Q33','FI');

-- =====================================================================
-- VUE 1 : attributs numeriques (budget, box office worldwide/US, duree)
-- =====================================================================
CREATE TEMPORARY TABLE tmp_num AS
SELECT 'budget' AS ATTRIBUTE,'tmdb' AS SOURCE, ID_WIKIDATA, TITLE, 1 AS N_ROWS,
       NULLIF(BUDGET,0) AS VMIN, NULLIF(BUDGET,0) AS VMAX FROM tmp_movies
UNION ALL
SELECT 'box_office_worldwide','tmdb',ID_WIKIDATA,TITLE,1,NULLIF(REVENUE,0),NULLIF(REVENUE,0) FROM tmp_movies
UNION ALL
SELECT 'runtime','tmdb',ID_WIKIDATA,TITLE,1,NULLIF(RUNTIME,0),NULLIF(RUNTIME,0) FROM tmp_movies
UNION ALL
SELECT 'box_office_us','omdb',t.ID_WIKIDATA,t.TITLE,1,
       NULLIF(o.BOX_OFFICE_USD,0),NULLIF(o.BOX_OFFICE_USD,0)
FROM tmp_movies t JOIN T_WC_OMDB_TITLE o ON o.ID_IMDB=t.ID_IMDB
UNION ALL
SELECT 'budget','wikidata',t.ID_WIKIDATA,t.TITLE,COUNT(*),MIN(f.AMOUNT),MAX(f.AMOUNT)
FROM tmp_movies t JOIN T_WC_T2S_WORK_FINANCE f
  ON f.ITEM_TYPE='movie' AND f.ID_WIKIDATA=t.ID_WIKIDATA
WHERE f.FINANCE_KIND='budget' AND f.CURRENCY_CODE='USD'
GROUP BY t.ID_WIKIDATA,t.TITLE
UNION ALL
SELECT 'box_office_worldwide','wikidata',t.ID_WIKIDATA,t.TITLE,COUNT(*),MIN(f.AMOUNT),MAX(f.AMOUNT)
FROM tmp_movies t JOIN T_WC_T2S_WORK_FINANCE f
  ON f.ITEM_TYPE='movie' AND f.ID_WIKIDATA=t.ID_WIKIDATA
WHERE f.FINANCE_KIND='box_office' AND f.IS_WORLDWIDE=1 AND f.CURRENCY_CODE='USD'
GROUP BY t.ID_WIKIDATA,t.TITLE
UNION ALL
SELECT 'box_office_us','wikidata',t.ID_WIKIDATA,t.TITLE,COUNT(*),MIN(f.AMOUNT),MAX(f.AMOUNT)
FROM tmp_movies t JOIN T_WC_T2S_WORK_FINANCE f
  ON f.ITEM_TYPE='movie' AND f.ID_WIKIDATA=t.ID_WIKIDATA
WHERE f.FINANCE_KIND='box_office' AND f.ID_PLACE_WIKIDATA='Q30' AND f.CURRENCY_CODE='USD'
GROUP BY t.ID_WIKIDATA,t.TITLE
UNION ALL
SELECT 'runtime','wikidata',t.ID_WIKIDATA,t.TITLE,COUNT(*),
   MIN(CASE q.UNIT_ID_WIKIDATA WHEN 'Q7727' THEN q.AMOUNT WHEN 'Q11574' THEN q.AMOUNT/60
            WHEN 'Q25235' THEN q.AMOUNT*60 ELSE q.AMOUNT END),
   MAX(CASE q.UNIT_ID_WIKIDATA WHEN 'Q7727' THEN q.AMOUNT WHEN 'Q11574' THEN q.AMOUNT/60
            WHEN 'Q25235' THEN q.AMOUNT*60 ELSE q.AMOUNT END)
FROM tmp_movies t
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA=t.ID_WIKIDATA
  AND s.ID_PROPERTY='P2047' AND s.VALUE_TYPE='quantity'
  AND (s.RANK IS NULL OR s.RANK<>'deprecated') AND COALESCE(s.DELETED,0)=0
JOIN T_WC_WIKIDATA_QUANTITY_VALUE q ON q.ID_STATEMENT=s.ID_STATEMENT
GROUP BY t.ID_WIKIDATA,t.TITLE;

SELECT TITLE, ATTRIBUTE,
   ROUND(TMDB)      AS TMDB,
   ROUND(WD_MAX)    AS WIKIDATA,
   ROUND(OMDB)      AS OMDB,
   WD_N,
   CASE WHEN WD_MIN<>WD_MAX THEN CONCAT(ROUND(WD_MIN),'..',ROUND(WD_MAX)) END AS WD_SPREAD,
   ROUND(ABS(TMDB-WD_MAX)/NULLIF(GREATEST(TMDB,WD_MAX),0)*100,1) AS PCT_TMDB_WD,
   ROUND(ABS(OMDB-WD_MAX)/NULLIF(GREATEST(OMDB,WD_MAX),0)*100,1) AS PCT_OMDB_WD
FROM (
  SELECT TITLE, ATTRIBUTE,
    MAX(CASE WHEN SOURCE='tmdb'     THEN VMAX END) AS TMDB,
    MAX(CASE WHEN SOURCE='omdb'     THEN VMAX END) AS OMDB,
    MAX(CASE WHEN SOURCE='wikidata' THEN VMIN END) AS WD_MIN,
    MAX(CASE WHEN SOURCE='wikidata' THEN VMAX END) AS WD_MAX,
    MAX(CASE WHEN SOURCE='wikidata' THEN N_ROWS END) AS WD_N
  FROM tmp_num GROUP BY TITLE, ATTRIBUTE
) z
ORDER BY TITLE, ATTRIBUTE;

-- =====================================================================
-- VUE 2 : realisateur par identite (ensembles de Q)
-- =====================================================================
CREATE TEMPORARY TABLE tmp_dir_tmdb AS
SELECT t.ID_WIKIDATA, t.TITLE,
   GROUP_CONCAT(DISTINCT p.ID_WIKIDATA ORDER BY p.ID_WIKIDATA SEPARATOR ',') AS Q_SET,
   SUM(p.ID_WIKIDATA IS NULL OR p.ID_WIKIDATA='') AS N_NO_Q,
   COUNT(*) AS N_DIR
FROM tmp_movies t
JOIN T_WC_TMDB_PERSON_MOVIE pm ON pm.ID_MOVIE=t.ID_MOVIE
  AND pm.CREW_JOB='Director' AND COALESCE(pm.DELETED,0)=0
LEFT JOIN T_WC_TMDB_PERSON p ON p.ID_PERSON=pm.ID_PERSON
GROUP BY t.ID_WIKIDATA, t.TITLE;

CREATE TEMPORARY TABLE tmp_dir_wd AS
SELECT t.ID_WIKIDATA, t.TITLE,
   GROUP_CONCAT(DISTINCT iv.ID_ITEM ORDER BY iv.ID_ITEM SEPARATOR ',') AS Q_SET,
   COUNT(DISTINCT iv.ID_ITEM) AS N_DIR
FROM tmp_movies t
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA=t.ID_WIKIDATA
  AND s.ID_PROPERTY='P57' AND s.VALUE_TYPE='item'
  AND (s.RANK IS NULL OR s.RANK<>'deprecated') AND COALESCE(s.DELETED,0)=0
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT=s.ID_STATEMENT
GROUP BY t.ID_WIKIDATA, t.TITLE;

SELECT m.TITLE,
   a.N_DIR AS TMDB_N, a.N_NO_Q AS TMDB_NO_Q, a.Q_SET AS TMDB_DIR_Q,
   b.N_DIR AS WD_N,   b.Q_SET AS WD_DIR_Q,
   CASE
     WHEN a.ID_WIKIDATA IS NULL AND b.ID_WIKIDATA IS NULL THEN 'both_silent'
     WHEN a.ID_WIKIDATA IS NULL THEN 'tmdb_silent'
     WHEN b.ID_WIKIDATA IS NULL THEN 'wd_silent'
     WHEN a.Q_SET IS NULL THEN 'tmdb_no_q'
     WHEN a.Q_SET=b.Q_SET THEN 'equal'
     ELSE 'differ' END AS VERDICT
FROM tmp_movies m
LEFT JOIN tmp_dir_tmdb a ON a.ID_WIKIDATA=m.ID_WIKIDATA
LEFT JOIN tmp_dir_wd   b ON b.ID_WIKIDATA=m.ID_WIKIDATA
ORDER BY VERDICT, m.TITLE;

-- =====================================================================
-- VUE 3 : pays d'origine (ISO TMDb vs Q Wikidata mappe en ISO)
-- =====================================================================
CREATE TEMPORARY TABLE tmp_cty_tmdb AS
SELECT t.ID_WIKIDATA, t.TITLE,
   GROUP_CONCAT(DISTINCT pc.COUNTRY_CODE ORDER BY pc.COUNTRY_CODE SEPARATOR ',') AS ISO_SET
FROM tmp_movies t
JOIN T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY pc ON pc.ID_MOVIE=t.ID_MOVIE
GROUP BY t.ID_WIKIDATA, t.TITLE;

CREATE TEMPORARY TABLE tmp_cty_wd AS
SELECT t.ID_WIKIDATA, t.TITLE,
   GROUP_CONCAT(DISTINCT COALESCE(ci.ISO2, iv.ID_ITEM) ORDER BY COALESCE(ci.ISO2, iv.ID_ITEM) SEPARATOR ',') AS ISO_SET,
   SUM(ci.ISO2 IS NULL) AS N_UNMAPPED
FROM tmp_movies t
JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA=t.ID_WIKIDATA
  AND s.ID_PROPERTY='P495' AND s.VALUE_TYPE='item'
  AND (s.RANK IS NULL OR s.RANK<>'deprecated') AND COALESCE(s.DELETED,0)=0
JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT=s.ID_STATEMENT
LEFT JOIN tmp_country_iso ci ON ci.ID_WIKIDATA=iv.ID_ITEM
GROUP BY t.ID_WIKIDATA, t.TITLE;

SELECT m.TITLE,
   a.ISO_SET AS TMDB_ISO, b.ISO_SET AS WD_ISO, b.N_UNMAPPED AS WD_Q_UNMAPPED,
   CASE
     WHEN a.ID_WIKIDATA IS NULL AND b.ID_WIKIDATA IS NULL THEN 'both_silent'
     WHEN a.ID_WIKIDATA IS NULL THEN 'tmdb_silent'
     WHEN b.ID_WIKIDATA IS NULL THEN 'wd_silent'
     WHEN a.ISO_SET=b.ISO_SET THEN 'equal'
     ELSE 'differ' END AS VERDICT
FROM tmp_movies m
LEFT JOIN tmp_cty_tmdb a ON a.ID_WIKIDATA=m.ID_WIKIDATA
LEFT JOIN tmp_cty_wd   b ON b.ID_WIKIDATA=m.ID_WIKIDATA
ORDER BY VERDICT, m.TITLE;

-- =====================================================================
-- VUE 4 : series, finance seule (TMDb muet par construction)
-- =====================================================================
SELECT se.TITLE, se.FIRST_AIR_YEAR, f.FINANCE_KIND,
       COALESCE(f.CURRENCY_CODE, f.ID_CURRENCY) AS CURRENCY,
       CASE WHEN f.IS_WORLDWIDE=1 THEN 'worldwide' ELSE f.ID_PLACE_WIKIDATA END AS PLACE,
       COUNT(*) AS N_ROWS, MIN(f.AMOUNT) AS AMOUNT_MIN, MAX(f.AMOUNT) AS AMOUNT_MAX
FROM tmp_series se
JOIN T_WC_T2S_WORK_FINANCE f ON f.ITEM_TYPE='serie' AND f.ID_WIKIDATA=se.ID_WIKIDATA
GROUP BY se.TITLE, se.FIRST_AIR_YEAR, f.FINANCE_KIND, CURRENCY, PLACE
ORDER BY se.TITLE, f.FINANCE_KIND, CURRENCY;

DROP TEMPORARY TABLE tmp_num, tmp_dir_tmdb, tmp_dir_wd, tmp_cty_tmdb, tmp_cty_wd,
     tmp_country_iso, tmp_movies, tmp_series;
