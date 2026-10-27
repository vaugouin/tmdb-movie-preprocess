-- =====================================================================
-- Sonde de reconciliation « argent » (budget, box office). LECTURE SEULE.
-- But : comparer un meme fait entre trois sources deja structurees, sur
-- cinq films temoins, pour de-risquer le schema de la future table
-- T_WC_T2S_SOURCE_CLAIM (topic reconciliation, RECONCILIATION-001).
--
--   TMDb   T_WC_TMDB_MOVIE        BUDGET, REVENUE (double ; USD, mondial, 1 valeur)
--   Wiki.  T_WC_T2S_WORK_FINANCE  process 75 (multi-lignes : devise, lieu, date)
--   OMDb   T_WC_OMDB_TITLE        BOX_OFFICE_USD (bigint ; box office US domestique)
--
-- Pivot de jointure : T_WC_TMDB_MOVIE porte ID_MOVIE (TMDb), ID_IMDB (tt...),
-- ID_WIKIDATA (Q...). Pieges respectes ici :
--   - TMDb REVENUE = box office MONDIAL  -> Wikidata IS_WORLDWIDE=1
--   - OMDb BOX_OFFICE_USD = box office US -> Wikidata ID_PLACE_WIKIDATA='Q30'
--   - Wikidata a n lignes par film -> on agrege et on expose le compte (N_ROWS)
--
-- Temoins : Avatar Q24871, Inception Q25188, Titanic Q44578,
--           The Matrix Q83495, The Dark Knight Q163872.
-- =====================================================================

CREATE TEMPORARY TABLE tmp_claims AS
WITH pivot AS (
    SELECT ID_MOVIE, ID_IMDB, ID_WIKIDATA, TITLE, RELEASE_YEAR, BUDGET, REVENUE
    FROM   T_WC_TMDB_MOVIE
    WHERE  ID_WIKIDATA IN ('Q24871','Q25188','Q44578','Q83495','Q163872')
),
c_tmdb AS (
    SELECT ID_MOVIE, TITLE, 'budget' AS ATTRIBUTE, 'tmdb' AS SOURCE, 1 AS N_ROWS,
           NULLIF(BUDGET,0) AS AMOUNT_MIN, NULLIF(BUDGET,0) AS AMOUNT_MAX,
           'USD' AS CURRENCY, 'worldwide' AS PLACE
    FROM pivot
    UNION ALL
    SELECT ID_MOVIE, TITLE, 'box_office_worldwide', 'tmdb', 1,
           NULLIF(REVENUE,0), NULLIF(REVENUE,0), 'USD', 'worldwide'
    FROM pivot
),
c_wd AS (
    SELECT p.ID_MOVIE, p.TITLE,
           CASE WHEN f.FINANCE_KIND='budget'    THEN 'budget'
                WHEN f.IS_WORLDWIDE=1           THEN 'box_office_worldwide'
                WHEN f.ID_PLACE_WIKIDATA='Q30'  THEN 'box_office_us'
                ELSE 'box_office_other' END AS ATTRIBUTE,
           'wikidata' AS SOURCE, COUNT(*) AS N_ROWS,
           MIN(f.AMOUNT) AS AMOUNT_MIN, MAX(f.AMOUNT) AS AMOUNT_MAX, 'USD' AS CURRENCY,
           CASE WHEN f.FINANCE_KIND='budget'   THEN 'worldwide'
                WHEN f.IS_WORLDWIDE=1          THEN 'worldwide'
                WHEN f.ID_PLACE_WIKIDATA='Q30' THEN 'US'
                ELSE 'other' END AS PLACE
    FROM   pivot p
    JOIN   T_WC_T2S_WORK_FINANCE f
           ON f.ID_ITEM = p.ID_MOVIE AND f.ITEM_TYPE='movie'
    WHERE  f.CURRENCY_CODE='USD'
    GROUP  BY p.ID_MOVIE, p.TITLE, ATTRIBUTE, PLACE
),
c_omdb AS (
    SELECT p.ID_MOVIE, p.TITLE, 'box_office_us' AS ATTRIBUTE, 'omdb' AS SOURCE, 1 AS N_ROWS,
           NULLIF(o.BOX_OFFICE_USD,0) AS AMOUNT_MIN, NULLIF(o.BOX_OFFICE_USD,0) AS AMOUNT_MAX,
           'USD' AS CURRENCY, 'US' AS PLACE
    FROM   pivot p
    JOIN   T_WC_OMDB_TITLE o ON o.ID_IMDB = p.ID_IMDB
)
SELECT * FROM c_tmdb
UNION ALL SELECT * FROM c_wd
UNION ALL SELECT * FROM c_omdb;

-- VUE A : format long = la future table de claims, brute (montre N_ROWS Wikidata)
SELECT TITLE, ATTRIBUTE, SOURCE, N_ROWS, AMOUNT_MIN, AMOUNT_MAX, PLACE
FROM   tmp_claims
ORDER  BY TITLE, ATTRIBUTE, SOURCE;

-- VUE B : comparaison cote a cote + ecart relatif (NULL = source muette)
SELECT TITLE, ATTRIBUTE, TMDB, WIKIDATA, OMDB,
       ROUND(ABS(TMDB-WIKIDATA)/NULLIF(GREATEST(TMDB,WIKIDATA),0)*100,1) AS PCT_TMDB_WD,
       ROUND(ABS(OMDB-WIKIDATA)/NULLIF(GREATEST(OMDB,WIKIDATA),0)*100,1) AS PCT_OMDB_WD
FROM (
    SELECT TITLE, ATTRIBUTE,
           MAX(CASE WHEN SOURCE='tmdb'     THEN AMOUNT_MAX END) AS TMDB,
           MAX(CASE WHEN SOURCE='wikidata' THEN AMOUNT_MAX END) AS WIKIDATA,
           MAX(CASE WHEN SOURCE='omdb'     THEN AMOUNT_MAX END) AS OMDB
    FROM   tmp_claims
    WHERE  ATTRIBUTE IN ('budget','box_office_worldwide','box_office_us')
    GROUP  BY TITLE, ATTRIBUTE
) z
ORDER BY TITLE, ATTRIBUTE;

DROP TEMPORARY TABLE tmp_claims;
