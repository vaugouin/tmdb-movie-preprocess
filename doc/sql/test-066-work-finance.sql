-- TMDB-MOVIE-PREPROCESS-066: acceptance of T_WC_T2S_WORK_FINANCE (process 75). READ ONLY.
-- Run after the first ./tmdb-movie-preprocess-finance.sh.
-- Expected, written BEFORE the run, from the census of 2026-10-20
-- (wikidata-crawler/doc/sql/check-029-external-id-census-20261020.txt):
--   A. about 7 200 budget and 7 800 box office rows on movies, a few dozen on series,
--      minus the deprecated statements (126 box office, 14 budget);
--   B. USD on about 80 % of the rows; EUR, GBP, AUD, RUB, CAD, JPY next;
--   C. worldwide (Q13780930) and the United States (Q30) lead the places; most places
--      other than worldwide find their T2S location;
--   D. Avatar (Q24871): budget 237 000 000 USD, worldwide 2 847 379 794 USD dated
--      2022-03-26, United States 760 507 625 USD, United Kingdom 93 442 625 GBP;
--   E. Inception (Q25188): worldwide 836 848 102 (the deprecated 829 895 144 absent),
--      United States 292 587 330.

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- A. Rows per family and kind
SELECT ITEM_TYPE, FINANCE_KIND, COUNT(*) AS ROWS_, COUNT(DISTINCT ID_ITEM) AS WORKS,
       SUM(DAT_POINT IS NOT NULL) AS WITH_DATE, SUM(DAT_START IS NOT NULL OR DAT_END IS NOT NULL) AS WITH_PERIOD_DATES,
       SUM(ID_PERIOD_WIKIDATA IS NOT NULL) AS WITH_PERIOD_ITEM, SUM(CHART_RANK IS NOT NULL) AS WITH_CHART_RANK
FROM T_WC_T2S_WORK_FINANCE
GROUP BY ITEM_TYPE, FINANCE_KIND;

-- B. Currencies
SELECT FINANCE_KIND, ID_CURRENCY, CURRENCY_CODE, COUNT(*) AS ROWS_
FROM T_WC_T2S_WORK_FINANCE
GROUP BY FINANCE_KIND, ID_CURRENCY, CURRENCY_CODE
ORDER BY ROWS_ DESC
LIMIT 15;

-- C. Places, and how many link to a T2S location
SELECT f.ID_PLACE_WIKIDATA, MAX(l.LOCATION_NAME) AS LOCATION_NAME, f.IS_WORLDWIDE,
       COUNT(*) AS ROWS_, SUM(f.ID_LOCATION IS NOT NULL) AS LINKED
FROM T_WC_T2S_WORK_FINANCE f
LEFT JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = f.ID_LOCATION
GROUP BY f.ID_PLACE_WIKIDATA, f.IS_WORLDWIDE
ORDER BY ROWS_ DESC
LIMIT 20;

-- D and E. Witnesses
SELECT f.ID_WIKIDATA, m.MOVIE_TITLE, f.FINANCE_KIND, f.AMOUNT, f.CURRENCY_CODE, f.ID_CURRENCY,
       f.ID_PLACE_WIKIDATA, l.LOCATION_NAME, f.IS_WORLDWIDE, f.DAT_POINT, f.DAT_START, f.DAT_END,
       f.ID_PERIOD_WIKIDATA, f.CHART_RANK, f.WIKIDATA_RANK
FROM T_WC_T2S_WORK_FINANCE f
LEFT JOIN T_WC_T2S_MOVIE m ON f.ITEM_TYPE = 'movie' AND m.ID_MOVIE = f.ID_ITEM
LEFT JOIN T_WC_T2S_LOCATION l ON l.ID_LOCATION = f.ID_LOCATION
WHERE f.ID_WIKIDATA IN ('Q24871', 'Q25188', 'Q44578', 'Q83495', 'Q163872')
ORDER BY f.ID_WIKIDATA, f.FINANCE_KIND, f.AMOUNT DESC;

-- F. Agreement with TMDb: worldwide USD box office against REVENUE, budget USD against BUDGET
SELECT f.FINANCE_KIND, COUNT(*) AS COMPARED,
       SUM(ABS(f.AMOUNT - IF(f.FINANCE_KIND = 'budget', m.BUDGET, m.REVENUE)) <= 0.05 * f.AMOUNT) AS WITHIN_5_PERCENT,
       SUM(IF(f.FINANCE_KIND = 'budget', m.BUDGET, m.REVENUE) IS NULL OR IF(f.FINANCE_KIND = 'budget', m.BUDGET, m.REVENUE) = 0) AS TMDB_EMPTY
FROM T_WC_T2S_WORK_FINANCE f
JOIN T_WC_T2S_MOVIE m ON f.ITEM_TYPE = 'movie' AND m.ID_MOVIE = f.ID_ITEM
WHERE f.CURRENCY_CODE = 'USD' AND (f.FINANCE_KIND = 'budget' OR f.IS_WORLDWIDE = 1)
GROUP BY f.FINANCE_KIND;
