-- =============================================================================
-- Review the Wikidata company links produced by Process 63 (pilot).
-- Purpose: after a run, judge match QUALITY so the allowlist / thresholds can be
-- tuned, then re-run. The audit trail is three columns the linker stamps:
--   ID_WIKIDATA     the matched Wikidata Q-id (click through to verify the entity)
--   WIKIDATA_LABEL  the English Wikidata label of that entity (compare to NAME)
--   CONFIDENCE      1.0 exact label match · 0.95 top-candidate title match ·
--                   0.92-1.0 fuzzy best-match. LOW confidence = review first.
-- =============================================================================

-- 1) Coverage: matched vs no-match vs not-yet-searched.
SELECT
    SUM(ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '')                       AS MATCHED,
    SUM(TIM_WIKIPEDIA_SEARCH IS NOT NULL AND (ID_WIKIDATA IS NULL OR ID_WIKIDATA = '')) AS SEARCHED_NO_MATCH,
    SUM(TIM_WIKIPEDIA_SEARCH IS NULL)                                        AS NOT_YET_SEARCHED,
    COUNT(*)                                                                 AS TOTAL
FROM T_WC_TMDB_COMPANY
WHERE (DELETED IS NULL OR DELETED = 0);

-- 2) Confidence distribution of the matches.
SELECT ROUND(CONFIDENCE, 2) AS CONFIDENCE_BAND, COUNT(*) AS COMPTE
FROM T_WC_TMDB_COMPANY
WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''
GROUP BY ROUND(CONFIDENCE, 2)
ORDER BY CONFIDENCE_BAND ASC;

-- 3) Most suspect matches first (lowest confidence) -- eyeball these for wrong links.
SELECT ID_COMPANY, NAME, WIKIDATA_LABEL, ID_WIKIDATA, CONFIDENCE, MOVIE_COUNT
FROM T_WC_TMDB_COMPANY
WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''
ORDER BY CONFIDENCE ASC, MOVIE_COUNT DESC
LIMIT 200;

-- 4) Label divergence: the Wikidata label differs from the company NAME.
--    A strong divergence often means a wrong-entity collision (e.g. NAME 'Orange'
--    linked to the fruit). Review these regardless of confidence.
SELECT ID_COMPANY, NAME, WIKIDATA_LABEL, ID_WIKIDATA, CONFIDENCE
FROM T_WC_TMDB_COMPANY
WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''
  AND LOWER(WIKIDATA_LABEL) <> LOWER(NAME)
ORDER BY MOVIE_COUNT DESC
LIMIT 200;

-- 5) High-value misses: important companies (many movies) the linker did not match,
--    so they can be checked manually or fed back into allowlist/threshold tuning.
SELECT ID_COMPANY, NAME, MOVIE_COUNT, SERIE_COUNT, TIM_WIKIPEDIA_SEARCH
FROM T_WC_TMDB_COMPANY
WHERE (ID_WIKIDATA IS NULL OR ID_WIKIDATA = '')
  AND TIM_WIKIPEDIA_SEARCH IS NOT NULL
  AND (DELETED IS NULL OR DELETED = 0)
ORDER BY MOVIE_COUNT DESC
LIMIT 200;
