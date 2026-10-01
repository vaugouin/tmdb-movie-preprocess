SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Process 70 (assertion refresh): why do evals 2434, 2436, 2439, 2481, 2502, 2506, 2508 skip?
-- Read-only. Hypothesis: the refresh SQL filters `<table>.DELETED = 0`, but processes 9 / 10
-- never wrote DELETED on the credit tables, so the column is NULL everywhere and the filter
-- matches nothing: 0 rows, reported until 2026-10-01 as "no usable integer ids returned".

-- 1. DELETED distribution on every table the refresh SQLs filter on.
SELECT 'T_WC_T2S_PERSON_MOVIE' AS TBL, DELETED, COUNT(*) AS N FROM T_WC_T2S_PERSON_MOVIE GROUP BY DELETED
UNION ALL
SELECT 'T_WC_T2S_PERSON_SERIE', DELETED, COUNT(*) FROM T_WC_T2S_PERSON_SERIE GROUP BY DELETED
UNION ALL
SELECT 'T_WC_T2S_MOVIE', DELETED, COUNT(*) FROM T_WC_T2S_MOVIE GROUP BY DELETED
UNION ALL
SELECT 'T_WC_T2S_SERIE', DELETED, COUNT(*) FROM T_WC_T2S_SERIE GROUP BY DELETED
UNION ALL
SELECT 'T_WC_T2S_PERSON', DELETED, COUNT(*) FROM T_WC_T2S_PERSON GROUP BY DELETED;

-- 2. IS_MOVIE distribution (eval 2502 also filters IS_MOVIE = 1).
SELECT IS_MOVIE, COUNT(*) AS N FROM T_WC_T2S_MOVIE GROUP BY IS_MOVIE;

-- 3. Eval 2502 taken apart: David Lynch directing credits, with and without the DELETED filters.
SELECT COUNT(*) AS LYNCH_DIRECTING_ALL,
       SUM(pm.DELETED = 0) AS PM_DELETED_0,
       SUM(m.DELETED = 0) AS M_DELETED_0,
       SUM(m.IS_MOVIE = 1) AS M_IS_MOVIE_1
  FROM T_WC_T2S_PERSON p
  JOIN T_WC_T2S_PERSON_MOVIE pm ON pm.ID_PERSON = p.ID_PERSON
  JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = pm.ID_MOVIE
 WHERE p.PERSON_NAME = 'David Lynch'
   AND pm.CREDIT_TYPE = 'crew'
   AND pm.CREW_DEPARTMENT = 'Directing';

-- 4. The seven skipped refresh SQLs, to read them.
SELECT ID_T2S_EVALUATION, ASSERTION_REFRESH_SQL
  FROM T_WC_T2S_EVALUATION
 WHERE ID_T2S_EVALUATION IN (2434, 2436, 2439, 2481, 2502, 2506, 2508)
 ORDER BY ID_T2S_EVALUATION;
