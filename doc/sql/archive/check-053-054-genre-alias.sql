-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-053 / -054: acceptance of Processes 50 and 51
-- ============================================================================
--
-- READ ONLY. Run after a pass that included Processes 50 and 51 (the main scope,
-- or TMDB_PREPROCESS_SCOPE=genre-alias). Every "must be 0" line must read 0.
--
-- Run on the VPS with ~/docker/tools/runsqlvaugouindb.sh (see ~/Code/tools/AGENTS.md).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET SESSION max_statement_time = 0;

SELECT NOW() AS measured_at;


-- ############################################################################
-- ### A . GENRES (Process 50, -053)                                        ###
-- ############################################################################

SELECT '=== A . genres ===' AS section;

SELECT 'T2S_GENRE' AS t2s_table,
       (SELECT COUNT(*) FROM T_WC_T2S_GENRE) AS t2s_rows,
       (SELECT COUNT(*) FROM T_WC_TMDB_GENRE) AS source_rows
UNION ALL
SELECT 'T2S_GENRE_LANG',
       (SELECT COUNT(*) FROM T_WC_T2S_GENRE_LANG),
       (SELECT COUNT(*) FROM T_WC_TMDB_GENRE_LANG);

SELECT 'genres whose name or flags differ from the source (must be 0)' AS check_name, COUNT(*) AS n
FROM T_WC_TMDB_GENRE g
LEFT JOIN T_WC_T2S_GENRE t ON t.ID_GENRE = g.id
WHERE t.ID_GENRE IS NULL
   OR NOT (t.GENRE_NAME <=> g.name
       AND t.APPLIES_TO_MOVIE <=> g.APPLIES_TO_MOVIE
       AND t.APPLIES_TO_SERIE <=> g.APPLIES_TO_SERIE)
UNION ALL
SELECT 'translations that differ from the source (must be 0)', COUNT(*)
FROM T_WC_TMDB_GENRE_LANG l
LEFT JOIN T_WC_T2S_GENRE_LANG t ON t.ID_ROW = l.ID_ROW
WHERE t.ID_ROW IS NULL
   OR NOT (t.ID_GENRE <=> l.id AND t.LANG <=> l.LANG AND t.GENRE_NAME <=> l.name)
UNION ALL
SELECT 'T2S_MOVIE_GENRE rows whose genre is missing (must be 0)', COUNT(*)
FROM T_WC_T2S_MOVIE_GENRE mg
LEFT JOIN T_WC_T2S_GENRE g ON g.ID_GENRE = mg.ID_GENRE
WHERE g.ID_GENRE IS NULL
UNION ALL
SELECT 'T2S_SERIE_GENRE rows whose genre is missing (must be 0)', COUNT(*)
FROM T_WC_T2S_SERIE_GENRE sg
LEFT JOIN T_WC_T2S_GENRE g ON g.ID_GENRE = sg.ID_GENRE
WHERE g.ID_GENRE IS NULL;

SELECT ID_GENRE, GENRE_NAME, APPLIES_TO_MOVIE, APPLIES_TO_SERIE
FROM T_WC_T2S_GENRE
ORDER BY GENRE_NAME;


-- ############################################################################
-- ### B . PERSON ALIASES (Process 51, -054)                                ###
-- ############################################################################

SELECT '=== B . person aliases ===' AS section;

SELECT (SELECT COUNT(*) FROM T_WC_T2S_PERSON_ALSO_KNOWN_AS) AS t2s_aliases,
       (SELECT COUNT(*) FROM T_WC_TMDB_PERSON_ALSO_KNOWN_AS) AS source_aliases,
       (SELECT COUNT(*) FROM T_WC_TMDB_PERSON_ALSO_KNOWN_AS a
         LEFT JOIN T_WC_T2S_PERSON p ON p.ID_PERSON = a.ID_PERSON
         WHERE p.ID_PERSON IS NULL) AS aliases_of_out_of_scope_persons;

SELECT 'T2S aliases whose person is not in T2S_PERSON (must be 0)' AS check_name, COUNT(*) AS n
FROM T_WC_T2S_PERSON_ALSO_KNOWN_AS t
LEFT JOIN T_WC_T2S_PERSON p ON p.ID_PERSON = t.ID_PERSON
WHERE p.ID_PERSON IS NULL
UNION ALL
SELECT 'source aliases of T2S persons missing from T2S (must be 0)', COUNT(*)
FROM T_WC_TMDB_PERSON_ALSO_KNOWN_AS a
JOIN T_WC_T2S_PERSON p ON p.ID_PERSON = a.ID_PERSON
LEFT JOIN T_WC_T2S_PERSON_ALSO_KNOWN_AS t ON t.ID_ROW = a.ID_ROW
WHERE t.ID_ROW IS NULL
UNION ALL
SELECT 'aliases whose content or generated keys differ from the source (must be 0)', COUNT(*)
FROM T_WC_T2S_PERSON_ALSO_KNOWN_AS t
JOIN T_WC_TMDB_PERSON_ALSO_KNOWN_AS a ON a.ID_ROW = t.ID_ROW
WHERE NOT (t.ID_PERSON <=> a.ID_PERSON
       AND t.PERSON_NAME <=> a.PERSON_NAME
       AND t.LANGUAGE_FAMILY <=> a.LANGUAGE_FAMILY
       AND t.DELETED <=> a.DELETED
       AND t.PERSON_NAME_NORM <=> a.PERSON_NAME_NORM
       AND t.PERSON_NAME_KEY <=> a.PERSON_NAME_KEY);

-- A few non-Latin aliases, to see the generated keys with one's own eyes.
SELECT t.ID_PERSON, p.PERSON_NAME AS canonical_name, t.PERSON_NAME AS alias,
       t.LANGUAGE_FAMILY, t.PERSON_NAME_NORM, t.PERSON_NAME_KEY
FROM T_WC_T2S_PERSON_ALSO_KNOWN_AS t
JOIN T_WC_T2S_PERSON p ON p.ID_PERSON = t.ID_PERSON
WHERE p.PERSON_NAME IN ('Hayao Miyazaki', 'Akira Kurosawa', 'Wong Kar-wai')
ORDER BY p.PERSON_NAME, t.ID_ROW
LIMIT 30;
