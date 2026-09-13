-- test-049-colour-source.sql
-- Recette de TMDB-MOVIE-PREPROCESS-049 (processus 64, la couleur depuis Wikidata P462
-- quand la ligne Format manque). Les attentes sont posees AVANT l'execution.
--
-- ⚠ Pas de requete information_schema avant les requetes sans qualification (piege
--   phpMyAdmin, voir AGENTS.md). Aucune ici.
--
-- Quand : apres la migration, apres un passage du processus 64 (scope wikidata-colour ou
-- main), et apres le processus 4 pour le bloc G.

-- A. Repartition par provenance. Attendu apres le premier passage : 'format_line' de
--    l'ordre de 50 000, 'wikidata' de l'ordre de 90 000 (films avec P462 et sans ligne),
--    'none' le reste. N_BOTH sous 'wikidata' est la liste a trier a la main (option A).
SELECT '(A) movies by COLOR_SOURCE' AS test;
SELECT COALESCE(COLOR_SOURCE, 'none') AS COLOR_SOURCE, COUNT(*) AS N,
       SUM(IS_COLOR = 1) AS N_COLOR, SUM(IS_BLACK_AND_WHITE = 1) AS N_BW,
       SUM(IS_COLOR = 1 AND IS_BLACK_AND_WHITE = 1) AS N_BOTH
FROM T_WC_TMDB_MOVIE
GROUP BY COALESCE(COLOR_SOURCE, 'none');

-- B. Aucun film signe 'wikidata' ne porte une ligne Format. Attendu : 0.
SELECT '(B) wikidata-signed movies that carry a Format line, expected 0' AS test;
SELECT COUNT(*) AS N
FROM T_WC_TMDB_MOVIE
WHERE COLOR_SOURCE = 'wikidata'
  AND WIKIPEDIA_FORMAT_LINE IS NOT NULL AND WIKIPEDIA_FORMAT_LINE <> '';

-- C. Temoin Pleasantville (TMDb 2657) : pas de ligne, P462 aux deux valeurs. Attendu :
--    IS_COLOR 1, IS_BLACK_AND_WHITE 1, COLOR_SOURCE 'wikidata', et deux lignes de jonction
--    (color_movie, black_and_white_movie).
SELECT '(C) Pleasantville 2657, expected 1 / 1 / wikidata and two medium_format rows' AS test;
SELECT ID_MOVIE, TITLE, IS_COLOR, IS_BLACK_AND_WHITE, COLOR_SOURCE, TIM_COLOR_SOURCE, WIKIPEDIA_FORMAT_LINE
FROM T_WC_TMDB_MOVIE WHERE ID_MOVIE = 2657;
SELECT mt.ID_MOVIE, t.DESCRIPTION, mt.DISPLAY_ORDER
FROM T_WC_T2S_MOVIE_TECHNICAL mt
JOIN T_WC_T2S_TECHNICAL t ON t.ID_TECHNICAL = mt.ID_TECHNICAL
WHERE mt.ID_MOVIE = 2657 AND t.TECHNICAL_TYPE = 'medium_format';

-- D. Temoin Colt .45 (TMDb 55472) : ligne corrigee sur Wikipedia le 2026-09-12 (Couleurs
--    Technicolor). Attendu, une fois la ligne recrawlee et reparsee par le processus 1 :
--    IS_COLOR 1, IS_BLACK_AND_WHITE 0, COLOR_SOURCE 'format_line'. Tant que la ligne en base
--    dit encore 'Noir et blanc', le film reste signe 'format_line' avec IS_BLACK_AND_WHITE 1,
--    et le processus 64 ne doit PAS y toucher (la ligne est non vide).
SELECT '(D) Colt .45 55472, expected format_line; IS_COLOR 1 once the line is recrawled' AS test;
SELECT ID_MOVIE, TITLE, IS_COLOR, IS_BLACK_AND_WHITE, COLOR_SOURCE, TIM_COLOR_SOURCE,
       WIKIPEDIA_FORMAT_LINE, DAT_WIKIPEDIA_FORMAT_LINE
FROM T_WC_TMDB_MOVIE WHERE ID_MOVIE = 55472;

-- E. Coherence jonction / flags pour les films 'wikidata'. Attendu : 0 et 0.
SELECT '(E) wikidata-signed movies whose junction rows disagree with the flags, expected 0 / 0' AS test;
SELECT
  (SELECT COUNT(*) FROM T_WC_TMDB_MOVIE m
    WHERE m.COLOR_SOURCE = 'wikidata' AND m.IS_COLOR = 1
      AND NOT EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE_TECHNICAL mt
                      JOIN T_WC_T2S_TECHNICAL t ON t.ID_TECHNICAL = mt.ID_TECHNICAL
                      WHERE mt.ID_MOVIE = m.ID_MOVIE AND t.DESCRIPTION = 'color_movie')) AS colour_flag_without_row,
  (SELECT COUNT(*) FROM T_WC_TMDB_MOVIE m
    WHERE m.COLOR_SOURCE = 'wikidata' AND COALESCE(m.IS_COLOR, 0) = 0
      AND EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE_TECHNICAL mt
                  JOIN T_WC_T2S_TECHNICAL t ON t.ID_TECHNICAL = mt.ID_TECHNICAL
                  WHERE mt.ID_MOVIE = m.ID_MOVIE AND t.DESCRIPTION = 'color_movie')) AS row_without_colour_flag;

-- F. Idempotence : lancer le processus deux fois de suite ; au second passage les compteurs
--    ...updatedcount et ...clearedcount doivent valoir 0, et ...error doit etre vide.
SELECT '(F) last run counters, expected 0 / 0 on a second consecutive run, error blank' AS test;
SELECT VAR_NAME, VAR_VALUE
FROM T_WC_SERVER_VARIABLE
WHERE DELETED = 0
  AND VAR_NAME IN ('strtmdbmoviepreprocesswikidatacolourupdatedcount',
                   'strtmdbmoviepreprocesswikidatacolourclearedcount',
                   'strtmdbmoviepreprocesswikidatacolouritemscount',
                   'strtmdbmoviepreprocesswikidatacolourlastrun',
                   'strtmdbmoviepreprocesswikidatacolourerror');

-- G. Le read-model suit, apres le processus 4. Attendu : la meme repartition que (A) pour
--    les films presents dans T2S (ADULT = 0 et ID_IMDB renseigne).
SELECT '(G) T2S movies by COLOR_SOURCE after process 4' AS test;
SELECT COALESCE(COLOR_SOURCE, 'none') AS COLOR_SOURCE, COUNT(*) AS N,
       SUM(IS_COLOR = 1 AND IS_BLACK_AND_WHITE = 1) AS N_BOTH
FROM T_WC_T2S_MOVIE
GROUP BY COALESCE(COLOR_SOURCE, 'none');

-- H. Le nombre de films aux deux flags, a dater dans l'article #7 (617 le 2026-09-12 avant
--    le processus 64 ; il monte apres).
SELECT '(H) movies both colour and black and white, dated' AS test;
SELECT NOW() AS measured_at, COUNT(*) AS N_BOTH
FROM T_WC_T2S_MOVIE WHERE IS_COLOR = 1 AND IS_BLACK_AND_WHITE = 1;
