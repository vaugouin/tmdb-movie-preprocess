-- FASTAPI-TEXT2SQL-194 : IMDB_VOTES sur T_WC_T2S_MOVIE et T_WC_T2S_SERIE
--
-- POURQUOI. Les notes TMDb (VOTE_AVERAGE) et leur compte de votes (VOTE_COUNT) ont ete
-- retires de l'API le 2026-08-06 : trop peu de votants, et une divergence mesuree qui
-- racontait l'histoire inverse d'IMDb (House of the Dragon saison 3 : 5,9 chez TMDb,
-- 8,29 chez IMDb). Effet de bord assume, films et series se sont retrouves sans aucun
-- compte de votes. Cette colonne le remet, cette fois sur la source qui compte.
--
-- Le type et le nom suivent T_WC_T2S_EPISODE, qui porte deja IMDB_VOTES int(11) : une
-- meme donnee doit porter le meme nom et le meme type partout dans le modele.
--
-- ORDRE DES OPERATIONS, il compte.
--   1. Jouer ce script.
--   2. Lancer le preprocess (l'UPDATE qui pose IMDB_RATING pose desormais IMDB_VOTES
--      dans la meme passe, sans balayage supplementaire).
--   3. SEULEMENT ENSUITE declarer IMDB_VOTES dans fastapi-text2sql/data/text_to_sql.md.
--      Declarer une colonne avant qu'elle existe et soit peuplee revient a pointer le
--      modele vers une colonne fantome : il ecrira du SQL qui echoue.

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE `T_WC_T2S_MOVIE`
  ADD COLUMN `IMDB_VOTES` int(11) DEFAULT NULL AFTER `IMDB_RATING_WEIGHTED`,
  ADD KEY `IMDB_VOTES` (`IMDB_VOTES`);

ALTER TABLE `T_WC_T2S_SERIE`
  ADD COLUMN `IMDB_VOTES` int(11) DEFAULT NULL AFTER `IMDB_RATING_WEIGHTED`,
  ADD KEY `IMDB_VOTES` (`IMDB_VOTES`);

-- Controle apres le run du preprocess : la couverture doit approcher celle de
-- IMDB_RATING, aux titres sans votes pres.
SELECT
    COUNT(*)                                              AS TOTAL,
    COUNT(IMDB_RATING)                                    AS AVEC_NOTE,
    COUNT(IMDB_VOTES)                                     AS AVEC_VOTES,
    ROUND(100 * COUNT(IMDB_VOTES) / COUNT(*), 1)          AS PCT_VOTES
FROM T_WC_T2S_MOVIE;

SELECT
    COUNT(*)                                              AS TOTAL,
    COUNT(IMDB_RATING)                                    AS AVEC_NOTE,
    COUNT(IMDB_VOTES)                                     AS AVEC_VOTES,
    ROUND(100 * COUNT(IMDB_VOTES) / COUNT(*), 1)          AS PCT_VOTES
FROM T_WC_T2S_SERIE;
