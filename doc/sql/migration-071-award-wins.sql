-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-071 : award wins, one link row per win (year, work, family)
-- ============================================================================
--
-- NOT YET APPLIED. To run once on the live base (voie 2, DDL) before the first pass of
-- Process 44 at version 1.1.54 or later:
--   cd ~/docker/tmdb-movie-preprocess
--   git pull
--   ~/docker/tools/runsqlvaugouindb.sh ~/docker/tmdb-movie-preprocess/doc/sql/migration-071-award-wins.sql
-- Until it has run, Process 44 skips itself (f_awardwinschemamissing) and says why in the
-- log and in the server variable strtmdbmoviepreprocessawardwinskipreason: the award
-- tables stay as they were, nothing breaks.
--
-- THE SHAPE. Until now T_WC_T2S_MOVIE_AWARD, T_WC_T2S_SERIE_AWARD and
-- T_WC_T2S_PERSON_AWARD held one row per (entity, award) pair and nothing else. After
-- this migration and the first build, a link row is ONE WIN: one P166 statement of
-- Wikidata and one year.
--   - ID_STATEMENT          the P166 statement the row comes from
--                           (T_WC_WIKIDATA_STATEMENT.ID_STATEMENT, stable across loads).
--   - AWARD_YEAR            the year of the win: qualifier P585 of the statement, else
--                           the year of the ceremony named by its qualifier P805. NULL
--                           when Wikidata gives neither. For the Oscars and the Cesars
--                           it is the year of the ceremony, not of the film.
--   - ID_CEREMONY_WIKIDATA  the ceremony (qualifier P805), a Wikidata id, when the
--                           statement names one for that year.
--   - ID_MOVIE_FOR, ID_SERIE_FOR (T_WC_T2S_PERSON_AWARD only)
--                           the work the person was rewarded for (qualifier P1686),
--                           resolved to a T2S movie or series. NULL when Wikidata does
--                           not say, or when the work is not a T2S movie or series.
--   - T_WC_T2S_AWARD.ID_AWARD_PARENT
--                           the family of a category: "Academy Award for Best Actress"
--                           points to "Academy Awards". One level, NULL when the
--                           category has no family among the award rows.
-- A statement carrying several years (733 of them on 2026-11-10) gives one row per
-- year. The logical key of a link row is (ID_STATEMENT, entity id, AWARD_YEAR).
--
-- WHAT CHANGES FOR READERS. The same (entity, award) pair can now appear on several
-- rows: a list of the awards of an entity, or of the winners of an award, needs
-- DISTINCT or GROUP BY; the number of wins is the number of rows. DISPLAY_ORDER is the
-- rank of the entity within the award and is the same on every row of that entity.
--
-- KEYS ARE LOGICAL, NOT DECLARED, like every T2S table. No UNIQUE key on the logical
-- key: AWARD_YEAR is nullable and a UNIQUE key would not see two NULL rows as equal;
-- Process 44 removes duplicates itself after each award.
--
-- RE-RUNNABLE: every ADD carries IF NOT EXISTS. On 161 454 link rows and 18 117 award
-- rows (2026-11-10) the three ALTERs take seconds.
--
-- COLLATION. Run with the runner's default (--force behaviour).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE `T_WC_T2S_MOVIE_AWARD`
  ADD COLUMN IF NOT EXISTS `ID_STATEMENT` bigint(20) DEFAULT NULL AFTER `ID_AWARD`,
  ADD COLUMN IF NOT EXISTS `AWARD_YEAR` int(11) DEFAULT NULL AFTER `ID_STATEMENT`,
  ADD COLUMN IF NOT EXISTS `ID_CEREMONY_WIKIDATA` varchar(20) DEFAULT NULL AFTER `AWARD_YEAR`,
  ADD KEY IF NOT EXISTS `ID_STATEMENT` (`ID_STATEMENT`),
  ADD KEY IF NOT EXISTS `AWARD_YEAR` (`AWARD_YEAR`);

ALTER TABLE `T_WC_T2S_SERIE_AWARD`
  ADD COLUMN IF NOT EXISTS `ID_STATEMENT` bigint(20) DEFAULT NULL AFTER `ID_AWARD`,
  ADD COLUMN IF NOT EXISTS `AWARD_YEAR` int(11) DEFAULT NULL AFTER `ID_STATEMENT`,
  ADD COLUMN IF NOT EXISTS `ID_CEREMONY_WIKIDATA` varchar(20) DEFAULT NULL AFTER `AWARD_YEAR`,
  ADD KEY IF NOT EXISTS `ID_STATEMENT` (`ID_STATEMENT`),
  ADD KEY IF NOT EXISTS `AWARD_YEAR` (`AWARD_YEAR`);

ALTER TABLE `T_WC_T2S_PERSON_AWARD`
  ADD COLUMN IF NOT EXISTS `ID_STATEMENT` bigint(20) DEFAULT NULL AFTER `ID_AWARD`,
  ADD COLUMN IF NOT EXISTS `AWARD_YEAR` int(11) DEFAULT NULL AFTER `ID_STATEMENT`,
  ADD COLUMN IF NOT EXISTS `ID_CEREMONY_WIKIDATA` varchar(20) DEFAULT NULL AFTER `AWARD_YEAR`,
  ADD COLUMN IF NOT EXISTS `ID_MOVIE_FOR` int(11) DEFAULT NULL AFTER `ID_CEREMONY_WIKIDATA`,
  ADD COLUMN IF NOT EXISTS `ID_SERIE_FOR` int(11) DEFAULT NULL AFTER `ID_MOVIE_FOR`,
  ADD KEY IF NOT EXISTS `ID_STATEMENT` (`ID_STATEMENT`),
  ADD KEY IF NOT EXISTS `AWARD_YEAR` (`AWARD_YEAR`),
  ADD KEY IF NOT EXISTS `ID_MOVIE_FOR` (`ID_MOVIE_FOR`),
  ADD KEY IF NOT EXISTS `ID_SERIE_FOR` (`ID_SERIE_FOR`);

ALTER TABLE `T_WC_T2S_AWARD`
  ADD COLUMN IF NOT EXISTS `ID_AWARD_PARENT` int(11) DEFAULT NULL AFTER `AWARD_TYPE`,
  ADD KEY IF NOT EXISTS `ID_AWARD_PARENT` (`ID_AWARD_PARENT`);

-- The result: twelve columns expected.
SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND ((TABLE_NAME IN ('T_WC_T2S_MOVIE_AWARD', 'T_WC_T2S_SERIE_AWARD', 'T_WC_T2S_PERSON_AWARD')
        AND COLUMN_NAME IN ('ID_STATEMENT', 'AWARD_YEAR', 'ID_CEREMONY_WIKIDATA', 'ID_MOVIE_FOR', 'ID_SERIE_FOR'))
    OR (TABLE_NAME = 'T_WC_T2S_AWARD' AND COLUMN_NAME = 'ID_AWARD_PARENT'))
ORDER BY TABLE_NAME, ORDINAL_POSITION;
