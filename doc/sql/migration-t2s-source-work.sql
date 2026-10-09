-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-055 : the three "based on" tables (Wikidata P144)
-- ============================================================================
--
-- NOT YET APPLIED. To run once on the live base before the first pass of Process 73
-- (scope source-works). Without these tables the process fails on a CREATE TABLE ...
-- LIKE, which is loud and therefore harmless.
--
-- THE SHAPE, DECIDED 2026-10-09 (method A). "Based on" is a relation between a work
-- studied (left) and the work it comes from (right).
--   - LEFT, the work studied, is always a T2S film or series: it is carried by the
--     choice of link table, T_WC_T2S_MOVIE_SOURCE_WORK or T_WC_T2S_SERIE_SOURCE_WORK,
--     the house convention (MOVIE_LOCATION / SERIE_LOCATION, MOVIE_TOPIC / SERIE_TOPIC).
--   - RIGHT, the source, is ALWAYS a row of T_WC_T2S_SOURCE_WORK, whatever its kind:
--     novel, manga, play, video game, tale, or itself a film or a series. When the
--     source is a T2S film or series, its row points to it through ID_MOVIE / ID_SERIE
--     (4,299 of the 25,222 links measured on 2026-10-07, 17 %).
-- One join, always the same; the kind is a filter on SOURCE_WORK_TYPE, never another
-- table. Rejected: three columns in the link table (ID_SOURCE_MOVIE, ID_SOURCE_SERIE,
-- ID_SOURCE_WORK). It saves one hop when a question compares the source's own columns,
-- but every mixed question ("what is Scarface 1983 based on", "the most adapted work")
-- becomes three branches, and the likely Text2SQL failure is then a forgotten branch:
-- a query that runs and returns an incomplete list, invisibly.
--
-- KEYS ARE LOGICAL, NOT DECLARED, like every T2S table: the tables are rebuilt in
-- _BUILD copies and swapped by RENAME, which declared foreign keys would fight.
-- Integrity is checked by the acceptance (doc/sql/test-055-post-run.sql).
--
-- COLLATION. Run with the runner's default (--force behaviour).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ============================================================================
-- 1. THE SOURCE WORK
-- ============================================================================
--
-- The common trunk of columns is the one of T_WC_T2S_LOCATION (migration-t2s-location.sql),
-- itself traced on T_WC_T2S_MOVEMENT.
--
-- TWO KEYS. ID_SOURCE_WORK, auto-increment primary key, kept stable from one rebuild to
-- the next (rule of -014: an id that changes every night is not an id). ID_WIKIDATA,
-- UNIQUE, the business key that joins the raw P144 value.
--
-- SOURCE_WORK_TYPE, closed list, words and never Q-ids (the lesson of
-- FASTAPI-TEXT2SQL-238): literary, comic, stage, game, screen, folklore, other.
-- 'other' holds what is not a work (characters, franchises, persons) and the works
-- outside the list (music, podcasts), each to be solved by its own ticket.
--
-- SOURCE_WORK_FORM, finer, NULL by default: manga, light_novel, play, musical. 'novel'
-- and 'short_story' arrive with P7937 on cached items (WIKIDATA-CRAWLER-021), without
-- a schema change.
--
-- SOURCE_WORK_YEAR: the year of the source, from P577 (publication date, cached since
-- 2026-08-17) or, for a T2S work, its release / first-air year. "The 1897 novel".
--
-- SOURCE_WORK_NAME_FR falls back to the English name when Wikidata has no French
-- label (49 % of the cached sources on 2026-10-07): a NULL French name would hide the
-- source from every French question.
--
-- IMDB_RATING, IMDB_RATING_WEIGHTED, POPULARITY: the source's own values when it is a
-- T2S work, otherwise the average over its adaptations, the convention of collections,
-- movements and locations. POSTER_PATH stays empty, a book has no TMDb image; the real
-- image comes from WIKIPEDIA_MAIN_IMAGE_URL (Process 71).
-- ============================================================================

CREATE TABLE IF NOT EXISTS `T_WC_T2S_SOURCE_WORK` (
  `ID_SOURCE_WORK`              int(11) NOT NULL AUTO_INCREMENT,
  `ID_WIKIDATA`                 varchar(20)  DEFAULT NULL,
  `SOURCE_WORK_NAME`            varchar(250) DEFAULT NULL,
  `SOURCE_WORK_NAME_FR`         varchar(250) DEFAULT NULL,
  `OVERVIEW`                    mediumtext   DEFAULT NULL,
  `SOURCE_WORK_TYPE`            varchar(20)  DEFAULT NULL,
  `SOURCE_WORK_FORM`            varchar(20)  DEFAULT NULL,
  `SOURCE_WORK_YEAR`            int(11)      DEFAULT NULL,
  `ID_MOVIE`                    int(11)      DEFAULT NULL,
  `ID_SERIE`                    int(11)      DEFAULT NULL,
  `SOURCE_WORK_SOURCE`          varchar(20)  DEFAULT NULL,
  `DELETED`                     int(5)       DEFAULT NULL,
  `DISPLAY_ORDER`               int(5)       DEFAULT NULL,
  `ID_CREATOR`                  int(5)       DEFAULT NULL,
  `DAT_CREAT`                   date         DEFAULT NULL,
  `ID_OWNER`                    int(5)       DEFAULT NULL,
  `TIM_UPDATED`                 datetime     DEFAULT NULL,
  `ID_USER_UPDATED`             int(5)       DEFAULT NULL,
  `MOVIE_COUNT`                 int(11)      DEFAULT NULL,
  `SERIE_COUNT`                 int(11)      DEFAULT NULL,
  `POSTER_PATH`                 varchar(200) DEFAULT NULL,
  `WIKIPEDIA_IMAGE_PATH`        varchar(500) DEFAULT NULL,
  `IMDB_RATING`                 double       DEFAULT NULL,
  `IMDB_RATING_WEIGHTED`        double       DEFAULT NULL,
  `POPULARITY`                  double       DEFAULT NULL,
  `TIM_WIKIDATA_COMPLETED`      datetime     DEFAULT NULL,
  `WIKIPEDIA_MAIN_IMAGE_URL`    varchar(1000) DEFAULT NULL,
  `WIKIPEDIA_MAIN_IMAGE_URL_FR` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`ID_SOURCE_WORK`),
  UNIQUE KEY `UK_T2S_SOURCE_WORK_ID_WIKIDATA` (`ID_WIKIDATA`),
  KEY `SOURCE_WORK_NAME` (`SOURCE_WORK_NAME`),
  KEY `SOURCE_WORK_NAME_FR` (`SOURCE_WORK_NAME_FR`),
  KEY `SOURCE_WORK_TYPE` (`SOURCE_WORK_TYPE`, `SOURCE_WORK_FORM`),
  KEY `SOURCE_WORK_YEAR` (`SOURCE_WORK_YEAR`),
  KEY `ID_MOVIE` (`ID_MOVIE`),
  KEY `ID_SERIE` (`ID_SERIE`),
  KEY `DELETED` (`DELETED`),
  KEY `MOVIE_COUNT` (`MOVIE_COUNT`),
  KEY `SERIE_COUNT` (`SERIE_COUNT`),
  KEY `IMDB_RATING` (`IMDB_RATING`),
  KEY `IMDB_RATING_WEIGHTED` (`IMDB_RATING_WEIGHTED`),
  KEY `POPULARITY` (`POPULARITY`),
  KEY `TIM_UPDATED` (`TIM_UPDATED`),
  KEY `DAT_CREAT` (`DAT_CREAT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- 2. THE TWO LINK TABLES
-- ============================================================================
--
-- The pair (work, source) is the fact itself: UNIQUE, so a rebuild is idempotent by
-- construction. Index order follows the queries: from a work to its sources (the
-- unique key leads with the work), from a source to its adaptations (ID_SOURCE_WORK).
-- ============================================================================

CREATE TABLE IF NOT EXISTS `T_WC_T2S_MOVIE_SOURCE_WORK` (
  `ID_ROW`          int(11) NOT NULL AUTO_INCREMENT,
  `ID_MOVIE`        int(11) NOT NULL,
  `ID_SOURCE_WORK`  int(11) NOT NULL,
  `DELETED`         int(5)   DEFAULT NULL,
  `DISPLAY_ORDER`   int(5)   DEFAULT NULL,
  `ID_CREATOR`      int(5)   DEFAULT NULL,
  `DAT_CREAT`       date     DEFAULT NULL,
  `ID_OWNER`        int(5)   DEFAULT NULL,
  `TIM_UPDATED`     datetime DEFAULT NULL,
  `ID_USER_UPDATED` int(5)   DEFAULT NULL,
  PRIMARY KEY (`ID_ROW`),
  UNIQUE KEY `UK_T2S_MOVIE_SOURCE_WORK` (`ID_MOVIE`, `ID_SOURCE_WORK`),
  KEY `ID_SOURCE_WORK` (`ID_SOURCE_WORK`),
  KEY `DELETED` (`DELETED`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `T_WC_T2S_SERIE_SOURCE_WORK` (
  `ID_ROW`          int(11) NOT NULL AUTO_INCREMENT,
  `ID_SERIE`        int(11) NOT NULL,
  `ID_SOURCE_WORK`  int(11) NOT NULL,
  `DELETED`         int(5)   DEFAULT NULL,
  `DISPLAY_ORDER`   int(5)   DEFAULT NULL,
  `ID_CREATOR`      int(5)   DEFAULT NULL,
  `DAT_CREAT`       date     DEFAULT NULL,
  `ID_OWNER`        int(5)   DEFAULT NULL,
  `TIM_UPDATED`     datetime DEFAULT NULL,
  `ID_USER_UPDATED` int(5)   DEFAULT NULL,
  PRIMARY KEY (`ID_ROW`),
  UNIQUE KEY `UK_T2S_SERIE_SOURCE_WORK` (`ID_SERIE`, `ID_SOURCE_WORK`),
  KEY `ID_SOURCE_WORK` (`ID_SOURCE_WORK`),
  KEY `DELETED` (`DELETED`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- The class table (T_WC_T2S_SOURCE_WORK_CLASS) is created by the process itself on
-- each pass, like T_WC_T2S_LOCATION_CLASS: it is a work table, not a served one.

SELECT 'migration-t2s-source-work: three tables present' AS STATUS,
       (SELECT COUNT(*) FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME IN ('T_WC_T2S_SOURCE_WORK', 'T_WC_T2S_MOVIE_SOURCE_WORK', 'T_WC_T2S_SERIE_SOURCE_WORK')) AS TABLES_FOUND;
