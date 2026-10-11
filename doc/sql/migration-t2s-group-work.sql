-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-059 : a group's works (T_WC_T2S_GROUP_MOVIE, T_WC_T2S_GROUP_SERIE)
-- ============================================================================
--
-- NOT YET APPLIED. To run once on the live base before the first pass of Process 74
-- (scope group-works, or main). Without these tables the process fails on its first
-- write, inside its own try: the error lands in strtmdbmoviepreprocessgroupworkerror
-- and the rest of the pipeline continues.
--
-- WHAT THE TABLES HOLD. One row per (group, work) where the work ATTACHES to the
-- group, under the rule decided by Philippe on 2026-10-11:
--   IS_GROUP_CREDITED = 1
--   OR (MEMBER_COUNT >= 2 AND MEMBER_SHARE >= 0.4)
-- Only P463 (member of) and custom groups are considered; P108 (employer) and P54
-- (sports team) groups never get works. The thresholds are constants of the process
-- (INT_GROUP_WORK_MIN_MEMBERS, DBL_GROUP_WORK_MIN_SHARE in tmdb_preprocess_helpers.py).
--
-- THE COLUMNS.
--   MEMBER_COUNT       distinct group members credited on the work (cast or crew; for a
--                      series, the series, season and episode credits rolled up).
--   MEMBER_SHARE       MEMBER_COUNT / T_WC_T2S_GROUP.PERSON_COUNT, 0..1, 4 decimals.
--   IS_GROUP_CREDITED  1 when the group's own Wikidata item is credited on the work
--                      (P161, P175, P58, P86, P170, P162, P57 or P725), else 0.
--   CREDIT_KINDS       'cast', 'crew' or 'cast,crew': the kinds of the members' credits.
--                      NULL when the group is credited but no member is.
--   DISPLAY_ORDER      per group: IS_GROUP_CREDITED desc, MEMBER_COUNT desc, then the
--                      release / first-air date asc (undated works last).
--
-- KEYS ARE LOGICAL, NOT DECLARED, like every T2S table. The pair (ID_GROUP, work) is
-- the fact itself: UNIQUE, so the upsert of the process is idempotent by construction.
-- Rows that no longer attach are deleted by the process, not flagged; DELETED is kept
-- for the house shape and always 0.
--
-- COLLATION. Run with the runner's default (--force behaviour).
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `T_WC_T2S_GROUP_MOVIE` (
  `ID_ROW`            int(11) NOT NULL AUTO_INCREMENT,
  `ID_GROUP`          int(11) NOT NULL,
  `ID_MOVIE`          int(11) NOT NULL,
  `MEMBER_COUNT`      int(11)     DEFAULT NULL,
  `MEMBER_SHARE`      double      DEFAULT NULL,
  `IS_GROUP_CREDITED` int(1)      DEFAULT NULL,
  `CREDIT_KINDS`      varchar(20) DEFAULT NULL,
  `DELETED`           int(5)      DEFAULT NULL,
  `DISPLAY_ORDER`     int(5)      DEFAULT NULL,
  `ID_CREATOR`        int(5)      DEFAULT NULL,
  `DAT_CREAT`         date        DEFAULT NULL,
  `ID_OWNER`          int(5)      DEFAULT NULL,
  `TIM_UPDATED`       datetime    DEFAULT NULL,
  `ID_USER_UPDATED`   int(5)      DEFAULT NULL,
  PRIMARY KEY (`ID_ROW`),
  UNIQUE KEY `UK_T2S_GROUP_MOVIE` (`ID_GROUP`, `ID_MOVIE`),
  KEY `ID_MOVIE` (`ID_MOVIE`),
  KEY `DELETED` (`DELETED`),
  KEY `MEMBER_COUNT` (`MEMBER_COUNT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `T_WC_T2S_GROUP_SERIE` (
  `ID_ROW`            int(11) NOT NULL AUTO_INCREMENT,
  `ID_GROUP`          int(11) NOT NULL,
  `ID_SERIE`          int(11) NOT NULL,
  `MEMBER_COUNT`      int(11)     DEFAULT NULL,
  `MEMBER_SHARE`      double      DEFAULT NULL,
  `IS_GROUP_CREDITED` int(1)      DEFAULT NULL,
  `CREDIT_KINDS`      varchar(20) DEFAULT NULL,
  `DELETED`           int(5)      DEFAULT NULL,
  `DISPLAY_ORDER`     int(5)      DEFAULT NULL,
  `ID_CREATOR`        int(5)      DEFAULT NULL,
  `DAT_CREAT`         date        DEFAULT NULL,
  `ID_OWNER`          int(5)      DEFAULT NULL,
  `TIM_UPDATED`       datetime    DEFAULT NULL,
  `ID_USER_UPDATED`   int(5)      DEFAULT NULL,
  PRIMARY KEY (`ID_ROW`),
  UNIQUE KEY `UK_T2S_GROUP_SERIE` (`ID_GROUP`, `ID_SERIE`),
  KEY `ID_SERIE` (`ID_SERIE`),
  KEY `DELETED` (`DELETED`),
  KEY `MEMBER_COUNT` (`MEMBER_COUNT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SELECT 'migration-t2s-group-work: two tables present' AS STATUS,
       (SELECT COUNT(*) FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME IN ('T_WC_T2S_GROUP_MOVIE', 'T_WC_T2S_GROUP_SERIE')) AS TABLES_FOUND;
