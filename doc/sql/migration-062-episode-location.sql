-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-062 : la table des lieux des episodes
-- ============================================================================
--
-- DDL : a lancer par Philippe (voie 2), le compte agent n'a pas le droit de creer.
-- Une seule fois, avant ou apres le deploiement du code 1.1.8 : le processus 72
-- verifie la presence de la table et de la colonne a chaque passage, et les ignore
-- tant qu'elles manquent. L'ordre des deux deploiements est donc libre.
--
-- POURQUOI UNE TABLE ET NON UNE REMONTEE VERS LA SERIE. Mesure du 2026-10-17
-- (test-062-episode-locations-20261017.txt) : 4 494 citations P840 / P915 sur 3 370
-- episodes T2S de 151 series ; 440 lieux que ni un film ni une serie ne citent ; sur
-- 3 378 liens episode-lieu-role, 2 672 (79 %) sont deja portes par la serie. Remonter
-- vers la serie n'apporterait donc presque rien, et perdrait l'information propre a
-- l'episode : l'episode de *The West Wing* qui se passe a Camp David, celui de
-- *Star Trek* tourne a Vasquez Rocks.
--
-- MEME FORME QUE T_WC_T2S_MOVIE_LOCATION ET T_WC_T2S_SERIE_LOCATION : LOCATION_ROLE
-- 'filming' (P915) ou 'narrative' (P840), cle unique sur le triplet, index dans les deux
-- sens. Le processus 72 la reconstruit en entier chaque nuit (BUILD puis RENAME), avec
-- les trois autres tables, dans le meme RENAME.
--
-- EPISODE_COUNT sur T_WC_T2S_LOCATION, a cote de MOVIE_COUNT et SERIE_COUNT.
--
-- ⚠ COLLATION. Lancer avec --force si une comparaison rend ERROR 1267. Voir AGENTS.md.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `T_WC_T2S_EPISODE_LOCATION` (
  `ID_ROW`          int(11) NOT NULL AUTO_INCREMENT,
  `ID_EPISODE`      int(11) NOT NULL,
  `ID_LOCATION`     int(11) NOT NULL,
  `LOCATION_ROLE`   varchar(20) NOT NULL,   -- 'filming' (P915) | 'narrative' (P840)
  `DELETED`         int(5)   DEFAULT NULL,
  `DISPLAY_ORDER`   int(5)   DEFAULT NULL,
  `ID_CREATOR`      int(5)   DEFAULT NULL,
  `DAT_CREAT`       date     DEFAULT NULL,
  `ID_OWNER`        int(5)   DEFAULT NULL,
  `TIM_UPDATED`     datetime DEFAULT NULL,
  `ID_USER_UPDATED` int(5)   DEFAULT NULL,
  PRIMARY KEY (`ID_ROW`),
  UNIQUE KEY `UK_T2S_EPISODE_LOCATION` (`ID_EPISODE`, `ID_LOCATION`, `LOCATION_ROLE`),
  KEY `IDX_LOCATION_ROLE` (`ID_LOCATION`, `LOCATION_ROLE`),
  KEY `ID_EPISODE` (`ID_EPISODE`),
  KEY `LOCATION_ROLE` (`LOCATION_ROLE`),
  KEY `DELETED` (`DELETED`),
  KEY `DISPLAY_ORDER` (`DISPLAY_ORDER`),
  KEY `TIM_UPDATED` (`TIM_UPDATED`),
  KEY `DAT_CREAT` (`DAT_CREAT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE `T_WC_T2S_LOCATION`
  ADD COLUMN IF NOT EXISTS `EPISODE_COUNT` int(11) DEFAULT NULL AFTER `SERIE_COUNT`,
  ADD KEY IF NOT EXISTS `EPISODE_COUNT` (`EPISODE_COUNT`);

-- Verification : la table existe, la colonne aussi.
SELECT '1. Table et colonne' AS SECTION;

SHOW TABLES LIKE 'T_WC_T2S_EPISODE_LOCATION';
SHOW COLUMNS FROM T_WC_T2S_LOCATION LIKE 'EPISODE_COUNT';

-- ----------------------------------------------------------------------------
-- ENSUITE. Le prochain passage du processus 72 (nuit, ou a la demande avec
-- ./tmdb-movie-preprocess-locations.sh puis ./tmdb-movie-preprocess-wikipedia-main-image.sh)
-- remplit la table. Recette : doc/sql/test-062-post-run.sql.
-- ----------------------------------------------------------------------------
