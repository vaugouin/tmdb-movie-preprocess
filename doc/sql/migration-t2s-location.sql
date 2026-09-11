-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-014 : les trois tables des lieux
-- ============================================================================
--
-- NON ENCORE APPLIQUE. Cible : API 1.1.19 (Green).
--
-- POURQUOI CES TABLES. Les lieux sont la SEULE famille d'entites sans read-model.
-- Recompenses, nominations, collections, mouvements, groupes et listes ont tous leur
-- table T_WC_T2S_* recopiee par le preprocessing ; les lieux, non. Toute question de
-- lieu traverse donc les tables Wikidata par necessite, pas par choix, et c'est ce qui
-- explique une mesure du 2026-09-10 : sur les 61 lignes de cache SQL servies sous
-- 1.1.18 qui lisaient la table plate, 55 etaient des lieux et 2 des recompenses.
-- Le desequilibre n'est pas un fait d'usage, il est structurel.
--
-- TROIS TABLES ET NON CINQ, ET LE ROLE EN COLONNE. La note d'origine de -014 listait
-- quatre tables d'association (MOVIE_FILMING, SERIE_FILMING, MOVIE_NARRATIVE,
-- SERIE_NARRATIVE). Ce n'est pas ce que sert l'API : elle fait UNE requete par majeure,
-- WHERE ID_PROPERTY IN ('P840','P915'), et projette la propriete comme colonne pour que
-- l'appelant distingue. Les deux roles voyagent ensemble et se separent a l'affichage ;
-- quatre tables imposeraient une UNION la ou il n'y en a pas.
--
-- LOCATION_ROLE VAUT 'filming' / 'narrative', ET NON 'P915' / 'P840'. Le modele de
-- langage ecrit du SQL contre cette colonne. Lui faire retenir des codes Wikidata est
-- exactement l'erreur que FASTAPI-TEXT2SQL-238 a corrigee.
--
-- PAS DE TABLE POUR LES PERSONNES. Les lieux de naissance sont deja traites ailleurs,
-- PLACE_OF_BIRTH vers COUNTRY_OF_BIRTH par tmdb-person-preprocess.
--
-- ⚠ COLLATION. Lancer avec --force. Voir AGENTS.md.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ============================================================================
-- 1. L'ENTITE
-- ============================================================================
--
-- Le tronc commun de colonnes est celui releve dans
-- Nestor/projets/t2s-backlog/topics/groups-multi-repo-management.md §9.1-bis, calque
-- ici sur T_WC_T2S_MOVEMENT, l'entite la plus proche (derivee de Wikidata, attachee
-- aux films et aux series).
--
-- ⚠ DEUX CLES, ET LES DEUX SONT NECESSAIRES.
--   ID_LOCATION, entier auto-incremente : c'est la cle primaire, parce que le garde
--   deterministe d'entite de reponse de fastapi-text2sql raisonne sur des colonnes
--   ID_<ENTITE> et que toutes les autres routes exposent un entier. La surface lieu
--   etait jusqu'ici l'exception, keyee sur le QID (/locations/Q90), et cette exception
--   se voyait de l'exterieur.
--   ID_WIKIDATA, UNIQUE : c'est la cle metier, celle qui rapproche de Wikidata. UNIQUE
--   et non un simple index, contrairement a MOVEMENT : un lieu est le meme lieu qu'on
--   y tourne ou qu'on y situe l'action, donc une seule ligne par QID. C'est le role,
--   porte par la table d'association, qui distingue les deux usages, pas l'entite.
--   Le contre-exemple est T_WC_T2S_GROUP, dont l'unicite est le couple
--   (ID_WIKIDATA, GROUP_SOURCE) parce que le meme item EST un groupe different selon
--   la propriete qui l'a produit. Ce n'est pas le cas ici.
--
-- LOCATION_TYPE : ville, pays, region, batiment, fiction. Derivable des aujourd'hui
-- par le test de cone P31/P279, deja en base, sans cout de crawl. Il sert a filtrer et
-- a desambiguiser l'embedding ("Paris, ville de France" plutot que "Paris"). Il ne
-- donne PAS le contenant : "en France" ne rendra pas les films tournes a Paris tant que
-- TMDB-MOVIE-PREPROCESS-048 n'est pas fait, faute de P131 en base.
--
-- LOCATION_SOURCE : 'wikidata' pour l'instant, seule source. La colonne existe pour
-- suivre la convention et pour le jour ou une source manuelle apparaitra.
--
-- POSTER_PATH restera vide : un lieu n'a pas d'image TMDb. La colonne est conservee
-- pour que la forme soit celle de toutes les autres entites, ce qui compte plus que
-- l'economie d'une colonne nulle. L'image reelle vient de WIKIPEDIA_MAIN_IMAGE_URL.
-- ============================================================================

CREATE TABLE IF NOT EXISTS `T_WC_T2S_LOCATION` (
  `ID_LOCATION`                 int(11) NOT NULL AUTO_INCREMENT,
  `ID_WIKIDATA`                 varchar(20)  DEFAULT NULL,
  `LOCATION_NAME`               varchar(250) DEFAULT NULL,
  `LOCATION_NAME_FR`            varchar(250) DEFAULT NULL,
  `OVERVIEW`                    mediumtext   DEFAULT NULL,
  `LOCATION_SOURCE`             varchar(20)  DEFAULT NULL,
  `LOCATION_TYPE`               varchar(20)  DEFAULT NULL,
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
  PRIMARY KEY (`ID_LOCATION`),
  UNIQUE KEY `UK_T2S_LOCATION_ID_WIKIDATA` (`ID_WIKIDATA`),
  KEY `LOCATION_NAME` (`LOCATION_NAME`),
  KEY `LOCATION_NAME_FR` (`LOCATION_NAME_FR`),
  KEY `LOCATION_TYPE` (`LOCATION_TYPE`),
  KEY `LOCATION_SOURCE` (`LOCATION_SOURCE`),
  KEY `DELETED` (`DELETED`),
  KEY `DISPLAY_ORDER` (`DISPLAY_ORDER`),
  KEY `MOVIE_COUNT` (`MOVIE_COUNT`),
  KEY `SERIE_COUNT` (`SERIE_COUNT`),
  KEY `IMDB_RATING` (`IMDB_RATING`),
  KEY `IMDB_RATING_WEIGHTED` (`IMDB_RATING_WEIGHTED`),
  KEY `POPULARITY` (`POPULARITY`),
  KEY `TIM_WIKIDATA_COMPLETED` (`TIM_WIKIDATA_COMPLETED`),
  KEY `TIM_UPDATED` (`TIM_UPDATED`),
  KEY `DAT_CREAT` (`DAT_CREAT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- 2. LES DEUX ASSOCIATIONS
-- ============================================================================
--
-- ⚠ LA CLE UNIQUE EST UN AJOUT DELIBERE par rapport a T_WC_T2S_MOVIE_MOVEMENT, qui
-- n'en a pas. Le triplet (film, lieu, role) est le fait lui-meme : l'ecrire deux fois
-- n'ajoute rien et fait compter double. La contrainte rend la reconstruction
-- idempotente au lieu de la faire dependre de la discipline du code appelant, et elle
-- coute un index que l'on voulait de toute facon.
--
-- L'ORDRE DES COLONNES DE L'INDEX SUIT LA REQUETE QUI COMPTE. L'endpoint part d'un lieu
-- et cherche ses films, d'ou (ID_LOCATION, LOCATION_ROLE). Le text2sql part parfois d'un
-- film et cherche ses lieux, d'ou l'index sur ID_MOVIE seul. Les deux sens sont servis.
-- ============================================================================

CREATE TABLE IF NOT EXISTS `T_WC_T2S_MOVIE_LOCATION` (
  `ID_ROW`          int(11) NOT NULL AUTO_INCREMENT,
  `ID_MOVIE`        int(11) NOT NULL,
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
  UNIQUE KEY `UK_T2S_MOVIE_LOCATION` (`ID_MOVIE`, `ID_LOCATION`, `LOCATION_ROLE`),
  KEY `IDX_LOCATION_ROLE` (`ID_LOCATION`, `LOCATION_ROLE`),
  KEY `ID_MOVIE` (`ID_MOVIE`),
  KEY `LOCATION_ROLE` (`LOCATION_ROLE`),
  KEY `DELETED` (`DELETED`),
  KEY `DISPLAY_ORDER` (`DISPLAY_ORDER`),
  KEY `TIM_UPDATED` (`TIM_UPDATED`),
  KEY `DAT_CREAT` (`DAT_CREAT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `T_WC_T2S_SERIE_LOCATION` (
  `ID_ROW`          int(11) NOT NULL AUTO_INCREMENT,
  `ID_SERIE`        int(11) NOT NULL,
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
  UNIQUE KEY `UK_T2S_SERIE_LOCATION` (`ID_SERIE`, `ID_LOCATION`, `LOCATION_ROLE`),
  KEY `IDX_LOCATION_ROLE` (`ID_LOCATION`, `LOCATION_ROLE`),
  KEY `ID_SERIE` (`ID_SERIE`),
  KEY `LOCATION_ROLE` (`LOCATION_ROLE`),
  KEY `DELETED` (`DELETED`),
  KEY `DISPLAY_ORDER` (`DISPLAY_ORDER`),
  KEY `TIM_UPDATED` (`TIM_UPDATED`),
  KEY `DAT_CREAT` (`DAT_CREAT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- 3. VERIFICATION
-- ============================================================================

SELECT '3. Tables creees' AS SECTION;

SELECT TABLE_NAME, TABLE_ROWS, ENGINE, TABLE_COLLATION
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('T_WC_T2S_LOCATION', 'T_WC_T2S_MOVIE_LOCATION', 'T_WC_T2S_SERIE_LOCATION')
ORDER BY TABLE_NAME;

-- ============================================================================
-- 4. CE QUE LE PROCESSUS 72 Y METTRA, pour dimensionner avant de le lancer
-- ============================================================================
--
-- Lecture seule. Donne le nombre de lieux distincts et le nombre d'associations que la
-- reconstruction produira, a partir des statements V2. C'est la meme definition d'un
-- lieu que celle du processus 209 d'embedding-update : un item cite en valeur de P840
-- ou P915. Rien a inventer, elle existe deja en code.
-- ============================================================================

SELECT '4. Volumetrie attendue' AS SECTION;

SELECT CASE st.ID_PROPERTY WHEN 'P915' THEN 'filming' ELSE 'narrative' END AS LOCATION_ROLE,
       COUNT(DISTINCT iv.ID_ITEM)                                          AS LIEUX_DISTINCTS,
       COUNT(DISTINCT m.ID_MOVIE)                                          AS FILMS,
       COUNT(*)                                                            AS ASSOCIATIONS
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
INNER JOIN T_WC_T2S_MOVIE m ON m.ID_WIKIDATA = st.ID_WIKIDATA
WHERE st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
GROUP BY st.ID_PROPERTY
ORDER BY LOCATION_ROLE;

SELECT CASE st.ID_PROPERTY WHEN 'P915' THEN 'filming' ELSE 'narrative' END AS LOCATION_ROLE,
       COUNT(DISTINCT iv.ID_ITEM)                                          AS LIEUX_DISTINCTS,
       COUNT(DISTINCT s.ID_SERIE)                                          AS SERIES,
       COUNT(*)                                                            AS ASSOCIATIONS
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
INNER JOIN T_WC_T2S_SERIE s ON s.ID_WIKIDATA = st.ID_WIKIDATA
WHERE st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
GROUP BY st.ID_PROPERTY
ORDER BY LOCATION_ROLE;

-- Le total de lieux distincts, films et series confondus : c'est le nombre de lignes
-- que T_WC_T2S_LOCATION portera.
SELECT COUNT(DISTINCT iv.ID_ITEM) AS LIEUX_A_CREER
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
WHERE st.ID_PROPERTY IN ('P840', 'P915')
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE m WHERE m.ID_WIKIDATA = st.ID_WIKIDATA
              UNION ALL
              SELECT 1 FROM T_WC_T2S_SERIE s WHERE s.ID_WIKIDATA = st.ID_WIKIDATA);

-- Et combien d'entre eux ont un libelle utilisable, ce qui conditionne l'embedding.
SELECT COUNT(*) AS LIEUX_TOTAL,
       SUM(CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')) IS NOT NULL
                  OR NULLIF(wi.LABEL_EN, '') IS NOT NULL THEN 1 ELSE 0 END) AS AVEC_LIBELLE_EN,
       SUM(CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.fr')) IS NOT NULL
                THEN 1 ELSE 0 END)                                          AS AVEC_LIBELLE_FR
FROM (
  SELECT DISTINCT iv.ID_ITEM
  FROM T_WC_WIKIDATA_STATEMENT st
  INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
  WHERE st.ID_PROPERTY IN ('P840', 'P915')
    AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
) lieux
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = lieux.ID_ITEM;
