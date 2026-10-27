-- =====================================================================
-- RECONCILIATION-001 : tables de la reconciliation multi-source
-- =====================================================================
-- Trois tables, figees apres les sondes de cadrage (finance, etendue) du
-- 2026-10-27 (doc/sql/probe-reconciliation-*.sql et leurs resultats).
--
--   1. T_WC_T2S_SOURCE_CLAIM      le fait brut par (entite, attribut, source, lieu, date, valeur)
--   2. T_WC_T2S_CLAIM_ATTRIBUTE   la metadonnee par attribut (type de valeur, mode de comparaison)
--   3. T_WC_T2S_VOCAB_MAP         le vocabulaire controle (Q -> code canonique)
--
-- DDL : voie 2. Philippe applique par ~/docker/tools/runsqlvaugouindb.sh
-- (cf. ~/.claude/rules/sql-vps.md). Le compte agent refuse toute DDL.
--
-- Collation utf8mb4_unicode_ci partout : ID_WIKIDATA et VALUE_QID se joignent
-- a T_WC_WIKIDATA_ITEM_VALUE.ID_ITEM, dont c'est la collation. Un ecart
-- provoque "Illegal mix of collations" (vu sur la sonde etendue, vue pays).
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. T_WC_T2S_SOURCE_CLAIM
-- ---------------------------------------------------------------------
-- Un fait = une ligne (forme triplet). Un attribut a entites multiples
-- (deux realisateurs, trois pays) produit PLUSIEURS lignes : la comparaison
-- d'ensembles devient alors une operation sur des lignes.
--
-- La valeur est typee : soit entite (VALUE_QID rempli, le reste des colonnes
-- VALUE_* NULL), soit numerique (VALUE_NUM[_MAX] + VALUE_UNIT, VALUE_QID NULL).
-- VALUE_NUM_MAX porte la borne haute d'un intervalle (montages multiples d'une
-- meme source : Avatar 2, 190..195 min) ; NULL si valeur ponctuelle.
--
-- Identite : (ENTITY_TYPE, ID_WIKIDATA, ATTRIBUTE, SOURCE, PLACE, AS_OF, VALUE_QID).
-- PLACE et AS_OF sont dans la cle : prouve par les ressorties (Titanic 18,3 %,
-- Frozen II 23 % sur le box office mondial) et par le box office US vs mondial.
-- Comme ces colonnes et VALUE_QID sont nullable, MySQL compte chaque NULL comme
-- distinct : une cle UNIQUE ne les contraindrait pas. On materialise donc
-- CLAIM_KEY = hash de la cle (NULL normalise en ''), calcule par le job, UNIQUE.
-- ---------------------------------------------------------------------
CREATE TABLE `T_WC_T2S_SOURCE_CLAIM` (
  `ID_CLAIM`      bigint(20)    NOT NULL AUTO_INCREMENT,
  `CLAIM_KEY`     char(64)      DEFAULT NULL,          -- SHA-256 de la cle d'identite (job)
  `ENTITY_TYPE`   varchar(20)   DEFAULT NULL,          -- movie | serie | person
  `ID_WIKIDATA`   varchar(20)   DEFAULT NULL,          -- l'entite (Q...), colonne d'identite
  `ID_ENTITY`     int(11)       DEFAULT NULL,          -- id interne (ID_MOVIE / ID_SERIE / ID_PERSON) si utile aux jointures
  `ATTRIBUTE`     varchar(40)   DEFAULT NULL,          -- director | country | budget | box_office | runtime ...
  `SOURCE`        varchar(20)   DEFAULT NULL,          -- tmdb | imdb | wikidata | wikipedia | omdb | movieparadise
  `PLACE`         varchar(20)   DEFAULT NULL,          -- Q du lieu pour un fait territorialise (box office US) ; NULL = non territorialise
  `AS_OF`         date          DEFAULT NULL,          -- date de reference d'un fait cumulatif (box office a telle ressortie) ; NULL = non date
  `VALUE_QID`     varchar(20)   DEFAULT NULL,          -- valeur-entite (un Q) ; NULL pour un litteral
  `VALUE_NUM`     decimal(20,4) DEFAULT NULL,          -- litteral numerique (borne basse si intervalle) ; NULL pour une entite
  `VALUE_NUM_MAX` decimal(20,4) DEFAULT NULL,          -- borne haute d'un intervalle ; NULL si valeur ponctuelle
  `VALUE_UNIT`    varchar(20)   DEFAULT NULL,          -- code d'unite canonique (min, USD ...) ; NULL pour une entite
  `VALUE_RAW`     varchar(500)  DEFAULT NULL,          -- la valeur telle que la source la donne (audit)
  `CONFIDENCE`    varchar(10)   DEFAULT NULL,          -- high (structure) | medium | low (litteral parse)
  `COLLECTED_AT`  datetime      DEFAULT NULL,          -- horodatage de fraicheur propre a la source
  `DELETED`       int(5)        DEFAULT NULL,
  `DAT_CREAT`     datetime      DEFAULT current_timestamp(),
  `TIM_UPDATED`   datetime      DEFAULT NULL,
  PRIMARY KEY (`ID_CLAIM`),
  UNIQUE KEY `CLAIM_KEY` (`CLAIM_KEY`),
  KEY `ENTITY` (`ENTITY_TYPE`,`ID_WIKIDATA`),
  KEY `ID_WIKIDATA` (`ID_WIKIDATA`),
  KEY `ATTRIBUTE` (`ATTRIBUTE`),
  KEY `SOURCE` (`SOURCE`),
  KEY `VALUE_QID` (`VALUE_QID`),
  KEY `RECON_SCAN` (`ENTITY_TYPE`,`ID_WIKIDATA`,`ATTRIBUTE`),
  KEY `COLLECTED_AT` (`COLLECTED_AT`),
  KEY `DELETED` (`DELETED`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------
-- 2. T_WC_T2S_CLAIM_ATTRIBUTE
-- ---------------------------------------------------------------------
-- La table des faits est uniforme, mais l'operateur de comparaison depend de
-- l'attribut : bande de tolerance pour un numerique, relation d'ensemble pour
-- une entite. Cette metadonnee porte la regle, le code de reconciliation la lit.
-- ---------------------------------------------------------------------
CREATE TABLE `T_WC_T2S_CLAIM_ATTRIBUTE` (
  `ATTRIBUTE`       varchar(40)  NOT NULL,             -- director | country | budget | box_office | runtime ...
  `VALUE_KIND`      varchar(10)  DEFAULT NULL,         -- entity | numeric
  `COMPARE_MODE`    varchar(20)  DEFAULT NULL,         -- set (entite) | tolerance | interval (numerique)
  `DEFAULT_UNIT`    varchar(20)  DEFAULT NULL,         -- min, USD ... ; NULL pour une entite
  `TOLERANCE_PCT`   decimal(6,3) DEFAULT NULL,         -- seuil d'ecart tolere pour COMPARE_MODE=tolerance
  `IS_SCOPED_PLACE` int(5)       DEFAULT NULL,         -- 1 si PLACE fait partie de l'identite (box_office)
  `IS_SCOPED_TIME`  int(5)       DEFAULT NULL,         -- 1 si AS_OF fait partie de l'identite (box_office cumulatif)
  `LABEL`           varchar(100) DEFAULT NULL,
  `LABEL_FR`        varchar(100) DEFAULT NULL,
  `DELETED`         int(5)       DEFAULT NULL,
  `DAT_CREAT`       datetime     DEFAULT current_timestamp(),
  PRIMARY KEY (`ATTRIBUTE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO `T_WC_T2S_CLAIM_ATTRIBUTE`
  (`ATTRIBUTE`,`VALUE_KIND`,`COMPARE_MODE`,`DEFAULT_UNIT`,`TOLERANCE_PCT`,`IS_SCOPED_PLACE`,`IS_SCOPED_TIME`,`LABEL`,`LABEL_FR`) VALUES
  ('director',   'entity',  'set',       NULL,  NULL, 0, 0, 'Director',            'Realisateur'),
  ('country',    'entity',  'set',       NULL,  NULL, 0, 0, 'Country of origin',   'Pays d''origine'),
  ('budget',     'numeric', 'tolerance', 'USD', 5.000, 0, 0, 'Budget',             'Budget'),
  ('box_office', 'numeric', 'tolerance', 'USD', 5.000, 1, 1, 'Box office',         'Box-office'),
  ('runtime',    'numeric', 'interval',  'min', NULL, 0, 0, 'Runtime',             'Duree');


-- ---------------------------------------------------------------------
-- 3. T_WC_T2S_VOCAB_MAP
-- ---------------------------------------------------------------------
-- Vocabulaire controle, obligatoire (prouve par le pays : Wikidata P495 donne
-- Q21 Angleterre / Q174193 Royaume-Uni la ou TMDb dit GB ; par la devise :
-- plus de codes que les 8 vus ; par l'unite de duree : seconde -> minute).
-- FACTOR_TO_CANONICAL convertit une valeur numerique vers l'unite canonique
-- (seconde -> min : 0.016667 ; heure -> min : 60) ; NULL pour un code pur.
-- ---------------------------------------------------------------------
CREATE TABLE `T_WC_T2S_VOCAB_MAP` (
  `ID_MAP`               int(11)       NOT NULL AUTO_INCREMENT,
  `DIMENSION`            varchar(20)   DEFAULT NULL,   -- country | currency | duration_unit
  `ID_WIKIDATA`          varchar(20)   DEFAULT NULL,   -- le Q source (pays, devise, unite)
  `CANONICAL_CODE`       varchar(20)   DEFAULT NULL,   -- ISO2 (GB), ISO4217 (USD), code d'unite (min)
  `FACTOR_TO_CANONICAL`  decimal(20,8) DEFAULT NULL,   -- facteur multiplicatif vers l'unite canonique ; NULL pour un code
  `LABEL`                varchar(100)  DEFAULT NULL,
  `DELETED`              int(5)        DEFAULT NULL,
  `DAT_CREAT`            datetime      DEFAULT current_timestamp(),
  PRIMARY KEY (`ID_MAP`),
  UNIQUE KEY `DIM_WIKIDATA` (`DIMENSION`,`ID_WIKIDATA`),
  KEY `DIMENSION` (`DIMENSION`),
  KEY `CANONICAL_CODE` (`CANONICAL_CODE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Amorce des unites de duree (distribution vue sur la sonde : Q7727 97,7 %).
INSERT INTO `T_WC_T2S_VOCAB_MAP`
  (`DIMENSION`,`ID_WIKIDATA`,`CANONICAL_CODE`,`FACTOR_TO_CANONICAL`,`LABEL`) VALUES
  ('duration_unit','Q7727',  'min', 1.00000000,  'minute'),
  ('duration_unit','Q11574', 'min', 0.01666667,  'seconde'),
  ('duration_unit','Q25235', 'min', 60.00000000, 'heure');

-- Les dimensions country et currency sont peuplees par un passage dedie
-- (RECONCILIATION-001, lot d'amorce), incluant les entites sous-nationales et
-- historiques (Q21 Angleterre -> GB, Q174193 Royaume-Uni 1801 -> GB, ...).
