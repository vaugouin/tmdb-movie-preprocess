-- ============================================================================
-- TMDB-MOVIE-PREPROCESS-055 : second measurement, types, keywords, characters
-- ============================================================================
--
-- READ ONLY. Follows test-055-based-on-drivers.sql (result 2026-10-07). Decisions taken
-- on that result (Philippe, 2026-10-07):
--   - SOURCE_WORK_TYPE, broad: literary, comic, stage, game, screen, folklore, other.
--     What is not a work (character, franchise, person) goes to 'other', each case to
--     be solved by its own ticket later; the source row keeps its Q-id for that.
--   - SOURCE_WORK_FORM, finer, may be empty: manga, light_novel, play, and later
--     novel / short_story once P7937 is cached (WIKIDATA-CRAWLER-021).
--   - Table T_WC_T2S_SOURCE_WORK, a dimension with English and French names.
--
-- FOUR PARTS.
--   A. Where the sources live, now with persons and characters, and their labels.
--   B. The P279 cones behind each type: size, conflicts between types, the decided
--      type with a priority order, and the cached sources no root catches.
--      The roots are the P31 classes MEASURED in step 1 (plus three named in the
--      comments), not guessed: the -014 lesson.
--   C. Does a TMDb "based on ..." keyword agree with the Wikidata type, on the works
--      that carry both? Decides whether the keyword is worth a fallback.
--   D. Characters linked to T2S works by Wikidata (P674 on the work, P453 qualifier on
--      a cast credit, P144 to a character): how many, for TMDB-MOVIE-PREPROCESS-012.
--
-- RUN 2 (after the result of 2026-10-07). Form 'musical' added for the dramatico-musical
-- cone, ranked above 'play' (run 1 gave 'play' to Swan Lake and The Magic Flute).
-- Roots added from part B4 of run 1: stage (drama, theatrical production, theatrical
-- work), screen (episode, season), game (tabletop RPG, board game, card game, Pokemon
-- classes), folklore (mythology); real events and products named in 'other'.
-- Re-running the same day needs the runner's -f.
--
-- PERFORMANCE. Every join goes through an indexed temporary table or an indexed
-- column. P453 is read from IDX_..._STATEMENT_QUALIFIER_PROPERTY, P674 and P144 from
-- IDX_..._STATEMENT_ID_PROPERTY. The cones are built once by one recursive CTE.
-- ============================================================================

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------------
-- Working tables (same base as the first script)
-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS TMP_055_T2S_WORK;
CREATE TEMPORARY TABLE TMP_055_T2S_WORK (
  ID_WIKIDATA  VARCHAR(50) NOT NULL,
  KIND         VARCHAR(10) NOT NULL,
  ID_WORK      INT NOT NULL,
  TITLE        VARCHAR(250) NULL,
  PRIMARY KEY (ID_WIKIDATA, KIND),
  KEY IDX_WORK (KIND, ID_WORK)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO TMP_055_T2S_WORK (ID_WIKIDATA, KIND, ID_WORK, TITLE)
SELECT m.ID_WIKIDATA, 'movie', m.ID_MOVIE, m.MOVIE_TITLE
FROM T_WC_T2S_MOVIE m
WHERE m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '';

INSERT IGNORE INTO TMP_055_T2S_WORK (ID_WIKIDATA, KIND, ID_WORK, TITLE)
SELECT s.ID_WIKIDATA, 'serie', s.ID_SERIE, s.SERIE_TITLE
FROM T_WC_T2S_SERIE s
WHERE s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '';

DROP TEMPORARY TABLE IF EXISTS TMP_055_LINK;
CREATE TEMPORARY TABLE TMP_055_LINK (
  KIND         VARCHAR(10) NOT NULL,
  ID_WORK      INT NOT NULL,
  ID_TARGET    VARCHAR(50) NOT NULL,
  PRIMARY KEY (KIND, ID_WORK, ID_TARGET),
  KEY IDX_TARGET (ID_TARGET)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO TMP_055_LINK (KIND, ID_WORK, ID_TARGET)
SELECT w.KIND, w.ID_WORK, iv.ID_ITEM
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
INNER JOIN TMP_055_T2S_WORK w ON w.ID_WIKIDATA = st.ID_WIKIDATA
WHERE st.ID_PROPERTY = 'P144'
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND COALESCE(st.DELETED, 0) = 0;

-- One row per distinct source, with its home, labels and final type.
-- HOME order: a T2S work first, then core entities, then the item cache.
DROP TEMPORARY TABLE IF EXISTS TMP_055_TARGET;
CREATE TEMPORARY TABLE TMP_055_TARGET (
  ID_TARGET   VARCHAR(50) NOT NULL,
  HOME        VARCHAR(30) NULL,
  LINKS       INT NOT NULL,
  MOVIES      INT NOT NULL,
  SERIES      INT NOT NULL,
  LABEL_EN    VARCHAR(500) NULL,
  LABEL_FR    VARCHAR(500) NULL,
  FINAL_TYPE  VARCHAR(20) NULL,
  FINAL_FORM  VARCHAR(20) NULL,
  PRIMARY KEY (ID_TARGET),
  KEY IDX_HOME (HOME)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_055_TARGET (ID_TARGET, LINKS, MOVIES, SERIES)
SELECT ID_TARGET, COUNT(*), SUM(KIND = 'movie'), SUM(KIND = 'serie')
FROM TMP_055_LINK
GROUP BY ID_TARGET;

UPDATE TMP_055_TARGET t
SET t.HOME = CASE
    WHEN EXISTS (SELECT 1 FROM TMP_055_T2S_WORK w WHERE w.ID_WIKIDATA = t.ID_TARGET AND w.KIND = 'movie') THEN '1 t2s_movie'
    WHEN EXISTS (SELECT 1 FROM TMP_055_T2S_WORK w WHERE w.ID_WIKIDATA = t.ID_TARGET AND w.KIND = 'serie') THEN '2 t2s_serie'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE wm WHERE wm.ID_WIKIDATA = t.ID_TARGET) THEN '3 core_movie_outside_t2s'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE ws WHERE ws.ID_WIKIDATA = t.ID_TARGET) THEN '4 core_serie_outside_t2s'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON wp WHERE wp.ID_WIKIDATA = t.ID_TARGET) THEN '5 core_person'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER wc WHERE wc.ID_WIKIDATA = t.ID_TARGET) THEN '6 core_character'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM wi WHERE wi.ID_WIKIDATA = t.ID_TARGET) THEN '7 cached_item'
    ELSE '8 missing_everywhere' END;

UPDATE TMP_055_TARGET t
LEFT JOIN T_WC_WIKIDATA_ITEM wi      ON wi.ID_WIKIDATA = t.ID_TARGET
LEFT JOIN T_WC_WIKIDATA_MOVIE wm     ON wm.ID_WIKIDATA = t.ID_TARGET
LEFT JOIN T_WC_WIKIDATA_SERIE ws     ON ws.ID_WIKIDATA = t.ID_TARGET
LEFT JOIN T_WC_WIKIDATA_PERSON wp    ON wp.ID_WIKIDATA = t.ID_TARGET
LEFT JOIN T_WC_WIKIDATA_CHARACTER wc ON wc.ID_WIKIDATA = t.ID_TARGET
SET t.LABEL_EN = COALESCE(
      JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')), NULLIF(wi.LABEL_EN, ''),
      JSON_UNQUOTE(JSON_EXTRACT(wm.LABELS_JSON, '$.en')), NULLIF(wm.LABEL_EN, ''),
      JSON_UNQUOTE(JSON_EXTRACT(ws.LABELS_JSON, '$.en')), NULLIF(ws.LABEL_EN, ''),
      JSON_UNQUOTE(JSON_EXTRACT(wp.LABELS_JSON, '$.en')), NULLIF(wp.LABEL_EN, ''),
      JSON_UNQUOTE(JSON_EXTRACT(wc.LABELS_JSON, '$.en')), NULLIF(wc.LABEL_EN, '')),
    t.LABEL_FR = COALESCE(
      JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.fr')),
      JSON_UNQUOTE(JSON_EXTRACT(wm.LABELS_JSON, '$.fr')),
      JSON_UNQUOTE(JSON_EXTRACT(ws.LABELS_JSON, '$.fr')),
      JSON_UNQUOTE(JSON_EXTRACT(wp.LABELS_JSON, '$.fr')),
      JSON_UNQUOTE(JSON_EXTRACT(wc.LABELS_JSON, '$.fr')));

-- ===========================================================================
-- A. WHERE THE SOURCES LIVE, AND THEIR LABELS
-- ===========================================================================
SELECT 'A1. Homes, persons and characters included' AS SECTION;

SELECT HOME, COUNT(*) AS SOURCES, SUM(LINKS) AS LINKS, SUM(MOVIES) AS LINKS_FROM_MOVIES, SUM(SERIES) AS LINKS_FROM_SERIES
FROM TMP_055_TARGET
GROUP BY HOME
ORDER BY HOME;

-- The persons named as a source: Philippe already answers "adaptations of Tolkien"
-- through crew credits, these are only counted.
SELECT 'A2. Persons named as a source, the 20 most linked' AS SECTION;

SELECT ID_TARGET, LABEL_EN, LINKS
FROM TMP_055_TARGET
WHERE HOME = '5 core_person'
ORDER BY LINKS DESC
LIMIT 20;

SELECT 'A3. Labels by home' AS SECTION;

SELECT HOME, COUNT(*) AS SOURCES,
       SUM(LABEL_EN IS NULL)                        AS NO_EN,
       SUM(LABEL_FR IS NULL)                        AS NO_FR,
       SUM(LABEL_EN IS NULL AND LABEL_FR IS NULL)   AS NO_EN_NO_FR,
       SUM(CASE WHEN LABEL_EN IS NULL THEN LINKS ELSE 0 END) AS LINKS_NO_EN
FROM TMP_055_TARGET
GROUP BY HOME
ORDER BY HOME;

-- The unlabelled sources the most linked (The Last of Us, Q1986744, was one).
SELECT ID_TARGET, HOME, LINKS, LABEL_FR,
       (SELECT GROUP_CONCAT(DISTINCT w.TITLE ORDER BY w.TITLE SEPARATOR ' | ')
        FROM TMP_055_LINK l INNER JOIN TMP_055_T2S_WORK w ON w.KIND = l.KIND AND w.ID_WORK = l.ID_WORK
        WHERE l.ID_TARGET = t.ID_TARGET) AS ADAPTATIONS
FROM TMP_055_TARGET t
WHERE LABEL_EN IS NULL
ORDER BY LINKS DESC
LIMIT 20;

-- ===========================================================================
-- B. THE CONES BEHIND EACH TYPE
--
-- PRIORITY decides when a source falls in several cones, the smallest wins:
--   0  not a work (character, franchise): 'other', by decision of 2026-10-07
--   1  game, 2 comic, 3 stage, 4 folklore, 5 screen
--   6  literary, LAST because "written work" and "literary work" are the broadest
--      cones and would otherwise swallow manga, comics and plays
--   7  works outside the type list (music, album, podcast): 'other'
-- Every root below is a P31 class counted in step 1 section 4, except Q95074
-- (fictional character, CHARACTER_ROOTS of wikidata-crawler), Q11424 (film) and
-- Q5398426 (television series). Q17537576 "creative work" is deliberately NOT a
-- root: it is the top of every cone and would decide nothing.
-- ===========================================================================

DROP TEMPORARY TABLE IF EXISTS TMP_055_ROOT;
CREATE TEMPORARY TABLE TMP_055_ROOT (
  ID_ROOT   VARCHAR(50) NOT NULL,
  TYPE      VARCHAR(20) NOT NULL,
  FORM      VARCHAR(20) NULL,
  FAMILY    VARCHAR(20) NOT NULL,
  PRIORITY  TINYINT NOT NULL,
  PRIMARY KEY (ID_ROOT)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_055_ROOT (ID_ROOT, TYPE, FORM, FAMILY, PRIORITY) VALUES
  -- 0, not a work
  ('Q95074',     'other',    NULL,          'character', 0),
  ('Q15632617',  'other',    NULL,          'character', 0),
  ('Q15773347',  'other',    NULL,          'character', 0),
  ('Q1114461',   'other',    NULL,          'character', 0),
  ('Q15773317',  'other',    NULL,          'character', 0),
  ('Q15711870',  'other',    NULL,          'character', 0),
  ('Q3658341',   'other',    NULL,          'character', 0),
  ('Q14514600',  'other',    NULL,          'character', 0),
  ('Q196600',    'other',    NULL,          'franchise', 0),
  -- 1, game
  ('Q7889',      'game',     NULL,          'game',      1),
  ('Q7058673',   'game',     NULL,          'game',      1),
  ('Q112144412', 'game',     NULL,          'game',      1),
  ('Q1643932',   'game',     NULL,          'game',      1),   -- tabletop role-playing game (added after run 1)
  ('Q131436',    'game',     NULL,          'game',      1),   -- board game (idem)
  ('Q734698',    'game',     NULL,          'game',      1),   -- collectible card game (idem)
  ('Q116774927', 'game',     NULL,          'game',      1),   -- unlabelled, the Pokemon game pairs (idem)
  ('Q116774997', 'game',     NULL,          'game',      1),   -- unlabelled, Pokemon remakes (idem)
  -- 2, comic
  ('Q21198342',  'comic',    'manga',       'comic',     2),
  ('Q865484',    'comic',    'manga',       'comic',     2),
  ('Q14406742',  'comic',    NULL,          'comic',     2),
  ('Q838795',    'comic',    NULL,          'comic',     2),
  ('Q1004',      'comic',    NULL,          'comic',     2),
  ('Q1760610',   'comic',    NULL,          'comic',     2),
  ('Q2831984',   'comic',    NULL,          'comic',     2),
  ('Q725377',    'comic',    NULL,          'comic',     2),
  ('Q3297186',   'comic',    NULL,          'comic',     2),
  ('Q115378877', 'comic',    NULL,          'comic',     2),
  ('Q7978994',   'comic',    NULL,          'comic',     2),
  ('Q213369',    'comic',    NULL,          'comic',     2),
  ('Q74262765',  'comic',    NULL,          'comic',     2),
  ('Q137637896', 'comic',    NULL,          'comic',     2),
  -- 3, stage. Form 'musical' covers every stage work with music (musical, opera,
  -- operetta, ballet) and wins over 'play' (fixed 2026-10-07: the dramatico-musical
  -- cone sits inside the dramatic-work cone, and Swan Lake came out as a 'play').
  ('Q116476516', 'stage',    'play',        'stage',     3),
  ('Q58483083',  'stage',    'musical',     'stage',     3),
  ('Q25372',     'stage',    'play',        'stage',     3),   -- drama (added after run 1)
  ('Q7777570',   'stage',    NULL,          'stage',     3),   -- theatrical production (idem)
  ('Q110013395', 'stage',    NULL,          'stage',     3),   -- theatrical work (idem)
  -- 4, folklore
  ('Q699',       'folklore', NULL,          'folklore',  4),
  ('Q1221280',   'folklore', NULL,          'folklore',  4),
  ('Q47451145',  'folklore', NULL,          'folklore',  4),
  ('Q4400636',   'folklore', NULL,          'folklore',  4),
  ('Q19718870',  'folklore', NULL,          'folklore',  4),   -- mythology by ethnic group (added after run 1)
  -- 5, screen (cached films and series; core ones are typed by their home)
  ('Q24856',     'screen',   NULL,          'screen',    5),
  ('Q11424',     'screen',   NULL,          'screen',    5),
  ('Q5398426',   'screen',   NULL,          'screen',    5),
  ('Q21191270',  'screen',   NULL,          'screen',    5),   -- television series episode (added after run 1)
  ('Q3464665',   'screen',   NULL,          'screen',    5),   -- television series season (idem)
  -- 6, literary
  ('Q104213567', 'literary', 'light_novel', 'literary',  6),
  ('Q7725634',   'literary', NULL,          'literary',  6),
  ('Q47461344',  'literary', NULL,          'literary',  6),
  ('Q571',       'literary', NULL,          'literary',  6),
  ('Q277759',    'literary', NULL,          'literary',  6),
  ('Q1667921',   'literary', NULL,          'literary',  6),
  ('Q13593966',  'literary', NULL,          'literary',  6),
  ('Q867335',    'literary', NULL,          'literary',  6),
  ('Q12799318',  'literary', NULL,          'literary',  6),
  ('Q59126',     'literary', NULL,          'literary',  6),
  ('Q8275050',   'literary', NULL,          'literary',  6),
  ('Q47068459',  'literary', NULL,          'literary',  6),
  ('Q3331189',   'literary', NULL,          'literary',  6),
  ('Q29154430',  'literary', NULL,          'literary',  6),
  -- 7, works outside the list
  ('Q105543609', 'other',    NULL,          'music',     7),
  ('Q482994',    'other',    NULL,          'music',     7),
  ('Q24634210',  'other',    NULL,          'podcast',   7),
  -- 7, real events and products, measured as unclassified in run 1: named only so
  -- the breakdown of 'other' says what they are.
  ('Q132821',    'other',    NULL,          'event',     7),   -- murder
  ('Q16738832',  'other',    NULL,          'event',     7),   -- criminal case
  ('Q2334719',   'other',    NULL,          'event',     7),   -- legal case
  ('Q178561',    'other',    NULL,          'event',     7),   -- battle
  ('Q25906438',  'other',    NULL,          'event',     7),   -- attempted coup d'etat
  ('Q1190554',   'other',    NULL,          'event',     7),   -- occurrence
  ('Q431289',    'other',    NULL,          'product',   7),   -- brand
  ('Q11422',     'other',    NULL,          'product',   7),   -- toy
  ('Q57663626',  'other',    NULL,          'product',   7);   -- toyline

-- The cones, every root at once. The CAST of the anchor is not decorative: without
-- it MariaDB types the recursive column on the literal's length (error 1406).
DROP TEMPORARY TABLE IF EXISTS TMP_055_CONE;
CREATE TEMPORARY TABLE TMP_055_CONE (
  ID_CLASS  VARCHAR(50) NOT NULL,
  ID_ROOT   VARCHAR(50) NOT NULL,
  PRIMARY KEY (ID_ROOT, ID_CLASS),
  KEY IDX_CLASS (ID_CLASS)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_055_CONE (ID_CLASS, ID_ROOT)
WITH RECURSIVE c (qid, root) AS (
  SELECT CAST(r.ID_ROOT AS CHAR(50)) COLLATE utf8mb4_unicode_ci,
         CAST(r.ID_ROOT AS CHAR(50)) COLLATE utf8mb4_unicode_ci
  FROM TMP_055_ROOT r
  UNION
  SELECT sc.ID_CHILD, c.root
  FROM T_WC_WIKIDATA_SUBCLASS sc JOIN c ON c.qid = sc.ID_PARENT
  WHERE sc.DELETED = 0
) SELECT qid, root FROM c;

-- The P31 classes of the cached sources.
DROP TEMPORARY TABLE IF EXISTS TMP_055_SRC_CLASS;
CREATE TEMPORARY TABLE TMP_055_SRC_CLASS (
  ID_TARGET VARCHAR(50) NOT NULL,
  ID_CLASS  VARCHAR(50) NOT NULL,
  PRIMARY KEY (ID_TARGET, ID_CLASS),
  KEY IDX_CLASS (ID_CLASS)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO TMP_055_SRC_CLASS (ID_TARGET, ID_CLASS)
SELECT t.ID_TARGET, iv.ID_ITEM
FROM TMP_055_TARGET t
INNER JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_WIKIDATA = t.ID_TARGET
       AND st.ID_PROPERTY = 'P31'
       AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
WHERE t.HOME = '7 cached_item';

-- Every root that catches a cached source.
DROP TEMPORARY TABLE IF EXISTS TMP_055_MATCH;
CREATE TEMPORARY TABLE TMP_055_MATCH (
  ID_TARGET VARCHAR(50) NOT NULL,
  ID_ROOT   VARCHAR(50) NOT NULL,
  TYPE      VARCHAR(20) NOT NULL,
  FORM      VARCHAR(20) NULL,
  FAMILY    VARCHAR(20) NOT NULL,
  PRIORITY  TINYINT NOT NULL,
  PRIMARY KEY (ID_TARGET, ID_ROOT),
  KEY IDX_ROOT (ID_ROOT)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO TMP_055_MATCH (ID_TARGET, ID_ROOT, TYPE, FORM, FAMILY, PRIORITY)
SELECT sc.ID_TARGET, r.ID_ROOT, r.TYPE, r.FORM, r.FAMILY, r.PRIORITY
FROM TMP_055_SRC_CLASS sc
INNER JOIN TMP_055_CONE c ON c.ID_CLASS = sc.ID_CLASS
INNER JOIN TMP_055_ROOT r ON r.ID_ROOT = c.ID_ROOT;

-- The decided type per cached source: the smallest priority wins.
DROP TEMPORARY TABLE IF EXISTS TMP_055_DECIDED;
CREATE TEMPORARY TABLE TMP_055_DECIDED (
  ID_TARGET     VARCHAR(50) NOT NULL,
  PRIO          TINYINT NOT NULL,
  TYPE          VARCHAR(20) NULL,
  FORM          VARCHAR(20) NULL,
  FAMILY        VARCHAR(20) NULL,
  TYPES_MATCHED VARCHAR(200) NULL,
  PRIMARY KEY (ID_TARGET)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_055_DECIDED (ID_TARGET, PRIO, TYPES_MATCHED)
SELECT ID_TARGET, MIN(PRIORITY),
       GROUP_CONCAT(DISTINCT CONCAT(PRIORITY, ':', FAMILY) ORDER BY PRIORITY, FAMILY SEPARATOR ' + ')
FROM TMP_055_MATCH
GROUP BY ID_TARGET;

UPDATE TMP_055_DECIDED d
SET d.TYPE   = (SELECT MIN(m.TYPE)   FROM TMP_055_MATCH m WHERE m.ID_TARGET = d.ID_TARGET AND m.PRIORITY = d.PRIO),
    -- Form ranking inside the winning type: 'musical' beats 'play' (an opera is also a
    -- dramatic work), 'manga' and 'light_novel' have no rival in their type.
    d.FORM   = (SELECT CASE WHEN SUM(m.FORM = 'musical') > 0 THEN 'musical' ELSE MAX(m.FORM) END
                FROM TMP_055_MATCH m WHERE m.ID_TARGET = d.ID_TARGET AND m.PRIORITY = d.PRIO),
    d.FAMILY = (SELECT MIN(m.FAMILY) FROM TMP_055_MATCH m WHERE m.ID_TARGET = d.ID_TARGET AND m.PRIORITY = d.PRIO);

-- Final type of every source: by home for core entities, by cone for cached items.
UPDATE TMP_055_TARGET t
LEFT JOIN TMP_055_DECIDED d ON d.ID_TARGET = t.ID_TARGET
SET t.FINAL_TYPE = CASE
      WHEN t.HOME IN ('1 t2s_movie', '2 t2s_serie', '3 core_movie_outside_t2s', '4 core_serie_outside_t2s') THEN 'screen'
      WHEN t.HOME IN ('5 core_person', '6 core_character') THEN 'other'
      WHEN t.HOME = '7 cached_item' THEN COALESCE(d.TYPE, '(unclassified)')
      ELSE '(missing)' END,
    t.FINAL_FORM = CASE WHEN t.HOME = '7 cached_item' THEN d.FORM END;

SELECT 'B1. Each root: cone size and sources caught' AS SECTION;

SELECT r.PRIORITY, r.TYPE, r.FORM, r.FAMILY, r.ID_ROOT,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')), NULLIF(wi.LABEL_EN, '')) AS ROOT_LABEL,
       (SELECT COUNT(*) FROM TMP_055_CONE c WHERE c.ID_ROOT = r.ID_ROOT)                     AS CONE_CLASSES,
       (SELECT COUNT(*) FROM TMP_055_MATCH m WHERE m.ID_ROOT = r.ID_ROOT)                    AS SOURCES_CAUGHT
FROM TMP_055_ROOT r
LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = r.ID_ROOT
ORDER BY r.PRIORITY, SOURCES_CAUGHT DESC;

-- Sources caught by more than one family. The priority order resolves them; this
-- list says whether the order is right (a manga caught by 'literary' too is
-- expected, a novel caught by 'game' would not be).
SELECT 'B2. Conflicts between families, the 25 most frequent' AS SECTION;

SELECT d.TYPES_MATCHED, d.TYPE AS DECIDED, COUNT(*) AS SOURCES,
       SUBSTRING(GROUP_CONCAT(COALESCE(t.LABEL_EN, t.ID_TARGET) ORDER BY t.LINKS DESC SEPARATOR ' | '), 1, 200) AS EXAMPLES
FROM TMP_055_DECIDED d
INNER JOIN TMP_055_TARGET t ON t.ID_TARGET = d.ID_TARGET
WHERE d.TYPES_MATCHED LIKE '% + %'
GROUP BY d.TYPES_MATCHED, d.TYPE
ORDER BY SOURCES DESC
LIMIT 25;

SELECT 'B3. Final distribution of SOURCE_WORK_TYPE and SOURCE_WORK_FORM' AS SECTION;

SELECT FINAL_TYPE, COALESCE(FINAL_FORM, '') AS FINAL_FORM,
       COUNT(*) AS SOURCES, SUM(LINKS) AS LINKS, SUM(MOVIES) AS LINKS_FROM_MOVIES, SUM(SERIES) AS LINKS_FROM_SERIES
FROM TMP_055_TARGET
GROUP BY FINAL_TYPE, FINAL_FORM
ORDER BY SOURCES DESC;

-- 'other' broken down by family, for the tickets that will take each family over.
SELECT CASE WHEN t.HOME = '5 core_person' THEN 'person'
            WHEN t.HOME = '6 core_character' THEN 'character (core)'
            ELSE CONCAT(d.FAMILY, ' (cached)') END AS OTHER_FAMILY,
       COUNT(*) AS SOURCES, SUM(t.LINKS) AS LINKS
FROM TMP_055_TARGET t
LEFT JOIN TMP_055_DECIDED d ON d.ID_TARGET = t.ID_TARGET
WHERE t.FINAL_TYPE = 'other'
GROUP BY OTHER_FAMILY
ORDER BY SOURCES DESC;

-- The roots that are still missing: P31 classes of the cached sources no cone caught.
SELECT 'B4. Unclassified cached sources, their 30 most frequent P31 classes' AS SECTION;

SELECT sc.ID_CLASS,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wc.LABELS_JSON, '$.en')), NULLIF(wc.LABEL_EN, '')) AS CLASS_LABEL,
       COUNT(DISTINCT t.ID_TARGET) AS SOURCES, SUM(t.LINKS) AS LINKS,
       SUBSTRING(GROUP_CONCAT(DISTINCT COALESCE(t.LABEL_EN, t.ID_TARGET) ORDER BY t.LINKS DESC SEPARATOR ' | '), 1, 200) AS EXAMPLES
FROM TMP_055_TARGET t
INNER JOIN TMP_055_SRC_CLASS sc ON sc.ID_TARGET = t.ID_TARGET
LEFT JOIN T_WC_WIKIDATA_ITEM wc ON wc.ID_WIKIDATA = sc.ID_CLASS
WHERE t.FINAL_TYPE = '(unclassified)'
GROUP BY sc.ID_CLASS, CLASS_LABEL
ORDER BY SOURCES DESC
LIMIT 30;

SELECT 'B5. Witnesses' AS SECTION;

SELECT w.KIND, w.ID_WORK, w.TITLE, t.ID_TARGET, t.LABEL_EN, t.HOME, t.FINAL_TYPE, t.FINAL_FORM
FROM TMP_055_T2S_WORK w
INNER JOIN TMP_055_LINK l ON l.KIND = w.KIND AND l.ID_WORK = w.ID_WORK
INNER JOIN TMP_055_TARGET t ON t.ID_TARGET = l.ID_TARGET
WHERE (w.KIND = 'movie' AND w.ID_WORK IN (694, 111, 329865))
   OR (w.KIND = 'serie' AND w.ID_WORK IN (100088))
ORDER BY w.KIND, w.ID_WORK;

-- ===========================================================================
-- C. DO THE TMDb "BASED ON ..." KEYWORDS AGREE WITH WIKIDATA?
--
-- On the works carrying both a mapped keyword and a P144, does one of the work's
-- sources have the type the keyword announces? The keyword ids come from step 1
-- section 8, measured. Excluded on purpose: 9672 / 364484 (true story), 328926
-- (real person), 10244 (cartoon) and 41011 (song, poem or rhyme), which do not
-- name a source type of the list.
-- A disagreement is not always a keyword error: "based on novel or book" on a remake
-- whose P144 points to the earlier film is legitimate. C3 shows which case dominates.
-- ===========================================================================

DROP TEMPORARY TABLE IF EXISTS TMP_055_KW_MAP;
CREATE TEMPORARY TABLE TMP_055_KW_MAP (
  ID_KEYWORD    INT NOT NULL,
  EXPECTED_TYPE VARCHAR(20) NOT NULL,
  EXPECTED_FORM VARCHAR(20) NULL,
  PRIMARY KEY (ID_KEYWORD)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_055_KW_MAP (ID_KEYWORD, EXPECTED_TYPE, EXPECTED_FORM) VALUES
  (818,    'literary', NULL),           -- novel or book
  (156866, 'literary', NULL),           -- short story
  (15101,  'literary', NULL),           -- children's book
  (14641,  'literary', NULL),           -- memoir or autobiography
  (246466, 'literary', NULL),           -- young adult novel
  (318933, 'literary', NULL),           -- web novel
  (239275, 'literary', NULL),           -- magazine, newspaper or article
  (319900, 'literary', 'light_novel'),  -- light novel
  (13141,  'comic',    'manga'),        -- manga
  (9717,   'comic',    NULL),           -- comic
  (18712,  'comic',    NULL),           -- graphic novel
  (288474, 'comic',    NULL),           -- webcomic or webtoon
  (290667, 'comic',    NULL),           -- manhua
  (323477, 'comic',    NULL),           -- manhwa
  (10181,  'stage',    NULL),           -- play or musical
  (41645,  'game',     NULL),           -- video game
  (222216, 'game',     NULL),           -- visual novel
  (179411, 'folklore', NULL),           -- fairy tale
  (185343, 'folklore', NULL),           -- myths, legends or folklore
  (269769, 'screen',   NULL),           -- tv series
  (165317, 'screen',   NULL),           -- movie
  (268132, 'screen',   NULL),           -- short
  (239125, 'screen',   NULL),           -- web series
  (222243, 'screen',   NULL),           -- anime
  (10542,  'other',    NULL);           -- toy

-- The types and forms of each work's sources.
DROP TEMPORARY TABLE IF EXISTS TMP_055_WORK_TYPE;
CREATE TEMPORARY TABLE TMP_055_WORK_TYPE (
  KIND       VARCHAR(10) NOT NULL,
  ID_WORK    INT NOT NULL,
  FINAL_TYPE VARCHAR(20) NOT NULL,
  FINAL_FORM VARCHAR(20) NOT NULL,
  PRIMARY KEY (KIND, ID_WORK, FINAL_TYPE, FINAL_FORM)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO TMP_055_WORK_TYPE (KIND, ID_WORK, FINAL_TYPE, FINAL_FORM)
SELECT l.KIND, l.ID_WORK, t.FINAL_TYPE, COALESCE(t.FINAL_FORM, '')
FROM TMP_055_LINK l
INNER JOIN TMP_055_TARGET t ON t.ID_TARGET = l.ID_TARGET;

-- (work, keyword) pairs, only for works that have a P144.
DROP TEMPORARY TABLE IF EXISTS TMP_055_KW_PAIR;
CREATE TEMPORARY TABLE TMP_055_KW_PAIR (
  KIND        VARCHAR(10) NOT NULL,
  ID_WORK     INT NOT NULL,
  ID_KEYWORD  INT NOT NULL,
  AGREE_TYPE  TINYINT NULL,
  AGREE_FORM  TINYINT NULL,
  FOUND_TYPES VARCHAR(200) NULL,
  PRIMARY KEY (KIND, ID_WORK, ID_KEYWORD),
  KEY IDX_KW (ID_KEYWORD)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO TMP_055_KW_PAIR (KIND, ID_WORK, ID_KEYWORD)
SELECT 'movie', mk.ID_MOVIE, mk.ID_KEYWORD
FROM T_WC_TMDB_MOVIE_KEYWORD mk
INNER JOIN TMP_055_KW_MAP km ON km.ID_KEYWORD = mk.ID_KEYWORD
WHERE COALESCE(mk.DELETED, 0) = 0
  AND EXISTS (SELECT 1 FROM TMP_055_LINK l WHERE l.KIND = 'movie' AND l.ID_WORK = mk.ID_MOVIE);

INSERT IGNORE INTO TMP_055_KW_PAIR (KIND, ID_WORK, ID_KEYWORD)
SELECT 'serie', sk.ID_SERIE, sk.ID_KEYWORD
FROM T_WC_TMDB_SERIE_KEYWORD sk
INNER JOIN TMP_055_KW_MAP km ON km.ID_KEYWORD = sk.ID_KEYWORD
WHERE COALESCE(sk.DELETED, 0) = 0
  AND EXISTS (SELECT 1 FROM TMP_055_LINK l WHERE l.KIND = 'serie' AND l.ID_WORK = sk.ID_SERIE);

UPDATE TMP_055_KW_PAIR p
INNER JOIN TMP_055_KW_MAP km ON km.ID_KEYWORD = p.ID_KEYWORD
SET p.AGREE_TYPE = EXISTS (SELECT 1 FROM TMP_055_WORK_TYPE wt
                           WHERE wt.KIND = p.KIND AND wt.ID_WORK = p.ID_WORK AND wt.FINAL_TYPE = km.EXPECTED_TYPE),
    p.AGREE_FORM = CASE WHEN km.EXPECTED_FORM IS NULL THEN NULL
                        ELSE EXISTS (SELECT 1 FROM TMP_055_WORK_TYPE wt
                                     WHERE wt.KIND = p.KIND AND wt.ID_WORK = p.ID_WORK AND wt.FINAL_FORM = km.EXPECTED_FORM) END,
    p.FOUND_TYPES = (SELECT GROUP_CONCAT(DISTINCT wt.FINAL_TYPE ORDER BY wt.FINAL_TYPE SEPARATOR ' + ')
                     FROM TMP_055_WORK_TYPE wt WHERE wt.KIND = p.KIND AND wt.ID_WORK = p.ID_WORK);

SELECT 'C1. Agreement per keyword' AS SECTION;

SELECT p.ID_KEYWORD, k.NAME, km.EXPECTED_TYPE, km.EXPECTED_FORM,
       COUNT(*)                                     AS WORKS_WITH_BOTH,
       SUM(p.AGREE_TYPE)                            AS AGREE_TYPE,
       ROUND(100 * SUM(p.AGREE_TYPE) / COUNT(*), 1) AS PCT_TYPE,
       SUM(p.AGREE_FORM)                            AS AGREE_FORM,
       ROUND(100 * SUM(p.AGREE_FORM) / NULLIF(SUM(p.AGREE_FORM IS NOT NULL), 0), 1) AS PCT_FORM
FROM TMP_055_KW_PAIR p
INNER JOIN TMP_055_KW_MAP km ON km.ID_KEYWORD = p.ID_KEYWORD
LEFT JOIN T_WC_TMDB_KEYWORD k ON k.ID_KEYWORD = p.ID_KEYWORD
GROUP BY p.ID_KEYWORD, k.NAME, km.EXPECTED_TYPE, km.EXPECTED_FORM
ORDER BY WORKS_WITH_BOTH DESC;

SELECT 'C2. Agreement overall, and per expected type' AS SECTION;

SELECT COALESCE(km.EXPECTED_TYPE, '(all)') AS EXPECTED_TYPE,
       COUNT(*) AS PAIRS, SUM(p.AGREE_TYPE) AS AGREE,
       ROUND(100 * SUM(p.AGREE_TYPE) / COUNT(*), 1) AS PCT
FROM TMP_055_KW_PAIR p
INNER JOIN TMP_055_KW_MAP km ON km.ID_KEYWORD = p.ID_KEYWORD
GROUP BY km.EXPECTED_TYPE WITH ROLLUP;

SELECT 'C3. Disagreements: what Wikidata says instead, the 30 most frequent' AS SECTION;

SELECT k.NAME AS KEYWORD, km.EXPECTED_TYPE, p.FOUND_TYPES, COUNT(*) AS WORKS,
       SUBSTRING(GROUP_CONCAT(w.TITLE ORDER BY w.TITLE SEPARATOR ' | '), 1, 150) AS EXAMPLES
FROM TMP_055_KW_PAIR p
INNER JOIN TMP_055_KW_MAP km ON km.ID_KEYWORD = p.ID_KEYWORD
LEFT JOIN T_WC_TMDB_KEYWORD k ON k.ID_KEYWORD = p.ID_KEYWORD
INNER JOIN TMP_055_T2S_WORK w ON w.KIND = p.KIND AND w.ID_WORK = p.ID_WORK
WHERE p.AGREE_TYPE = 0
GROUP BY k.NAME, km.EXPECTED_TYPE, p.FOUND_TYPES
ORDER BY WORKS DESC
LIMIT 30;

-- ===========================================================================
-- D. CHARACTERS LINKED TO T2S WORKS BY WIKIDATA, for TMDB-MOVIE-PREPROCESS-012
--
-- The idea: a character counts when Wikidata ties it to a T2S work, instead of
-- filtering 1.5 million TMDb character strings. Three ties:
--   P674  "characters", a statement on the work;
--   P453  "character role", a qualifier on a P161 cast statement of the work, which
--         also gives the actor (fixed 2026-07-30, one row per occurrence);
--   P144  "based on", when the source is a character (part B, family 'character').
-- ===========================================================================

DROP TEMPORARY TABLE IF EXISTS TMP_055_CHAR_LINK;
CREATE TEMPORARY TABLE TMP_055_CHAR_LINK (
  KIND     VARCHAR(10) NOT NULL,
  ID_WORK  INT NOT NULL,
  ID_CHAR  VARCHAR(50) NOT NULL,
  VIA      VARCHAR(5) NOT NULL,
  PRIMARY KEY (KIND, ID_WORK, ID_CHAR, VIA),
  KEY IDX_CHAR (ID_CHAR)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO TMP_055_CHAR_LINK (KIND, ID_WORK, ID_CHAR, VIA)
SELECT w.KIND, w.ID_WORK, iv.ID_ITEM, 'P674'
FROM T_WC_WIKIDATA_STATEMENT st
INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT
INNER JOIN TMP_055_T2S_WORK w ON w.ID_WIKIDATA = st.ID_WIKIDATA
WHERE st.ID_PROPERTY = 'P674'
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND COALESCE(st.DELETED, 0) = 0;

INSERT IGNORE INTO TMP_055_CHAR_LINK (KIND, ID_WORK, ID_CHAR, VIA)
SELECT w.KIND, w.ID_WORK, qiv.ID_ITEM, 'P453'
FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER sq
INNER JOIN T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE qiv ON qiv.ID_STATEMENT_QUALIFIER = sq.ID_STATEMENT_QUALIFIER
INNER JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_STATEMENT = sq.ID_STATEMENT
INNER JOIN TMP_055_T2S_WORK w ON w.ID_WIKIDATA = st.ID_WIKIDATA
WHERE sq.ID_QUALIFIER_PROPERTY = 'P453'
  AND COALESCE(sq.DELETED, 0) = 0
  AND st.ID_PROPERTY = 'P161'
  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')
  AND COALESCE(st.DELETED, 0) = 0;

INSERT IGNORE INTO TMP_055_CHAR_LINK (KIND, ID_WORK, ID_CHAR, VIA)
SELECT l.KIND, l.ID_WORK, l.ID_TARGET, 'P144'
FROM TMP_055_LINK l
INNER JOIN TMP_055_TARGET t ON t.ID_TARGET = l.ID_TARGET
LEFT JOIN TMP_055_DECIDED d ON d.ID_TARGET = l.ID_TARGET
WHERE t.HOME = '6 core_character' OR d.FAMILY = 'character';

-- One row per distinct character, with its home, labels and reach.
DROP TEMPORARY TABLE IF EXISTS TMP_055_CHAR;
CREATE TEMPORARY TABLE TMP_055_CHAR (
  ID_CHAR   VARCHAR(50) NOT NULL,
  HOME      VARCHAR(30) NULL,
  WORKS     INT NOT NULL,
  MOVIES    INT NOT NULL,
  SERIES    INT NOT NULL,
  VIAS      VARCHAR(20) NULL,
  LABEL_EN  VARCHAR(500) NULL,
  LABEL_FR  VARCHAR(500) NULL,
  PRIMARY KEY (ID_CHAR),
  KEY IDX_WORKS (WORKS)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO TMP_055_CHAR (ID_CHAR, WORKS, MOVIES, SERIES, VIAS)
SELECT ID_CHAR,
       COUNT(DISTINCT KIND, ID_WORK),
       COUNT(DISTINCT CASE WHEN KIND = 'movie' THEN ID_WORK END),
       COUNT(DISTINCT CASE WHEN KIND = 'serie' THEN ID_WORK END),
       GROUP_CONCAT(DISTINCT VIA ORDER BY VIA SEPARATOR '+')
FROM TMP_055_CHAR_LINK
GROUP BY ID_CHAR;

UPDATE TMP_055_CHAR c
SET c.HOME = CASE
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER wc WHERE wc.ID_WIKIDATA = c.ID_CHAR) THEN '1 core_character'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM wi WHERE wi.ID_WIKIDATA = c.ID_CHAR) THEN '2 cached_item'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON wp WHERE wp.ID_WIKIDATA = c.ID_CHAR) THEN '3 core_person'
    WHEN EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE wm WHERE wm.ID_WIKIDATA = c.ID_CHAR)
      OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE ws WHERE ws.ID_WIKIDATA = c.ID_CHAR) THEN '4 core_movie_or_serie'
    ELSE '5 missing_everywhere' END;

UPDATE TMP_055_CHAR c
LEFT JOIN T_WC_WIKIDATA_CHARACTER wc ON wc.ID_WIKIDATA = c.ID_CHAR
LEFT JOIN T_WC_WIKIDATA_ITEM wi      ON wi.ID_WIKIDATA = c.ID_CHAR
LEFT JOIN T_WC_WIKIDATA_PERSON wp    ON wp.ID_WIKIDATA = c.ID_CHAR
SET c.LABEL_EN = COALESCE(
      JSON_UNQUOTE(JSON_EXTRACT(wc.LABELS_JSON, '$.en')), NULLIF(wc.LABEL_EN, ''),
      JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')), NULLIF(wi.LABEL_EN, ''),
      JSON_UNQUOTE(JSON_EXTRACT(wp.LABELS_JSON, '$.en')), NULLIF(wp.LABEL_EN, '')),
    c.LABEL_FR = COALESCE(
      JSON_UNQUOTE(JSON_EXTRACT(wc.LABELS_JSON, '$.fr')),
      JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.fr')),
      JSON_UNQUOTE(JSON_EXTRACT(wp.LABELS_JSON, '$.fr')));

SELECT 'D1. Character links per Wikidata tie' AS SECTION;

SELECT VIA, COUNT(*) AS LINKS, COUNT(DISTINCT KIND, ID_WORK) AS WORKS,
       SUM(KIND = 'movie') AS LINKS_FROM_MOVIES, SUM(KIND = 'serie') AS LINKS_FROM_SERIES,
       COUNT(DISTINCT ID_CHAR) AS CHARACTERS
FROM TMP_055_CHAR_LINK
GROUP BY VIA WITH ROLLUP;

SELECT COUNT(*) AS DISTINCT_CHARACTERS_ALL_TIES FROM TMP_055_CHAR;

SELECT 'D2. Where the characters live, and their labels' AS SECTION;

SELECT HOME, COUNT(*) AS CHARACTERS, SUM(WORKS) AS WORK_LINKS,
       SUM(LABEL_EN IS NULL) AS NO_EN, SUM(LABEL_FR IS NULL) AS NO_FR
FROM TMP_055_CHAR
GROUP BY HOME
ORDER BY HOME;

-- The selection threshold: how many characters reach 1, 2-4, 5-9, 10+ T2S works.
SELECT 'D3. Characters by number of T2S works' AS SECTION;

SELECT CASE WHEN WORKS = 1 THEN '1'
            WHEN WORKS <= 4 THEN '2-4'
            WHEN WORKS <= 9 THEN '5-9'
            ELSE '10+' END AS WORKS_BUCKET,
       COUNT(*) AS CHARACTERS
FROM TMP_055_CHAR
GROUP BY WORKS_BUCKET
ORDER BY MIN(WORKS);

SELECT 'D4. Sizes to compare with' AS SECTION;

SELECT (SELECT COUNT(*) FROM T_WC_WIKIDATA_CHARACTER WHERE COALESCE(DELETED, 0) = 0) AS WIKIDATA_CHARACTER_ROWS,
       (SELECT COUNT(*) FROM T_WC_T2S_CHARACTER)                                     AS T2S_CHARACTER_ROWS,
       (SELECT COUNT(*) FROM TMP_055_CHAR)                                           AS CHARACTERS_TIED_TO_T2S;

SELECT 'D5. The 30 characters tied to the most T2S works, and Maigret (Q830561)' AS SECTION;

SELECT ID_CHAR, LABEL_EN, LABEL_FR, HOME, WORKS, MOVIES, SERIES, VIAS
FROM TMP_055_CHAR
ORDER BY WORKS DESC
LIMIT 30;

SELECT ID_CHAR, LABEL_EN, LABEL_FR, HOME, WORKS, MOVIES, SERIES, VIAS
FROM TMP_055_CHAR
WHERE ID_CHAR = 'Q830561';

-- ---------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS TMP_055_CHAR;
DROP TEMPORARY TABLE IF EXISTS TMP_055_CHAR_LINK;
DROP TEMPORARY TABLE IF EXISTS TMP_055_KW_PAIR;
DROP TEMPORARY TABLE IF EXISTS TMP_055_WORK_TYPE;
DROP TEMPORARY TABLE IF EXISTS TMP_055_KW_MAP;
DROP TEMPORARY TABLE IF EXISTS TMP_055_DECIDED;
DROP TEMPORARY TABLE IF EXISTS TMP_055_MATCH;
DROP TEMPORARY TABLE IF EXISTS TMP_055_SRC_CLASS;
DROP TEMPORARY TABLE IF EXISTS TMP_055_CONE;
DROP TEMPORARY TABLE IF EXISTS TMP_055_ROOT;
DROP TEMPORARY TABLE IF EXISTS TMP_055_TARGET;
DROP TEMPORARY TABLE IF EXISTS TMP_055_LINK;
DROP TEMPORARY TABLE IF EXISTS TMP_055_T2S_WORK;
