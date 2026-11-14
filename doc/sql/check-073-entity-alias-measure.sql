-- TMDB-MOVIE-PREPROCESS-073 measure, and FASTAPI-TEXT2SQL-360 step 3 recheck: read-only
-- (SELECT and temporary tables).
-- Run (voie 1):
--   ssh vps-agent run tmdb-movie-preprocess/doc/sql/check-073-entity-alias-measure.sql
--
-- Part 1 (step 3 of -360, after check-069-alias-after-p1476-20261110.txt). That check looked
-- for the ten title forms that fell to ChromaDB on 1.1.20 by exact string and found none. Here
-- each form is looked up by its distinctive words, in the TMDb titles and in the title alias
-- tables, so a form that differs by punctuation or an article still shows.
--
-- Part 2 (-073). For every entity type still on ChromaDB: living T2S rows, rows linked to
-- Wikidata, which V2 table holds them, the Wikidata forms (labels en/fr/mul, aliases en/fr)
-- left once the T2S main names of the row are removed, and the collisions between entities of
-- one type. Then the share of nomination QIDs that are also awards (the open point of -073).

SELECT NOW() AS NOW_;

-- 1A. P1476 and the alias tables today
SELECT '1A. P1476 statements' AS STEP, COUNT(*) AS N, MAX(TIM_UPDATED) AS LAST_UPDATE
FROM T_WC_WIKIDATA_STATEMENT WHERE ID_PROPERTY = 'P1476' AND COALESCE(DELETED, 0) = 0;

SELECT '1A. movie alias' AS STEP, ALIAS_SOURCE, COUNT(*) AS N, MAX(TIM_UPDATED) AS LAST_UPDATE
FROM T_WC_T2S_MOVIE_TITLE_ALIAS WHERE COALESCE(DELETED, 0) = 0 GROUP BY ALIAS_SOURCE;

SELECT '1A. serie alias' AS STEP, ALIAS_SOURCE, COUNT(*) AS N, MAX(TIM_UPDATED) AS LAST_UPDATE
FROM T_WC_T2S_SERIE_TITLE_ALIAS WHERE COALESCE(DELETED, 0) = 0 GROUP BY ALIAS_SOURCE;

-- 1B. The miss forms by their distinctive words
CREATE TEMPORARY TABLE tmp_miss (FORM varchar(200), PAT varchar(100)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO tmp_miss VALUES
 ('detective dee sea dragon', '%Detective Dee%'),
 ('Le Faucon contre-attaque', '%Faucon contre%'),
 ('Vingt-quatre heures pour tuer', '%heures pour tuer%'),
 ('Les tambours de la rivière Mohawk', '%Mohawk%'),
 ('Trois petites bières', '%petites bi_res%'),
 ('La Veuve insatiable', '%Veuve insatiable%'),
 ('Évaluez tous les risques', '%valuez tous%'),
 ('Au-delà de la vallée des poupées', '%vall_e des poup_es%'),
 ('Star Wars: Episode IV - A New Hope', '%Episode IV%New Hope%'),
 ('Star Wars : épisode IV - Un nouvel espoir', '%nouvel espoir%');

SELECT '1B. in TMDb titles' AS STEP, q.FORM, m.ID_MOVIE, m.MOVIE_TITLE, m.MOVIE_TITLE_FR, m.ORIGINAL_TITLE
FROM tmp_miss q
JOIN T_WC_T2S_MOVIE m ON (m.MOVIE_TITLE LIKE q.PAT OR m.MOVIE_TITLE_FR LIKE q.PAT OR m.ORIGINAL_TITLE LIKE q.PAT)
WHERE COALESCE(m.DELETED, 0) = 0
ORDER BY q.FORM, m.ID_MOVIE;

SELECT '1B. in movie alias' AS STEP, q.FORM, a.ID_MOVIE, m.MOVIE_TITLE, a.ALIAS_SOURCE, a.LANG_CODE, a.TITLE
FROM tmp_miss q
JOIN T_WC_T2S_MOVIE_TITLE_ALIAS a ON a.TITLE LIKE q.PAT AND COALESCE(a.DELETED, 0) = 0
JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = a.ID_MOVIE
ORDER BY q.FORM, a.ID_MOVIE, a.ALIAS_SOURCE;

-- 2A. Entities of the types still on ChromaDB
CREATE TEMPORARY TABLE tmp_ent (
  TYP varchar(20), ID_ENT int, ID_WIKIDATA varchar(50), NAME_EN varchar(500), NAME_FR varchar(500),
  KEY (ID_WIKIDATA), KEY (TYP, ID_ENT)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_ent SELECT 'topic', ID_TOPIC, NULLIF(ID_WIKIDATA, ''), TOPIC_NAME, TOPIC_NAME_FR FROM T_WC_T2S_TOPIC WHERE COALESCE(DELETED, 0) = 0;
INSERT INTO tmp_ent SELECT 'list', ID_T2S_LIST, NULLIF(ID_WIKIDATA, ''), LIST_NAME, LIST_NAME_FR FROM T_WC_T2S_LIST WHERE COALESCE(DELETED, 0) = 0;
INSERT INTO tmp_ent SELECT 'group', ID_GROUP, NULLIF(ID_WIKIDATA, ''), GROUP_NAME, GROUP_NAME_FR FROM T_WC_T2S_GROUP WHERE COALESCE(DELETED, 0) = 0;
INSERT INTO tmp_ent SELECT 'character', ID_CHARACTER, NULLIF(ID_WIKIDATA, ''), WIKIDATA_NAME, NULL FROM T_WC_T2S_CHARACTER WHERE COALESCE(DELETED, 0) = 0;
INSERT INTO tmp_ent SELECT 'collection', ID_T2S_COLLECTION, NULLIF(ID_WIKIDATA, ''), COLLECTION_NAME, COLLECTION_NAME_FR FROM T_WC_T2S_COLLECTION WHERE COALESCE(DELETED, 0) = 0;
INSERT INTO tmp_ent SELECT 'death', ID_DEATH, NULLIF(ID_WIKIDATA, ''), DEATH_NAME, DEATH_NAME_FR FROM T_WC_T2S_DEATH WHERE COALESCE(DELETED, 0) = 0;
INSERT INTO tmp_ent SELECT 'award', ID_AWARD, NULLIF(ID_WIKIDATA, ''), AWARD_NAME, AWARD_NAME_FR FROM T_WC_T2S_AWARD WHERE COALESCE(DELETED, 0) = 0;
INSERT INTO tmp_ent SELECT 'nomination', ID_NOMINATION, NULLIF(ID_WIKIDATA, ''), NOMINATION_NAME, NOMINATION_NAME_FR FROM T_WC_T2S_NOMINATION WHERE COALESCE(DELETED, 0) = 0;
INSERT INTO tmp_ent SELECT 'movement', ID_MOVEMENT, NULLIF(ID_WIKIDATA, ''), MOVEMENT_NAME, MOVEMENT_NAME_FR FROM T_WC_T2S_MOVEMENT WHERE COALESCE(DELETED, 0) = 0;
INSERT INTO tmp_ent SELECT 'location', ID_LOCATION, NULLIF(ID_WIKIDATA, ''), LOCATION_NAME, LOCATION_NAME_FR FROM T_WC_T2S_LOCATION WHERE COALESCE(DELETED, 0) = 0;
INSERT INTO tmp_ent SELECT 'source_work', ID_SOURCE_WORK, NULLIF(ID_WIKIDATA, ''), SOURCE_WORK_NAME, SOURCE_WORK_NAME_FR FROM T_WC_T2S_SOURCE_WORK WHERE COALESCE(DELETED, 0) = 0;

-- 2B. Rows, rows linked to Wikidata, and the V2 table that holds the QID
SELECT '2B. coverage' AS STEP, e.TYP, COUNT(*) AS ROWS_, COUNT(e.ID_WIKIDATA) AS WITH_QID,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_ITEM w WHERE w.ID_WIKIDATA = e.ID_WIKIDATA AND COALESCE(w.DELETED, 0) = 0)) AS IN_ITEM,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER w WHERE w.ID_WIKIDATA = e.ID_WIKIDATA AND COALESCE(w.DELETED, 0) = 0)) AS IN_CHARACTER,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE w WHERE w.ID_WIKIDATA = e.ID_WIKIDATA AND COALESCE(w.DELETED, 0) = 0)) AS IN_MOVIE,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE w WHERE w.ID_WIKIDATA = e.ID_WIKIDATA AND COALESCE(w.DELETED, 0) = 0)) AS IN_SERIE,
       SUM(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON w WHERE w.ID_WIKIDATA = e.ID_WIKIDATA AND COALESCE(w.DELETED, 0) = 0)) AS IN_PERSON
FROM tmp_ent e GROUP BY e.TYP ORDER BY e.TYP;

-- 2C. Wikidata forms per entity, from ITEM and CHARACTER (the two generic V2 tables)
CREATE TEMPORARY TABLE tmp_w (
  ID_WIKIDATA varchar(50), LABELS_JSON longtext, ALIASES_JSON longtext, PRIMARY KEY (ID_WIKIDATA)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO tmp_w SELECT w.ID_WIKIDATA, w.LABELS_JSON, w.ALIASES_JSON FROM T_WC_WIKIDATA_ITEM w
  WHERE COALESCE(w.DELETED, 0) = 0 AND w.ID_WIKIDATA IN (SELECT ID_WIKIDATA FROM tmp_ent WHERE ID_WIKIDATA IS NOT NULL);
-- A temporary table cannot be read twice in one statement: the primary key drops the QIDs already taken from ITEM
INSERT IGNORE INTO tmp_w SELECT w.ID_WIKIDATA, w.LABELS_JSON, w.ALIASES_JSON FROM T_WC_WIKIDATA_CHARACTER w
  WHERE COALESCE(w.DELETED, 0) = 0 AND w.ID_WIKIDATA IN (SELECT ID_WIKIDATA FROM tmp_ent WHERE ID_WIKIDATA IS NOT NULL);

CREATE TEMPORARY TABLE tmp_form (
  TYP varchar(20), ID_ENT int, SRC varchar(10), LANG varchar(5), FORM varchar(500),
  KEY (TYP, FORM(100)), KEY (TYP, ID_ENT)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO tmp_form SELECT e.TYP, e.ID_ENT, 'label', l.LANG,
  CONVERT(LEFT(TRIM(JSON_UNQUOTE(JSON_EXTRACT(w.LABELS_JSON, CONCAT('$.', l.LANG)))), 500) USING utf8mb4) COLLATE utf8mb4_unicode_ci
FROM tmp_ent e JOIN tmp_w w ON w.ID_WIKIDATA = e.ID_WIKIDATA
CROSS JOIN (SELECT 'en' AS LANG UNION ALL SELECT 'fr' UNION ALL SELECT 'mul') l
WHERE JSON_EXTRACT(w.LABELS_JSON, CONCAT('$.', l.LANG)) IS NOT NULL;

INSERT INTO tmp_form SELECT e.TYP, e.ID_ENT, 'alias', 'en', CONVERT(LEFT(TRIM(j.F), 500) USING utf8mb4) COLLATE utf8mb4_unicode_ci
FROM tmp_ent e JOIN tmp_w w ON w.ID_WIKIDATA = e.ID_WIKIDATA
CROSS JOIN JSON_TABLE(w.ALIASES_JSON, '$.en[*]' COLUMNS (F varchar(1000) CHARACTER SET utf8mb4 PATH '$')) AS j
WHERE w.ALIASES_JSON IS NOT NULL AND j.F IS NOT NULL AND TRIM(j.F) <> '';

INSERT INTO tmp_form SELECT e.TYP, e.ID_ENT, 'alias', 'fr', CONVERT(LEFT(TRIM(j.F), 500) USING utf8mb4) COLLATE utf8mb4_unicode_ci
FROM tmp_ent e JOIN tmp_w w ON w.ID_WIKIDATA = e.ID_WIKIDATA
CROSS JOIN JSON_TABLE(w.ALIASES_JSON, '$.fr[*]' COLUMNS (F varchar(1000) CHARACTER SET utf8mb4 PATH '$')) AS j
WHERE w.ALIASES_JSON IS NOT NULL AND j.F IS NOT NULL AND TRIM(j.F) <> '';

-- Forms equal to a main name of their own row are not new (unicode_ci: case and accents fold)
DELETE f FROM tmp_form f JOIN tmp_ent e ON e.TYP = f.TYP AND e.ID_ENT = f.ID_ENT
WHERE f.FORM = e.NAME_EN OR f.FORM = e.NAME_FR OR f.FORM = '';

-- One row per (entity, form), the first source kept
CREATE TEMPORARY TABLE tmp_kept (
  TYP varchar(20), ID_ENT int, FORM varchar(500), SRC varchar(10),
  PRIMARY KEY (TYP, ID_ENT, FORM(150))
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT IGNORE INTO tmp_kept SELECT TYP, ID_ENT, FORM, SRC FROM tmp_form ORDER BY TYP, ID_ENT, SRC DESC;

SELECT '2C. new forms' AS STEP, TYP, COUNT(*) AS FORMS, COUNT(DISTINCT ID_ENT) AS ENTITIES,
       SUM(SRC = 'label') AS FROM_LABEL, SUM(SRC = 'alias') AS FROM_ALIAS
FROM tmp_kept GROUP BY TYP ORDER BY TYP;

-- 2D. Collisions within a type: a form held by several entities, or equal to another entity's main name
SELECT '2D. shared forms' AS STEP, TYP, COUNT(*) AS FORMS_SHARED, SUM(N) AS ROWS_INVOLVED
FROM (SELECT TYP, FORM, COUNT(DISTINCT ID_ENT) AS N FROM tmp_kept GROUP BY TYP, FORM HAVING N > 1) x
GROUP BY TYP ORDER BY TYP;

SELECT '2D. form = main name of another' AS STEP, k.TYP, COUNT(*) AS N
FROM tmp_kept k
WHERE EXISTS (SELECT 1 FROM tmp_ent e WHERE e.TYP = k.TYP AND e.ID_ENT <> k.ID_ENT
              AND (e.NAME_EN = k.FORM OR e.NAME_FR = k.FORM))
GROUP BY k.TYP ORDER BY k.TYP;

-- 2E. Samples: five entities with the most new forms, per type
SELECT '2E. samples' AS STEP, x.TYP, x.ID_ENT, e.NAME_EN, x.N, x.SOME_FORMS
FROM (
  SELECT TYP, ID_ENT, COUNT(*) AS N, LEFT(GROUP_CONCAT(FORM ORDER BY FORM SEPARATOR ' | '), 200) AS SOME_FORMS,
         ROW_NUMBER() OVER (PARTITION BY TYP ORDER BY COUNT(*) DESC, ID_ENT) AS RN
  FROM tmp_kept GROUP BY TYP, ID_ENT
) x JOIN tmp_ent e ON e.TYP = x.TYP AND e.ID_ENT = x.ID_ENT
WHERE x.RN <= 5 ORDER BY x.TYP, x.RN;

-- 2F. Nominations against awards: is a nomination the same Wikidata item as an award?
-- (awards copied apart: a temporary table cannot be read twice in one statement)
CREATE TEMPORARY TABLE tmp_award (ID_WIKIDATA varchar(50), NAME_EN varchar(500), KEY (ID_WIKIDATA), KEY (NAME_EN(100)))
  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO tmp_award SELECT ID_WIKIDATA, NAME_EN FROM tmp_ent WHERE TYP = 'award';
SELECT '2F. nomination QIDs' AS STEP, COUNT(*) AS NOMINATIONS, COUNT(n.ID_WIKIDATA) AS WITH_QID,
       SUM(EXISTS (SELECT 1 FROM tmp_award a WHERE a.ID_WIKIDATA = n.ID_WIKIDATA)) AS QID_ALSO_AWARD,
       SUM(EXISTS (SELECT 1 FROM tmp_award a WHERE a.NAME_EN = n.NAME_EN)) AS NAME_ALSO_AWARD
FROM tmp_ent n WHERE n.TYP = 'nomination';
