-- =============================================================================
-- Migration: WIKIPEDIA_MAIN_IMAGE_URL / _FR on every T2S entity table
--
-- WHY (closes WIKIDATA-CRAWLER-015 via WIKIPEDIA-CRAWLER-020)
-- ------------------------------------------------------------------
-- The Wikipedia lead image of an entity lives today in its V1 row
-- (T_WC_WIKIDATA_MOVIE_V1.WIKIPEDIA_POSTER_PATH, _PERSON_V1.WIKIPEDIA_PROFILE_PATH,
-- _ITEM_V1.WIKIPEDIA_IMAGE_PATH). V2 carries no image column at all and should not:
-- a Wikipedia lead image is Wikipedia data, not a Wikidata statement. As long as the
-- image lives only in V1, V1 cannot be decommissioned.
--
-- wikipedia-crawler already writes it to its own home, keyed per language, in
-- T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL. These columns are the SERVING copy in the
-- T2S layer, which is where consumers already read: 172 read sites across five repos
-- (fastapi-text2sql 66, tmdb-front 38, wikipedia-crawler 32, tmdb-movie-preprocess 31,
-- voice-agent 5) move from one local column to another local column, instead of each
-- learning a join on a table they do not know.
--
-- TWO COLUMNS, NOT ONE, and that is the point. V1 had a single image column per
-- entity while the crawler runs once per language, so the second language silently
-- overwrote the first (that is how collection 4845 lost its English lead image to a
-- French portal banner). Storing en and fr separately makes that impossible and gives
-- consumers a localized image they never had.
--
-- NAMING. Layer convention is bare name for English, _FR suffix for French, as in
-- MOVIE_TITLE / MOVIE_TITLE_FR. The WIKIPEDIA_ prefix keeps the provenance visible,
-- as the V1 columns did. One single name across all tables, deliberately: V1 named
-- the same thing three ways depending on the entity, and every consumer paid for it.
--
-- TYPE. varchar(1000): these are absolute URLs. Despite their _PATH suffix the V1
-- columns already hold URLs, not paths, and the same variable feeds both writes
-- (wikipedia_page_writer.py:233 and :320), so the migration of readers needs no
-- transformation of the value.
--
-- SAFE ON A LIVE DB: adding a nullable column is a metadata-only ALTER on MariaDB
-- 10.3+ (ALGORITHM=INSTANT). Nothing reads these columns until the readers are moved.
-- Run once, BEFORE fill-wikipedia-main-image.sql.
-- =============================================================================

ALTER TABLE T_WC_T2S_MOVIE       ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_SERIE       ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_PERSON      ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_SEASON      ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_EPISODE     ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_CHARACTER   ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_ITEM        ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_AWARD       ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_NOMINATION  ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_DEATH       ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_GROUP       ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_MOVEMENT    ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_COLLECTION  ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_LIST        ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_TOPIC       ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
ALTER TABLE T_WC_T2S_TECHNICAL   ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL varchar(1000) DEFAULT NULL, ADD COLUMN WIKIPEDIA_MAIN_IMAGE_URL_FR varchar(1000) DEFAULT NULL;
