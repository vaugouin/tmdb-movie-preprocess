-- =============================================================================
-- Migration: Wikidata linking columns on T_WC_TMDB_COMPANY (pilot)
-- For Process 63 (TMDB_PREPROCESS_SCOPE=wikidata-companies), which stamps the
-- Wikidata id/label/confidence found for each company, mirroring the keyword
-- linker (Process 60) on T_WC_TMDB_KEYWORD.
-- Run once on the live DB BEFORE running Process 63 (the DDL dump in
-- tmdb-crawler/doc/sql/TMDb-tables.sql is CREATE TABLE, not idempotent).
-- Column types mirror T_WC_TMDB_KEYWORD exactly.
-- =============================================================================
ALTER TABLE T_WC_TMDB_COMPANY
    ADD COLUMN ID_WIKIDATA          varchar(50)  DEFAULT NULL AFTER ORIGIN_COUNTRY,
    ADD COLUMN WIKIDATA_LABEL       varchar(300) DEFAULT NULL AFTER ID_WIKIDATA,
    ADD COLUMN CONFIDENCE           double       DEFAULT NULL AFTER WIKIDATA_LABEL,
    ADD COLUMN TIM_WIKIPEDIA_SEARCH datetime     DEFAULT NULL AFTER CONFIDENCE,
    ADD KEY ID_WIKIDATA          (ID_WIKIDATA),
    ADD KEY WIKIDATA_LABEL       (WIKIDATA_LABEL),
    ADD KEY CONFIDENCE           (CONFIDENCE),
    ADD KEY TIM_WIKIPEDIA_SEARCH (TIM_WIKIPEDIA_SEARCH);
