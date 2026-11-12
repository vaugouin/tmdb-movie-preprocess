# T2S schema notes

Hand-written notes about the tables and columns of `doc/sql/T2S-tables.sql`.

They live here, and not in the `.sql`, because every `*-tables.sql` file of `doc/sql/` is a dump: `structurevaugouindb.sh` (tmdb-front, `doc/sql/`) exports the structure of the live base and the files are copied to every repo that needs them, so each repo holds the valid structure. A comment written in a dump is lost at the next export. The four notes below were moved out of `T2S-tables.sql` on 2026-11-10.

Add a note here when a ticket creates or changes a table in a way the DDL alone does not tell: who creates it, how it is rebuilt, what a column means.

| Table | Column | Ticket (version) | Note |
|---|---|---|---|
| `T_WC_T2S_LOCATION` | `OVERVIEW_FR` | TMDB-MOVIE-PREPROCESS-064 (1.1.20) | French Wikidata description, no English fallback. Added by process 72 itself (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`), no migration. |
| `T_WC_T2S_LOCATION` | `LATITUDE`, `LONGITUDE` | TMDB-MOVIE-PREPROCESS-075 (1.1.59) | `decimal(9,6)`, degrees, after `LOCATION_TYPE`. Wikidata P625, Earth globe only (`Q2` or no globe), one statement per place for both columns (preferred rank first, then the lowest `ID_STATEMENT`, deprecated excluded). NULL means no terrestrial coordinate (fiction, no P625, other globe). Created by `doc/sql/migration-075-location-coordinates.sql` (voie 2), filled by process 72 step 4d every night (skipped while the columns are missing), first fill by `doc/sql/backfill-075-location-coordinates.sql`. Wikidata precision is not kept: a `country` or `region` row is a shape, not a point. For the place map of the phase 3 result representations. |
| `T_WC_T2S_LOCATION_ANCESTOR` | (table) | TMDB-MOVIE-PREPROCESS-048 (1.1.19) | Place-in-place closure over Wikidata P131 / P17, best rank, the place itself at `DEPTH` 0. Created by process 72 itself (`CREATE TABLE IF NOT EXISTS`), rebuilt nightly and swapped with `T_WC_T2S_LOCATION`. Both ends are living rows of `T_WC_T2S_LOCATION`; `DEPTH` counts Wikidata hops, not administrative levels. |
| `T_WC_T2S_MOVIE_TITLE_ALIAS` | (table) | TMDB-MOVIE-PREPROCESS-069 | Process 52: full rebuild every night (DDL carried by `STR_TITLE_ALIAS_DDL` in `tmdb_preprocess_helpers.py`). `ID_ROW` is renumbered at every run. |
| `T_WC_T2S_SERIE_TITLE_ALIAS` | (table) | TMDB-MOVIE-PREPROCESS-069 | Process 52: full rebuild every night (DDL carried by `STR_TITLE_ALIAS_DDL` in `tmdb_preprocess_helpers.py`). `ID_ROW` is renumbered at every run. |
