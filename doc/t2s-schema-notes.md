# T2S schema notes

Hand-written notes about the tables and columns of `doc/sql/T2S-tables.sql`.

They live here, and not in the `.sql`, because every `*-tables.sql` file of `doc/sql/` is a dump: `structurevaugouindb.sh` (tmdb-front, `doc/sql/`) exports the structure of the live base and the files are copied to every repo that needs them, so each repo holds the valid structure. A comment written in a dump is lost at the next export. The four notes below were moved out of `T2S-tables.sql` on 2026-11-10.

Add a note here when a ticket creates or changes a table in a way the DDL alone does not tell: who creates it, how it is rebuilt, what a column means.

| Table | Column | Ticket (version) | Note |
|---|---|---|---|
| `T_WC_T2S_LOCATION` | `OVERVIEW_FR` | TMDB-MOVIE-PREPROCESS-064 (1.1.20) | French Wikidata description, no English fallback. Added by process 72 itself (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`), no migration. |
| `T_WC_T2S_LOCATION_ANCESTOR` | (table) | TMDB-MOVIE-PREPROCESS-048 (1.1.19) | Place-in-place closure over Wikidata P131 / P17, best rank, the place itself at `DEPTH` 0. Created by process 72 itself (`CREATE TABLE IF NOT EXISTS`), rebuilt nightly and swapped with `T_WC_T2S_LOCATION`. Both ends are living rows of `T_WC_T2S_LOCATION`; `DEPTH` counts Wikidata hops, not administrative levels. |
| `T_WC_T2S_MOVIE_TITLE_ALIAS` | (table) | TMDB-MOVIE-PREPROCESS-069 | Process 52: full rebuild every night (DDL carried by `STR_TITLE_ALIAS_DDL` in `tmdb_preprocess_helpers.py`). `ID_ROW` is renumbered at every run. |
| `T_WC_T2S_SERIE_TITLE_ALIAS` | (table) | TMDB-MOVIE-PREPROCESS-069 | Process 52: full rebuild every night (DDL carried by `STR_TITLE_ALIAS_DDL` in `tmdb_preprocess_helpers.py`). `ID_ROW` is renumbered at every run. |
