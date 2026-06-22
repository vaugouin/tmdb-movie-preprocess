# AGENTS.md - Agent Guide for tmdb-movie-preprocess

This file gives you the agentic context you need to work on this codebase safely. For project overview, features, install / deploy steps and human-facing security / performance / troubleshooting material, read @README.md — that file is canonical and not duplicated here.

This is the single canonical guide for autonomous coding agents in this repository. Assistant-specific files such as @CLAUDE.md, and any future tool-specific guide such as `GEMINI.md`, should only point here and should not duplicate repository instructions.

Deeper specs live in their own files:
- @doc/sql/*.sql — reference DDL for the database schema (`T2S-tables.sql`, `TMDb-tables.sql`, `Wikidata-tables.sql`, `Wikipedia-tables.sql`, `T_WC_SERVER_VARIABLE.sql`); treat these files as read-only unless the user explicitly asks you to edit schema documentation
- @EXTEND_T2S_TECHNICAL.md — completed impact analysis for the `T_WC_T2S_TECHNICAL` extension (medium-format / aspect-ratio rows, §12.5 junction-enrichment contract); historical reference

- For any project update, keep documentation aligned:
  - Update `README.md` for user-facing behavior, configuration, setup, deployment, troubleshooting, or verification changes. The README carries the full per-process reference (Processes 1–62) and the source → target table map — keep it authoritative for those.
  - Update this file only when agent workflow or safety context changes.

---

## Related repositories (project ecosystem)

This is the core **preprocessing** stage of the larger movie/TV database system (owned by GitHub user `vaugouin`, all siblings under `C:\Users\vaugo\Code\<repo>` and `github.com/vaugouin/<repo>`, converging on one shared MySQL/MariaDB instance with `T_WC_*` tables). It reads the raw source tables produced by the upstream crawlers — TMDb (`T_WC_TMDB_*`), Wikidata (`T_WC_WIKIDATA_*`), Wikipedia (live API + `T_WC_WIKIPEDIA_*`) and IMDb (`T_WC_IMDB_*`) — and consolidates them into the denormalized `T_WC_T2S_*` read-model. That read-model is consumed downstream by the `fastapi-text2sql` API and the `embedding-update` semantic index.

This repo is parallel to the sibling preprocessing repos `tmdb-person-preprocess` and `keywords-processing`. The canonical sibling-repo roster lives in `tmdb-front/doc/related-repositories/related-repositories.txt`.

## Where things live (file → role)

| Path | Role |
|------|------|
| `tmdb-movie-preprocess.py` | Main ETL script (~370 KB, single module). Opens one DB connection, walks the `arrprocessscope` dict, and runs each process step inline. Entry point (`CMD` in the Dockerfile). |
| `tmdb_preprocess_helpers.py` | Stateless helper functions imported by the main script: format-line parsing / extraction (`extract_color_technology`, `extract_film_technology`, `extract_sound_technology`, `extract_format_components`, `clean_format_line`, `validate_format_line`), technical-junction writers (`load_technical_ids`, `write_movie_technical_junction`, `refresh_technical_movie_count`), Wikimedia/Wikidata linking (`f_wikimediarequest`, `f_wikipediasearchcandidates`, `f_wikipediaresolvepage`, `f_wikidataentitysummary`, `f_linktmdbkeywordtowikidata`), custom-list query builders (`f_buildcustomaggregatequery`, `f_buildcustomorderbyclause`, `f_getcustomsortby`), the shared image lookup (`f_getwikidataimagepath`), retry wrapper (`execute_sql_with_retry`), chunked DataFrame upsert (`batch_update_data`). |
| `citizenphil.py` | House DB-helper module, imported as `cp`. Lazy connection (`f_getconnection`), generic idempotent upsert (`f_sqlupdatearray`), server-variable get/set (`f_getservervariable` / `f_setservervariable`), SQL-string escaping (`f_stringtosql`), and small value/query utilities. Loads `.env` itself at import time. |
| `requirements.txt` | Python deps: `pymysql`, `pandas`, `numpy`, `requests`, `pytz`, `psutil`, `python-dotenv`. |
| `Dockerfile` | `python:3.10.5-slim-buster`; installs requirements, copies the source, `CMD ["python", "./tmdb-movie-preprocess.py"]`. |
| `tmdb-movie-preprocess.sh` | Host launcher: builds the image and `docker run -d --rm --network=host --env-file <host path>`. |
| `on.sh` / `off.sh` | Small start/stop wrappers. |
| `.env.example` | Template for the runtime env file (DB + Wikimedia knobs). |
| `doc/sql/` | Reference DDL for the schema this repo reads and writes. Read-only unless asked. |
| `data/` | Static inputs: `closed_vocabularies.json` (technical-spec alias vocabularies), format-line CSVs. |
| `archive/` | Retired / out-of-scope code and notes. Not in the default run; treat as historical. |

## Code conventions

- **House Hungarian notation** on locals and globals: `str*` (string), `lng*` / `int*` (integer), `dbl*` (float), `arr*` (dict/list). Booleans are often stored as `int*` and written to the DB as `0`/`1`. Follow this when adding code.
- **Function prefixes:** public helpers are `f_*` (e.g. `f_sqlupdatearray`, `f_linktmdbkeywordtowikidata`); private/internal helpers use a leading underscore (e.g. `_movie_technical_target_rows`, `_build_aspect_ratio_lookup`). A few newer utility functions use plain snake_case (`extract_color_technology`, `batch_update_data`, `check_memory`).
- **DB access goes through `citizenphil` (`cp`)** — do not open ad-hoc connections. Use `cp.f_getconnection()` for the shared connection and `cp.f_sqlupdatearray(table, dict, where, addstdfields)` for idempotent single-row upserts. Always escape literals with `cp.f_stringtosql()` (note: most bulk SQL is built as f-strings — keep using parameterised/escaped values for any user/source-derived text).
- **Standard audit columns:** when `f_sqlupdatearray` is called with `intaddstdfields = 1`, it auto-fills `TIM_UPDATED`, `DELETED`, `DAT_CREAT`, `ID_CREATOR`, `ID_OWNER`, `ID_USER_UPDATED` on insert. Pass `0` for tables that don't carry them.
- **Progress is reported via server variables**, not logs alone: `cp.f_setservervariable(name, value, desc, 0)` persists current process / sub-process / current entity ID to `T_WC_SERVER_VARIABLE` for external monitoring. Keep updating these when you add a step.
- **Multiple cursors** (`cursor`, `cursor2` … `cursor5`) are opened up front so independent reads/writes can interleave within one process.
- **Idempotent upserts:** copy steps use `INSERT … ON DUPLICATE KEY UPDATE` plus a scoped stale-delete (delete rows in the processed ID range that no longer exist in source), so the pipeline is safe to re-run.
- **Batch / chunk processing:** row-copy processes iterate source IDs in chunks of 1000; `batch_update_data` flushes DataFrame updates in batches of 1000.
- **Staging rebuild + atomic swap:** the newer, performance-sensitive rebuilds (junction/image/video tables, `T2S_ITEM`, company/network) build a full `*_BUILD` table, validate row count, swap with `RENAME TABLE` (→ `*_OLD`), and drop `*_OLD` after success. See README "Common Patterns".
- **Error handling:** MySQL errors funnel through `cp.f_handlemysqlerror`; lock-wait timeouts (errno 1205) are retried/skipped rather than fatal. The top-level `try/except` rolls back on a `pymysql.MySQLError`.
- **Emoji log markers** (✅/⚠️/❌) are used in console output intentionally — match the existing style. Source data is multilingual; keep everything UTF-8.

## Pipeline / process steps

The script defines ordered dicts `arrprocessscopemain` / `arrprocessscopewikidatatopics` near the top of `tmdb-movie-preprocess.py` and selects one into `arrprocessscope` from the `TMDB_PREPROCESS_SCOPE` env var (`main` default, or `wikidata-topics`), then runs `for intindex, strdesc in arrprocessscope.items():` — **process selection is this dict, and execution order is dict insertion order, not numeric order.** Many candidate scopes are present as commented-out lines; to run a subset, comment the active selection and uncomment (or add) a narrower dict. There is **no internal scheduler loop** in the active script — it runs the scope once and exits; recurrence is handled externally (Docker run + host cron / launcher). **Process 60 (Wikidata keyword linker) is decoupled** into the `wikidata-topics` scope so it can be scheduled separately (its own cron / `tmdb-movie-preprocess-wikidata-topics.sh`) instead of blocking the ~3h45m of the main run; it is NOT in `main`. The archived Process 30 (`archive/TMDB_MOVIE_LANG_META.py`) is not in any default scope, and its spaCy-based lemmatisation has been removed from the codebase.

`main` scope (insertion order): `0` (HTML-unescape `T_WC_CUSTOM_LIST`), `1` (`WIKIPEDIA_FORMAT_LINE` + technical-junction enrichment), `2` (`T2S_MOVIE_TECHNICAL`), `62` (link Wikidata → T2S technical), `3` (`T2S_TOPIC`), `41` (`T2S_COLLECTION`), `61` (link Wikidata → collections), `42` (`T2S_LIST`), `43` (`T2S_GROUP`), `44` (`T2S_AWARD`), `47` (`T2S_NOMINATION`), `45` (`T2S_MOVEMENT`), `46` (`T2S_DEATH`), then the bulk copy/rebuild steps `4`–`40` (`T2S_MOVIE`, `T2S_SERIE`, `T2S_PERSON`, `T2S_COMPANY`, `T2S_NETWORK`, the person/genre/company/network/country/language link tables, image & video tables, seasons/episodes and their links/media, and finally `T2S_ITEM`). The `wikidata-topics` scope is just `60` (link Wikidata → topics). The bulk copies `4`/`5`/`6`/`27`/`28` use an incremental `TIM_UPDATED` watermark (server variables `strtmdbmoviepreprocesst2s{movie,serie,person,season,episode}lastrun`) with a full-table stale-delete + once-per-run enrichment; `27`/`28` also OR-in the parent serie (and, for `28`, season) `TIM_UPDATED` so a newly-qualifying parent re-pulls its children. The IMDb weighted-rating average is precomputed once per run in `4`/`5`/`27`/`28`. The rebuild+swap steps `7` (`T2S_COMPANY`) and `8` (`T2S_NETWORK`) compute their `MOVIE_COUNT`/`SERIE_COUNT` with a set-based reset-then-update keyed on `ID_COMPANY`/`ID_NETWORK` (replacing a per-row `f_sqlupdatearray` loop grouped by `NAME`).

Main sub-pipeline families:
- **Source cleanup** — step 0 decodes HTML-escaped text in `T_WC_CUSTOM_LIST` so downstream labels are clean.
- **Technical specs** — step 1 parses `WIKIPEDIA_FORMAT_LINE` on `T_WC_TMDB_MOVIE` and writes the movie↔technical junction; step 2 builds the `T_WC_T2S_TECHNICAL` dimension; step 62 backfills Wikidata IDs/labels on it.
- **Wikidata/Wikipedia linking** — steps 60/61/62 use the throttled Wikimedia request wrapper (retry/backoff on HTTP 429) and the `P31` blocked-types filter to attach Wikidata QIDs to keywords, collections, and technical rows.
- **Dimension builders from Wikidata properties** — groups (P463/P108/P54), awards (P166), nominations (P1411), deaths (P509/P1196), plus custom-list-driven collections/lists/movements/groups.
- **Bulk T2S copy + rebuild** — the long tail of `T_WC_TMDB_*` → `T_WC_T2S_*` copies (chunked upsert) and junction/media rebuilds (staging swap).

Per-process inputs, outputs, filters and operations are documented exhaustively in @README.md (Process Reference + Source → Target Table Map) — consult it rather than re-deriving from code, but verify against code before changing behavior.

## Database tables

Verified prefixes this repo touches: source `T_WC_TMDB_*`, `T_WC_WIKIDATA_*`, `T_WC_IMDB_*` (e.g. `T_WC_IMDB_MOVIE_RATING_IMPORT`), the cross-cutting `T_WC_CUSTOM_LIST`, and `T_WC_SERVER_VARIABLE`; targets are the `T_WC_T2S_*` read-model.

Representative source tables read: `T_WC_TMDB_MOVIE`, `T_WC_TMDB_SERIE`, `T_WC_TMDB_PERSON`, `T_WC_TMDB_COMPANY`, `T_WC_TMDB_NETWORK`, `T_WC_TMDB_KEYWORD`, `T_WC_TMDB_LIST` / `T_WC_TMDB_LIST_LANG`, `T_WC_TMDB_COLLECTION` / `T_WC_TMDB_COLLECTION_LANG`, `T_WC_TMDB_SEASON`, `T_WC_TMDB_EPISODE`, the `T_WC_TMDB_PERSON_*` / `T_WC_TMDB_MOVIE_*` / `T_WC_TMDB_SERIE_*` relation, image and video tables, `T_WC_WIKIDATA_ITEM_V1`, `T_WC_WIKIDATA_ITEM_PROPERTY`, `T_WC_WIKIDATA_PERSON_V1` (and `_MOVIE_V1` / `_SERIE_V1` via the image-path helper), and `T_WC_IMDB_MOVIE_RATING_IMPORT`.

Representative `T_WC_T2S_*` targets written: the core entities `T_WC_T2S_MOVIE`, `T_WC_T2S_SERIE`, `T_WC_T2S_PERSON`, `T_WC_T2S_COMPANY`, `T_WC_T2S_NETWORK`, `T_WC_T2S_SEASON`, `T_WC_T2S_EPISODE`, `T_WC_T2S_ITEM`; the dimensions `T_WC_T2S_TECHNICAL`, `T_WC_T2S_TOPIC`, `T_WC_T2S_COLLECTION`, `T_WC_T2S_LIST`, `T_WC_T2S_GROUP`, `T_WC_T2S_AWARD`, `T_WC_T2S_NOMINATION`, `T_WC_T2S_MOVEMENT`, `T_WC_T2S_DEATH`, `T_WC_T2S_CHARACTER`; and the junction tables (`T_WC_T2S_PERSON_MOVIE`, `T_WC_T2S_PERSON_SERIE`, `T_WC_T2S_PERSON_SEASON`, `T_WC_T2S_PERSON_EPISODE`, `*_GENRE`, `*_COMPANY`, `*_NETWORK`, `*_PRODUCTION_COUNTRY`, `*_SPOKEN_LANGUAGE`, `*_IMAGE`, `*_VIDEO`, `*_TECHNICAL`, `*_COLLECTION`, `*_LIST`, `*_GROUP`, `*_AWARD`, `*_NOMINATION`, `*_MOVEMENT`, `*_DEATH`, `*_CHARACTER`). A few source tables are also written back (counts/flags): `T_WC_TMDB_MOVIE` (technical flags), `T_WC_TMDB_KEYWORD` (counts + Wikidata link), `T_WC_TMDB_COMPANY` / `T_WC_TMDB_NETWORK` (counts). The full authoritative map is in @README.md.

When changing schema, update the matching DDL in `doc/sql/` and quote only table/column names you have verified in code or DDL.

## SQL Object Naming Conventions

The shared database follows these conventions (consistent across sibling repos) — keep new objects aligned:

- **Tables:** uppercase snake case with a domain prefix — `T_WC_*` (shared core), `T_WC_T2S_*` (this repo's read-model targets), `T_WC_TMDB_*`, `T_WC_WIKIDATA_*`, `T_WC_WIKIPEDIA_*` (source tables), `T_WC_IMDB_*` (IMDb imports). The namespace prefix is configurable via `DB_NAMESPACE` (default `T_WC_`).
- **Primary keys:** `ID_{ENTITY}` (e.g. `ID_MOVIE`, `ID_SERIE`, `ID_PERSON`, `ID_TECHNICAL`).
- **Foreign keys:** reuse the referenced PK name (e.g. `ID_MOVIE` in a junction table references `T_WC_T2S_MOVIE.ID_MOVIE`).
- **Dates / timestamps:** `DAT_*` for dates (`DAT_RELEASE`, `DAT_FIRST_AIR`, `DAT_CREAT`), `TIM_*` for datetimes (`TIM_UPDATED`, `TIM_WIKIPEDIA_SEARCH`).
- **Boolean flags:** `IS_*` (`IS_VALID_FORMAT`, `IS_PERSON`, `IS_EMPTY`), plus the soft-delete `DELETED` (0/1).
- **Ordering / counts:** `DISPLAY_ORDER`; `*_COUNT` (`MOVIE_COUNT`, `SERIE_COUNT`, `PERSON_COUNT`, `NAME_WORD_COUNT`).
- **Search / normalisation columns:** `*_NORM` / `*_KEY` for normalised search values.

## Configuration & secrets

Configuration is environment-driven. `citizenphil.py` calls `load_dotenv()` on the `.env` next to it at import time; in containers the values come from `--env-file` instead (see README "Secrets handling").

Verified env vars (see `.env.example`):
- **Database:** `DB_HOST`, `DB_PORT` (default 3306), `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_NAMESPACE` (default `T_WC_`).
- **Locale:** `USER_TIMEZONE` (default `Europe/Paris`; used for all `TIM_*`/`DAT_*` stamps).
- **Wikimedia (Processes 60/61/62):** `WIKIMEDIA_USER_AGENT`, `WIKIMEDIA_REQUEST_DELAY_SECONDS`, `WIKIMEDIA_BACKOFF_SECONDS`, `WIKIMEDIA_MAX_RETRIES`, `WIKIMEDIA_TIMEOUT_SECONDS`.
- **TMDb (optional, read by `citizenphil`):** `TMDB_API_DOMAIN_URL`, `TMDB_API_KEY`, `TMDB_API_TOKEN` — used only if TMDb HTTP calls are exercised; the default scope is DB-only.

There is **no OpenAI / LLM dependency** in this repo. Secrets must never be committed or baked into the image: `.env` is git-ignored and docker-ignored, and the runtime env file lives outside the source tree (e.g. `/home/debian/docker/tmdb-movie-preprocess/.env`). The PyMySQL connection sets `local_infile=True` — keep that in mind if you touch connection setup.

## Encoding

All source and target data is multilingual (English + French labels, accented and non-Latin text). Treat all files as **UTF-8**; do not introduce code that assumes ASCII or a non-UTF-8 locale, and keep DB reads/writes UTF-8-clean.

---

**Last Updated**: 2026-06-01
**Current Version**: 1.0.0
