# tmdb-movie-preprocess

ETL pipeline that transforms raw TMDb/IMDb/Wikidata source tables into pre-processed `T2S_*` (Text-to-Search) tables optimised for downstream search and recommendation features.

---

## Overview

The script iterates over a dictionary of process indices (`arrprocessscope`). Each entry maps an integer key (`intindex`) to a description string. Processes are executed in dictionary order.

```python
arrprocessscope = {1: '...', 4: '...', 42: '...', ...}
for intindex, strdesc in arrprocessscope.items():
    ...
```

The current default scope runs processes: **1, 2, 62, 60, 3, 41, 42, 43, 44, 47, 45, 46, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 32, 33, 34, 35, 40**.

Progress is tracked server-side via `cp.f_setservervariable()`. Multiple cursor objects (`cursor`, `cursor2` … `cursor5`) allow parallel DB operations within a single process.

Recent performance-sensitive T2S rebuilds now use a **staging-table rebuild + atomic rename swap** pattern instead of chunked upsert/delete synchronization. In those branches, the script builds a full `*_BUILD` table, validates it where needed, atomically swaps it into place with `RENAME TABLE`, and then drops the previous `*_OLD` table automatically after a successful swap.

Wikimedia API calls used by process `60` support the following environment variables: `WIKIMEDIA_USER_AGENT`, `WIKIMEDIA_REQUEST_DELAY_SECONDS`, `WIKIMEDIA_BACKOFF_SECONDS`, `WIKIMEDIA_MAX_RETRIES`, and `WIKIMEDIA_TIMEOUT_SECONDS`.

---

## Docker

The project ships a `Dockerfile` and a launcher script (`tmdb-movie-preprocess.sh`) for running the pipeline in a container.

### Secrets handling

Secrets (DB credentials, user agents, etc.) are **never baked into the image**. They are injected at runtime from a host-managed env file via Docker's `--env-file` flag.

- `.env` is listed in `.dockerignore` so it is excluded from the build context and cannot end up in image layers, build cache, or registries.
- The `Dockerfile` does **not** `COPY .env` and does **not** declare secrets in `ENV` lines. Only non-sensitive defaults belong in the image.
- The runtime env file is expected to live **outside the application source tree**, e.g. `/home/debian/docker/tmdb-movie-preprocess/.env`, owned and permissioned by the host operator.

### Build

```bash
docker build -t tmdb-movie-preprocess-python-app .
```

### Run

Pass secrets from a host-managed env file with `--env-file`:

```bash
docker run -d --rm \
    --network="host" \
    --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
    --name tmdb-movie-preprocess \
    tmdb-movie-preprocess-python-app
```

Adjust the path after `--env-file` to wherever the host operator stores the runtime env file for this project. Use `.env.example` as a template for the variables that must be defined.

The included `tmdb-movie-preprocess.sh` wraps build + run and already uses `--env-file` with the host path above; update that path if your deployment layout differs.

---

## Process Reference

### Process 1 — WIKIPEDIA_FORMAT_LINE

Parses the `WIKIPEDIA_FORMAT_LINE` field on `T_WC_TMDB_MOVIE` to extract technical presentation metadata.

**Reads:** `T_WC_TMDB_MOVIE.WIKIPEDIA_FORMAT_LINE`, `T_WC_TMDB_MOVIE.DAT_WIKIPEDIA_FORMAT_LINE`
**Writes:** `T_WC_TMDB_MOVIE` (technical flag columns), `T_WC_T2S_MOVIE_TECHNICAL` (medium_format + aspect_ratio junction, §12.5)

**Incremental selection (watermark).** Rather than re-parsing every movie that has a format line on every run, Process 1 only processes movies whose `WIKIPEDIA_FORMAT_LINE` was (re)stamped since the **last successful run**. The upstream crawler sets `DAT_WIKIPEDIA_FORMAT_LINE` (a `datetime`, indexed) whenever it writes `WIKIPEDIA_FORMAT_LINE`, so that column is the change marker:

```sql
SELECT ID_MOVIE, WIKIPEDIA_FORMAT_LINE FROM T_WC_TMDB_MOVIE
WHERE WIKIPEDIA_FORMAT_LINE IS NOT NULL AND WIKIPEDIA_FORMAT_LINE <> ''
  AND DAT_WIKIPEDIA_FORMAT_LINE >= DATE_SUB('<last_run>', INTERVAL 60 MINUTE)
ORDER BY ID_MOVIE ASC
```

- The watermark is the **start time of the previous successful run**, stored in the server variable `strtmdbmoviepreprocesswikipediaformatlinelastrun` and written **only after the run completes** (so a crash leaves the old watermark and the failed window is retried).
- On the **first run** (watermark empty) it falls back to a **full scan** to backfill everything.
- A configurable look-back buffer (`lngformatlinelookbackminutes`, default **60 min**) is subtracted from the watermark to absorb clock skew between the crawler host and this process, so a row stamped near the boundary is never missed (re-processing is idempotent).
- If no rows changed since the last run, the parse/junction work is skipped entirely and only the watermark is re-stamped.

**Operations:**
- Normalises the format line string (lowercase, cleaning).
- Extracts: colour/B&W flag, silent flag, 3D flag, colour technology, film technology, aspect ratio, film format, sound system, sound technology, number of audio tracks.
- Validates the resulting format line and sets `IS_VALID_FORMAT`.
- Batch-updates the source table in place.
- Rebuilds the `medium_format` + `aspect_ratio` junction rows in `T_WC_T2S_MOVIE_TECHNICAL` for the processed movies (per-movie, scoped re-sync — see §12.5), then refreshes `MOVIE_COUNT` on `T_WC_T2S_TECHNICAL`.

> **Note:** selection is keyed on `DAT_WIKIPEDIA_FORMAT_LINE`, and the **`wikipedia-crawler` repo is the guarantor** of that marker: it writes `WIKIPEDIA_FORMAT_LINE` and `DAT_WIKIPEDIA_FORMAT_LINE = NOW()` (`Europe/Paris`) in the same upsert (`wikipedia_crawler.py`, `arrcouples` write of `T_WC_TMDB_MOVIE`), so the date always advances whenever the format line changes — and in the same timezone this process stamps its watermark. To force a full re-parse, clear the watermark (set `strtmdbmoviepreprocesswikipediaformatlinelastrun` to empty / delete the row).

---

### Process 2 — WIKIPEDIA_FORMAT_LINE → T2S_MOVIE_TECHNICAL

Creates and links `T2S_TECHNICAL` dimension records from the technical fields populated by Process 1.

**Reads:** `T_WC_TMDB_MOVIE` (COLOR_TECHNOLOGY, FILM_TECHNOLOGY, SOUND_SYSTEM, SOUND_TECHNOLOGY, FILM_FORMAT)
**Writes:** `T_WC_T2S_TECHNICAL`, `T_WC_T2S_MOVIE_TECHNICAL`

**Subprocesses:** `color_technology`, `film_technology`, `sound_technology`, `sound_system`, `film_format`

**Operations:**
- Builds a lookup dictionary from `T_WC_T2S_TECHNICAL`.
- For each movie, splits pipe-separated technical values and resolves them against the lookup (exact then case-insensitive).
- Inserts new technical descriptions into `T_WC_T2S_TECHNICAL` if not found.
- Links movies to technical specs in `T_WC_T2S_MOVIE_TECHNICAL`, removing stale links.

---

### Process 3 — T2S_TOPIC

Populates the `T_WC_T2S_TOPIC` dimension from TMDb keywords.

**Reads:** `T_WC_TMDB_KEYWORD`, `T_WC_TMDB_PERSON`, `T_WC_TMDB_MOVIE_KEYWORD`, `T_WC_TMDB_SERIE_KEYWORD`, `T_WC_T2S_MOVIE`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_TMDB_KEYWORD` (counts + KPIs), `T_WC_T2S_TOPIC`, `T_WC_T2S_MOVIE_TOPIC`, `T_WC_T2S_SERIE_TOPIC`

**Subprocesses:** `en-keyword`

> **Note:** Only the `en-keyword` subprocess is active. The list/collection subprocesses (`en-list`, `fr-list`, `en-collection`, `fr-collection`) are retired — lists and collections are no longer copied into `T_WC_T2S_TOPIC` (they live in `T_WC_T2S_LIST` / `T_WC_T2S_COLLECTION` via Processes 42 / 41). The leftover `list`- and `collection`-sourced rows are unconditionally purged at the end of this process.

**Operations:**

**Rolling refresh batch (selection strategy).** Rather than reprocessing every qualifying keyword on every run, Process 3 rotates through them over a configurable cycle (default **30 days**) using the `TIM_T2S_TOPIC_REFRESH` timestamp on `T_WC_TMDB_KEYWORD` — the same pattern as the Wikidata linker in Processes 60/62. At the top of the process a single batch is selected:

```sql
SELECT ID_KEYWORD FROM T_WC_TMDB_KEYWORD
WHERE (USED_FOR_T2S_TOPIC > 0 OR USE_FOR_TAGGING > 0)
  AND (TIM_T2S_TOPIC_REFRESH IS NULL OR TIM_T2S_TOPIC_REFRESH < (NOW() - INTERVAL 30 DAY))
ORDER BY CASE WHEN TIM_T2S_TOPIC_REFRESH IS NULL THEN 0 ELSE 1 END,
         TIM_T2S_TOPIC_REFRESH ASC, ID_KEYWORD ASC
LIMIT <batch_size>
```

- The selection is restricted to keywords flagged `USED_FOR_T2S_TOPIC > 0` **OR** `USE_FOR_TAGGING > 0` (not the entire keyword table), and only those never refreshed or last refreshed more than `lngrefreshcycledays` (30) days ago.
- `LIMIT` is **auto-sized** to `ceil(qualifying_count × 1.3 / cycle_days)` so the whole qualifying set rotates within the cycle with ~30% headroom; never-refreshed rows (NULL) are processed first. The first run spreads the initial NULL backlog across the cycle instead of doing all of it at once.
- **The MOVIE_COUNT, SERIE_COUNT, KPI, and topic-build passes are all scoped to this single batch** so each keyword's counts, KPIs and topic rows are rebuilt together and stay mutually consistent.
- **Stamping (stamp-then-skip):** each keyword in the batch has `TIM_T2S_TOPIC_REFRESH` set to the current time up-front, before its topic is built. If the keyword errors out mid-processing it has already rotated out of the batch and is not retried until the next cycle.

The per-keyword work for the selected batch:
- Resets `MOVIE_COUNT`/`SERIE_COUNT` to 0 for the batch, then recomputes `MOVIE_COUNT` (from `T_WC_T2S_MOVIE`) and `SERIE_COUNT` (from `T_WC_T2S_SERIE`); updates `T_WC_TMDB_KEYWORD`.
- Computes per-keyword KPIs: `NAME_WORD_COUNT`, `IS_PERSON` (keyword name matches a `T_WC_TMDB_PERSON.NAME`), `IS_EMPTY` (total movie+serie count < 2).
- For each selected keyword, queries linked movies (`T_WC_TMDB_MOVIE_KEYWORD` ⋈ `T_WC_T2S_MOVIE`) and series (`T_WC_TMDB_SERIE_KEYWORD` ⋈ `T_WC_T2S_SERIE`), both filtered to `ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''` and ordered by `IMDB_RATING_WEIGHTED DESC`.
- Upserts the keyword into `T_WC_T2S_TOPIC` **only when it resolves to ≥ 2 elements** (movies + series). Topics resolving to 0 or 1 element are deleted if they already exist. On upsert, the `T_WC_T2S_MOVIE_TOPIC` / `T_WC_T2S_SERIE_TOPIC` junction rows are deleted and rebuilt with sequential `DISPLAY_ORDER`.
- **Stale delete:** removes `T_WC_T2S_TOPIC` rows with `TOPIC_TYPE IS NULL`, all `list`/`collection`-sourced rows, and `keyword`-sourced rows whose `ID_RECORD` is no longer in `T_WC_TMDB_KEYWORD WHERE USED_FOR_T2S_TOPIC > 0 OR USE_FOR_TAGGING > 0`.
- Post-processing: refreshes `IMDB_RATING`, `IMDB_RATING_WEIGHTED`, `POPULARITY` on `T_WC_T2S_TOPIC` (keyword topics) by averaging across linked movies, then series. The stale-delete and rating post-pass below run over the **full** `T_WC_T2S_TOPIC` table on every run, independent of the rolling batch, so keywords that lose their qualifying flag are cleaned up promptly.

> **Migration (rolling-refresh batch).** The batch rotation requires one new column on `T_WC_TMDB_KEYWORD`. If you are upgrading an existing database, run:
> ```sql
> ALTER TABLE T_WC_TMDB_KEYWORD
>   ADD COLUMN TIM_T2S_TOPIC_REFRESH datetime DEFAULT NULL AFTER TIM_WIKIPEDIA_SEARCH,
>   ADD INDEX TIM_T2S_TOPIC_REFRESH (TIM_T2S_TOPIC_REFRESH);
> ```
> On first run every keyword has `TIM_T2S_TOPIC_REFRESH = NULL` and is therefore "due"; the auto-sized `LIMIT` spreads this initial backlog across the cycle. To force an immediate full refresh of a keyword, set its `TIM_T2S_TOPIC_REFRESH` back to `NULL`.

---

### Process 4 — T2S_MOVIE

Copies filtered movie records from `T_WC_TMDB_MOVIE` into the T2S layer.

**Reads:** `T_WC_TMDB_MOVIE`
**Writes:** `T_WC_T2S_MOVIE`

**Filter:** `ADULT = 0` AND `ID_IMDB` not null/empty

**Operations:**
- Processes in chunks of 1000 records by `ID_MOVIE` range.
- `INSERT … ON DUPLICATE KEY UPDATE` for ~34 fields including title, IMDb ID, release date, ratings, Wikidata ID, technical flags, and financial data.
- Deletes records within the processed range that no longer exist in the source.

---

### Process 5 — T2S_SERIE

Copies filtered series records from `T_WC_TMDB_SERIE` into the T2S layer.

**Reads:** `T_WC_TMDB_SERIE`, `T_WC_IMDB_MOVIE_RATING_IMPORT`
**Writes:** `T_WC_T2S_SERIE`

**Filter:** `ADULT = 0` AND `ID_IMDB` not null/empty

**Operations:**
- Processes in chunks of 1000 records by `ID_SERIE` range.
- `INSERT … ON DUPLICATE KEY UPDATE` for ~30 fields.
- Additional UPDATE step: enriches `IMDB_RATING` from `T_WC_IMDB_MOVIE_RATING_IMPORT` via `ID_IMDB` join.
- Deletes records within processed range that no longer exist in source.

---

### Process 6 — T2S_PERSON

Copies filtered person records from `T_WC_TMDB_PERSON` into the T2S layer, enriched with Wikidata data.

**Reads:** `T_WC_TMDB_PERSON`, `T_WC_WIKIDATA_PERSON_V1`
**Writes:** `T_WC_T2S_PERSON`

**Filter:** `ADULT = 0` AND `ID_IMDB` not null/empty AND `ID_WIKIDATA` not null/empty

**Operations:**
- Processes in chunks of 1000 records.
- `INSERT … ON DUPLICATE KEY UPDATE` for ~24 fields.
- Additional UPDATE step: enriches `WIKIDATA_NAME`, `ALIASES`, `INSTANCE_OF` from `T_WC_WIKIDATA_PERSON_V1`.
- Deletes stale records within processed ranges.

---

### Process 7 — T2S_COMPANY

Computes movie/serie counts per production company and copies qualifying companies into the T2S layer.

**Reads:** `T_WC_TMDB_COMPANY`, `T_WC_TMDB_MOVIE_COMPANY`, `T_WC_TMDB_SERIE_COMPANY`
**Writes:** `T_WC_TMDB_COMPANY` (counts), `T_WC_T2S_COMPANY`

**Operations:**
- Computes and updates `MOVIE_COUNT` and `SERIE_COUNT` on `T_WC_TMDB_COMPANY`.
- Copies companies with at least one movie or serie into `T_WC_T2S_COMPANY` in 1000-record chunks.
- Deletes stale records.

---

### Process 48 — T2S_CHARACTER

Builds the character dimension from acting credits in `T_WC_T2S_PERSON_MOVIE` and `T_WC_T2S_PERSON_SERIE`.

**Reads:** `T_WC_T2S_PERSON_MOVIE`, `T_WC_T2S_PERSON_SERIE`, `T_WC_T2S_MOVIE`, `T_WC_T2S_SERIE`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_CHARACTER`, `T_WC_T2S_MOVIE_CHARACTER`, `T_WC_T2S_SERIE_CHARACTER`, `T_WC_T2S_PERSON_CHARACTER`

**Filter:** `CREDIT_TYPE = 'cast'` AND `DELETED = 0 (or NULL)` AND `CAST_CHARACTER` not null/empty

**Operations:**
- Inserts missing characters into `T_WC_T2S_CHARACTER` based on `CAST_CHARACTER` (no splitting).
- Rebuilds character junction tables from person-movie and person-serie acting credits.
- Updates per-character KPIs:
  - `MOVIE_COUNT`, `SERIE_COUNT`, `PERSON_COUNT`
  - `IMDB_RATING` (averaged across linked movies and series)
  - `POPULARITY` (averaged across linked persons)
- Full stale delete: removes characters no longer present in either source credit table.

### Process 8 — T2S_NETWORK

Computes serie counts per broadcast network and copies qualifying networks into the T2S layer.

**Reads:** `T_WC_TMDB_NETWORK`, `T_WC_TMDB_SERIE_NETWORK`
**Writes:** `T_WC_TMDB_NETWORK` (counts), `T_WC_T2S_NETWORK`

**Operations:**
- Computes and updates `SERIE_COUNT` on `T_WC_TMDB_NETWORK`.
- Copies networks with `SERIE_COUNT > 0` into `T_WC_T2S_NETWORK` in 1000-record chunks.
- Deletes stale records.

---

### Process 9 — T2S_PERSON_MOVIE

Links persons to movies in the T2S layer, validating that both ends exist in T2S tables.

**Reads:** `T_WC_TMDB_PERSON_MOVIE`, `T_WC_T2S_PERSON`, `T_WC_T2S_MOVIE`
**Writes:** `T_WC_T2S_PERSON_MOVIE`

**Operations:**
- Processes in chunks of 1000 records.
- Validates FK existence in `T_WC_T2S_PERSON` and `T_WC_T2S_MOVIE` before inserting.
- `INSERT … ON DUPLICATE KEY UPDATE` for credit fields: type, character, department, job, display order.
- Deletes stale records within processed ranges.

---

### Process 10 — T2S_PERSON_SERIE

Links persons to series in the T2S layer (same pattern as Process 9).

**Reads:** `T_WC_TMDB_PERSON_SERIE`, `T_WC_T2S_PERSON`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_PERSON_SERIE`

**Operations:** Same as Process 9 but for series.

---

### Process 11 — T2S_MOVIE_GENRE

Copies movie↔genre relations into the T2S layer (filtered to movies that exist in `T_WC_T2S_MOVIE`).

**Reads:** `T_WC_TMDB_MOVIE_GENRE`, `T_WC_T2S_MOVIE`
**Writes:** `T_WC_T2S_MOVIE_GENRE`

**Operations:**
- Rebuilds the full target into `T_WC_T2S_MOVIE_GENRE_BUILD` in one pass.
- Uses `ROW_NUMBER()` ranking during the rebuild to compute stable `DISPLAY_ORDER` values.
- Atomically swaps `T_WC_T2S_MOVIE_GENRE_BUILD` into place with `RENAME TABLE`.
- Drops `T_WC_T2S_MOVIE_GENRE_OLD` automatically after a successful swap.

---

### Process 12 — T2S_SERIE_GENRE

Copies serie↔genre relations into the T2S layer (filtered to series that exist in `T_WC_T2S_SERIE`).

**Reads:** `T_WC_TMDB_SERIE_GENRE`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_SERIE_GENRE`

**Operations:** Same as Process 11 but for series, using a full rebuild into `T_WC_T2S_SERIE_GENRE_BUILD`, atomic rename swap, and automatic cleanup of `T_WC_T2S_SERIE_GENRE_OLD`.

---

### Process 13 — T2S_MOVIE_COMPANY

Copies movie↔company relations into the T2S layer.

**Reads:** `T_WC_TMDB_MOVIE_COMPANY`, `T_WC_T2S_MOVIE`, `T_WC_T2S_COMPANY`
**Writes:** `T_WC_T2S_MOVIE_COMPANY`

**Operations:** Full staging-table rebuild with atomic swap; validates existence of both `ID_MOVIE` and `ID_COMPANY` in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 14 — T2S_SERIE_COMPANY

Copies serie↔company relations into the T2S layer.

**Reads:** `T_WC_TMDB_SERIE_COMPANY`, `T_WC_T2S_SERIE`, `T_WC_T2S_COMPANY`
**Writes:** `T_WC_T2S_SERIE_COMPANY`

**Operations:** Same as Process 13 but for series.

---

### Process 15 — T2S_SERIE_NETWORK

Copies serie↔network relations into the T2S layer.

**Reads:** `T_WC_TMDB_SERIE_NETWORK`, `T_WC_T2S_SERIE`, `T_WC_T2S_NETWORK`
**Writes:** `T_WC_T2S_SERIE_NETWORK`

**Operations:** Full staging-table rebuild with atomic swap; validates existence of both `ID_SERIE` and `ID_NETWORK` in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 16 — T2S_MOVIE_PRODUCTION_COUNTRY

Copies movie production countries into the T2S layer.

**Reads:** `T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY`, `T_WC_T2S_MOVIE`
**Writes:** `T_WC_T2S_MOVIE_PRODUCTION_COUNTRY`

**Operations:** Full staging-table rebuild with atomic swap; validates `ID_MOVIE` exists in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 17 — T2S_SERIE_PRODUCTION_COUNTRY

Copies serie production countries into the T2S layer.

**Reads:** `T_WC_TMDB_SERIE_PRODUCTION_COUNTRY`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_SERIE_PRODUCTION_COUNTRY`

**Operations:** Same as Process 16 but for series.

---

### Process 18 — T2S_MOVIE_SPOKEN_LANGUAGE

Copies movie spoken languages into the T2S layer.

**Reads:** `T_WC_TMDB_MOVIE_SPOKEN_LANGUAGE`, `T_WC_T2S_MOVIE`
**Writes:** `T_WC_T2S_MOVIE_SPOKEN_LANGUAGE`

**Operations:** Full staging-table rebuild with atomic swap; validates `ID_MOVIE` exists in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 19 — T2S_SERIE_SPOKEN_LANGUAGE

Copies serie spoken languages into the T2S layer.

**Reads:** `T_WC_TMDB_SERIE_SPOKEN_LANGUAGE`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_SERIE_SPOKEN_LANGUAGE`

**Operations:** Same as Process 18 but for series.

---

### Process 20 — T2S_COMPANY_IMAGE

Copies company images into the T2S layer.

**Reads:** `T_WC_TMDB_COMPANY_IMAGE`, `T_WC_T2S_COMPANY`
**Writes:** `T_WC_T2S_COMPANY_IMAGE`

**Operations:** Full staging-table rebuild with atomic swap; validates `ID_COMPANY` exists in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 21 — T2S_MOVIE_IMAGE

Copies movie images into the T2S layer.

**Reads:** `T_WC_TMDB_MOVIE_IMAGE`, `T_WC_T2S_MOVIE`
**Writes:** `T_WC_T2S_MOVIE_IMAGE`

**Operations:** Full staging-table rebuild with atomic swap; validates `ID_MOVIE` exists in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 22 — T2S_NETWORK_IMAGE

Copies network images into the T2S layer.

**Reads:** `T_WC_TMDB_NETWORK_IMAGE`, `T_WC_T2S_NETWORK`
**Writes:** `T_WC_T2S_NETWORK_IMAGE`

**Operations:** Full staging-table rebuild with atomic swap; validates `ID_NETWORK` exists in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 23 — T2S_PERSON_IMAGE

Copies person images into the T2S layer.

**Reads:** `T_WC_TMDB_PERSON_IMAGE`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_PERSON_IMAGE`

**Operations:** Full staging-table rebuild with atomic swap; validates `ID_PERSON` exists in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 24 — T2S_SERIE_IMAGE

Copies serie images into the T2S layer.

**Reads:** `T_WC_TMDB_SERIE_IMAGE`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_SERIE_IMAGE`

**Operations:** Full staging-table rebuild with atomic swap; validates `ID_SERIE` exists in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 25 — T2S_MOVIE_VIDEO

Copies movie videos into the T2S layer.

**Reads:** `T_WC_TMDB_MOVIE_VIDEO`, `T_WC_T2S_MOVIE`
**Writes:** `T_WC_T2S_MOVIE_VIDEO`

**Operations:** Full staging-table rebuild with atomic swap; validates `ID_MOVIE` exists in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 26 — T2S_SERIE_VIDEO

Copies serie videos into the T2S layer.

**Reads:** `T_WC_TMDB_SERIE_VIDEO`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_SERIE_VIDEO`

**Operations:** Same as Process 25 but for series.

---

### Process 27 — T2S_SEASON

Copies TV season records from `T_WC_TMDB_SEASON` into the T2S layer, gated by membership of the parent series in `T_WC_T2S_SERIE`.

**Reads:** `T_WC_TMDB_SEASON`, `T_WC_T2S_SERIE`, `T_WC_IMDB_MOVIE_RATING_IMPORT`
**Writes:** `T_WC_T2S_SEASON`

**Filter:** `ID_SERIE` exists in `T_WC_T2S_SERIE`

**Operations:**
- Processes in chunks of 1000 records by `ID_SEASON` range.
- `INSERT … ON DUPLICATE KEY UPDATE` for ~22 fields (renames `TITLE` → `SEASON_TITLE`; drops crawler-only `TIM_*_COMPLETED` flags).
- Enriches `IMDB_RATING` and `IMDB_RATING_WEIGHTED` from `T_WC_IMDB_MOVIE_RATING_IMPORT` via `ID_IMDB` join when the season carries an IMDb id.
- Deletes records within the processed range that no longer exist in source (or whose parent series dropped out of T2S).

---

### Process 28 — T2S_EPISODE

Copies TV episode records from `T_WC_TMDB_EPISODE` into the T2S layer, gated by membership of the parent series and season.

**Reads:** `T_WC_TMDB_EPISODE`, `T_WC_T2S_SERIE`, `T_WC_T2S_SEASON`, `T_WC_IMDB_MOVIE_RATING_IMPORT`
**Writes:** `T_WC_T2S_EPISODE`

**Filter:** `ID_SERIE` exists in `T_WC_T2S_SERIE` AND `ID_SEASON` exists in `T_WC_T2S_SEASON`

**Operations:**
- Processes in chunks of 1000 records by `ID_EPISODE` range.
- `INSERT … ON DUPLICATE KEY UPDATE` for ~27 fields (renames `TITLE` → `EPISODE_TITLE`; drops crawler-only `TIM_*_COMPLETED` flags).
- Enriches `IMDB_RATING` and `IMDB_RATING_WEIGHTED` from `T_WC_IMDB_MOVIE_RATING_IMPORT` (most episodes lack an `ID_IMDB`, so this populates a sparse minority).
- Deletes records within the processed range that no longer exist in source.

---

### Process 29 — T2S_PERSON_SEASON

Links persons to seasons in the T2S layer (cast, crew, aggregate-credits roles), validating that person, series, and season all exist in T2S.

**Reads:** `T_WC_TMDB_PERSON_SEASON`, `T_WC_T2S_PERSON`, `T_WC_T2S_SERIE`, `T_WC_T2S_SEASON`
**Writes:** `T_WC_T2S_PERSON_SEASON`

**Operations:**
- Processes in chunks of 1000 records by `ID_TMDB_PERSON_SEASON` range.
- `INSERT … ON DUPLICATE KEY UPDATE` for credit fields: type, character, department, job, `TOTAL_EPISODE_COUNT`, display order.
- The target row id is the source `ID_TMDB_PERSON_SEASON` (same convention as Processes 9 / 10).
- Deletes stale records within processed ranges.

---

### Process 31 — T2S_PERSON_EPISODE

Links persons to episodes in the T2S layer (cast, crew, guest stars), validating that person, series, season, and episode all exist in T2S.

**Reads:** `T_WC_TMDB_PERSON_EPISODE`, `T_WC_T2S_PERSON`, `T_WC_T2S_SERIE`, `T_WC_T2S_SEASON`, `T_WC_T2S_EPISODE`
**Writes:** `T_WC_T2S_PERSON_EPISODE`

**Operations:**
- Same pattern as Process 29, with the four-way FK existence gate.
- Carries `CREDIT_TYPE ∈ {cast, crew, guest}`.

---

### Process 32 — T2S_SEASON_IMAGE

Copies season images (posters, backdrops) into the T2S layer.

**Reads:** `T_WC_TMDB_SEASON_IMAGE`, `T_WC_T2S_SEASON`
**Writes:** `T_WC_T2S_SEASON_IMAGE`

**Operations:** Full staging-table rebuild with atomic swap; validates `ID_SEASON` exists in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 33 — T2S_EPISODE_IMAGE

Copies episode stills into the T2S layer.

**Reads:** `T_WC_TMDB_EPISODE_IMAGE`, `T_WC_T2S_EPISODE`
**Writes:** `T_WC_T2S_EPISODE_IMAGE`

**Operations:** Same as Process 32 but for episodes; validates `ID_EPISODE` exists in T2S.

---

### Process 34 — T2S_SEASON_VIDEO

Copies season videos (trailers, clips) into the T2S layer.

**Reads:** `T_WC_TMDB_SEASON_VIDEO`, `T_WC_T2S_SEASON`
**Writes:** `T_WC_T2S_SEASON_VIDEO`

**Operations:** Full staging-table rebuild with atomic swap; validates `ID_SEASON` exists in T2S; automatically drops the previous `_OLD` table after a successful swap.

---

### Process 35 — T2S_EPISODE_VIDEO

Copies episode videos into the T2S layer.

**Reads:** `T_WC_TMDB_EPISODE_VIDEO`, `T_WC_T2S_EPISODE`
**Writes:** `T_WC_T2S_EPISODE_VIDEO`

**Operations:** Same as Process 34 but for episodes; validates `ID_EPISODE` exists in T2S.

---

### Process 60 — Link Wikidata items to topics

Links TMDb keywords to Wikidata items before process `3` builds `T_WC_T2S_TOPIC`, and spreads the work over rolling daily batches.

**Reads:** `T_WC_TMDB_KEYWORD`, Wikipedia API, Wikidata API
**Writes:** `T_WC_TMDB_KEYWORD`

**Selection strategy:**
- Processes up to `3000` keywords per run.
- Selects rows where `NAME` is not null/empty.
- Orders by `TIM_WIKIPEDIA_SEARCH ASC, ID_KEYWORD ASC` so never-checked and oldest-checked keywords are processed first.
- Updates `TIM_WIKIPEDIA_SEARCH` for matched, unmatched, and exception cases so the batch rotates across the full keyword table over time.

**Operations:**
- Searches Wikipedia candidates for each keyword and resolves the selected page to a Wikidata item.
- Validates the Wikidata entity type against a blocked `P31` set to reject media-work entity classes that should not become generic topics.
- Stores the matched Wikidata ID, English Wikidata label, and match confidence on `T_WC_TMDB_KEYWORD`.
- Persists the last attempted search timestamp in `TIM_WIKIPEDIA_SEARCH` even when no match is found or a request fails.
- Uses a throttled Wikimedia request wrapper with retry/backoff on HTTP `429` responses.

**Updated columns on `T_WC_TMDB_KEYWORD`:**
- `ID_WIKIDATA`
- `WIKIDATA_LABEL`
- `CONFIDENCE`
- `TIM_WIKIPEDIA_SEARCH`

---

### Process 62 — Link Wikidata items to T2S technical

Backfills `ID_WIKIDATA` on `T_WC_T2S_TECHNICAL` by searching Wikipedia for each technical entity's `DESCRIPTION` and resolving the matched page to a Wikidata item. Also enriches rows whose `ID_WIKIDATA` was set manually (or by a legacy process) but whose `WIKIDATA_LABEL` is still empty. Runs immediately after process `2` so downstream consumers see fresh Wikidata IDs within the same run.

**Reads:** `T_WC_T2S_TECHNICAL`, Wikipedia API, Wikidata API
**Writes:** `T_WC_T2S_TECHNICAL`

**Selection strategy:**
- Selects rows where `DESCRIPTION` is not null/empty and `DELETED` is null or `0`, and either
    - `ID_WIKIDATA` is null/empty (linking branch), or
    - `ID_WIKIDATA` is set but `WIKIDATA_LABEL` is null/empty (enrichment branch for pre-existing manual links).
- Orders by `TIM_WIKIPEDIA_SEARCH ASC, ID_TECHNICAL ASC` so never-checked and oldest-checked rows are processed first.
- Updates `TIM_WIKIPEDIA_SEARCH` for matched, unmatched, and exception cases so no-match rows do not block the queue on retry.

**Operations:**
- **Linking branch** (`ID_WIKIDATA` empty) — reuses the topic-linking helper (`f_linktmdbkeywordtowikidata`) so the candidate search, page resolution, and `P31` blocked-types filter behave identically to Process 60. Stores the matched Wikidata ID, English Wikidata label, and match confidence (the fuzzy score returned by the helper).
- **Enrichment branch** (`ID_WIKIDATA` already set) — trusts the existing QID, skips the Wikipedia search, and calls `f_wikidataentitysummary` directly to fetch the English label. Stores `WIKIDATA_LABEL` and `CONFIDENCE = 1.0` (trusted manual link). The `P31` blocked-types filter is intentionally bypassed for these rows because a human chose the QID on purpose.
- Persists the last attempted search timestamp in `TIM_WIKIPEDIA_SEARCH` even when no label is resolved or a request fails.
- Uses the shared throttled Wikimedia request wrapper with retry/backoff on HTTP `429` responses.

**Updated columns on `T_WC_T2S_TECHNICAL`:**
- `ID_WIKIDATA`
- `WIKIDATA_LABEL`
- `CONFIDENCE`
- `TIM_WIKIPEDIA_SEARCH`

> If you are upgrading an existing database, run:
> ```sql
> ALTER TABLE T_WC_T2S_TECHNICAL
>   ADD COLUMN WIKIDATA_LABEL varchar(255) DEFAULT NULL AFTER ID_WIKIDATA,
>   ADD COLUMN CONFIDENCE double DEFAULT NULL AFTER WIKIDATA_LABEL,
>   ADD COLUMN TIM_WIKIPEDIA_SEARCH datetime DEFAULT NULL AFTER CONFIDENCE;
> ```

---

### Process 30 — TMDB_MOVIE_LANG_META

Generates language-specific, NLP-preprocessed metadata for movies. Runs daily or every Wednesday.

**Reads:** `T_WC_TMDB_MOVIE`, `T_WC_TMDB_MOVIE_LANG`, `T_WC_TMDB_KEYWORD`, `T_WC_WIKIDATA_ITEM_V1`
**Writes:** `T_WC_TMDB_MOVIE_LANG_META`, `T_WC_TMDB_MOVIE_LANG_PREPROCESSED`

**Filter:** `ID_IMDB` and `ID_WIKIDATA` both not null/empty

**Operations:**
- For each movie, retrieves localised title and overview.
- Lemmatised keywords and overview text using the French spaCy model (`fr_core_news_lg`). _(Removed — the spaCy dependency is no longer part of this project.)_
- Processes format line technical specs.
- Inserts normalised, language-specific records into `T_WC_TMDB_MOVIE_LANG_META`.
- Inserts preprocessed text into `T_WC_TMDB_MOVIE_LANG_PREPROCESSED` for similarity analysis.

---

### Process 40 — T2S_ITEM

Copies Wikidata item records (English + French labels) into the T2S layer.

**Reads:** `T_WC_WIKIDATA_ITEM_V1`
**Writes:** `T_WC_T2S_ITEM`

**Operations:**
- Rebuilds the full target in `T_WC_T2S_ITEM_BUILD` from `T_WC_WIKIDATA_ITEM_V1`.
- Preserves `WIKIPEDIA_IMAGE_PATH` and `INSTANCE_OF` from the English Wikidata source rows.
- Applies French-label enrichment to the build table before the atomic rename swap.
- Atomically swaps the build table into place and automatically drops `T_WC_T2S_ITEM_OLD` after a successful swap.

---

### Process 41 — T2S_COLLECTION

Populates `T_WC_T2S_COLLECTION` from TMDb lists and collections, with linked movies and series.

**Reads:** `T_WC_TMDB_LIST`, `T_WC_TMDB_LIST_LANG`, `T_WC_TMDB_COLLECTION`, `T_WC_TMDB_COLLECTION_LANG`, `T_WC_CUSTOM_LIST` (TARGET_TABLE = 2), `T_WC_TMDB_MOVIE`, `T_WC_TMDB_SERIE`, `T_WC_T2S_MOVIE`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_COLLECTION`, `T_WC_T2S_MOVIE_COLLECTION`, `T_WC_T2S_SERIE_COLLECTION`

**Subprocesses:** `en-list`, `fr-list`, `en-collection`, `fr-collection`, `custom-collection`

**Operations:**
- For each list/collection record, queries associated movies and series filtered by `ADULT = 0` and `ID_WIKIDATA IS NOT NULL`.
- **en-list / fr-list:** Movie links in `T_WC_T2S_MOVIE_COLLECTION` are written in `DAT_RELEASE ASC` order; series links in `T_WC_T2S_SERIE_COLLECTION` are written in `DAT_FIRST_AIR DESC` order.
- **en-collection / fr-collection:** Movie links in `T_WC_T2S_MOVIE_COLLECTION` are written in `DAT_RELEASE ASC` order.
- **custom-collection:** Processes records from `T_WC_CUSTOM_LIST` where `TARGET_TABLE = 2`. Elements are resolved using up to three cumulative mechanisms:
  - **Mechanism 1 (IMDb list):** Parses `tt\d+` IDs from the `ID_IMDB_LIST` field; preserves input order via SQL `FIELD()`.
  - **Mechanism 2 (Wikidata):** Extracts a `P\d+` property and `Q\d+` item from `WIKIDATA_PROPERTIES`; joins against `T_WC_WIKIDATA_ITEM_PROPERTY`.
  - **Mechanism 3 (TMDb keyword):** Parses `T_WC_TMDB_KEYWORD.NAME = '...'` from `TMDB_ELEMENTS`; joins against `T_WC_TMDB_MOVIE_KEYWORD` / `T_WC_TMDB_SERIE_KEYWORD`.
- **custom-collection ordering:** `SORT_BY` controls display order for both movies and series:
  - `1` = original IMDb list order ascending
  - `2` = original IMDb list order descending
  - `3` = `IMDB_RATING_WEIGHTED` ascending
  - `4` = `IMDB_RATING_WEIGHTED` descending
  - `5` = release date ascending (`DAT_RELEASE` for movies, `DAT_FIRST_AIR` for series)
  - `6` = release date descending (`DAT_RELEASE` for movies, `DAT_FIRST_AIR` for series)
- **custom-collection rating source:** `IMDB_RATING_WEIGHTED` is read from `T_WC_T2S_MOVIE` / `T_WC_T2S_SERIE` for score-based ordering, while release dates and IMDb IDs still come from `T_WC_TMDB_MOVIE` / `T_WC_TMDB_SERIE`.
- When a custom collection resolves a Wikidata `Q...` item, `ID_WIKIDATA` and `WIKIPEDIA_IMAGE_PATH` are populated using the shared helper lookup across Wikidata item/movie/serie/person tables.
- Inserts/updates the collection record, then upserts linked movie and serie entries with sequential display order based on the SQL result order.
- Skips records with fewer than 2 total elements; deletes any existing record for those.
- Post-processing: updates `IMDB_RATING` and `IMDB_RATING_WEIGHTED` on `T_WC_T2S_COLLECTION` from linked movies.
- Full stale delete: removes `T_WC_T2S_COLLECTION` rows whose source record is no longer present in the corresponding TMDb source table or `T_WC_CUSTOM_LIST`.

---

### Process 42 — T2S_LIST

Populates `T_WC_T2S_LIST` from TMDb lists and custom lists, with linked movies and series.

**Reads:** `T_WC_TMDB_LIST`, `T_WC_TMDB_LIST_LANG`, `T_WC_CUSTOM_LIST` (TARGET_TABLE = 1), `T_WC_TMDB_MOVIE`, `T_WC_TMDB_SERIE`, `T_WC_T2S_MOVIE`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_LIST`, `T_WC_T2S_MOVIE_LIST`, `T_WC_T2S_SERIE_LIST`

**Subprocesses:** `en-list`, `fr-list`, `custom-list`, `list-delete`

**Operations:**
- **en-list / fr-list:** Copies TMDb lists (English and French) with their linked movies and series.
- **custom-list:** Processes records from `T_WC_CUSTOM_LIST` where `TARGET_TABLE = 1`. Elements are resolved using up to three cumulative mechanisms:
  - **Mechanism 1 (IMDb list):** Parses `tt\d+` IDs from the `ID_IMDB_LIST` field; preserves input order via SQL `FIELD()`.
  - **Mechanism 2 (Wikidata):** Extracts a `P\d+` property and `Q\d+` item from `WIKIDATA_PROPERTIES`; joins against `T_WC_WIKIDATA_ITEM_PROPERTY`.
  - **Mechanism 3 (TMDb keyword):** Parses `T_WC_TMDB_KEYWORD.NAME = '...'` from `TMDB_ELEMENTS`; joins against `T_WC_TMDB_MOVIE_KEYWORD` / `T_WC_TMDB_SERIE_KEYWORD`.
- **custom-list ordering:** `SORT_BY` controls display order for both movies and series:
  - `1` = original IMDb list order ascending
  - `2` = original IMDb list order descending
  - `3` = `IMDB_RATING_WEIGHTED` ascending
  - `4` = `IMDB_RATING_WEIGHTED` descending
  - `5` = release date ascending (`DAT_RELEASE` for movies, `DAT_FIRST_AIR` for series)
  - `6` = release date descending (`DAT_RELEASE` for movies, `DAT_FIRST_AIR` for series)
- When multiple mechanisms match, results are combined with `UNION ALL`, deduplicated by element ID, and ordered according to `SORT_BY`.
- **custom-list rating source:** `IMDB_RATING_WEIGHTED` is read from `T_WC_T2S_MOVIE` / `T_WC_T2S_SERIE` for score-based ordering, while release dates and IMDb IDs still come from `T_WC_TMDB_MOVIE` / `T_WC_TMDB_SERIE`.
- **list-delete:** Removes list records that no longer have a corresponding source entry.
- All mechanisms apply filter: `ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''`.
- When a custom list resolves a Wikidata `Q...` item, `ID_WIKIDATA` and `WIKIPEDIA_IMAGE_PATH` are populated using the shared helper lookup across Wikidata item/movie/serie/person tables.
- Post-processing: updates `IMDB_RATING` and `IMDB_RATING_WEIGHTED` on `T_WC_T2S_LIST` from linked movies.

---

### Process 43 — T2S_GROUP

Builds person groups from Wikidata membership, employment, and sports-team relationships, and from custom group definitions.

**Reads:** `T_WC_WIKIDATA_ITEM_PROPERTY`, `T_WC_WIKIDATA_ITEM_V1`, `T_WC_CUSTOM_LIST` (TARGET_TABLE = 3), `T_WC_TMDB_PERSON`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_GROUP`, `T_WC_T2S_PERSON_GROUP`

**Subprocesses / Wikidata properties:**
- `en-group` → P463 (member of)
- `en-employer` → P108 (employer)
- `en-sports-team` → P54 (member of sports team)
- `custom-group` → `T_WC_CUSTOM_LIST` (TARGET_TABLE = 3)

**Operations:**
- For each Wikidata property/item pair, retrieves the item's English and French labels and description; `WIKIPEDIA_IMAGE_PATH` is resolved through the shared helper lookup.
- Inserts/updates `T_WC_T2S_GROUP`.
- Queries persons linked to the item via Wikidata (ordered by popularity) and upserts into `T_WC_T2S_PERSON_GROUP`.
- **custom-group:** Builds groups from `T_WC_CUSTOM_LIST` (TARGET_TABLE = 3) and resolves member persons using up to three cumulative mechanisms (IMDb list `nm\d+`, Wikidata property/item, or a TMDb person name expression).
- When a custom group resolves a Wikidata `Q...` item, `ID_WIKIDATA` is set from that item and `WIKIPEDIA_IMAGE_PATH` is populated through the same helper lookup.
- **custom-group ordering:** `SORT_BY` controls display order for persons:
  - `1` = original IMDb list order ascending
  - `2` = original IMDb list order descending
  - `3` = `IMDB_RATING`/`POPULARITY` ascending
  - `4` = `IMDB_RATING`/`POPULARITY` descending
  - `5` = `DAT_RELEASE`/`BIRTHDAY` ascending
  - `6` = `DAT_RELEASE`/`BIRTHDAY` descending
- Full stale delete: removes custom groups whose source record no longer exists in `T_WC_CUSTOM_LIST`.

---

### Process 44 — T2S_AWARD

Builds award records from the Wikidata "award received" property (P166).

**Reads:** `T_WC_WIKIDATA_ITEM_PROPERTY`, `T_WC_WIKIDATA_ITEM_V1`, `T_WC_T2S_MOVIE`, `T_WC_T2S_SERIE`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_AWARD`, `T_WC_T2S_MOVIE_AWARD`, `T_WC_T2S_SERIE_AWARD`, `T_WC_T2S_PERSON_AWARD`

**Operations:**
- Selects all distinct Wikidata items used as values of property P166.
- For each award item, retrieves English/French label and description; `WIKIPEDIA_IMAGE_PATH` is resolved with the shared helper lookup.
- Inserts/updates `T_WC_T2S_AWARD`.
- Links movies and series that received the award in `IMDB_RATING_WEIGHTED DESC` order, and links persons in their existing person ordering, into the respective junction tables with incremental display order.
- Post-processing: updates average `IMDB_RATING`, `IMDB_RATING_WEIGHTED`, and `POPULARITY` on `T_WC_T2S_AWARD` from linked entities.

---

### Process 45 — T2S_MOVEMENT

Populates `T_WC_T2S_MOVEMENT` from custom lists that define cinematic movements.

**Reads:** `T_WC_CUSTOM_LIST` (TARGET_TABLE = 4), `T_WC_TMDB_MOVIE`, `T_WC_TMDB_SERIE`, `T_WC_T2S_MOVIE`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_MOVEMENT`, `T_WC_T2S_MOVIE_MOVEMENT`, `T_WC_T2S_SERIE_MOVEMENT`

**Subprocesses:** `custom-movement`, `movement-delete`

**Operations:**
- **custom-movement:** For each active custom list (TARGET_TABLE = 4, DELETED = 0), resolves member movies and series using the same three cumulative mechanisms as Process 42 (IMDb list, Wikidata property/item, TMDb keyword).
- **custom-movement ordering:** `SORT_BY` controls display order for both movies and series:
  - `1` = original IMDb list order ascending
  - `2` = original IMDb list order descending
  - `3` = `IMDB_RATING_WEIGHTED` ascending
  - `4` = `IMDB_RATING_WEIGHTED` descending
  - `5` = release date ascending (`DAT_RELEASE` for movies, `DAT_FIRST_AIR` for series)
  - `6` = release date descending (`DAT_RELEASE` for movies, `DAT_FIRST_AIR` for series)
- **custom-movement rating source:** `IMDB_RATING_WEIGHTED` is read from `T_WC_T2S_MOVIE` / `T_WC_T2S_SERIE` for score-based ordering, while release dates and IMDb IDs still come from `T_WC_TMDB_MOVIE` / `T_WC_TMDB_SERIE`.
- When a custom movement resolves a Wikidata `Q...` item, `ID_WIKIDATA` and `WIKIPEDIA_IMAGE_PATH` are populated using the shared helper lookup across Wikidata item/movie/serie/person tables.
- Skips records with fewer than 2 total elements; deletes any existing record for those.
- **movement-delete:** Removes movement records whose source custom list no longer exists or has been deleted.
- Post-processing: updates `IMDB_RATING` and `IMDB_RATING_WEIGHTED` on `T_WC_T2S_MOVEMENT` from linked movies; cascades orphan cleanup to `T_WC_T2S_MOVIE_MOVEMENT` and `T_WC_T2S_SERIE_MOVEMENT`.
- All mechanisms apply filter: `ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''`.

---

### Process 46 — T2S_DEATH

Builds death-related dimension records from Wikidata death properties.

**Reads:** `T_WC_WIKIDATA_ITEM_PROPERTY`, `T_WC_WIKIDATA_ITEM_V1`, `T_WC_TMDB_PERSON`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_DEATH`, `T_WC_T2S_PERSON_DEATH`

**Subprocesses / Wikidata properties:**
- `en-cause-of-death` → P509
- `en-manner-of-death` → P1196

**Operations:**
- For each Wikidata item used as a value of P509/P1196, retrieves English/French labels and description; `WIKIPEDIA_IMAGE_PATH` is resolved with the shared helper lookup.
- Inserts/updates `T_WC_T2S_DEATH` and links persons into `T_WC_T2S_PERSON_DEATH` ordered by person popularity.
- Full stale delete: removes `T_WC_T2S_DEATH` rows no longer present in `T_WC_WIKIDATA_ITEM_PROPERTY` for the corresponding property.

---

### Process 47 — T2S_NOMINATION

Builds nomination records from the Wikidata "nominated for" property (P1411).

**Reads:** `T_WC_WIKIDATA_ITEM_PROPERTY`, `T_WC_WIKIDATA_ITEM_V1`, `T_WC_T2S_MOVIE`, `T_WC_T2S_SERIE`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_NOMINATION`, `T_WC_T2S_MOVIE_NOMINATION`, `T_WC_T2S_SERIE_NOMINATION`, `T_WC_T2S_PERSON_NOMINATION`

**Operations:**
- Selects all distinct Wikidata items used as values of property P1411.
- For each nomination item, retrieves English/French label and description; `WIKIPEDIA_IMAGE_PATH` is resolved with the shared helper lookup.
- Inserts/updates `T_WC_T2S_NOMINATION`.
- Links movies, series, and persons that have the nomination (via Wikidata property join) into the respective junction tables with incremental display order.
- Full stale delete: removes `T_WC_T2S_NOMINATION` rows no longer present in `T_WC_WIKIDATA_ITEM_PROPERTY` for P1411.

---

## Common Patterns

| Pattern | Description |
|---------|-------------|
| Chunk processing | Most copy processes iterate over source IDs in batches of 1000 to avoid memory pressure. |
| `INSERT … ON DUPLICATE KEY UPDATE` | Idempotent upsert — safe to re-run without duplicating data. |
| Staging rebuild + atomic swap | Processes 7, 8, 11–26, 32–35, and 40 rebuild eligible targets into `*_BUILD`, atomically swap with `RENAME TABLE`, and automatically drop `*_OLD` after success. |
| `cp.f_sqlupdatearray()` | Generic upsert helper from the `citizenphil` module. |
| Multi-mechanism element resolution | Processes 41 (custom-collection), 42, and 45 combine multiple sources with `UNION ALL` + `GROUP BY`. |
| Wikidata filter | Any movie or serie written to a T2S junction table must satisfy `ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''`. |
| Shared Wikidata image lookup | `f_getwikidataimagepath()` resolves the first non-empty `WIKIPEDIA_IMAGE_PATH` from `T_WC_WIKIDATA_ITEM_V1`, `T_WC_WIKIDATA_MOVIE_V1`, `T_WC_WIKIDATA_SERIE_V1`, then `T_WC_WIKIDATA_PERSON_V1`. |
| Full stale delete | Some dimension processes delete parent rows whose source record (TMDb list/collection/custom list or Wikidata property) no longer exists, in addition to orphan link cleanup. |
| Server variables | `cp.f_setservervariable()` persists the current process/record for external monitoring. |

---

## Source → Target Table Map

| Source tables | Target T2S tables |
|---------------|-------------------|
| T_WC_TMDB_MOVIE | T_WC_T2S_MOVIE, T_WC_TMDB_MOVIE_LANG_META |
| T_WC_TMDB_SERIE | T_WC_T2S_SERIE |
| T_WC_TMDB_PERSON | T_WC_T2S_PERSON |
| T_WC_TMDB_COMPANY | T_WC_T2S_COMPANY |
| T_WC_TMDB_NETWORK | T_WC_T2S_NETWORK |
| T_WC_TMDB_PERSON_MOVIE | T_WC_T2S_PERSON_MOVIE |
| T_WC_TMDB_PERSON_SERIE | T_WC_T2S_PERSON_SERIE |
| T_WC_TMDB_MOVIE_GENRE | T_WC_T2S_MOVIE_GENRE |
| T_WC_TMDB_SERIE_GENRE | T_WC_T2S_SERIE_GENRE |
| T_WC_TMDB_MOVIE_COMPANY | T_WC_T2S_MOVIE_COMPANY |
| T_WC_TMDB_SERIE_COMPANY | T_WC_T2S_SERIE_COMPANY |
| T_WC_TMDB_SERIE_NETWORK | T_WC_T2S_SERIE_NETWORK |
| T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY | T_WC_T2S_MOVIE_PRODUCTION_COUNTRY |
| T_WC_TMDB_SERIE_PRODUCTION_COUNTRY | T_WC_T2S_SERIE_PRODUCTION_COUNTRY |
| T_WC_TMDB_MOVIE_SPOKEN_LANGUAGE | T_WC_T2S_MOVIE_SPOKEN_LANGUAGE |
| T_WC_TMDB_SERIE_SPOKEN_LANGUAGE | T_WC_T2S_SERIE_SPOKEN_LANGUAGE |
| T_WC_TMDB_COMPANY_IMAGE | T_WC_T2S_COMPANY_IMAGE |
| T_WC_TMDB_MOVIE_IMAGE | T_WC_T2S_MOVIE_IMAGE |
| T_WC_TMDB_NETWORK_IMAGE | T_WC_T2S_NETWORK_IMAGE |
| T_WC_TMDB_PERSON_IMAGE | T_WC_T2S_PERSON_IMAGE |
| T_WC_TMDB_SERIE_IMAGE | T_WC_T2S_SERIE_IMAGE |
| T_WC_TMDB_MOVIE_VIDEO | T_WC_T2S_MOVIE_VIDEO |
| T_WC_TMDB_SERIE_VIDEO | T_WC_T2S_SERIE_VIDEO |
| T_WC_TMDB_SEASON | T_WC_T2S_SEASON |
| T_WC_TMDB_EPISODE | T_WC_T2S_EPISODE |
| T_WC_TMDB_PERSON_SEASON | T_WC_T2S_PERSON_SEASON |
| T_WC_TMDB_PERSON_EPISODE | T_WC_T2S_PERSON_EPISODE |
| T_WC_TMDB_SEASON_IMAGE | T_WC_T2S_SEASON_IMAGE |
| T_WC_TMDB_EPISODE_IMAGE | T_WC_T2S_EPISODE_IMAGE |
| T_WC_TMDB_SEASON_VIDEO | T_WC_T2S_SEASON_VIDEO |
| T_WC_TMDB_EPISODE_VIDEO | T_WC_T2S_EPISODE_VIDEO |
| T_WC_TMDB_LIST / T_WC_CUSTOM_LIST (TARGET_TABLE=1) | T_WC_T2S_LIST, T_WC_T2S_MOVIE_LIST, T_WC_T2S_SERIE_LIST |
| T_WC_TMDB_COLLECTION / T_WC_CUSTOM_LIST (TARGET_TABLE=2) | T_WC_T2S_COLLECTION, T_WC_T2S_MOVIE_COLLECTION, T_WC_T2S_SERIE_COLLECTION |
| T_WC_TMDB_KEYWORD / T_WC_TMDB_LIST / T_WC_TMDB_COLLECTION | T_WC_T2S_TOPIC |
| T_WC_TMDB_MOVIE (technical fields) | T_WC_T2S_TECHNICAL, T_WC_T2S_MOVIE_TECHNICAL |
| T_WC_WIKIDATA_ITEM_V1 | T_WC_T2S_ITEM |
| T_WC_WIKIDATA_ITEM_PROPERTY (P463, P108) / T_WC_CUSTOM_LIST (TARGET_TABLE=3) | T_WC_T2S_GROUP, T_WC_T2S_PERSON_GROUP |
| T_WC_WIKIDATA_ITEM_PROPERTY (P166) | T_WC_T2S_AWARD, T_WC_T2S_MOVIE_AWARD, T_WC_T2S_SERIE_AWARD, T_WC_T2S_PERSON_AWARD |
| T_WC_CUSTOM_LIST (TARGET_TABLE=4) | T_WC_T2S_MOVEMENT, T_WC_T2S_MOVIE_MOVEMENT, T_WC_T2S_SERIE_MOVEMENT |
| T_WC_WIKIDATA_ITEM_PROPERTY (P509, P1196) | T_WC_T2S_DEATH, T_WC_T2S_PERSON_DEATH |
| T_WC_WIKIDATA_ITEM_PROPERTY (P1411) | T_WC_T2S_NOMINATION, T_WC_T2S_MOVIE_NOMINATION, T_WC_T2S_SERIE_NOMINATION, T_WC_T2S_PERSON_NOMINATION |
| T_WC_T2S_PERSON_MOVIE / T_WC_T2S_PERSON_SERIE (acting credits) | T_WC_T2S_CHARACTER, T_WC_T2S_MOVIE_CHARACTER, T_WC_T2S_SERIE_CHARACTER, T_WC_T2S_PERSON_CHARACTER |
