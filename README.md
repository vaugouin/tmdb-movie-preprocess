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

The default scope runs processes: **1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 40, 41, 42, 43, 44, 45, 46, 47**.

Progress is tracked server-side via `cp.f_setservervariable()`. Multiple cursor objects (`cursor`, `cursor2` … `cursor5`) allow parallel DB operations within a single process.

---

## Process Reference

### Process 1 — WIKIPEDIA_FORMAT_LINE

Parses the `WIKIPEDIA_FORMAT_LINE` field on `T_WC_TMDB_MOVIE` to extract technical presentation metadata.

**Reads:** `T_WC_TMDB_MOVIE.WIKIPEDIA_FORMAT_LINE`
**Writes:** `T_WC_TMDB_MOVIE` (technical flag columns)

**Operations:**
- Normalises the format line string (lowercase, cleaning).
- Extracts: colour/B&W flag, silent flag, 3D flag, colour technology, film technology, aspect ratio, film format, sound system, sound technology, number of audio tracks.
- Validates the resulting format line and sets `IS_VALID_FORMAT`.
- Batch-updates the source table in place.

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

Populates the `T_WC_T2S_TOPIC` dimension from TMDb lists, collections, and keywords.

**Reads:** `T_WC_TMDB_LIST`, `T_WC_TMDB_LIST_LANG`, `T_WC_TMDB_COLLECTION`, `T_WC_TMDB_COLLECTION_LANG`, `T_WC_TMDB_KEYWORD`, `T_WC_TMDB_PERSON`
**Writes:** `T_WC_TMDB_KEYWORD` (counts), `T_WC_T2S_TOPIC`

**Subprocesses:** `en-list`, `fr-list`, `en-collection`, `fr-collection`, `en-keyword`

**Operations:**
- Computes `MOVIE_COUNT` and `SERIE_COUNT` per keyword; updates `T_WC_TMDB_KEYWORD`.
- Computes per-keyword KPIs: `NAME_WORD_COUNT`, `IS_PERSON` (keyword matches a person name), `IS_EMPTY` (total count < 2).
- Copies TMDb lists (English + French), collections (English + French), and keywords into `T_WC_T2S_TOPIC`.

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
- Additional UPDATE step: enriches `IMDB_RATING` / `IMDB_RATING_ADJUSTED` from `T_WC_IMDB_MOVIE_RATING_IMPORT` via `ID_IMDB` join.
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

### Process 20 — TMDB_KEYWORD *(stub)*

Reserved for keyword processing. Not yet implemented.

---

### Process 30 — TMDB_MOVIE_LANG_META

Generates language-specific, NLP-preprocessed metadata for movies. Runs daily or every Wednesday.

**Reads:** `T_WC_TMDB_MOVIE`, `T_WC_TMDB_MOVIE_LANG`, `T_WC_TMDB_KEYWORD`, `T_WC_WIKIDATA_ITEM_V1`
**Writes:** `T_WC_TMDB_MOVIE_LANG_META`, `T_WC_TMDB_MOVIE_LANG_PREPROCESSED`

**Filter:** `ID_IMDB` and `ID_WIKIDATA` both not null/empty

**Operations:**
- For each movie, retrieves localised title and overview.
- Lemmatises keywords and overview text using the French spaCy model (`fr_core_news_lg`).
- Processes format line technical specs.
- Inserts normalised, language-specific records into `T_WC_TMDB_MOVIE_LANG_META`.
- Inserts preprocessed text into `T_WC_TMDB_MOVIE_LANG_PREPROCESSED` for similarity analysis.

---

### Process 40 — T2S_ITEM

Copies Wikidata item records (English + French labels) into the T2S layer.

**Reads:** `T_WC_WIKIDATA_ITEM_V1`
**Writes:** `T_WC_T2S_ITEM`

**Operations:**
- Processes in chunks of 1000 records by `ID_ROW`.
- `INSERT … ON DUPLICATE KEY UPDATE` for English item fields: `ID_WIKIDATA`, `ITEM_LABEL`, `ALIASES`, `DESCRIPTION`, `WIKIPEDIA_IMAGE_PATH`, `INSTANCE_OF`.
- Additional UPDATE step: adds `ITEM_LABEL_FR` from the French rows of the same source table.
- Deletes stale records.

---

### Process 41 — T2S_COLLECTION

Populates `T_WC_T2S_COLLECTION` from TMDb lists and collections, with linked movies and series.

**Reads:** `T_WC_TMDB_LIST`, `T_WC_TMDB_LIST_LANG`, `T_WC_TMDB_COLLECTION`, `T_WC_TMDB_COLLECTION_LANG`, `T_WC_TMDB_MOVIE`, `T_WC_TMDB_SERIE`
**Writes:** `T_WC_T2S_COLLECTION`, `T_WC_T2S_MOVIE_COLLECTION`, `T_WC_T2S_SERIE_COLLECTION`

**Subprocesses:** `en-list`, `fr-list`, `en-collection`, `fr-collection`

**Operations:**
- For each list/collection record, queries associated movies and series filtered by `ADULT = 0` and `ID_WIKIDATA IS NOT NULL`.
- Inserts/updates the collection record, then upserts linked movie and serie entries with display order.
- Skips records with fewer than 2 total elements; deletes any existing record for those.
- Full stale delete: removes `T_WC_T2S_COLLECTION` rows whose source record is no longer present in the corresponding TMDb source table.

---

### Process 42 — T2S_LIST

Populates `T_WC_T2S_LIST` from TMDb lists and custom lists, with linked movies and series.

**Reads:** `T_WC_TMDB_LIST`, `T_WC_TMDB_LIST_LANG`, `T_WC_CUSTOM_LIST` (TARGET_TABLE = 1), `T_WC_TMDB_MOVIE`, `T_WC_TMDB_SERIE`
**Writes:** `T_WC_T2S_LIST`, `T_WC_T2S_MOVIE_LIST`, `T_WC_T2S_SERIE_LIST`

**Subprocesses:** `en-list`, `fr-list`, `custom-list`, `list-delete`

**Operations:**
- **en-list / fr-list:** Copies TMDb lists (English and French) with their linked movies and series.
- **custom-list:** Processes records from `T_WC_CUSTOM_LIST` where `TARGET_TABLE = 1`. Elements are resolved using up to three cumulative mechanisms:
  - **Mechanism 1 (IMDb list):** Parses `tt\d+` IDs from the `ID_IMDB_LIST` field; preserves input order via SQL `FIELD()`.
  - **Mechanism 2 (Wikidata):** Extracts a `P\d+` property and `Q\d+` item from `WIKIDATA_PROPERTIES`; joins against `T_WC_WIKIDATA_ITEM_PROPERTY`.
  - **Mechanism 3 (TMDb keyword):** Parses `T_WC_TMDB_KEYWORD.NAME = '...'` from `TMDB_ELEMENTS`; joins against `T_WC_TMDB_MOVIE_KEYWORD` / `T_WC_TMDB_SERIE_KEYWORD`.

  When multiple mechanisms match, results are combined with `UNION ALL` and deduplicated (`MAX(DISPLAY_ORDER) GROUP BY`).
- **list-delete:** Removes list records that no longer have a corresponding source entry.
- All mechanisms apply filter: `ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''`.
- Post-processing: updates `IMDB_RATING` / `IMDB_RATING_ADJUSTED` on `T_WC_T2S_LIST` from linked movies.

---

### Process 43 — T2S_GROUP

Builds person groups from Wikidata membership and employment relationships.

**Reads:** `T_WC_WIKIDATA_ITEM_PROPERTY`, `T_WC_WIKIDATA_ITEM_V1`, `T_WC_TMDB_PERSON`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_GROUP`, `T_WC_T2S_GROUP_PERSON`

**Subprocesses / Wikidata properties:**
- `en-group` → P463 (member of)
- `en-employer` → P108 (employer)

**Operations:**
- For each Wikidata property/item pair, retrieves the item's English and French labels, description, and Wikipedia image.
- Inserts/updates `T_WC_T2S_GROUP`.
- Queries persons linked to the item via Wikidata (ordered by popularity) and upserts into `T_WC_T2S_GROUP_PERSON`.

---

### Process 44 — T2S_AWARD

Builds award records from the Wikidata "award received" property (P166).

**Reads:** `T_WC_WIKIDATA_ITEM_PROPERTY`, `T_WC_WIKIDATA_ITEM_V1`, `T_WC_T2S_MOVIE`, `T_WC_T2S_SERIE`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_AWARD`, `T_WC_T2S_MOVIE_AWARD`, `T_WC_T2S_SERIE_AWARD`, `T_WC_T2S_PERSON_AWARD`

**Operations:**
- Selects all distinct Wikidata items used as values of property P166.
- For each award item, retrieves English/French label, description, and image.
- Inserts/updates `T_WC_T2S_AWARD`.
- Links movies, series, and persons that received the award (via Wikidata property join) into the respective junction tables with incremental display order.
- Post-processing: updates average `IMDB_RATING` / `IMDB_RATING_ADJUSTED` and `POPULARITY` on `T_WC_T2S_AWARD` from linked entities.

---

### Process 45 — T2S_MOVEMENT

Populates `T_WC_T2S_MOVEMENT` from custom lists that define cinematic movements.

**Reads:** `T_WC_CUSTOM_LIST` (TARGET_TABLE = 4), `T_WC_TMDB_MOVIE`, `T_WC_TMDB_SERIE`
**Writes:** `T_WC_T2S_MOVEMENT`, `T_WC_T2S_MOVIE_MOVEMENT`, `T_WC_T2S_SERIE_MOVEMENT`

**Subprocesses:** `custom-movement`, `movement-delete`

**Operations:**
- **custom-movement:** For each active custom list (TARGET_TABLE = 4, DELETED = 0), resolves member movies and series using the same three cumulative mechanisms as Process 42 (IMDb list, Wikidata property/item, TMDb keyword).
- Skips records with fewer than 2 total elements; deletes any existing record for those.
- **movement-delete:** Removes movement records whose source custom list no longer exists or has been deleted.
- Post-processing: updates `IMDB_RATING` / `IMDB_RATING_ADJUSTED` on `T_WC_T2S_MOVEMENT` from linked movies; cascades orphan cleanup to `T_WC_T2S_MOVIE_MOVEMENT` and `T_WC_T2S_SERIE_MOVEMENT`.
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
- For each Wikidata item used as a value of P509/P1196, retrieves English/French labels, description, and image.
- Inserts/updates `T_WC_T2S_DEATH` and links persons into `T_WC_T2S_PERSON_DEATH` ordered by person popularity.
- Full stale delete: removes `T_WC_T2S_DEATH` rows no longer present in `T_WC_WIKIDATA_ITEM_PROPERTY` for the corresponding property.

---

### Process 47 — T2S_NOMINATION

Builds nomination records from the Wikidata "nominated for" property (P1411).

**Reads:** `T_WC_WIKIDATA_ITEM_PROPERTY`, `T_WC_WIKIDATA_ITEM_V1`, `T_WC_T2S_MOVIE`, `T_WC_T2S_SERIE`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_NOMINATION`, `T_WC_T2S_MOVIE_NOMINATION`, `T_WC_T2S_SERIE_NOMINATION`, `T_WC_T2S_PERSON_NOMINATION`

**Operations:**
- Selects all distinct Wikidata items used as values of property P1411.
- For each nomination item, retrieves English/French label, description, and image.
- Inserts/updates `T_WC_T2S_NOMINATION`.
- Links movies, series, and persons that have the nomination (via Wikidata property join) into the respective junction tables with incremental display order.
- Full stale delete: removes `T_WC_T2S_NOMINATION` rows no longer present in `T_WC_WIKIDATA_ITEM_PROPERTY` for P1411.

---

## Common Patterns

| Pattern | Description |
|---------|-------------|
| Chunk processing | Most copy processes iterate over source IDs in batches of 1000 to avoid memory pressure. |
| `INSERT … ON DUPLICATE KEY UPDATE` | Idempotent upsert — safe to re-run without duplicating data. |
| `cp.f_sqlupdatearray()` | Generic upsert helper from the `citizenphil` module. |
| Multi-mechanism element resolution | Processes 42 and 45 combine IMDb, Wikidata, and TMDb keyword sources with `UNION ALL` + `GROUP BY`. |
| Wikidata filter | Any movie or serie written to a T2S junction table must satisfy `ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''`. |
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
| T_WC_TMDB_LIST / T_WC_CUSTOM_LIST (TARGET_TABLE=1) | T_WC_T2S_LIST, T_WC_T2S_MOVIE_LIST, T_WC_T2S_SERIE_LIST |
| T_WC_TMDB_COLLECTION | T_WC_T2S_COLLECTION, T_WC_T2S_MOVIE_COLLECTION, T_WC_T2S_SERIE_COLLECTION |
| T_WC_TMDB_KEYWORD / T_WC_TMDB_LIST / T_WC_TMDB_COLLECTION | T_WC_T2S_TOPIC |
| T_WC_TMDB_MOVIE (technical fields) | T_WC_T2S_TECHNICAL, T_WC_T2S_MOVIE_TECHNICAL |
| T_WC_WIKIDATA_ITEM_V1 | T_WC_T2S_ITEM |
| T_WC_WIKIDATA_ITEM_PROPERTY (P463, P108) | T_WC_T2S_GROUP, T_WC_T2S_PERSON_GROUP |
| T_WC_WIKIDATA_ITEM_PROPERTY (P166) | T_WC_T2S_AWARD, T_WC_T2S_MOVIE_AWARD, T_WC_T2S_SERIE_AWARD, T_WC_T2S_PERSON_AWARD |
| T_WC_CUSTOM_LIST (TARGET_TABLE=4) | T_WC_T2S_MOVEMENT, T_WC_T2S_MOVIE_MOVEMENT, T_WC_T2S_SERIE_MOVEMENT |
| T_WC_WIKIDATA_ITEM_PROPERTY (P509, P1196) | T_WC_T2S_DEATH, T_WC_T2S_PERSON_DEATH |
| T_WC_WIKIDATA_ITEM_PROPERTY (P1411) | T_WC_T2S_NOMINATION, T_WC_T2S_MOVIE_NOMINATION, T_WC_T2S_SERIE_NOMINATION, T_WC_T2S_PERSON_NOMINATION |
