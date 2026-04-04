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
  - `IMDB_RATING`, `IMDB_RATING_ADJUSTED` (averaged across linked movies and series)
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
- Chunked copy by `ID_ROW` (1000 rows).
- `INSERT … ON DUPLICATE KEY UPDATE` of link fields.
- Range-limited stale delete: removes rows in the processed `ID_ROW` range no longer present in TMDB source (with the same movie existence filter).

---

### Process 12 — T2S_SERIE_GENRE

Copies serie↔genre relations into the T2S layer (filtered to series that exist in `T_WC_T2S_SERIE`).

**Reads:** `T_WC_TMDB_SERIE_GENRE`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_SERIE_GENRE`

**Operations:** Same as Process 11 but for series.

---

### Process 13 — T2S_MOVIE_COMPANY

Copies movie↔company relations into the T2S layer.

**Reads:** `T_WC_TMDB_MOVIE_COMPANY`, `T_WC_T2S_MOVIE`, `T_WC_T2S_COMPANY`
**Writes:** `T_WC_T2S_MOVIE_COMPANY`

**Operations:** Chunked upsert + range-limited stale delete; validates existence of both `ID_MOVIE` and `ID_COMPANY` in T2S.

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

**Operations:** Chunked upsert + range-limited stale delete; validates existence of both `ID_SERIE` and `ID_NETWORK` in T2S.

---

### Process 16 — T2S_MOVIE_PRODUCTION_COUNTRY

Copies movie production countries into the T2S layer.

**Reads:** `T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY`, `T_WC_T2S_MOVIE`
**Writes:** `T_WC_T2S_MOVIE_PRODUCTION_COUNTRY`

**Operations:** Chunked upsert + range-limited stale delete; validates `ID_MOVIE` exists in T2S.

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

**Operations:** Chunked upsert + range-limited stale delete; validates `ID_MOVIE` exists in T2S.

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

**Operations:** Chunked upsert + range-limited stale delete; validates `ID_COMPANY` exists in T2S.

---

### Process 21 — T2S_MOVIE_IMAGE

Copies movie images into the T2S layer.

**Reads:** `T_WC_TMDB_MOVIE_IMAGE`, `T_WC_T2S_MOVIE`
**Writes:** `T_WC_T2S_MOVIE_IMAGE`

**Operations:** Chunked upsert + range-limited stale delete; validates `ID_MOVIE` exists in T2S.

---

### Process 22 — T2S_NETWORK_IMAGE

Copies network images into the T2S layer.

**Reads:** `T_WC_TMDB_NETWORK_IMAGE`, `T_WC_T2S_NETWORK`
**Writes:** `T_WC_T2S_NETWORK_IMAGE`

**Operations:** Chunked upsert + range-limited stale delete; validates `ID_NETWORK` exists in T2S.

---

### Process 23 — T2S_PERSON_IMAGE

Copies person images into the T2S layer.

**Reads:** `T_WC_TMDB_PERSON_IMAGE`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_PERSON_IMAGE`

**Operations:** Chunked upsert + range-limited stale delete; validates `ID_PERSON` exists in T2S.

---

### Process 24 — T2S_SERIE_IMAGE

Copies serie images into the T2S layer.

**Reads:** `T_WC_TMDB_SERIE_IMAGE`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_SERIE_IMAGE`

**Operations:** Chunked upsert + range-limited stale delete; validates `ID_SERIE` exists in T2S.

---

### Process 25 — T2S_MOVIE_VIDEO

Copies movie videos into the T2S layer.

**Reads:** `T_WC_TMDB_MOVIE_VIDEO`, `T_WC_T2S_MOVIE`
**Writes:** `T_WC_T2S_MOVIE_VIDEO`

**Operations:** Chunked upsert + range-limited stale delete; validates `ID_MOVIE` exists in T2S.

---

### Process 26 — T2S_SERIE_VIDEO

Copies serie videos into the T2S layer.

**Reads:** `T_WC_TMDB_SERIE_VIDEO`, `T_WC_T2S_SERIE`
**Writes:** `T_WC_T2S_SERIE_VIDEO`

**Operations:** Same as Process 25 but for series.

---

### Process 60 — TMDB_KEYWORD *(stub)*

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

**Reads:** `T_WC_TMDB_LIST`, `T_WC_TMDB_LIST_LANG`, `T_WC_TMDB_COLLECTION`, `T_WC_TMDB_COLLECTION_LANG`, `T_WC_CUSTOM_LIST` (TARGET_TABLE = 2), `T_WC_TMDB_MOVIE`, `T_WC_TMDB_SERIE`
**Writes:** `T_WC_T2S_COLLECTION`, `T_WC_T2S_MOVIE_COLLECTION`, `T_WC_T2S_SERIE_COLLECTION`

**Subprocesses:** `en-list`, `fr-list`, `en-collection`, `fr-collection`, `custom-collection`

**Operations:**
- For each list/collection record, queries associated movies and series filtered by `ADULT = 0` and `ID_WIKIDATA IS NOT NULL`.
- **custom-collection:** Processes records from `T_WC_CUSTOM_LIST` where `TARGET_TABLE = 2`. Elements are resolved using up to three cumulative mechanisms:
  - **Mechanism 1 (IMDb list):** Parses `tt\d+` IDs from the `ID_IMDB_LIST` field; preserves input order via SQL `FIELD()`.
  - **Mechanism 2 (Wikidata):** Extracts a `P\d+` property and `Q\d+` item from `WIKIDATA_PROPERTIES`; joins against `T_WC_WIKIDATA_ITEM_PROPERTY`.
  - **Mechanism 3 (TMDb keyword):** Parses `T_WC_TMDB_KEYWORD.NAME = '...'` from `TMDB_ELEMENTS`; joins against `T_WC_TMDB_MOVIE_KEYWORD` / `T_WC_TMDB_SERIE_KEYWORD`.
- Inserts/updates the collection record, then upserts linked movie and serie entries with display order.
- Skips records with fewer than 2 total elements; deletes any existing record for those.
- Full stale delete: removes `T_WC_T2S_COLLECTION` rows whose source record is no longer present in the corresponding TMDb source table or `T_WC_CUSTOM_LIST`.

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

Builds person groups from Wikidata membership and employment relationships, and from custom group definitions.

**Reads:** `T_WC_WIKIDATA_ITEM_PROPERTY`, `T_WC_WIKIDATA_ITEM_V1`, `T_WC_CUSTOM_LIST` (TARGET_TABLE = 3), `T_WC_TMDB_PERSON`, `T_WC_T2S_PERSON`
**Writes:** `T_WC_T2S_GROUP`, `T_WC_T2S_PERSON_GROUP`

**Subprocesses / Wikidata properties:**
- `en-group` → P463 (member of)
- `en-employer` → P108 (employer)
- `custom-group` → `T_WC_CUSTOM_LIST` (TARGET_TABLE = 3)

**Operations:**
- For each Wikidata property/item pair, retrieves the item's English and French labels, description, and Wikipedia image.
- Inserts/updates `T_WC_T2S_GROUP`.
- Queries persons linked to the item via Wikidata (ordered by popularity) and upserts into `T_WC_T2S_GROUP_PERSON`.
- **custom-group:** Builds groups from `T_WC_CUSTOM_LIST` (TARGET_TABLE = 3) and resolves member persons using up to three cumulative mechanisms (IMDb list `nm\d+`, Wikidata property/item, or a TMDb person name expression).
- Full stale delete: removes custom groups whose source record no longer exists in `T_WC_CUSTOM_LIST`.

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
| Multi-mechanism element resolution | Processes 41 (custom-collection), 42, and 45 combine multiple sources with `UNION ALL` + `GROUP BY`. |
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
