# Extending `T_WC_T2S_TECHNICAL` — Impact Analysis

Status: **COMPLETE — PRs 2 + 3 + 4 schema, config, and the pre-processing program (§12.5) have all landed.** Schema + config landed 2026-05-20; the §12.5 junction-enrichment pre-processing step is now implemented in [tmdb-movie-preprocess.py:254-298](tmdb-movie-preprocess.py#L254-L298) (helpers in [tmdb_preprocess_helpers.py:974-1163](tmdb_preprocess_helpers.py#L974-L1163)). Proposal C (metadata collection on `/movies/{id}`) and the `get_technical` MCP tool + `/technicals/{id}` endpoint from PR-5 shipped on 2026-05-15 in commit `9eb8460`. On 2026-05-20 the remaining decisions from §11 were resolved, the merged PR-2 + PR-3 + PR-4 migration was specified in §12, **and most of it was executed the same day**:

- ✅ **§12.2** — 17 new rows inserted into `T_WC_T2S_TECHNICAL`: IDs 57–60 (4 `medium_format` classification rows: `color_movie`, `black_and_white_movie`, `silent_movie`, `3d_movie`) and IDs 61–73 (13 `aspect_ratio` rows: `1.33` / `1.37` / `1.43` / `1.66` / `1.78` / `1.85` / `2.00` / `2.20` / `2.35` / `2.39` / `2.40` / `2.55` / `2.76`). Wikidata linkage populated. The §12 plan originally listed 16 aspect-ratio rows; `1.77` / `1.89` / `1.90` were dropped as too niche to seed and can be added later if source data demands it.
- ✅ **§12.3.A** — `closed_vocab.py:162-176` REGEXP flipped from comma- to dot-decimal.
- ✅ **§12.3.B** — both halves done: `Aspect_ratio` aliases retargeted to dot-decimal in `data/closed_vocabularies.json`, **and** the 26 new `Technical_format` classification aliases (EN + FR — `color` / `couleur` / `b&w` / `noir et blanc` / `silent` / `muet` / `3d` / `stéréoscopique`, etc.) appended.
- ⏭️ **§12.3.C** — **deliberately skipped.** The SQL cache was manually cleared for `ASPECT_RATIO` queries; no `strapiversion` bump.
- ✅ **§12.4** — `T_WC_T2S_MOVIE.ASPECT_RATIO` rewritten from comma- to dot-decimal.
- ✅ **§12.5** — the pre-processing program adaptation is **implemented**. The junction enrichment (medium_format + aspect_ratio) runs inside the `WIKIPEDIA_FORMAT_LINE` step ([tmdb-movie-preprocess.py:254-298](tmdb-movie-preprocess.py#L254-L298)) via `load_technical_ids` / `write_movie_technical_junction` / `refresh_technical_movie_count` ([tmdb_preprocess_helpers.py:974-1163](tmdb_preprocess_helpers.py#L974-L1163)). All §12.5.1–§12.5.7 sub-requirements (fail-loud ID resolution, classification backfill, multi-ratio per movie, scoped idempotent delete, MOVIE_COUNT refresh, logging) are covered.

Proposals A.1, B.2, retirement of the denormalized text columns, and PR-6 (series-side junction `T_WC_T2S_SERIE_TECHNICAL`) remain deferred. **This initiative is now functionally complete; §12.5 documents the implemented pre-processing contract.**
Date: 2026-05-17 (analysis); 2026-05-20 (decisions locked in, §12 added, schema + config edits applied); 2026-05-31 (§12.5 pre-processing program verified implemented; doc reconciled with code + DB dump)

---

## 1. Context & motivation

Three related changes to the `T_WC_T2S_TECHNICAL` reference model are under consideration:

1. **Move aspect ratio into `T_WC_T2S_TECHNICAL`.**
   `T_WC_T2S_MOVIE.ASPECT_RATIO` is a single VARCHAR column today. A movie can ship in several aspect ratios across versions (theatrical 2.35, home video 1.78, IMAX 1.43) — a single-valued column cannot represent that. Modeling aspect ratio as rows in `T_WC_T2S_TECHNICAL` (linked many-to-many through the existing `T_WC_T2S_MOVIE_TECHNICAL` junction) matches how every other technical attribute is already represented.

2. **Add `color_movie` / `black_and_white_movie` / `silent_movie` rows in `T_WC_T2S_TECHNICAL`.**
   The boolean flags `IS_COLOR`, `IS_BLACK_AND_WHITE`, `IS_SILENT` already live on `T_WC_T2S_MOVIE`. Promoting them to technical rows lets these classifications surface inside the `/movies` metadata collection, alongside the other technical metadata (sound systems, film formats, etc.).

3. **Expose a metadata collection on `/movies/{id}`.** ✅ **Shipped 2026-05-15.**
   The endpoint already returned one-to-many collections (genres, topics, lists, collections, movements, companies, posters). It did **not** previously return anything from `T_WC_T2S_MOVIE_TECHNICAL`. That gap is now closed — see [main.py:1728-1735](main.py#L1728-L1735) (junction query) and [main.py:1776](main.py#L1776) (response field).

The user's primary concern: not breaking the existing pipeline — entity extraction, closed-vocabulary alias matching, SQL generation, entity resolution, SQL caching, the `/movies` payload, and the MCP tools. Every option below is rated on those axes.

---

## 2. Current model inventory

This is the anchor every later section refers back to. All file:line references verified at 2026-05-17.

### 2.1 Schema

| Concern | Today | File:line |
|---|---|---|
| Aspect ratio (per movie) | `T_WC_T2S_MOVIE.ASPECT_RATIO varchar(20)`, indexed | [doc/sql/T2S-tables.sql:653](doc/sql/T2S-tables.sql#L653), [:703](doc/sql/T2S-tables.sql#L703) |
| Color flag | `T_WC_T2S_MOVIE.IS_COLOR int(11)`, indexed | [doc/sql/T2S-tables.sql:647](doc/sql/T2S-tables.sql#L647), [:698](doc/sql/T2S-tables.sql#L698) |
| Black & white flag | `T_WC_T2S_MOVIE.IS_BLACK_AND_WHITE int(11)`, indexed | [doc/sql/T2S-tables.sql:648](doc/sql/T2S-tables.sql#L648), [:699](doc/sql/T2S-tables.sql#L699) |
| Silent flag | `T_WC_T2S_MOVIE.IS_SILENT int(11)`, indexed | [doc/sql/T2S-tables.sql:649](doc/sql/T2S-tables.sql#L649), [:700](doc/sql/T2S-tables.sql#L700) |
| 3D flag | `T_WC_T2S_MOVIE.IS_3D int(11)`, indexed | [doc/sql/T2S-tables.sql:650](doc/sql/T2S-tables.sql#L650), [:701](doc/sql/T2S-tables.sql#L701) |
| Technical reference table | `T_WC_T2S_TECHNICAL(ID_TECHNICAL, ID_RECORD, ID_WIKIDATA, WIKIDATA_LABEL, CONFIDENCE, TIM_WIKIPEDIA_SEARCH, DESCRIPTION, DESCRIPTION_FR, OVERVIEW, TECHNICAL_TYPE, …, MOVIE_COUNT, SERIE_COUNT, WIKIPEDIA_IMAGE_PATH, IMDB_RATING, IMDB_RATING_WEIGHTED, POPULARITY)` — 56 pre-existing rows (IDs 1–56) + 17 rows added 2026-05-20 (IDs 57–73) = **73 active rows** | [doc/sql/T2S-tables.sql:1987-2027](doc/sql/T2S-tables.sql#L1987-L2027); standalone dump at [archive/T_WC_T2S_TECHNICAL.sql](archive/T_WC_T2S_TECHNICAL.sql) |
| Junction | `T_WC_T2S_MOVIE_TECHNICAL(ID_MOVIE, ID_TECHNICAL, …)` | [doc/sql/T2S-tables.sql:1013-1034](doc/sql/T2S-tables.sql#L1013-L1034) |
| Series-side junction | **Does not exist yet.** | — |

**Note**: the technical table was extended in commit `9eb8460` with first-class-entity columns (`ID_WIKIDATA`, `DESCRIPTION_FR`, `OVERVIEW`, `MOVIE_COUNT`, `SERIE_COUNT`, `WIKIPEDIA_IMAGE_PATH`, `IMDB_RATING`, `IMDB_RATING_WEIGHTED`, `POPULARITY`) so it can participate in the same payload shape as topics/lists/collections/movements (Wikipedia images, popularity ranking, multilingual labels). Any new rows inserted under Proposals A or B should populate these columns where applicable. The table subsequently gained three Wikidata-linker columns (`WIKIDATA_LABEL`, `CONFIDENCE`, `TIM_WIKIPEDIA_SEARCH`) populated by the "Link Wikidata items to T2S technical" pre-process step (index 62); the 17 new rows carry `ID_WIKIDATA` / `WIKIDATA_LABEL` / `CONFIDENCE` / `TIM_WIKIPEDIA_SEARCH` as of the 2026-05-20 dump.

`TECHNICAL_TYPE` value counts (per the 2026-05-20 dump, [archive/T_WC_T2S_TECHNICAL.sql](archive/T_WC_T2S_TECHNICAL.sql)):

| TECHNICAL_TYPE | Count |
|---|---|
| `color_technology` | 16 |
| `film_format` | 6 |
| `film_technology` | 18 |
| `sound_system` | 9 |
| `sound_technology` | 7 |
| `medium_format` *(added 2026-05-20, IDs 57–60)* | 4 |
| `aspect_ratio` *(added 2026-05-20, IDs 61–73)* | 13 |
| **Total** | **73** |

The 56 in the table heading below and elsewhere in this analysis refers to the pre-extension baseline (IDs 1–56); the live table now holds 73 active rows.

### 2.2 The two resolver paths

The pipeline already implements two distinct resolution strategies for closed vocabularies; understanding which path each entity uses is essential for the impact analysis below.

**ID-based path (`Technical_format`):**
- Canonical map loaded once at startup: `SELECT ID_TECHNICAL AS id, DESCRIPTION AS name FROM T_WC_T2S_TECHNICAL WHERE DESCRIPTION IS NOT NULL AND (DELETED = 0 OR DELETED IS NULL)` — [closed_vocab.py:191-194](closed_vocab.py#L191-L194), [:250-254](closed_vocab.py#L250-L254).
- `resolve_technical(raw_value)` returns an integer ID using RapidFuzz with `SCORE_CUTOFF=85` and `SCORE_MARGIN=5` — [closed_vocab.py:340-355](closed_vocab.py#L340-L355).
- Substitution branch in [entity.py:369-386](entity.py#L369-L386): converts ID to string and replaces `'{{Technical_format1}}'` and `{{Technical_format1}}` with the bare integer (no quotes). The SQL uses `T_WC_T2S_MOVIE_TECHNICAL.ID_TECHNICAL = 27`.

**String-canonical path (`Aspect_ratio`, `Status_name`, `Serie_type`, `Department_name`):**
- Canonical map loaded once: `SELECT DISTINCT ASPECT_RATIO AS V FROM T_WC_T2S_MOVIE WHERE ASPECT_RATIO IS NOT NULL AND ASPECT_RATIO REGEXP '^[0-9]+,[0-9]+$'` — [closed_vocab.py:162-172](closed_vocab.py#L162-L172), [:220-224](closed_vocab.py#L220-L224). The REGEXP filter is deliberate: it admits only well-formed comma-decimal values like `'1,33'`, `'1,85'`, `'2,35'`, excluding noisy DB variants like `'4:3'`, `'4/3'`, `'16:9'`, `'235:1'` so those fall through to the JSON alias layer.
- `closed_vocab.resolve("Aspect_ratio", raw_value)` returns the canonical comma-decimal string.
- Substitution branch in [entity.py:388-421](entity.py#L388-L421): wraps the canonical in single quotes. The SQL ends up with `T_WC_T2S_MOVIE.ASPECT_RATIO = '1,85'`.

### 2.3 Aliases (`data/closed_vocabularies.json`)

- `Aspect_ratio` block — [data/closed_vocabularies.json:106-172](data/closed_vocabularies.json#L106-L172): aliases targeting comma-decimal canonicals (`"1.85" → "1,85"`, `"4:3" → "1,33"`, `"widescreen" → "1,85"`, `"Academy ratio" → "1,37"`, `"16:9" → "1,78"`, etc.). The header comment at [data/closed_vocabularies.json:107](data/closed_vocabularies.json#L107) explains the REGEXP filtering rationale and **explicitly states** that `anamorphic` / `scope` / `cinemascope` belong under `Technical_format`, not here.
- `Technical_format` block — [data/closed_vocabularies.json:205-237](data/closed_vocabularies.json#L205-L237): aliases targeting canonical `DESCRIPTION` strings (`"35mm" → "35 mm"`, `"scope" → "cinemascope"`, `"imax format" → "imax"`, `"5.1 surround" → "5.1"`, etc.).

### 2.4 Prompts

- **Entity extraction** ([data/entity_extraction.md](data/entity_extraction.md)):
  - `Aspect_ratio` placeholder defined at [lines 193-209](data/entity_extraction.md#L193-L209): "extract only when the question filters or asks about the movie's aspect ratio".
  - `Technical_format` placeholder defined at [lines 231-249](data/entity_extraction.md#L231-L249): covers sound systems, color technologies, film technologies, sound technologies, film formats.
  - Boundary rules at [lines 314-317](data/entity_extraction.md#L314-L317) (Aspect_ratio) and [line 336](data/entity_extraction.md#L336) (Technical_format) cross-reference each other.
  - **The prompt teaches the LLM that these are two distinct entity types**, and downstream prompts assume the split.

- **Text-to-SQL** ([data/text_to_sql.md](data/text_to_sql.md)):
  - Schema dump exposes `ASPECT_RATIO VARCHAR(20)`, `IS_COLOR INT`, `IS_BLACK_AND_WHITE INT`, `IS_SILENT INT` as columns on `T_WC_T2S_MOVIE` — [lines 81-84](data/text_to_sql.md#L81-L84).
  - Junction `T_WC_T2S_MOVIE_TECHNICAL(ID_ROW, ID_MOVIE, ID_TECHNICAL)` shown at [lines 276-280](data/text_to_sql.md#L276-L280).
  - Color/B&W/silent SQL rule at [lines 736-739](data/text_to_sql.md#L736-L739): "Use `IS_COLOR = 1` for color movies … `IS_BLACK_AND_WHITE = 1` for black and white … `IS_SILENT = 1` for silent movies."
  - `Technical_format` placeholder substitution rule at [lines 776-779](data/text_to_sql.md#L776-L779): `T_WC_T2S_MOVIE_TECHNICAL.ID_TECHNICAL = '{{Technical_format1}}'`.

### 2.5 `/movies/{id}` endpoint — **`technicals` collection shipped**

- Defined at [main.py:1641-1792](main.py#L1641-L1792). Top of the function does `SELECT * FROM T_WC_T2S_MOVIE WHERE ID_MOVIE = %s` ([main.py:1670](main.py#L1670)) — every column comes for free, including `ASPECT_RATIO`, `IS_COLOR`, `IS_BLACK_AND_WHITE`, `IS_SILENT`, `IS_3D`.
- Now returns: `genres`, `companies`, `production_countries`, `spoken_languages`, `topics`, `lists`, `collections`, `movements`, **`technicals`**, `awards`, `nominations`, `cast`, `crew`, `posters`, `backdrops`, `wikipedia_images`.
- The `technicals` query at [main.py:1728-1735](main.py#L1728-L1735) returns `ID_TECHNICAL, DESCRIPTION, DESCRIPTION_FR, TECHNICAL_TYPE, WIKIPEDIA_IMAGE_PATH, IMDB_RATING_WEIGHTED, POPULARITY`, ordered by `mt.DISPLAY_ORDER ASC`. Adding new technical rows under Proposals A or B will surface here automatically with no further endpoint change.

### 2.6 `/technicals/{id}` endpoint — **shipped**

- Defined at [main.py:2283-2329](main.py#L2283-L2329). Returns all columns of the technical itself plus a `movies` array, a `siblings` array (other rows sharing the same `TECHNICAL_TYPE`, ordered by `MOVIE_COUNT DESC`), and `wikipedia_images`.
- Docstring at [main.py:2294](main.py#L2294) explicitly notes the missing `T_WC_T2S_SERIE_TECHNICAL` and the absence of a `series` array.

### 2.7 MCP exposure — **`get_technical` shipped**

- `get_movie` MCP tool wraps `/movies/{id}` and now documents the `technicals` field inline — docstring at [main.py:2600-2611](main.py#L2600-L2611); payload example at [MCP.md:502-504](MCP.md#L502-L504); inline `ASPECT_RATIO` / `IS_COLOR` / `IS_BLACK_AND_WHITE` / `IS_SILENT` still present in the same example at [MCP.md:464-467](MCP.md#L464-L467).
- `get_technical` MCP tool wraps `/technicals/{id}` — docstring at [main.py:2735-2741](main.py#L2735-L2741); MCP source listing at [MCP.md:218-232](MCP.md#L218-L232); routing entry at [MCP.md:86](MCP.md#L86) and [MCP.md:378](MCP.md#L378).
- All 15 entity getters now exist: movie / series / person / company / network / collection / topic / list / movement / technical / group / death / award / nomination / location.

### 2.8 SQL cache

- The `/search/text2sql` pipeline caches resolved SQL by question hash. Any change to a column referenced by cached SQL silently invalidates those rows: subsequent cache hits return SQL that no longer matches the schema. (Cache implementation lives in the same file; flushes are surgical `DELETE … WHERE SQL_QUERY LIKE '%COL%'` filters.)

---

## 3. Proposal A — Aspect ratio in `T_WC_T2S_TECHNICAL` — *open*

Two options are documented below. The user has not committed to either.

### 3.1 Option A.1 — Full migration to ID-based resolution

**Model change**
- Insert ~12 rows in `T_WC_T2S_TECHNICAL` with `TECHNICAL_TYPE='aspect_ratio'`, one per canonical ratio (`1.33`, `1.37`, `1.66`, `1.78`, `1.85`, `2.20`, `2.35`, `2.39`, `2.40`, `2.55`, IMAX `1.43`, anamorphic widths if relevant). Populate the new first-class columns (`ID_WIKIDATA`, `DESCRIPTION_FR`, `OVERVIEW`, `WIKIPEDIA_IMAGE_PATH`) where available — these now surface automatically through the `technicals` collection and `/technicals/{id}` siblings list.
- Convention recommendation: use **dot-decimal** `DESCRIPTION` (e.g. `"1.85"`) to align with the existing readable-canonical style of `T_WC_T2S_TECHNICAL`. Comma-decimal is a French SQL-storage detail that should not leak into a normalized reference table.
- Backfill `T_WC_T2S_MOVIE_TECHNICAL` from existing `T_WC_T2S_MOVIE.ASPECT_RATIO` values, normalizing comma → dot and mapping noisy forms (`'4:3' → 1.33`, `'235:1' → 2.35`, `'16:9' → 1.78`).
- Drop or deprecate `T_WC_T2S_MOVIE.ASPECT_RATIO` and its index.

**Pipeline impact (Option A.1)**

| Stage | Change | File:line |
|---|---|---|
| Schema | Drop `ASPECT_RATIO` column + index; insert ~12 rows in `T_WC_T2S_TECHNICAL`; backfill rows in `T_WC_T2S_MOVIE_TECHNICAL` | [doc/sql/T2S-tables.sql:653](doc/sql/T2S-tables.sql#L653), [:703](doc/sql/T2S-tables.sql#L703), [:1987](doc/sql/T2S-tables.sql#L1987) |
| Canonical loader | Delete `_ASPECT_RATIO_QUERY` block; delete the `loaded["Aspect_ratio"] = …` block. New rows arrive automatically through the existing `Technical_format` canonical map. | [closed_vocab.py:162-172](closed_vocab.py#L162-L172), [:220-224](closed_vocab.py#L220-L224) |
| Aliases JSON — Decision point | (a) Fold all 54 `Aspect_ratio` aliases into the `Technical_format` block, retargeting from comma-decimal to dot-decimal DESCRIPTIONs (`"4:3" → "1.33"`, `"widescreen" → "1.85"`). OR (b) Keep `Aspect_ratio` as a separate alias namespace whose values now reference DESCRIPTIONs. Option (a) is cleaner and removes the rationale documented at line 107; option (b) preserves extraction-time separation but adds dispatch complexity. **The 107 comment will need to be rewritten or removed in both cases.** | [data/closed_vocabularies.json:106-172](data/closed_vocabularies.json#L106-L172) |
| Entity extraction prompt — Decision point | (a) Retire the `Aspect_ratio` placeholder entirely; everything resolves to `Technical_format`. (b) Keep `Aspect_ratio` as a placeholder sub-type for prompt readability; resolver must dispatch it to `resolve_technical` rather than `resolve("Aspect_ratio", …)`. Option (a) is simpler. | [data/entity_extraction.md:193-209](data/entity_extraction.md#L193-L209), [:314-317](data/entity_extraction.md#L314-L317) |
| Resolver dispatch | Remove `Aspect_ratio` from the string-canonical branch in `resolve_entities`. If the placeholder is kept (extraction-prompt option b), add a forwarding rule: `key.startswith("Aspect_ratio")` → `resolve_technical(raw_value)` → numeric ID substitution. If the placeholder is retired, no resolver code change is required. | [entity.py:388-421](entity.py#L388-L421) |
| Text-to-SQL prompt | Remove `ASPECT_RATIO VARCHAR(20)` from the schema dump. Update the placeholder substitution rule from single-quoted string to numeric ID. Add a worked example of `JOIN T_WC_T2S_MOVIE_TECHNICAL mt ON mt.ID_MOVIE = m.ID_MOVIE JOIN T_WC_T2S_TECHNICAL t ON t.ID_TECHNICAL = mt.ID_TECHNICAL WHERE t.TECHNICAL_TYPE = 'aspect_ratio' AND t.ID_TECHNICAL = …`. The LLM also needs guidance that aspect_ratio is now `TECHNICAL_TYPE='aspect_ratio'` rather than a column. | [data/text_to_sql.md:84](data/text_to_sql.md#L84), [:276-280](data/text_to_sql.md#L276-L280), [:776-779](data/text_to_sql.md#L776-L779) |
| `/movies/{id}` | Remove `ASPECT_RATIO` from the row payload, or compute a primary ratio from the technicals collection for backward compat. | [main.py:1670](main.py#L1670) |
| MCP docs | Remove `"ASPECT_RATIO"` from the inline JSON at [MCP.md:467](MCP.md#L467) and from the resource listing at [MCP.md:582](MCP.md#L582); update `get_movie` docstring at [main.py:2602-2611](main.py#L2602-L2611). | as listed |
| SQL cache | **All cached SQL referencing `T_WC_T2S_MOVIE.ASPECT_RATIO` becomes wrong.** Flush via `DELETE FROM <cache_table> WHERE SQL_QUERY LIKE '%ASPECT_RATIO%'` before deploy, or accept a cache cold period. | (cache impl in `search_text2sql` of [main.py](main.py)) |
| Evaluation | Run `eval/text2sql-eval.py` against questions covering aspect ratio (Academy ratio, widescreen, 2.35, "shot in scope") and noisy aliases (`4:3`, `16:9`). Add at least one regression where a single movie has multiple ratios. | [eval/](eval/) |
| AGENTS.md / closed-vocab-entity-plan.md | Both reference `Aspect_ratio` as a "string-canonical lookup entity" / "**SHIPPED**" pattern. Both need updating to reflect the new path. | [AGENTS.md:44](AGENTS.md#L44), [:128](AGENTS.md#L128), [:298](AGENTS.md#L298); [closed-vocab-entity-plan.md:67-72](closed-vocab-entity-plan.md#L67-L72), [:169](closed-vocab-entity-plan.md#L169), [:295](closed-vocab-entity-plan.md#L295) |

**Risks (Option A.1)**
- **Alias retargeting must be exhaustive.** All 54 `Aspect_ratio` aliases currently target comma-decimal canonicals. Each must be remapped to a dot-decimal `DESCRIPTION` value. Missing one breaks resolution silently — the LLM emits `{{Aspect_ratio1}}` and the resolver leaves it unresolved.
- **The REGEXP filter at [closed_vocab.py:171](closed_vocab.py#L171) is no longer needed**, but its role (excluding noisy DB values from canonicals so aliases fire) is still required during backfill — `'4:3'`, `'235:1'`, etc. must be normalized when inserting into `T_WC_T2S_MOVIE_TECHNICAL`, not carried over verbatim.
- **The SQL-prompt change is the largest lift**: the LLM has been trained over many evaluation iterations to write `T_WC_T2S_MOVIE.ASPECT_RATIO = '1,85'`. Switching to a JOIN pattern requires re-running evals end-to-end to confirm no regressions.
- **Evaluation-pinned questions** in `eval/data/evaluation/` may have expected-SQL strings that reference the old column directly; those golden files would need updating.

### 3.2 Option A.2 — Dual model (column stays, junction added)

**Model change**
- Keep `T_WC_T2S_MOVIE.ASPECT_RATIO` as the primary/theatrical ratio (fast single-value indexed filter, no JOIN).
- Add aspect-ratio rows to `T_WC_T2S_TECHNICAL` *and* link via `T_WC_T2S_MOVIE_TECHNICAL` **only when a movie has multiple ratios**. Single-ratio movies stay column-only.
- The shipped `technicals` collection at [main.py:1728-1735](main.py#L1728-L1735) automatically picks them up — no endpoint code change required. The inline `ASPECT_RATIO` field at the top of the response still carries the primary ratio.

**Pipeline impact (Option A.2)**

| Stage | Change |
|---|---|
| Schema | Additive only — new rows in `T_WC_T2S_TECHNICAL` and `T_WC_T2S_MOVIE_TECHNICAL`. Column and index preserved. |
| Canonical loader | Unchanged. Both `Aspect_ratio` (column-derived) and `Technical_format` (table-derived) maps continue to exist. Sync risk: if a ratio is only ever in `T_WC_T2S_TECHNICAL` (never in `T_WC_T2S_MOVIE.ASPECT_RATIO`), the `Aspect_ratio` canonical map won't know about it. Mitigation: backfill the column with the primary ratio whenever a movie has at least one. |
| Aliases JSON | Unchanged. |
| Entity extraction prompt | Unchanged. |
| Resolver dispatch | Unchanged. |
| Text-to-SQL prompt | **Riskiest change in this option.** Adding the junction path as a *second* way to filter aspect ratio invites LLM inconsistency. Recommendation: do NOT mention the junction path for aspect ratio in the SQL prompt; the column path remains the only documented filter, and the junction exists purely to back the `/movies` metadata collection. |
| `/movies/{id}` | **No code change required.** The shipped `technicals` query already surfaces them. The inline `ASPECT_RATIO` field continues to carry the primary ratio. |
| MCP docs | Optional: note in the `get_movie` docstring that aspect ratios may also appear in the `technicals` array for multi-ratio releases. |
| SQL cache | Unaffected. |
| Evaluation | Add one regression case for a multi-ratio movie's `/movies/{id}` response. No text2sql regressions expected. |

**Risks (Option A.2)**
- **Two sources of truth** for the same concept. A film whose primary theatrical ratio is 2.35 and whose home video is 1.78 has `ASPECT_RATIO='2,35'` in the column *and* both 2.35 and 1.78 in the junction. A reader has to know which to trust for which purpose.
- **Mild sync burden**: any backfill or ingestion job that writes aspect ratios has to write to both places, or write to the junction first and derive the column.
- **No SQL-prompt risk if the junction path is explicitly hidden from the SQL prompt.** The user keeps full backward compatibility with every existing alias, the REGEXP filter, the entity extraction prompt, and the cached SQL.

### 3.3 A.1 vs A.2 — recommendation

**Ship A.2 first.** It is additive, low-risk, and now genuinely "free" given the metadata collection already shipped — the new rows surface automatically with no further endpoint changes. Defer A.1 until either (a) the column starts feeling like dead weight (rarely happens once it serves "primary ratio"), or (b) data-quality issues force normalization. The conceptual cleanliness of A.1 is real but does not outweigh the pipeline risk today.

---

## 4. Proposal B — Color / B&W / Silent in `T_WC_T2S_TECHNICAL` — *open*

### 4.1 Option B.1 — Keep flags AND add technical rows

**Model change**
- Insert three rows in `T_WC_T2S_TECHNICAL`. Suggested values:

  | `DESCRIPTION` | `TECHNICAL_TYPE` |
  |---|---|
  | `color_movie` | `medium_format` |
  | `black_and_white_movie` | `medium_format` |
  | `silent_movie` | `medium_format` |

  Rationale: `medium_format` reads parallel to `film_format` (which is a physical format) without colliding with it. Alternative naming under discussion: `movie_classification`. The user should pick one before insertion. Populate `DESCRIPTION_FR`, `ID_WIKIDATA`, `OVERVIEW` for full parity with the first-class entity payload now that those columns exist.
- Backfill `T_WC_T2S_MOVIE_TECHNICAL` from the existing flags: every movie with `IS_COLOR=1` gets a row pointing at the `color_movie` ID, etc.
- **Keep the flag columns and their indexes** on `T_WC_T2S_MOVIE`.

**Pipeline impact (Option B.1)**

| Stage | Change | File:line |
|---|---|---|
| Schema | Insert 3 rows in `T_WC_T2S_TECHNICAL`; backfill `T_WC_T2S_MOVIE_TECHNICAL` from flags; columns + indexes preserved. | [doc/sql/T2S-tables.sql:1987](doc/sql/T2S-tables.sql#L1987) |
| Canonical loader | **Unchanged.** New rows are picked up automatically by `_TECHNICAL_CANONICALS_QUERY` at startup. | [closed_vocab.py:191-194](closed_vocab.py#L191-L194) |
| Aliases JSON | Add to the `Technical_format` block: `"color" → "color_movie"`, `"colour" → "color_movie"`, `"in color" → "color_movie"`, `"color film" → "color_movie"`, `"black and white" → "black_and_white_movie"`, `"black-and-white" → "black_and_white_movie"`, `"B&W" → "black_and_white_movie"`, `"BW" → "black_and_white_movie"`, `"monochrome" → "black_and_white_movie"`, `"silent" → "silent_movie"`, `"silent film" → "silent_movie"`. Avoid adding `"talkie"` as a negation alias — the resolver is positive-match only, and negation should stay in the SQL prompt. | [data/closed_vocabularies.json:205-237](data/closed_vocabularies.json#L205-L237) |
| Entity extraction prompt | **Important risk.** Today the LLM is taught at [data/text_to_sql.md:736-739](data/text_to_sql.md#L736-L739) to translate "color movies" / "B&W films" / "silent films" directly into `IS_COLOR = 1` etc. at SQL-generation time — they are not extracted as placeholders. If we now extend `Technical_format` extraction at [data/entity_extraction.md:231-249](data/entity_extraction.md#L231-L249) to include "color movie" / "B&W film" / "silent film", the LLM might extract a placeholder where the existing rule expects raw words. Two paths exist (see below). Recommended: leave the extraction prompt unchanged; the new technical rows are **only** for the metadata collection, not for filtering. |
| Text-to-SQL prompt | **Two valid sub-options:** (i) Leave the rule at [lines 736-739](data/text_to_sql.md#L736-L739) untouched; `IS_COLOR = 1` remains the only documented filter path; the junction rows exist purely for `/movies` payload. (ii) Document both paths and mark `IS_COLOR = 1` as preferred. Sub-option (i) is dramatically safer for evaluation. |
| Resolver dispatch | **Unchanged.** Even if the SQL prompt eventually documents the junction path, `Technical_format` already dispatches to `resolve_technical()` for the new rows. | [entity.py:369-386](entity.py#L369-L386) |
| `/movies/{id}` | **No code change required.** Color / B&W / silent now appear in two places: top-level `IS_COLOR / IS_BLACK_AND_WHITE / IS_SILENT` flags AND the existing `technicals` collection (which already surfaces them once the rows are inserted). Decision: keep both for backward compat with existing MCP consumers, or only the collection (cleaner but breaks anyone reading the flags directly). | [main.py:1641-1792](main.py#L1641-L1792) |
| MCP docs | Optional: note the new rows in `get_movie` docstring at [main.py:2600-2611](main.py#L2600-L2611) and example payload at [MCP.md:502-504](MCP.md#L502-L504). |
| SQL cache | **Unaffected.** Existing `IS_COLOR = 1` SQL stays valid. |
| Evaluation | Add regression: "color movies of 1939" must still generate `IS_COLOR = 1`; "silent films of the 1920s" → `IS_SILENT = 1`; "black and white movies with Bogart" → `IS_BLACK_AND_WHITE = 1`. Add a `/movies/{id}` payload assertion that the technicals array includes the matching row. | [eval/](eval/) |

**Risks (Option B.1)**
- Mostly: **the LLM might start picking the junction path inconsistently** if the SQL prompt ever documents it. Mitigation: don't document it. The flags stay the sole filter path; the junction is metadata-collection-only.
- **Backfill drift**: if `IS_COLOR` is later updated for a movie but the junction row isn't rewritten, the two diverge. Mitigation: add a one-shot backfill script; if the data is mutable later, consider a DB trigger or fold the update into the ingestion path.

### 4.2 Option B.2 — Full migration (drop flags)

**Model change**
- Drop the columns `IS_COLOR`, `IS_BLACK_AND_WHITE`, `IS_SILENT` and their indexes.
- All filtering goes through the junction.

**Pipeline impact (Option B.2)**

Everything in B.1 *plus*:

| Stage | Change |
|---|---|
| Schema | Drop three columns + three indexes. Backfill the junction *exhaustively* (B.1 does this too, but here the flags are no longer the safety net). |
| Text-to-SQL prompt | Rewrite the rules at [data/text_to_sql.md:736-739](data/text_to_sql.md#L736-L739) entirely. Every "color movie" / "B&W" / "silent" question now requires a JOIN. The LLM must be taught the new pattern. |
| SQL cache | **All cached SQL referencing `IS_COLOR`, `IS_BLACK_AND_WHITE`, `IS_SILENT` becomes wrong.** Flush via `DELETE … WHERE SQL_QUERY LIKE '%IS_COLOR%' OR SQL_QUERY LIKE '%IS_BLACK_AND_WHITE%' OR SQL_QUERY LIKE '%IS_SILENT%'`. The "color movies of 1939" question is *extremely* common — this is a high-impact cache cold. |
| Performance | Every color/B&W/silent filter now does a JOIN instead of a single indexed column scan. Not catastrophic, but measurable on large result sets. |
| MCP docs | The inline `"IS_COLOR": 1` payload at [MCP.md:464-466](MCP.md#L464-L466) is gone. Any downstream consumer reading these fields breaks silently. |

**Risks (Option B.2)**
- High SQL-cache churn for very common queries.
- Performance regression for the most common filters in the corpus.
- Breaking change for MCP / API consumers reading the flags directly.

### 4.3 B.1 vs B.2 — recommendation

**B.1 is the strongly recommended path.** The flags are cheap, well-indexed, well-loved by the LLM, and exposed inline on `/movies/{id}` for backward compat. Adding three rows + ~few thousand junction entries costs nothing and unlocks the existing metadata collection. B.2 is conceptually cleaner but trades real performance and stability for that cleanliness.

---

## 5. Proposal C — Metadata collection on `/movies/{id}` — **✅ SHIPPED**

Shipped in commit `9eb8460` on 2026-05-15. Preserved here for the historical record and to anchor the references in Proposals A and B.

### 5.1 Response shape (as shipped)

The `technicals` field in the JSON response sits between `movements` and `awards` ([main.py:1775-1777](main.py#L1775-L1777)):

```json
"technicals": [
  {"ID_TECHNICAL": 29, "DESCRIPTION": "imax", "DESCRIPTION_FR": null, "TECHNICAL_TYPE": "sound_system", "WIKIPEDIA_IMAGE_PATH": null, "IMDB_RATING_WEIGHTED": null, "POPULARITY": null}
]
```

Once Proposals A and B land, the same array starts surfacing `aspect_ratio` and `medium_format` rows automatically — no further endpoint changes needed.

### 5.2 What actually shipped

- **Endpoint code**: new query block in `get_movie` at [main.py:1728-1735](main.py#L1728-L1735); response field at [main.py:1776](main.py#L1776).
- **MCP docstring**: `technicals` documented in `get_movie` at [main.py:2600-2611](main.py#L2600-L2611) and listed at [MCP.md:580](MCP.md#L580).
- **Example payload**: [MCP.md:502-504](MCP.md#L502-L504).
- **No prompt change.** No resolver change. No closed-vocab change. No cache invalidation. No SQL-generation risk.
- **`get_technical(id)` MCP tool**: also shipped (the originally-speculative PR-5). Implementation at [main.py:2283-2329](main.py#L2283-L2329); MCP source at [MCP.md:218-232](MCP.md#L218-L232); docs at [MCP.md:669-676](MCP.md#L669-L676). Returns the technical plus associated movies and sibling technicals sharing the same `TECHNICAL_TYPE` (ordered by `MOVIE_COUNT DESC`), enabling navigation between related formats (e.g. from `technicolor` to the other `color_technology` rows).
- **Promoted entity columns**: `T_WC_T2S_TECHNICAL` gained `ID_RECORD`, `ID_WIKIDATA`, `DESCRIPTION_FR`, `OVERVIEW`, `MOVIE_COUNT`, `SERIE_COUNT`, `WIKIPEDIA_IMAGE_PATH`, `IMDB_RATING`, `IMDB_RATING_WEIGHTED`, `POPULARITY` so technicals participate in the same first-class payload shape as topics/lists/collections/movements. Any rows inserted under Proposals A or B should populate these where applicable.
- **Series side**: `/series/{id}` does **not** yet have a junction (`T_WC_T2S_SERIE_TECHNICAL` does not exist). Explicitly flagged in the `get_technical` docstring at [main.py:2294](main.py#L2294). The metadata-collection pattern would mirror this once that junction is added.

---

## 6. Additional metadata candidates worth adding to `T_WC_T2S_TECHNICAL`

The user explicitly asked. Ranked by fit:

1. **`IS_3D` → `3d_movie` technical row** ([doc/sql/T2S-tables.sql:650](doc/sql/T2S-tables.sql#L650)). Direct analog of Proposal B. Same B.1 / B.2 tradeoff applies; same recommendation (keep the flag, add the technical row). High value, low risk.

2. **Frame rate / HFR (high frame rate)**. No flag exists today. Pure addition. Modern releases (48 fps, 60 fps, 120 fps) are increasingly distinctive metadata for cinephile audiences. Safe to add as `TECHNICAL_TYPE='frame_rate'` rows.

3. **Stereoscopic / VR formats**. Pure addition. Same rationale as HFR.

4. **Color spaces** (Rec. 709, Rec. 2020, DCI-P3). Relevant for 4K HDR releases and restorations. Pure addition; `TECHNICAL_TYPE='color_space'`.

5. **HDR formats** (Dolby Vision, HDR10, HDR10+, HLG). Pure addition; `TECHNICAL_TYPE='hdr_format'`.

What does **not** fit `T_WC_T2S_TECHNICAL`:

- **`IS_DOCUMENTARY` / `IS_SHORT_FILM`** ([doc/sql/T2S-tables.sql:658-659](doc/sql/T2S-tables.sql#L658-L659)). These are genre/format classifications, not technical attributes. They belong with the existing genre model or on `T_WC_T2S_MOVIE` itself. Documenting this consideration here means it won't be raised again next quarter.
- **Decade / runtime bucket / release-window** classifications. Derivable from `RELEASE_YEAR` and `RUNTIME` columns. Materializing them in a reference table provides no benefit.
- **Language / country**. Already in dedicated tables (`T_WC_T2S_MOVIE_SPOKEN_LANGUAGE`, `T_WC_T2S_MOVIE_PRODUCTION_COUNTRY`).
- **Genre**. Already in `T_WC_TMDB_GENRE` and the `T_WC_T2S_MOVIE_GENRE` / `T_WC_T2S_SERIE_GENRE` junctions.

### 6.1 Cleanup opportunity surfaced during analysis

`T_WC_T2S_MOVIE` carries four denormalized text columns that duplicate data already in `T_WC_T2S_TECHNICAL`:

- `COLOR_TECHNOLOGY varchar(100)` ([doc/sql/T2S-tables.sql:651](doc/sql/T2S-tables.sql#L651))
- `FILM_TECHNOLOGY mediumtext` ([:652](doc/sql/T2S-tables.sql#L652))
- `FILM_FORMAT varchar(50)` ([:654](doc/sql/T2S-tables.sql#L654))
- `SOUND_SYSTEM mediumtext` ([:655](doc/sql/T2S-tables.sql#L655))
- `SOUND_TECHNOLOGY varchar(200)` ([:656](doc/sql/T2S-tables.sql#L656))

These hold free-text duplicates of what the junction already represents canonically. They are the strongest candidates for deletion in a future cleanup pass. Risk profile is identical to Option B.2: every column is in the SQL-generation schema dump ([data/text_to_sql.md](data/text_to_sql.md)) and almost certainly referenced in cached SQL. Flag as a deliberate follow-up, not part of this work.

---

## 7. Cross-cutting impacts

These apply across most of the options above:

- **Closed-vocab startup log** at [closed_vocab.py:257-258](closed_vocab.py#L257-L258) prints a per-entity count summary. Useful smoke check: after deploying Proposals B + new aspect-ratio rows, the `Technical_format` count should jump by exactly the number of inserted rows.
- **`data_watcher` hot-reload** at [closed_vocab.py:144](closed_vocab.py#L144). Alias JSON changes hot-reload at runtime. Schema changes (new DB rows) do not — they require a restart because canonicals are loaded only by `init()`.
- **Series side blind spot**. Proposals A and B operate only on movies. `T_WC_T2S_SERIE_TECHNICAL` does not exist; series cannot carry aspect ratio or color/B&W/silent metadata today. The shipped `/technicals/{id}` endpoint already acknowledges this gap in its docstring at [main.py:2294](main.py#L2294). If technical metadata becomes a first-class concern for series, the series-side junction is the natural next step (plus an analogous `technicals` collection on `/series/{id}` at [main.py:1796-1934](main.py#L1796-L1934)).
- **Backward compat with MCP consumers**. Any MCP client (web UI, third-party agents using `get_movie`) reading `ASPECT_RATIO`, `IS_COLOR`, `IS_BLACK_AND_WHITE`, `IS_SILENT` from the inline movie payload will break silently under Options A.1 and B.2. The recommended path (A.2 + B.1) keeps every existing field intact.
- **AGENTS.md** at [lines 44, 128, 298](AGENTS.md#L44) and **closed-vocab-entity-plan.md** at [lines 67-72, 169, 295](closed-vocab-entity-plan.md#L67-L72) both reference `Aspect_ratio` as a string-canonical lookup; both need updates if Proposal A.1 ships.

---

## 8. Recommendation

Ship the smallest change that unlocks the user's primary goal (the `/movies` metadata collection — **done**), then layer the rest only as data and need warrant.

1. ✅ **Done — Proposal C.** `technicals` collection on `/movies/{id}` plus `get_technical` MCP tool and `/technicals/{id}` endpoint shipped in commit `9eb8460`.
2. ✅ **Implemented — Proposal B.1 (color_movie / black_and_white_movie / silent_movie).** Schema + aliases shipped 2026-05-20; junction backfill from the flags implemented in §12.5. Boolean flags stay as the documented filter path; junction rows are metadata-only.
3. ✅ **Implemented — `IS_3D → 3d_movie`** following the same B.1 pattern. Merged with the PR-2 migration per the 2026-05-20 decision; junction backfill in §12.5.
4. ✅ **Implemented — Proposal A.2 (dual model) plus dot-decimal canonical retarget.** Aspect-ratio rows under `TECHNICAL_TYPE='aspect_ratio'` shipped together with a one-shot canonical-convention flip from comma-decimal to dot-decimal for the column, the loader REGEXP, and every alias target. See §12. The multi-ratio junction backfill is implemented in §12.5. The `strapiversion` bump was skipped in favor of a manual cache clear (§12.3.C).
5. **Eventual cleanup** — retire the denormalized `COLOR_TECHNOLOGY / FILM_TECHNOLOGY / FILM_FORMAT / SOUND_SYSTEM / SOUND_TECHNOLOGY` text columns on `T_WC_T2S_MOVIE`. Same risk profile as B.2; defer until SQL-cache churn is acceptable.

A.1 (full aspect-ratio migration to ID-based resolution) is deliberately **not** in the recommended sequence. Its conceptual cleanliness is real, but the cost — 54 alias retargets, SQL-prompt rewrite, cache flush, evaluation re-runs, AGENTS.md updates — does not pay for itself unless and until the dual model from A.2 becomes visibly redundant.

---

## 9. Phased migration sequence (per-PR breakdown)

| PR | Scope | Files touched | Risk | Status |
|---|---|---|---|---|
| PR-1 | Add `technicals` collection to `/movies/{id}` | [main.py:1728-1776](main.py#L1728-L1776), [MCP.md:502-504](MCP.md#L502-L504), [main.py:2600-2611](main.py#L2600-L2611) | None — purely additive | ✅ Shipped (commit `9eb8460`) |
| PR-5 | Add `get_technical(id)` MCP tool + `/technicals/{id}` route | [main.py:2283-2329](main.py#L2283-L2329), [main.py:2735-2741](main.py#L2735-L2741), [MCP.md:218-232](MCP.md#L218-L232) | Low — net-new surface area | ✅ Shipped (commit `9eb8460`) |
| PR-2 | Insert color_movie / black_and_white_movie / silent_movie rows; backfill junction from flags; add `Technical_format` aliases | DB migration + [data/closed_vocabularies.json](data/closed_vocabularies.json); no prompt change, no code change | Low — new rows appear in metadata collection only | ✅ **Complete** — schema + aliases shipped 2026-05-20 (rows 57-59 in [archive/T_WC_T2S_TECHNICAL.sql](archive/T_WC_T2S_TECHNICAL.sql)); junction backfill implemented (§12.5) |
| PR-3 | Same pattern for `IS_3D → 3d_movie` | DB migration + [data/closed_vocabularies.json](data/closed_vocabularies.json) | Low | ✅ **Complete** — schema + aliases shipped 2026-05-20 (row 60); junction backfill implemented (§12.5) |
| PR-4 | Aspect-ratio rows + junction backfill (A.2 dual model) + dot-decimal canonical retarget | DB migration + one-shot UPDATE of `T_WC_T2S_MOVIE.ASPECT_RATIO` + REGEXP edit in [closed_vocab.py:171](closed_vocab.py#L171) + alias retarget in [data/closed_vocabularies.json:106-172](data/closed_vocabularies.json#L106-L172) + `strapiversion` bump | Low-medium — additive on the table side; the column-data backfill silently invalidates cached SQL referencing comma-decimal literals, handled by manual cache clear | ✅ **Complete** — schema (rows 61-73) + column UPDATE + REGEXP + JSON retarget shipped 2026-05-20 (`strapiversion` bump skipped — cache cleared manually); junction backfill implemented (§12.5) |
| PR-6 (deferred) | `T_WC_T2S_SERIE_TECHNICAL` junction + `technicals` collection on `/series/{id}` | DB migration + [main.py:1796-1934](main.py#L1796-L1934) + MCP docs | Low — additive, mirrors movie side | Deferred (out of scope per 2026-05-20 decision) |

Future cleanup pass (B.2-style migration of flags, A.1-style migration of aspect ratio, retirement of denormalized text columns) is out of scope for this analysis and should each warrant its own design pass when prioritized.

---

## 10. Verification plan

For each remaining PR:

- **Closed-vocab init smoke**: restart the service; confirm the per-entity count summary printed at [closed_vocab.py:257-258](closed_vocab.py#L257-L258) reflects the expected `Technical_format` delta (e.g., `+3` after PR-2).
- **`/movies/{id}` manual probe**: pick a movie ID known to carry rich technical metadata (a Technicolor 70mm Dolby Stereo release like *Lawrence of Arabia* or *2001*); verify the `technicals` array is populated with the correct rows and `technical_type` values. The query is already wired ([main.py:1728-1735](main.py#L1728-L1735)).
- **`/technicals/{id}` manual probe**: pick an ID (e.g. `technicolor`); verify the `movies` array and `siblings` array (other `color_technology` rows ordered by `MOVIE_COUNT DESC`) are populated.
- **MCP smoke**: call `mcp__claude_ai_text2sql__get_movie` and `mcp__claude_ai_text2sql__get_technical` for the same IDs; verify both return the expected payloads.
- **Eval baseline**: `python eval/text2sql-eval.py` against the full evaluation set; diff CSV output against the previous baseline. **Expectation under the recommended path: zero text2sql regressions** because no prompt is changed.
- **Targeted NL queries** (manual or scripted):
  - "color films of 1939" → SQL should still use `IS_COLOR = 1` (flag path, not junction).
  - "silent films of the 1920s" → `IS_SILENT = 1`.
  - "black and white movies with Bogart" → `IS_BLACK_AND_WHITE = 1`.
  - "Technicolor films" → `T_WC_T2S_MOVIE_TECHNICAL` JOIN with the technicolor ID (junction path, unchanged).
  - Each query's `/movies/{id}` follow-up should now show the relevant rows in the `technicals` collection.

---

## 11. Open questions for follow-up — **all resolved 2026-05-20**

All five questions below were decisions, not impacts; they were settled on 2026-05-20 and the implementation contract is now §12. The original framing is preserved verbatim and the resolution is appended in bold so the reasoning trail stays auditable.

1. **`DESCRIPTION` convention for aspect-ratio rows.** Dot-decimal (`"1.85"`) is recommended for alignment with the rest of `T_WC_T2S_TECHNICAL`. Alternative: named convention (`"Widescreen 1.85"`). The choice affects aliases and SQL JOIN output ergonomics.
   - **Resolved: dot-decimal** (`1.33`, `1.85`, `2.39`, …). The decision also propagates to `T_WC_T2S_MOVIE.ASPECT_RATIO` (one-shot UPDATE), the loader REGEXP in [closed_vocab.py:171](closed_vocab.py#L171), and every alias target in [data/closed_vocabularies.json:106-172](data/closed_vocabularies.json#L106-L172). See §12.1, §12.3, §12.4.
2. **`TECHNICAL_TYPE` for color / B&W / silent rows.** Recommended: `medium_format`. Alternatives: `movie_classification`, `presentation_format`. Pick one before PR-2.
   - **Resolved: `medium_format`.** Applies to the four classification rows (color / B&W / silent / 3D).
3. **Dual exposure on `/movies/{id}`** (Option B.1): keep `IS_COLOR / IS_BLACK_AND_WHITE / IS_SILENT` flags inline *and* in the technicals array, or only in the array? Recommended: keep both for backward compat.
   - **Resolved: keep both.** The four flag fields stay in the inline `/movies/{id}` payload; the new rows additionally surface through the `technicals` collection. Same pattern extends to `IS_3D`.
4. **First-class entity columns for new rows.** With `T_WC_T2S_TECHNICAL` now carrying `ID_WIKIDATA`, `DESCRIPTION_FR`, `OVERVIEW`, `WIKIPEDIA_IMAGE_PATH`, `IMDB_RATING_WEIGHTED`, `POPULARITY`, any newly inserted aspect-ratio or color/B&W/silent rows should populate these where data exists. Open question: are these well-defined for `color_movie` / `silent_movie` style classification rows, or should they be left NULL?
   - **Resolved: NULL at insert, backfill on the existing Wikidata/Wikipedia search workflow pass.** `DESCRIPTION_FR` populated inline for the four classification rows where the French translation is trivial (`couleur`, `noir et blanc`, `muet`, `3D`); all other first-class columns left NULL and filled in later by whatever workflow stamped `TIM_WIKIPEDIA_SEARCH` on the existing rows.
5. **Series side** (`T_WC_T2S_SERIE_TECHNICAL`): now explicitly flagged as missing in the shipped `/technicals/{id}` docstring at [main.py:2294](main.py#L2294). In scope for this initiative, or a separate workstream? Recommended: separate workstream (PR-6 above).
   - **Resolved: out of scope.** `T_WC_T2S_SERIE_TECHNICAL` is not created in this initiative. The `/technicals/{id}` docstring at [main.py:2294](main.py#L2294) continues to acknowledge the gap.

---

## 12. Implementation spec — PR-2 + PR-3 + PR-4 (locked in 2026-05-20)

The decisions from §11 are operationalized here. This section is the single source of truth for both the implementing agent (DB migration, code/config edits) and the pre-processing program owner (junction-table enrichment). The analysis sections (§3, §4) explain *why* these choices were made and remain the reference for any future reconsideration.

### 12.1 Decisions taken

| Question (§11) | Decision |
|---|---|
| §11.1 — Aspect-ratio `DESCRIPTION` convention | **Dot-decimal** (`1.33`, `1.85`, `2.39`, …) — applies to `T_WC_T2S_TECHNICAL.DESCRIPTION`, `T_WC_T2S_MOVIE.ASPECT_RATIO`, the loader REGEXP, and every alias target. |
| §11.2 — `TECHNICAL_TYPE` for color / B&W / silent / 3D | **`medium_format`**. |
| §11.3 — Dual exposure on `/movies/{id}` | **Keep both.** Flag fields stay in the inline payload; new rows additionally surface through the `technicals` collection. |
| §11.4 — First-class entity columns for new rows | **NULL at insert** (except trivial `DESCRIPTION_FR` for the 4 classification rows); the existing Wikidata/Wikipedia-search workflow backfills the rest. |
| §11.5 — Series side | **Out of scope.** No `T_WC_T2S_SERIE_TECHNICAL` in this initiative. |

### 12.2 SQL — insert new rows into `T_WC_T2S_TECHNICAL` — ✅ **Applied 2026-05-20**

Executed once on 2026-05-20. `1.77` / `1.89` / `1.90` were dropped from the planned 16-ratio set as too niche to seed; **13 aspect-ratio rows shipped (IDs 61–73), not 16**. The reference SQL below is preserved for traceability and matches what was actually inserted (less those three rows). Wikidata linkage was populated through the existing Wikipedia-search workflow after the inserts.

```sql
START TRANSACTION;

-- 4 classification rows (IDs 57-60), TECHNICAL_TYPE='medium_format'
INSERT INTO `T_WC_T2S_TECHNICAL`
  (ID_TECHNICAL, DESCRIPTION, DESCRIPTION_FR, TECHNICAL_TYPE,
   DELETED, ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED)
VALUES
  (57, 'color_movie',           'couleur',       'medium_format', 0, 1, CURDATE(), 1, NOW(), 1),
  (58, 'black_and_white_movie', 'noir et blanc', 'medium_format', 0, 1, CURDATE(), 1, NOW(), 1),
  (59, 'silent_movie',          'muet',          'medium_format', 0, 1, CURDATE(), 1, NOW(), 1),
  (60, '3d_movie',              '3D',            'medium_format', 0, 1, CURDATE(), 1, NOW(), 1);

-- 13 aspect-ratio rows (IDs 61-73), TECHNICAL_TYPE='aspect_ratio', dot-decimal DESCRIPTION
INSERT INTO `T_WC_T2S_TECHNICAL`
  (ID_TECHNICAL, DESCRIPTION, TECHNICAL_TYPE,
   DELETED, ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED)
VALUES
  (61, '1.33', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (62, '1.37', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (63, '1.43', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (64, '1.66', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (65, '1.78', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (66, '1.85', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (67, '2.00', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (68, '2.20', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (69, '2.35', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (70, '2.39', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (71, '2.40', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (72, '2.55', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1),
  (73, '2.76', 'aspect_ratio', 0, 1, CURDATE(), 1, NOW(), 1);

COMMIT;
```

Rationale for the 13-ratio set: covers the most common Hollywood / European canonicals (the seven already implied by previous comma-decimal aliases: `1.33` / `1.37` / `1.66` / `1.78` / `1.85` / `2.35` / `2.39` / `2.40` / `2.55`) plus four additions anticipated from richer source data (`1.43` IMAX, `2.00` Univisium, `2.20` 70mm theatrical, `2.76` Ultra Panavision 70). `1.77` / `1.89` / `1.90` were skipped as too niche to seed; the pre-processing program logs them as unknown if encountered. Add more rows later if source data demands.

`ID_WIKIDATA` was populated for the 17 rows after insert by the existing Wikidata search workflow (`TIM_WIKIPEDIA_SEARCH` stamped 2026-05-20). `OVERVIEW`, `WIKIPEDIA_IMAGE_PATH`, IMDb ratings, and `POPULARITY` are still NULL and will be filled by subsequent enrichment passes.

### 12.3 Code / config edits — ✅ **Applied 2026-05-20** (except §12.3.C, deliberately skipped)

A, B, and D below shipped together with §12.2 on 2026-05-20; C was skipped after the SQL cache was cleared manually. These three edits are the difference between "rows inserted" and "rows actually integrated into the pipeline" — preserved here as the historical contract.

**A. `closed_vocab.py` — switch the Aspect_ratio loader REGEXP from comma to dot.** ✅ **Applied 2026-05-20.** At [closed_vocab.py:162-176](closed_vocab.py#L162-L176):

```python
_ASPECT_RATIO_QUERY = (
    # Restrict canonicals to well-formed DOT-decimal values (e.g. '1.33',
    # '1.85', '2.35'). ...
    "SELECT DISTINCT ASPECT_RATIO AS V FROM T_WC_T2S_MOVIE "
    "WHERE ASPECT_RATIO IS NOT NULL "
    "AND ASPECT_RATIO REGEXP '^[0-9]+\\.[0-9]+$'"
)
```

The escape sequence `\\.` in the Python string literal → `\.` in the MariaDB regex matches the literal dot (otherwise `.` would match any character and `1X33` would match too). The change takes effect at the next API restart; until then the previous canonical map remains in memory.

**B. `data/closed_vocabularies.json` — retarget every `Aspect_ratio` alias to dot-decimal, and (separately) add the 4 classification aliases under `Technical_format`.**

In the `Aspect_ratio` block at [lines 106-172](data/closed_vocabularies.json#L106-L172): ✅ **Applied 2026-05-20.**

- `_comment` rewritten to describe dot-decimal targets and the new REGEXP. Cross-reference to `Technical_format` for `anamorphic / scope / cinemascope` retained.
- Every value flipped from comma- to dot-decimal (`"4:3": "1.33"`, `"widescreen": "1.85"`, etc.).
- Added bare comma-decimal forms as aliases (`"1,33" → "1.33"`, …) so French-notation user input still resolves correctly.
- Added aliases for the four new ratios that didn't exist in the previous canonical set: `1.43` (IMAX), `2.00` (Univisium, with `"2:1"` and `"univisium"`), `2.20`, `2.76` (with `"ultra panavision 70"` and `"mgm camera 65"`).
- The 12 self-referential `"X.YZ": "X,YZ"` entries (e.g. `"1.33": "1,33"`) were dropped — once the canonical is dot-decimal, `"1.33"` is a direct canonical hit and the alias is redundant.
- File takes effect via hot-reload (~5 s) but resolution requires the §12.4 column UPDATE to be applied first; otherwise the canonical map is empty and `resolve('Aspect_ratio', ...)` returns `None` (see resolver early-return at [closed_vocab.py:66-67](closed_vocab.py#L66-L67)).

In the `Technical_format` block at [lines 205-237](data/closed_vocabularies.json#L205-L237), append the new aliases: ✅ **Applied 2026-05-20** — 26 EN + FR aliases shipped covering the four `medium_format` canonicals:

```
"color":            "color_movie",
"colour":           "color_movie",
"in color":         "color_movie",
"color film":       "color_movie",
"colored":          "color_movie",
"couleur":          "color_movie",
"en couleur":       "color_movie",
"film en couleur":  "color_movie",
"black and white":  "black_and_white_movie",
"black-and-white":  "black_and_white_movie",
"black & white":    "black_and_white_movie",
"b&w":              "black_and_white_movie",
"bw":               "black_and_white_movie",
"monochrome":       "black_and_white_movie",
"noir et blanc":    "black_and_white_movie",
"n&b":              "black_and_white_movie",
"silent":           "silent_movie",
"silent film":      "silent_movie",
"silent movie":     "silent_movie",
"muet":             "silent_movie",
"film muet":        "silent_movie",
"3d":               "3d_movie",
"3-d":              "3d_movie",
"stereoscopic":     "3d_movie",
"film 3d":          "3d_movie",
"stéréoscopique":   "3d_movie"
```

Do **not** add negation aliases (`"talkie"`, `"sound film"`, `"in color" → not B&W`, etc.) — the resolver is positive-match only; negation belongs to the SQL prompt and the flag stays the documented filter path.

These aliases are dual-purpose: (1) they let `resolve_technical("color")` etc. return the right `ID_TECHNICAL` if the LLM ever extracts these as a `Technical_format` placeholder, and (2) more importantly, the **pre-processing program reads this JSON** to normalize source-data terms (`"color"`, `"couleur"`, `"B&W"`, `"noir et blanc"`, `"silent"`, `"muet"`, `"3D"`, …) to the canonical `DESCRIPTION` value when populating `T_WC_T2S_MOVIE_TECHNICAL`. The §12.5 spec relies on these aliases being present.

**C. `strapiversion` bump at [main.py:105](main.py#L105).** ⏭️ **Skipped 2026-05-20** — the SQL cache was manually cleared for `ASPECT_RATIO` queries, so the version-filter invalidation path is unnecessary for this migration. If a future change re-introduces canonical-value drift in cached SQL, prefer the version-bump path documented here (AGENTS.md Gotcha #2) over manual cache surgery.

**D. `data/text_to_sql.md` — no change.** The flag-based rules at [lines 736-739](data/text_to_sql.md#L736-L739) (`IS_COLOR = 1`, `IS_BLACK_AND_WHITE = 1`, `IS_SILENT = 1`) and the column-based `ASPECT_RATIO = '...'` rule stay as-is — they remain the documented filter path. Do **not** add a JOIN path for color / B&W / silent / 3D / aspect_ratio filtering; that would re-introduce the LLM inconsistency the §3.2 / §4.1 risk notes warn against. The schema dump already lists `ASPECT_RATIO VARCHAR(20)`; once the column data is dot-decimal, no schema-dump edit is needed because the dump documents the *type*, not the value convention.

### 12.4 One-shot data backfill for `T_WC_T2S_MOVIE.ASPECT_RATIO` (comma → dot) — ✅ **Applied 2026-05-20**

Executed once on 2026-05-20. Rewrote canonical comma-decimal values to dot-decimal. Noisy variants (`'4:3'`, `'235:1'`, `'16:9'`, etc.) stay as-is and are normalized by the pre-processing program (§12.5.3) when it next ingests the affected movies — that program now owns ratio normalization for both the column and the junction.

```sql
-- Rewrite canonical comma-decimal values to dot-decimal
UPDATE T_WC_T2S_MOVIE
   SET ASPECT_RATIO = REPLACE(ASPECT_RATIO, ',', '.')
 WHERE ASPECT_RATIO REGEXP '^[0-9]+,[0-9]+$';
```

Sanity check after the UPDATE:

```sql
SELECT ASPECT_RATIO, COUNT(*) FROM T_WC_T2S_MOVIE
 WHERE ASPECT_RATIO IS NOT NULL
 GROUP BY ASPECT_RATIO
 ORDER BY 2 DESC;
-- Expect: every well-formed entry is dot-decimal; noisy variants (`'4:3'`, `'16:9'`, etc.)
-- remain as-is and will be normalized on next pre-processing pass.
```

### 12.5 Pre-processing program specification — `T_WC_T2S_MOVIE_TECHNICAL` enrichment — ✅ **Implemented**

This subsection is the contract the pre-processing program implements against. It is concrete and exhaustive — the program needs no extra context to adapt to the new rows. Everything the program reads from is already in place as of 2026-05-20: the 17 rows in `T_WC_T2S_TECHNICAL` (§12.2), the dot-decimal column data in `T_WC_T2S_MOVIE.ASPECT_RATIO` (§12.4), the dot-decimal `Aspect_ratio` aliases and the new `Technical_format` classification aliases in `data/closed_vocabularies.json` (§12.3.B). The program writes to `T_WC_T2S_MOVIE_TECHNICAL` and (for the primary aspect ratio) `T_WC_T2S_MOVIE.ASPECT_RATIO`.

> **Implementation note (2026-05-31).** This spec is now realized in code. The enrichment runs inside the `WIKIPEDIA_FORMAT_LINE` step (process index 1), immediately after `batch_update_data` writes the parsed flags and `ASPECT_RATIO` column: see [tmdb-movie-preprocess.py:254-298](tmdb-movie-preprocess.py#L254-L298). The three helpers live in [tmdb_preprocess_helpers.py](tmdb_preprocess_helpers.py): `load_technical_ids` ([:974](tmdb_preprocess_helpers.py#L974), §12.5.1), `write_movie_technical_junction` + `_movie_technical_target_rows` ([:1065](tmdb_preprocess_helpers.py#L1065) / [:1026](tmdb_preprocess_helpers.py#L1026), §12.5.2–§12.5.5), and `refresh_technical_movie_count` ([:1149](tmdb_preprocess_helpers.py#L1149), §12.5.6). The primary `ASPECT_RATIO` column value (step 4 below) is written by `batch_update_data` from the `ASPECT_RATIO` field of `extract_format_components`; the junction rows are written from the separate `ASPECT_RATIO_LIST` field, so the single-value column and the multi-row junction are populated from the same parse but by two different writers. Sub-sections below are the as-built description, not a to-do list.

#### 12.5.1 Startup — cache the 17 lookups by DESCRIPTION + TECHNICAL_TYPE

Never hardcode integer IDs in the program — IDs can shift if the migration is re-run with different baselines. Resolve once at start of run and cache in memory:

```sql
SELECT DESCRIPTION, ID_TECHNICAL
  FROM T_WC_T2S_TECHNICAL
 WHERE TECHNICAL_TYPE IN ('medium_format', 'aspect_ratio')
   AND (DELETED = 0 OR DELETED IS NULL);
```

The result populates two dictionaries the program uses everywhere below:

```
classification_id = {
  'color_movie':           57,
  'black_and_white_movie': 58,
  'silent_movie':          59,
  '3d_movie':              60,
}

aspect_ratio_id = {
  '1.33': 61, '1.37': 62, '1.43': 63, '1.66': 64,
  '1.78': 65, '1.85': 66, '2.00': 67, '2.20': 68,
  '2.35': 69, '2.39': 70, '2.40': 71, '2.55': 72,
  '2.76': 73,
}
```

The IDs above are the actual values shipped 2026-05-20 and reflected in [archive/T_WC_T2S_TECHNICAL.sql](archive/T_WC_T2S_TECHNICAL.sql#L116-L132). The implemented `load_technical_ids` resolves them dynamically by `(DESCRIPTION, TECHNICAL_TYPE)` at startup rather than hardcoding them as integers — if anyone re-seeds or re-numbers the table in the future, the lookup adapts; the inline values are documentation, not source code.

If any expected `DESCRIPTION` key is missing from the result of the startup query, **fail loudly** — do not silently skip; that signals the migration is incomplete. Conversely, if the source data contains a ratio that does not normalize to one of the 13 dot-decimal canonicals above (e.g. `1.77`, `1.89`, `1.90` — present in the column but not seeded as `T_WC_T2S_TECHNICAL` rows), log the offending raw value + movie ID and skip the junction insert for that ratio. If those ratios become common enough to warrant rows, extend `T_WC_T2S_TECHNICAL` and §12.5.4 together.

#### 12.5.2 Classification backfill (color / B&W / silent / 3D)

**Source of truth:** the four flag columns on `T_WC_T2S_MOVIE` (`IS_COLOR`, `IS_BLACK_AND_WHITE`, `IS_SILENT`, `IS_3D`). The junction rows are a denormalized projection used only by the `/movies/{id}` `technicals` collection.

For each movie `m`:

| Flag | Action |
|---|---|
| `m.IS_COLOR = 1` | emit junction row → `classification_id['color_movie']` |
| `m.IS_BLACK_AND_WHITE = 1` | emit junction row → `classification_id['black_and_white_movie']` |
| `m.IS_SILENT = 1` | emit junction row → `classification_id['silent_movie']` |
| `m.IS_3D = 1` | emit junction row → `classification_id['3d_movie']` |

Rules:
- Treat NULL flag as 0 (no row emitted).
- A movie with **both** `IS_COLOR = 1` and `IS_BLACK_AND_WHITE = 1` (e.g. *Wizard of Oz*, *Schindler's List*) emits **two** rows — one per flag. This is the expected behaviour, not an error.
- `DISPLAY_ORDER` in the junction row: use `1, 2, 3, 4` in the order color / B&W / silent / 3D for any subset emitted. The `/movies/{id}` endpoint orders `ORDER BY mt.DISPLAY_ORDER ASC` ([main.py:1728-1735](main.py#L1728-L1735)) so a stable order produces a stable payload.

#### 12.5.3 Aspect-ratio backfill — **multi-ratio per movie is required**

> ### Critical requirement
> **The pre-processing program MUST create one junction row per ratio when the source data lists multiple aspect ratios for a single film.** Real source data routinely lists 2-3 ratios for the same movie — theatrical (e.g. `2.39`), home video (`1.78`), IMAX (`1.43`). All of them belong in `T_WC_T2S_MOVIE_TECHNICAL`. The single-valued `T_WC_T2S_MOVIE.ASPECT_RATIO` column holds only the **primary** (theatrical) ratio for backward compatibility with the closed-vocab `Aspect_ratio` canonical map and with existing SQL.
>
> A movie with three source-supplied ratios produces **three** rows in `T_WC_T2S_MOVIE_TECHNICAL` (all sharing the same `ID_MOVIE`, each with a different `ID_TECHNICAL` and incrementing `DISPLAY_ORDER`) **plus one** value in `T_WC_T2S_MOVIE.ASPECT_RATIO` (the primary).

Algorithm — for each movie `m` with raw aspect-ratio data (a list of 0-N raw strings from the source):

1. **Normalize each raw entry** to a dot-decimal canonical using the table in §12.5.4. Drop case, drop surrounding whitespace and quotes before matching.
2. **Drop duplicates** after normalization. If a source provides both `"2.40"` and `"2.39"` they collapse to the most specific known canonical (here, both remain distinct in the table since both rows exist; merging is a future-A.1 concern, not for the script).
3. **Drop entries that don't normalize** — log them with the movie ID and the raw string so they can be reviewed and the normalization map extended if needed. Do not invent rules from a single noisy sample.
4. **Choose the primary ratio.** Theatrical preferred; otherwise the first source-supplied entry. Write its dot-decimal canonical to `T_WC_T2S_MOVIE.ASPECT_RATIO`. Leave NULL only if the source list is empty.
5. **For each surviving normalized DESCRIPTION** (primary + the others):
   - Look up `aspect_ratio_id[DESCRIPTION]` (must exist; otherwise it's a §12.2 / §12.5.1 mismatch — fail loudly).
   - Emit one junction row in `T_WC_T2S_MOVIE_TECHNICAL`:
     - `ID_MOVIE = m.ID_MOVIE`
     - `ID_TECHNICAL = aspect_ratio_id[DESCRIPTION]`
     - `DISPLAY_ORDER = 1` for the primary, then `2, 3, ...` for the rest in source order.
     - other columns: per the table's defaults.

Single-source-ratio is the common case today; multi-ratio is what unlocks the original motivation in §1.1 ("a movie can ship in several aspect ratios across versions"). The same algorithm handles both — `N=1` is just the degenerate case.

#### 12.5.4 Normalization map for aspect ratios

Apply to each raw entry: lowercase, strip leading/trailing whitespace and surrounding quotes, then match against any of the patterns in the same row of the table. Match by the **target's set of patterns**, not by string equality — a single raw value can match exactly one row.

| Raw value patterns (any of these, case-insensitive) | Normalized DESCRIPTION |
|---|---|
| `1,33`, `1.33`, `4:3`, `4/3`, `4x3`, `1:33`, `1.33:1`, `1,33:1`, `133:100`, `fullscreen`, `full screen` | `1.33` |
| `1,37`, `1.37`, `1:37`, `1.37:1`, `1,37:1`, `137:100`, `academy`, `academy ratio`, `academy aperture` | `1.37` |
| `1,43`, `1.43`, `1.43:1`, `imax`, `imax 70mm`, `imax 1.43` | `1.43` |
| `1,66`, `1.66`, `1:66`, `1.66:1`, `1,66:1`, `5:3`, `15:9`, `166:100`, `european widescreen` | `1.66` |
| `1,78`, `1.78`, `1:78`, `1.78:1`, `1,78:1`, `16:9`, `16/9`, `16x9`, `178:100`, `hdtv` | `1.78` |
| `1,85`, `1.85`, `1:85`, `1.85:1`, `1,85:1`, `widescreen`, `flat`, `us widescreen`, `185:100` | `1.85` |
| `2,00`, `2.00`, `2:1`, `univisium` | `2.00` |
| `2,20`, `2.20`, `70mm widescreen` | `2.20` |
| `2,35`, `2.35`, `2:35`, `2.35:1`, `2,35:1`, `235:1`, `cinemascope` (pre-1970), `scope` (pre-1970) | `2.35` |
| `2,39`, `2.39`, `2:39`, `2.39:1`, `2,39:1`, `239:1`, `21:9`, `modern anamorphic` | `2.39` |
| `2,40`, `2.40`, `2.40:1`, `2,40:1`, `240:1` | `2.40` |
| `2,55`, `2.55`, `2:55`, `2.55:1`, `2,55:1`, `255:1`, `cinemascope 55` | `2.55` |
| `2,76`, `2.76`, `2.76:1`, `2,76:1`, `ultra panavision`, `mgm camera 65` | `2.76` |

Notes:
- **13 canonicals, not 16.** `1.77`, `1.89`, `1.90` were deliberately skipped at seed time (2026-05-20) — they exist as values in `T_WC_T2S_MOVIE.ASPECT_RATIO` but no `T_WC_T2S_TECHNICAL` row was created. The program **must not** create junction rows for raw values that normalize to `1.77` / `1.89` / `1.90`; log them with the movie ID and skip. If those ratios later prove common, extend the `T_WC_T2S_TECHNICAL` table **and** this normalization table together.
- `2.35` / `2.39` / `2.40` are intentionally kept distinct in the table even though some catalogs treat them as the same. If your source data does not distinguish them reliably and they collapse to a single value, fold them in the normalization map (e.g. all `2.40` → `2.39`); do **not** delete the DB rows because cached references and the `/technicals/{id}` `siblings` array would silently break.
- `scope`, `anamorphic`, `cinemascope` (without an era qualifier) are **ambiguous** between an aspect ratio and a film technology. The doc-level rule is that the *bare* terms `anamorphic` / `scope` / `cinemascope` live under `Technical_format` (the existing film_technology row `cinemascope` is ID 5; see [data/closed_vocabularies.json](data/closed_vocabularies.json#L215)). Use them as aspect-ratio aliases only when the source explicitly pairs them with an era or a numeric value.

#### 12.5.5 Idempotency — re-sync pattern

The pre-processing program must be safe to re-run. Scope every DELETE by `TECHNICAL_TYPE` so the program never touches rows owned by other ingestion paths.

```sql
-- Re-sync classification rows for one movie (scope: medium_format only)
DELETE FROM T_WC_T2S_MOVIE_TECHNICAL
 WHERE ID_MOVIE = ?
   AND ID_TECHNICAL IN (
       SELECT ID_TECHNICAL FROM T_WC_T2S_TECHNICAL
        WHERE TECHNICAL_TYPE = 'medium_format'
   );

-- Re-sync aspect-ratio rows for one movie (scope: aspect_ratio only)
DELETE FROM T_WC_T2S_MOVIE_TECHNICAL
 WHERE ID_MOVIE = ?
   AND ID_TECHNICAL IN (
       SELECT ID_TECHNICAL FROM T_WC_T2S_TECHNICAL
        WHERE TECHNICAL_TYPE = 'aspect_ratio'
   );
```

Then INSERT the rows computed in §12.5.2 and §12.5.3.

**Do not** wipe rows belonging to sibling types (`sound_system`, `color_technology`, `film_technology`, `sound_technology`, `film_format`) — those are owned by separate ingestion code and unrelated to this initiative. A blanket `DELETE WHERE ID_MOVIE = ?` would clobber them.

#### 12.5.6 Post-pass — refresh the denormalized `MOVIE_COUNT` counter

Run once after the bulk backfill completes (and again on every subsequent script run):

```sql
UPDATE T_WC_T2S_TECHNICAL t
   SET MOVIE_COUNT = (
       SELECT COUNT(*) FROM T_WC_T2S_MOVIE_TECHNICAL mt
        WHERE mt.ID_TECHNICAL = t.ID_TECHNICAL
   )
 WHERE t.TECHNICAL_TYPE IN ('medium_format', 'aspect_ratio');
```

`MOVIE_COUNT` drives the `/technicals/{id}` `siblings` array ordering ([main.py:2316-2322](main.py#L2316-L2322)) — refresh keeps that ordering accurate.

#### 12.5.7 Logging the program must produce

For every script run:
- Count of classification junction rows written (broken down by `color_movie / B&W / silent / 3D`).
- Count of aspect-ratio junction rows written, plus **count of movies that got 2+ rows** (the multi-ratio case — the metric that proves the multi-ratio path is exercised).
- Count and sample list of raw aspect-ratio entries that failed normalization, with the offending movie IDs.
- Count of movies whose ASPECT_RATIO column was updated to a new primary value.
- Wall-clock for each phase.

These logs are the only visibility into whether the multi-ratio requirement is being satisfied — surface them prominently.

### 12.6 Verification

After §12.2 + §12.3 + §12.4 + the first §12.5 script run, in order:

1. **DB state checks** —
   ```sql
   SELECT TECHNICAL_TYPE, COUNT(*) FROM T_WC_T2S_TECHNICAL
    WHERE (DELETED = 0 OR DELETED IS NULL) GROUP BY TECHNICAL_TYPE;
   -- Expect: medium_format=4, aspect_ratio=13, plus the existing 56 rows
   --         across color_technology, film_format, film_technology, sound_system, sound_technology.

   SELECT COUNT(DISTINCT ID_MOVIE) FROM T_WC_T2S_MOVIE_TECHNICAL mt
     JOIN T_WC_T2S_TECHNICAL t ON t.ID_TECHNICAL = mt.ID_TECHNICAL
    WHERE t.TECHNICAL_TYPE = 'aspect_ratio';
   -- Expect: close to the count of T_WC_T2S_MOVIE rows with non-NULL ASPECT_RATIO,
   --         possibly higher once multi-ratio source data is processed.

   SELECT ID_MOVIE, COUNT(*) AS n
     FROM T_WC_T2S_MOVIE_TECHNICAL mt
     JOIN T_WC_T2S_TECHNICAL t ON t.ID_TECHNICAL = mt.ID_TECHNICAL
    WHERE t.TECHNICAL_TYPE = 'aspect_ratio'
    GROUP BY ID_MOVIE HAVING n > 1 LIMIT 20;
   -- The multi-ratio sample. Expected to be non-empty once multi-ratio source data lands.

   SELECT ASPECT_RATIO, COUNT(*) FROM T_WC_T2S_MOVIE
    WHERE ASPECT_RATIO IS NOT NULL GROUP BY ASPECT_RATIO ORDER BY 2 DESC LIMIT 20;
   -- Expect: dot-decimal canonicals only; no leftover comma-decimal values.
   ```

2. **Restart the API.** Required because `closed_vocab.py` changed.

3. **Closed-vocab init log** ([closed_vocab.py:257-258](closed_vocab.py#L257-L258)) should show `Technical_format` canonicals up by **17** (4 + 13) and `Aspect_ratio` canonical count consistent with the dot-decimal values in the column.

4. **Cache filter sanity.** After the `strapiversion` bump, new requests populate fresh cache rows with dot-decimal `ASPECT_RATIO = '1.85'` SQL. Old comma-decimal rows still exist under the previous version but are filtered out by the version predicate (AGENTS.md Gotcha #2).

5. **End-to-end NL queries** (manual):
   - "movies in 1.85" → `ASPECT_RATIO = '1.85'` (column path, dot-decimal).
   - "widescreen films of the 60s" → `ASPECT_RATIO = '1.85'` (alias path → dot-decimal target).
   - "4:3 movies" → `ASPECT_RATIO = '1.33'` (alias path).
   - "color movies of 1939" → `IS_COLOR = 1` (flag path, unchanged).
   - "silent films of the 1920s" → `IS_SILENT = 1` (flag path, unchanged).
   - "Technicolor films" → JOIN with `ID_TECHNICAL = 4` (existing, unchanged).
   - "/movies/{id}" follow-up on each → `technicals` collection includes the matched row.
   - "/technicals/{id}" on `1.85` (ID **66** in the seed migration) → `movies` populated, `siblings` array shows the other 12 `aspect_ratio` rows ordered by `MOVIE_COUNT DESC`.

6. **Rollback notes.** Reversible in pieces: schema additions are pure INSERTs; alias retarget is a `git revert`; the `ASPECT_RATIO` column backfill is reversible via `UPDATE T_WC_T2S_MOVIE SET ASPECT_RATIO = REPLACE(ASPECT_RATIO, '.', ',') WHERE ASPECT_RATIO REGEXP '^[0-9]+\\.[0-9]+$';`; the `closed_vocab.py` REGEXP is a one-line edit. The `strapiversion` bump cannot be cleanly reverted once new cache rows exist under it — use a forward bump if rollback proves necessary.

### 12.7 What is explicitly NOT in scope

- `T_WC_T2S_SERIE_TECHNICAL` and any series-side junction work (PR-6, deferred).
- Any change to the text-to-SQL prompt at [data/text_to_sql.md](data/text_to_sql.md).
- Any resolver dispatch change in [entity.py](entity.py).
- Retirement of the `IS_COLOR / IS_BLACK_AND_WHITE / IS_SILENT / IS_3D` flag columns (B.2 — explicitly rejected).
- Retirement of the denormalized `COLOR_TECHNOLOGY / FILM_TECHNOLOGY / FILM_FORMAT / SOUND_SYSTEM / SOUND_TECHNOLOGY` text columns on `T_WC_T2S_MOVIE` (future cleanup pass).
- Full migration of aspect ratio to ID-based resolution (A.1 — explicitly rejected; A.2 dual model is what shipped).
