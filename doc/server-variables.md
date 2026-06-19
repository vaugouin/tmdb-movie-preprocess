# Server variables — cross-repository telemetry

> **Scope.** This document describes the **server variable** mechanism shared across the data-management
> repositories listed in [related-repositories.txt](related-repositories.txt): what server variables are,
> the `str<repo><entity><field>` naming convention, where they are stored / managed / displayed, and a
> per-repository catalogue of the VAR_NAME prefixes in use. It is the cross-repo companion to the
> per-entity telemetry sections in the entity docs (e.g. §7 of
> [groups-multi-repo-management.md](groups-multi-repo-management.md)).

---

## 1. What a server variable is

A **server variable** is a single row in the `T_WC_SERVER_VARIABLE` table — a typed key/value record the
batch processes use to publish their **work-in-progress state** to a shared, queryable place. Unlike log
lines, server variables are *current-state* (last write wins): "where is the crawler now", "how many rows
did the last scope process", "when did this run start", "what is the configurable LIMIT for this scope".

They serve three roles:

1. **Progress / monitoring** — a dashboard or operator can read the latest `currentvalue`, `wikidataid`,
   counts and timestamps without attaching to the process.
2. **Resumability** — a process restarted mid-run reads back its own `…id` / `…resumeid` to continue
   where it left off rather than rescanning from the top.
3. **Tuning** — a handful of variables are **inputs** the operator sets (e.g. `…limit`,
   `…instanceof`), read by the process at startup with a sensible default if unset.

Key columns of `T_WC_SERVER_VARIABLE`: `VAR_NAME` (the unique key), `VAR_VALUE` (the current value, as
text), `DESCRIPTION` / `LONG_DESC` (human label), `TIM_UPDATED` (last write), `DELETED`.

---

## 2. Naming convention — `str<repo><entity><field>`

Every variable name is a lower-case concatenation, no separators:

```
str  +  <repo/process>  +  [<entity>]  +  <field>
```

| Segment | Examples | Notes |
|---------|----------|-------|
| `str` | — | universal prefix (the codebase's Hungarian-style "string" tag) |
| `<repo/process>` | `tmdbcrawler`, `wikipediacrawler`, `sparqlcrawler`, `sparqlcrawlert2s`, `embeddingupdate`, `tmdbmoviepreprocess` | identifies the producing program; SPARQL T2S scopes add a `t2s` infix |
| `<entity>` | `group`, `person`, `movie`, `serie`, `award`, `death`, `collection`, … | present for per-entity processes; absent for whole-program variables |
| `<field>` | `id`, `wikidataid`, `currentprocess`, `currentvalue`, `processedcount`, `processedseconds`, `limit`, `startdatetime`, `enddatetime`, `englishcount`, `frenchcount`, `deletereport`, `notdeletereport` | the specific datum |

Example decompositions:

- `strsparqlcrawlert2sgrouppropertiesprocessedcount` → sparql-crawler · T2S · **group** properties · processed count
- `strwikipediacrawlergroupstartdatetime` → wikipedia-crawler · **group** · run start timestamp
- `strembeddingupdategroupnotdeletereport` → embedding-update · **group** · "not deleted" report

The shared `<field>` vocabulary (reused across repos/entities):

| Field | Meaning |
|-------|---------|
| `id` / `resumeid` | current (or resume) primary-key position |
| `wikidataid` | current Wikidata Q-number being processed |
| `currentprocess` | label of the sub-process currently running |
| `currentvalue` | current item label / batch range being worked |
| `processedcount` | number of rows the last query/scope returned or processed |
| `processedseconds` | elapsed wall-clock for the last scope |
| `limit` | **input** — tunable `LIMIT` applied to the scope's SQL |
| `startdatetime` / `enddatetime` | run window |
| `englishcount` / `frenchcount` | per-language counts (Wikipedia) |
| `deletereport` / `notdeletereport` | embedding-sync delete/keep summaries |

---

## 3. Storage, CRUD and display

| Concern | Location | Notes |
|---------|----------|-------|
| Table | `T_WC_SERVER_VARIABLE` | full export: [doc/sql/T_WC_SERVER_VARIABLE.sql](../sql/T_WC_SERVER_VARIABLE.sql) |
| Read/write from code | `cp.f_getservervariable(name, lang)` / `cp.f_setservervariable(name, value, description, lang)` | the `citizenphil` helper used by every Python process; `f_getservervariable` returns `""` when unset, the idiom being "if empty, seed a default and `f_setservervariable` it" |
| CRUD (web) | [html/back/srvvar.php](../../html/back/srvvar.php) | thin wrapper over the generic `form-process-everything.inc.php` form processor |
| Display (web) | [lib/srvvar.inc.php](../../lib/srvvar.inc.php) | renders **one section per VAR_NAME prefix** |

The display script keeps an ordered `$arrprefixes` array; each prefix becomes an `<h2>` section listing
every `T_WC_SERVER_VARIABLE` row whose `VAR_NAME LIKE '<prefix>%'`, newest-updated first. Adding a new
process/entity means appending its prefix to that array.

---

## 4. Producer catalogue (by repository)

Prefixes currently registered in [lib/srvvar.inc.php](../../lib/srvvar.inc.php). A bare repo prefix (e.g.
`strtmdbcrawler`) catches the program-wide variables; per-entity prefixes catch the entity-scoped ones.

| Repository | VAR_NAME prefix(es) |
|------------|---------------------|
| `tmdb-crawler` | `strtmdbcrawlertmdbid`, `strtmdbcrawlerchanges`, `strtmdbcrawler` |
| `imdb-crawler` | `strimdbcrawler` |
| `tmdb-movie-preprocess` | `strtmdbmoviepreprocess` + a per-entity run window for **every** process: dimension derivations `topic`, `collection`, `list`, `group`, `award`, `movement`, `death`, `nomination`, `character`; copy/rebuild steps `movie`, `serie`, `person`, `company`, `network`, `personmovie`, `personserie`, `moviegenre`, `seriegenre`, `moviecompany`, `seriecompany`, `serienetwork`, `movieproductioncountry`, `serieproductioncountry`, `moviespokenlanguage`, `seriespokenlanguage`, `companyimage`, `movieimage`, `networkimage`, `personimage`, `serieimage`, `movievideo`, `serievideo`, `season`, `episode`, `personseason`, `personepisode`, `seasonimage`, `episodeimage`, `seasonvideo`, `episodevideo`, `item`; utility/link steps `customlistunescape`, `wikipediaformatline`, `movietechnical`, `characteralt`, `topicwikidatalink`, `collectionwikidatalink`, `technicalwikidatalink` |
| `tmdb-person-preprocess` | `strtmdbpersonpreprocess` |
| `wikipedia-crawler` | `strwikipediacrawler` + per-entity: `movie`, `person`, `item`, `serie`, `season`, `episode`, `other`, `keyword`, `topic`, `death`, **`group`**, `tmdbcollection`, `collection`, `technical`, `character`, `award`, `nomination`, `movement`, `list` |
| `wikidata-crawler` | `strwikidatacrawler` |
| `sparql-crawler` | content scopes: `strsparqlcrawler` + `movie`, `person`, `item`, `serie`, `season`, `episode` · T2S scopes (118–127): `strsparqlcrawlert2s` + `collection`, `list`, `movement`, `death`, **`group`**, `topic`, `award`, `nomination`, `technical`, `character` |
| alternate SPARQL crawler | `strsparqlaltcrawler` + `movie`, `person`, `serie`, `season`, `episode`, `character`, `item` |
| `embedding-update` | `strembeddingupdate` + per-entity: `movement`, `death`, `collection`, `award`, `nomination`, `location`, **`group`**, `network`, `company`, `person`, `serie`, `movie`, `topic`, `character` |
| `sqlite-plex-to-tmdb` | `strsqliteplex` |
| `plex-duplicates` | `strplexduplicates` |
| `movieparadise` | `strcopymovieparadisetomysql` |
| `fastapi-text2sql` (eval) | `strtext2sqleval` |
| `selenium-tmdb` | `strseleniumtmdb` |

> **Derivation-stage telemetry.** `tmdb-movie-preprocess` is the *derivation heart* of every
> person-related T2S entity, and **every process now publishes a per-entity run window** — emitted
> via the shared `EntityTelemetry` helper (`tmdb_preprocess_helpers.py`) and registered ahead of the
> generic `strtmdbmoviepreprocess` prefix in `lib/srvvar.inc.php` so each gets its own display
> section. Two tiers:
> - **Full derivation set** (`kind="derivation"`) — `startdatetime`, `enddatetime`, `id`,
>   `wikidataid`, `currentprocess`, `currentvalue`, `processedcount`, `createdcount`, `deletedcount`,
>   `processedseconds`: the dimension derivations `topic` (3), `collection` (41), `list` (42),
>   `group` (43), `award` (44), `movement` (45), `death` (46), `nomination` (47), `character` (48).
>   `group`/`death` emit the same fields via bespoke code rather than the helper.
> - **Run-window set** (`kind="copy"`) — `startdatetime`, `enddatetime`, `processedseconds`: the bulk
>   copy/rebuild steps (4–40) and the utility / Wikidata-linking steps `customlistunescape` (0),
>   `wikipediaformatline` (1), `movietechnical` (2), `characteralt` (49), `topicwikidatalink` (60),
>   `collectionwikidatalink` (61), `technicalwikidatalink` (62).
>
> **Cross-process duration ranking (optimization candidates).** On top of the per-entity windows above,
> the main loop measures a *uniform* wall-clock for **every** process iteration (independent of each
> step's bespoke telemetry) and publishes two program-wide variables under the bare
> `strtmdbmoviepreprocess` catch-all prefix (no new `srvvar.inc.php` prefix needed):
> - `strtmdbmoviepreprocessprocesselapsedseconds<index>` — elapsed seconds of process `<index>` in the
>   last run (e.g. `…processelapsedseconds6` for `T2S_PERSON`), for live per-process monitoring.
> - `strtmdbmoviepreprocessprocessdurationranking` — a single consolidated, **longest-first** string of
>   `"<index>:<label>=<seconds>s | …"` for the whole run, so the slowest processes (the optimization
>   candidates) are visible at a glance without collecting the ~45 per-entity `processedseconds`
>   variables. The same ranking is also printed to the run log as a "Process duration ranking" table.
>
> **Incremental watermark (state/input).** `strtmdbmoviepreprocesswikipediaformatlinelastrun` holds the
> start datetime of the last **successful** WIKIPEDIA_FORMAT_LINE run (process 1). The process reads it
> at startup and only re-parses movies whose `DAT_WIKIPEDIA_FORMAT_LINE >= watermark − buffer`, then
> rewrites it on success. Clear it (empty value / delete the row) to force a full re-parse. Also under
> the bare catch-all prefix.

---

## 5. Adding server variables for a new entity / process

1. **Emit** from the process with `f_setservervariable("str<repo><entity><field>", value, description, 0)`,
   reading inputs with `f_getservervariable(...)` and seeding a default when empty.
2. **Reuse** the shared `<field>` vocabulary in §2 — don't invent a new word for "processed count".
3. **Register** the new prefix in `lib/srvvar.inc.php`'s `$arrprefixes` so it gets a display section
   (place per-entity prefixes *before* the bare repo prefix, so the specific match wins ordering).
4. **Document** the entity's variables in that entity's multi-repo doc (telemetry section), and add the
   producing-repo row here if the repo is new.

---

*Companion to the per-entity multi-repo docs. The prefix catalogue reflects
[lib/srvvar.inc.php](../../lib/srvvar.inc.php) at the time of writing; that array is the live source of
truth — verify against it before relying on an exact prefix.*
