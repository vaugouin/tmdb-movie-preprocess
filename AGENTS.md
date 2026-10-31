# AGENTS.md - Agent Guide for tmdb-movie-preprocess

Rules an agent needs to work on this codebase safely. @README.md is canonical for everything else: scopes, Docker usage, the per-process reference (inputs, outputs, filters, operations), common patterns and the source → target table map. Consult it rather than re-deriving from code, but verify against code before changing behaviour. Update it for any user-facing change; update this file only when agent workflow or safety context changes. `CLAUDE.md` and any other tool-specific file only point here.

Deeper specs: @doc/sql/*.sql (reference DDL, read-only unless asked); @EXTEND_T2S_TECHNICAL.md (completed `T_WC_T2S_TECHNICAL` impact analysis, historical).

---

## Related repositories (project ecosystem)

Core preprocessing stage of **Agent BBB** (owner `vaugouin`; sibling roster in `%USERPROFILE%/Nestor/projets/t2s-backlog/topics/related-repositories.txt`). Reads the source tables of the crawlers (`T_WC_TMDB_*`, `T_WC_WIKIDATA_*`, `T_WC_WIKIPEDIA_*`, `T_WC_IMDB_*`) and builds the `T_WC_T2S_*` read model consumed by `fastapi-text2sql` and `embedding-update`. Siblings at the same stage: `tmdb-person-preprocess`, `keywords-processing`.

## Where things live (file → role)

- `tmdb-movie-preprocess.py`: the whole pipeline (one large module), each process inline in the `arrprocessscope` loop.
- `tmdb_preprocess_helpers.py`: stateless helpers and the newer set-based builders (`f_buildlocationtables`, `f_buildgroupworktables`, `f_buildworkfinancetable`, award cone, Wikimedia linking, custom-list query builders). Their docstrings and comments carry the reasoning and measurements; read them before changing a builder.
- `citizenphil.py` (`cp`): house DB layer, loads `.env` itself at import.
- `doc/sql/`: **every `.sql` of this repo**: reference DDL, one-shot migrations (`migration-*`, `alter-*`, `fill-*`, applied by hand, none read by the pipeline), audit and test queries. Stack-wide convention (2026-08-20): never at the root, never in a `doc/queries/`. Its two exceptions (a `.sql` executed by code, a `.sql` belonging to a write procedure) are not triggered here.
- `archive/`: retired code, not in any scope.

## Code conventions

- **House Hungarian notation**: `str*`, `lng*` / `int*` (integers; booleans as `int*` written `0`/`1`), `dbl*`, `arr*` (dict/list).
- Public helpers `f_*`, private `_*`; a few newer utilities are plain snake_case.
- **DB access only through `cp`** (`cp.f_getconnection()`, `cp.f_sqlupdatearray(table, dict, where, addstdfields)`); no ad-hoc connections. Escape literals with `cp.f_stringtosql()`, since most bulk SQL is built as f-strings. `intaddstdfields = 1` fills `TIM_UPDATED`, `DELETED`, `DAT_CREAT`, `ID_CREATOR`, `ID_OWNER`, `ID_USER_UPDATED`; pass `0` for tables without them.
- Report progress through `cp.f_setservervariable(...)`, and keep doing so in any step you add.
- **Run clock is `dblrunstarttime` / `dblrunendtime`, never `start_time`**: several processes reassign a local `start_time`, and the total runtime once measured only the last of them (an 87-minute run reported 58 s). Give any new timer a local name.
- Lock-wait timeouts and deadlocks (1205, 1213: `MYSQL_RETRYABLE_ERROR_CODES`) are retried/skipped, not fatal; use `execute_sql_with_retry` for new heavy writes.
- Emoji log markers (✅/⚠️/❌) are intentional. All data is multilingual: keep everything UTF-8, never assume ASCII.

## Pipeline / process steps

**Process selection is the dict `arrprocessscopemain` (or another scope chosen by `TMDB_PREPROCESS_SCOPE`), and execution order is dict insertion order, not numeric order.** The run executes once and exits; scheduling is external. Ordering constraints: 74 after 43 and 9, 10, 29, 31; 75 after 72; 72 and 73 before 71; 71 (image copier) last. Process 60 is not in `main` (own scope and cron). Step 64 never writes a movie signed `COLOR_SOURCE = 'format_line'` or carrying a non-empty Format line: the line always wins.

**⚠ `strdesc` est la variable de boucle du pipeline, ne la masquez pas dans un bloc de processus.** Elle est relue **apres** le bloc (classement des durees, description de la variable serveur du temps ecoule), et une boucle `for ... strdesc in ...` interne survit a sa boucle : le rapport titre alors le processus avec une autre valeur, sans erreur visible. Prefixer les variables de boucle locales par l'entite (`strlocationdesc`).

**⚠ Une table reconstruite par BUILD puis RENAME perd son AUTO_INCREMENT, donc son identifiant.** `CREATE TABLE ... LIKE` ne reporte pas le compteur et un `INSERT ... SELECT` sans l'identifiant renumerote tout, en silence (vu le 2026-09-13 : `ID_LOCATION` changeait chaque nuit). Un `ID_<ENTITE>` s'attribue une fois et ne se reattribue jamais. Toute nouvelle table T2S reconstruite par ce patron reprend les trois insertions de `f_buildlocationtables` (etape 1c, commentee) : (i) lignes connues avec leur identifiant reporte, (ii) lignes sorties du perimetre gardees en `DELETED = 1`, (iii) nouvelles apres `AUTO_INCREMENT = MAX + 1`. Jamais une seule insertion melant identifiants explicites et NULL. Recette : `doc/sql/test-014-location-id-stability.sql`, avant et apres un passage.

**Les lieux (processus 72) : decisions a ne pas defaire.** Le raisonnement et les mesures sont dans les commentaires de `f_buildlocationtables` et la docstring de `_f_buildlocationancestor` (`tmdb_preprocess_helpers.py`).

- Libelles (-064) : **un `LOCATION_NAME_FR` egal a `LOCATION_NAME` veut dire « pas de libelle francais propre »**, sans drapeau ; ne pas reintroduire de NULL pour le dire. `OVERVIEW_FR` n'a pas de repli sur l'anglais.
- Hierarchie `T_WC_T2S_LOCATION_ANCESTOR` (-048) : le lieu lui-meme a la profondeur 0 ; meilleur rang par (item, propriete) ; le parcours traverse des items qui ne sont pas des lieux T2S et ne garde que les paires de lieux vivants ; **jamais `LOCATION_TYPE`** dans le calcul (classification fausse par endroits). Un journal qui atteint `INT_LOCATION_ANCESTOR_MAX_DEPTH` (`3d-2. ATTENTION`) signale un cycle. Consommateur : le prompt de fastapi-text2sql (`data/text_to_sql.md`, section Locations).
- Lieux transfrontaliers (-067), trois decisions de Philippe du 2026-10-21 : les lieux disputes vont sous chaque pays revendique ; la Normandie reste sous la France (`TUPLE_LOCATION_P17_DROP`, seule exception nommee) ; seuls les parents `P17` connus de la base comptent.

**Les processus 72, 73 et 75 sautent quand wikidata-crawler ecrit les tables Wikidata (-063).** `f_wikidataloadbusy()` lit les variables serveur du crawler ; un saut laisse la table de la veille, ecrit une ligne `<n>: SKIPPED, ...` et sa raison en variable serveur, et n'ecrase pas les comptes du dernier passage reussi. Tout nouveau processus qui lit les tables cibles Wikidata doit passer par cette garde.

## Database tables

Tables read and written: README § Source → Target Table Map. When changing schema, update the matching DDL in `doc/sql/`, and quote only table/column names verified in code or DDL.

### `T_WC_T2S_AWARD_CLASS`, and why processes 44 and 47 can skip themselves

`f_awardconeguard()` rebuilds the P279 cone under `Q618779` at the head of 44 and 47 and skips the process (rather than raising) when the cone is below `LNG_AWARD_CONE_FLOOR`; its docstring explains why. The award rule exists once, in `f_awardconefilter()`, shared by each process's driving query and purge `DELETE`: never re-inline it, or the purge and the build drift and the process recreates nightly what it deletes. Its subqueries use the aliases `st` and `iv`: a host query must not use them (silent self-correlation). Watch `strtmdbmoviepreprocessawardconeskipstreak`: three skips in a row means the crawler dependency is broken.

## Recettes SQL : deux pieges qui ne se voient pas sur le poste de l'auteur

**⚠ Ne jamais placer une requete sur `information_schema` AVANT des instructions qui nomment des tables sans les qualifier.** phpMyAdmin bascule alors la base courante et la suite echoue (« #1109 Table inconnue dans information_schema ») ; le client `mariadb` en ligne de commande ne montre pas le defaut. Mettre ces requetes **en dernier**, ou qualifier les noms.

**⚠ `information_schema.INNODB_TRX` et `PROCESSLIST` demandent le privilege global `PROCESS`, que le compte applicatif n'a pas.** Ne pas le lui donner (il expose le texte de toutes les requetes de tous les utilisateurs). Lancer ces sections depuis phpMyAdmin en `root`, isolees du reste du fichier pour que leur echec n'emporte pas les autres.

## SQL Object Naming Conventions

- Tables uppercase snake case with a domain prefix: `T_WC_*` (core), `T_WC_T2S_*` (this repo's targets), `T_WC_TMDB_*`, `T_WC_WIKIDATA_*`, `T_WC_WIKIPEDIA_*`, `T_WC_IMDB_*`. The prefix is configurable via `DB_NAMESPACE` (default `T_WC_`).
- Keys `ID_{ENTITY}`; foreign keys reuse the referenced key name.
- Columns: `DAT_*` dates, `TIM_*` datetimes, `IS_*` flags plus soft-delete `DELETED`, `DISPLAY_ORDER`, `*_COUNT`, `*_NORM` / `*_KEY` for normalised search values.

## Configuration & secrets

Env vars: `.env.example` and README § Secrets handling. Secrets are never committed nor baked into the image; the runtime env file lives outside the source tree. There is **no OpenAI / LLM dependency** here. The PyMySQL connection sets `local_infile=True`: keep it in mind if you touch connection setup.

**Clock rule (2026-10-19, DEPLOYMENT-015, TMDB-MOVIE-PREPROCESS-060).** Every naive `DATETIME`/`DATE` this repo writes is **naive Europe/Paris**: always `datetime.now(cp.paris_tz)`, never a bare `datetime.now()` (the container runs in UTC, the VPS host stays `Etc/UTC`), and never a fixed `+02:00`. A human-read date with an offset uses the zone too. Compare a Paris column against a cutoff computed in Python with `cp.paris_tz`, not against `NOW()`. The SQL-side `NOW()`/`CURDATE()` writes (`ASSERTION_REFRESH_LAST`, `DAT_CREAT`/`TIM_UPDATED` of the Wikidata-sourced T2S rows in `tmdb_preprocess_helpers.py`) follow the MariaDB clock, which is UTC until TMDB-FRONT-100 sets it to Europe/Paris: values written before that switch are UTC. `TIM_WIKIPEDIA_SEARCH` values written by versions before 1.1.18 are naive UTC as well.

---

**Last Updated**: 2026-10-31
**Current Version**: 1.1.35

## Backlog (Nestor second-brain)

Tasks `TMDB-MOVIE-PREPROCESS-NNN` (status, priority, quick wins) live in the Nestor repo; consult before implementing: `C:\Users\vaugo\Nestor\projets\t2s-backlog\repos\tmdb-movie-preprocess.md` (dashboard: `...\t2s-backlog\index.md`). These paths exist only on Philippe's PC, not on the VPS or cloud agents.
