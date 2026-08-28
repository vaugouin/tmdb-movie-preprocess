import time
import html
import json
import os
import pymysql.cursors
import requests
import citizenphil as cp
import pandas as pd
import psutil
import re
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher

MYSQL_RETRYABLE_ERROR_CODES = {1205, 1213}
CLOSED_VOCABULARIES_CACHE = None


class EntityTelemetry:
    """Per-entity server-variable telemetry for a T2S preprocess step.

    Publishes the same family of variables the group (process 43) and death
    (process 46) derivations emit (see ``doc/server-variables.md``): a run
    window (``startdatetime`` / ``enddatetime``), running counts
    (``processedcount`` and, for derivations, ``createdcount`` /
    ``deletedcount``), the current position (``currentprocess`` /
    ``wikidataid`` / ``currentvalue`` / ``id``) and the elapsed
    ``processedseconds``. Variable names follow the
    ``str<repo><entity><field>`` convention with the repo prefix
    ``strtmdbmoviepreprocess``.

    ``kind='derivation'`` (the default) emits the full set including
    created/deleted counts; ``kind='copy'`` emits only the run window,
    processed count and elapsed seconds — appropriate for the bulk
    copy/rebuild steps that do not track per-record create/delete tallies.
    """

    _PREFIX = "strtmdbmoviepreprocess"

    def __init__(self, entity, intindex, label=None, kind="derivation"):
        self.entity = entity
        self.intindex = intindex
        self.label = label or entity
        self.kind = kind
        self.processedcount = 0
        self.createdcount = 0
        self.deletedcount = 0
        self._start = None
        self._track_processed = (kind == "derivation")

    def _set(self, field, value, desc):
        cp.f_setservervariable(f"{self._PREFIX}{self.entity}{field}", str(value), desc, 0)

    def _d(self, text):
        return f"{text} the T2S {self.label} {self.kind} (process {self.intindex})"

    def begin(self):
        """Stamp the start datetime, blank the end datetime and seed counts to 0."""
        self._start = time.time()
        self.processedcount = 0
        self.createdcount = 0
        self.deletedcount = 0
        strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        self._set("startdatetime", strnow, self._d("Start datetime of the last run of"))
        self._set("enddatetime", "", self._d("End datetime of the last run of"))
        if self.kind == "derivation":
            self._set("processedcount", 0, self._d("Number of records processed by"))
            self._set("createdcount", 0, self._d("Number of records created/updated by"))
            self._set("deletedcount", 0, self._d("Number of records deleted by"))

    def position(self, recordid=None, currentvalue=None, currentprocess=None, increment=True):
        """Publish the current record position; increments processedcount by default."""
        if increment:
            self.processedcount += 1
        if currentprocess is not None:
            self._set("currentprocess", currentprocess, self._d("Current source/sub-process in"))
        if recordid is not None:
            self._set("wikidataid", recordid, self._d("Current Wikidata/record id in"))
        if currentvalue is not None:
            self._set("currentvalue", currentvalue, self._d("Current record name in"))
        self._set("processedcount", self.processedcount, self._d("Number of records processed by"))

    def set_processed(self, n):
        """Set processedcount to an externally known total (e.g. a chunk row count)."""
        self.processedcount = n
        self._track_processed = True
        self._set("processedcount", n, self._d("Number of records processed by"))

    def set_entity_id(self, entityid):
        """Publish the id of the record currently created/updated."""
        self._set("id", entityid, self._d("Current record ID processed by"))

    def created(self, n=1):
        self.createdcount += n

    def deleted(self, n=1):
        self.deletedcount += n

    def finish(self):
        """Stamp the end datetime, finalize counts and the elapsed seconds."""
        strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        self._set("enddatetime", strnow, self._d("End datetime of the last run of"))
        if self._track_processed:
            self._set("processedcount", self.processedcount, self._d("Number of records processed by"))
        if self.kind == "derivation":
            self._set("createdcount", self.createdcount, self._d("Number of records created/updated by"))
            self._set("deletedcount", self.deletedcount, self._d("Number of records deleted by"))
        elapsed = (time.time() - self._start) if self._start else 0.0
        self._set("processedseconds", f"{elapsed:.2f}", self._d("Elapsed seconds of the last run of"))


def execute_sql_with_retry(connection, cursor, sql, label, max_attempts=5, retry_delay_seconds=2.0):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            cursor.execute(sql)
            connection.commit()
            return
        except pymysql.MySQLError as exc:
            last_error = exc
            error_code = exc.args[0] if exc.args else None
            connection.rollback()
            if error_code not in MYSQL_RETRYABLE_ERROR_CODES or attempt == max_attempts:
                raise
            wait_seconds = retry_delay_seconds * attempt
            print(
                f"Retryable MySQL error during {label} (attempt {attempt}/{max_attempts}, code={error_code}). "
                f"Retrying in {wait_seconds:.1f} seconds..."
            )
            time.sleep(wait_seconds)
    if last_error is not None:
        raise last_error


def f_tmdbpersonsetusedfortags(lngpersonid):
    if lngpersonid > 0:
        cursor2 = cp.connectioncp.cursor()
        strsqlupdate = "UPDATE T_WC_TMDB_PERSON SET USED_FOR_SIMILARITY = 1 WHERE ID_PERSON = " + str(lngpersonid)
        # print(strsqlupdate)
        cursor2.execute(strsqlupdate)
        # Commit the changes to the database
        cp.connectioncp.commit()


def f_wikidataitemproperties(strlang, stritemidwikidata, strpropertyid, strsep):
    strsql = ""
    strsql += "SELECT T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM, T_WC_WIKIDATA_ITEM_V1.LABEL, T_WC_WIKIDATA_ITEM_V1.ALIASES, T_WC_WIKIDATA_ITEM_V1.DESCRIPTION, T_WC_WIKIDATA_ITEM_V1.LANG "
    strsql += "FROM T_WC_WIKIDATA_ITEM_PROPERTY "
    strsql += "LEFT JOIN T_WC_WIKIDATA_ITEM_V1 ON T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM = T_WC_WIKIDATA_ITEM_V1.ID_WIKIDATA "
    strsql += "WHERE T_WC_WIKIDATA_ITEM_PROPERTY.ID_WIKIDATA = '" + stritemidwikidata + "' "
    # strsql += "AND T_WC_WIKIDATA_ITEM_V1.LANG = '" + strlang + "' "
    strsql += "AND T_WC_WIKIDATA_ITEM_PROPERTY.ID_PROPERTY = '" + strpropertyid + "' "
    strsql += "ORDER BY T_WC_WIKIDATA_ITEM_PROPERTY.DISPLAY_ORDER "
    # print(strsql)
    strwikidatatext = ""
    strwikidatatextlang = ""
    stritemlabelsadded = "|"
    cursor2 = cp.connectioncp.cursor()
    cursor2.execute(strsql)
    results2 = cursor2.fetchall()
    for row2 in results2:
        stritemid = row2['ID_ITEM']
        stritemlabel = row2['LABEL']
        stritemaliases = row2['ALIASES']
        stritemlang = row2['LANG']
        if stritemlabel:
            if stritemlabel != "" and stritemlabel != stritemid:
                if "|" + stritemlabel + "|" not in stritemlabelsadded:
                    if strwikidatatext != "":
                        strwikidatatext += strsep
                    strwikidatatext += stritemlabel
                    stritemlabelsadded += stritemlabel + "|"
                if stritemlang:
                    if stritemlang == strlang:
                        strwikidatatextlang = stritemlabel
    if strwikidatatextlang != "":
        return strwikidatatextlang
    else:
        return strwikidatatext


def check_memory():
    """Check and display system memory information"""
    memory_info = psutil.virtual_memory()
    print(f"Total Memory: {memory_info.total / (1024 ** 3):.2f} GB")
    print(f"Available Memory: {memory_info.available / (1024 ** 3):.2f} GB")
    print(f"Used Memory: {memory_info.used / (1024 ** 3):.2f} GB")
    print(f"Free Memory: {memory_info.free / (1024 ** 3):.2f} GB")
    print(f"Memory Usage: {memory_info.percent}%")
    return memory_info.available / (1024 ** 3)


def f_getcustomsortby(row, intdefaultsortby):
    intsortby = row['SORT_BY'] if 'SORT_BY' in row else None
    if intsortby in [1, 2, 3, 4, 5, 6]:
        return intsortby
    return intdefaultsortby


def f_buildcustomorderbyclause(intsortby, strscorefield, stridfield):
    if intsortby == 1:
        return f"ORDER BY CASE WHEN ORIGINAL_ORDER IS NULL THEN 1 ELSE 0 END, ORIGINAL_ORDER ASC, {stridfield} ASC "
    elif intsortby == 2:
        return f"ORDER BY CASE WHEN ORIGINAL_ORDER IS NULL THEN 1 ELSE 0 END, ORIGINAL_ORDER DESC, {stridfield} ASC "
    elif intsortby == 3:
        return f"ORDER BY CASE WHEN {strscorefield} IS NULL THEN 1 ELSE 0 END, {strscorefield} ASC, {stridfield} ASC "
    elif intsortby == 5:
        return f"ORDER BY CASE WHEN SORT_DATE IS NULL THEN 1 ELSE 0 END, SORT_DATE ASC, {stridfield} ASC "
    elif intsortby == 6:
        return f"ORDER BY CASE WHEN SORT_DATE IS NULL THEN 1 ELSE 0 END, SORT_DATE DESC, {stridfield} ASC "
    return f"ORDER BY CASE WHEN {strscorefield} IS NULL THEN 1 ELSE 0 END, {strscorefield} DESC, {stridfield} ASC "


def f_buildcustomaggregatequery(arrsqlsources, stridfield, strscorefield, intsortby):
    if not arrsqlsources:
        return ""
    strorderby = f_buildcustomorderbyclause(intsortby, strscorefield, stridfield)
    if len(arrsqlsources) > 1:
        strsql = f"SELECT {stridfield}, MIN(ORIGINAL_ORDER) AS ORIGINAL_ORDER, MAX({strscorefield}) AS {strscorefield}, MIN(SORT_DATE) AS SORT_DATE FROM ("
        strsql += "UNION ALL ".join(arrsqlsources)
        strsql += f") combined GROUP BY {stridfield} "
        strsql += strorderby
        return strsql
    return arrsqlsources[0] + strorderby


def f_getwikidatalabel(strwikidataid, strlang="fr", blnallentitytables=False):
    """Resolve an entity's label in one language.

    WIKIDATA-CRAWLER-017, 2026-08-17. V1 stores one ROW per language
    (T_WC_WIKIDATA_ITEM_V1.LABEL WHERE LANG='fr'); V2 stores one JSON DOCUMENT per
    entity, LABELS_JSON, holding every language of the dump. This function is the single
    place that knows the difference, so the callers stop carrying a hand-written SELECT
    each.

    V2 FIRST, V1 as a fallback, and the fallback is the whole point of doing it this way
    now. V2 does not carry every entity V1 knows: measured 2026-07-30, a large set of QIDs
    present in V1 is absent from every V2 table, and almost all of them hold a French
    label. Reading V2 alone would silently empty those labels. Falling back keeps every
    label on screen while the gap closes. DELETE THE FALLBACK WITH THE V1 TABLES.

    LANGUAGE FALLBACK INSIDE V2, added 2026-08-18, and it is not a convenience. V1 read
    Wikidata through SPARQL, whose label service silently falls back to English when the
    requested language is missing. Measured that day: of the 100 271 entities V2 knows but
    whose LABELS_JSON has no 'fr' key, 100 186 (99,92 %) carried the SAME text in V1's
    'fr' and 'en' rows, and Wikidata itself confirms it has no French label for them
    (Q8093 Nintendo, Q9684 The New York Times, Q2013 Wikidata).

    V1 was not lying: for a proper noun, English IS the right French display. So this
    fallback REPRODUCES a behaviour that was useful, instead of dropping it. The gain is
    not cosmetic: those 100 186 entities move from "served by the V1 fallback" to "served
    by V2 alone", which cuts the V1 dependency of French labels from 51 % to roughly 36 %,
    the remainder being the entities V2 simply does not import (-011).

    Order: requested language, then English, then the V1 row. LABELS_JSON before LABEL_EN
    in both cases, the scalar column being populated on part of the rows only.

    The language code is validated against a 2-3 letter pattern before it reaches the JSON
    path: it never comes from user input here, but a path built by concatenation deserves
    the guard anyway.
    """
    if not strwikidataid:
        return ""
    if not re.fullmatch(r"[a-z]{2,3}", strlang or ""):
        strlang = "fr"
    strjsonpath = "$." + strlang
    cursor2 = cp.connectioncp.cursor()
    # T_WC_WIKIDATA_ITEM SEULEMENT PAR DEFAUT (TMDB-MOVIE-PREPROCESS-036, 2026-08-18).
    #
    # Les appelants de cette fonction resolvent le libelle d une RECOMPENSE, d un GROUPE,
    # d une NOMINATION ou d un DECES : autant d entites qui vivent dans ITEM. Chercher
    # aussi dans MOVIE, SERIE et PERSON n avait aucun sens, et c etait dangereux.
    #
    # Mesure du 2026-08-18 : 7 805 des 44 084 lignes de T_WC_T2S_AWARD portent un
    # ID_WIKIDATA qui designe un FILM, sequelle du mecanisme decrit en -036 (la requete
    # SPARQL de V1 aplatit la valeur principale et ses qualificatifs sous la meme
    # propriete). L ancien code lisait ITEM_V1 seul, ne trouvait rien, et rendait du vide.
    # En elargissant a quatre tables, cette fonction aurait trouve le TITRE DU FILM et
    # l aurait ecrit dans AWARD_NAME_FR : pire qu un vide, car un blanc se remarque et une
    # valeur plausible et fausse ne se remarque pas. Le controle avant/apres l aurait meme
    # compte comme un GAIN de remplissage.
    #
    # blnallentitytables=True n a de sens que pour un diagnostic qui veut voir ou une
    # entite se trouve reellement, jamais pour alimenter une colonne.
    arrv2tables = ["T_WC_WIKIDATA_ITEM"]
    if blnallentitytables:
        arrv2tables = arrv2tables + [
            "T_WC_WIKIDATA_MOVIE",
            "T_WC_WIKIDATA_SERIE",
            "T_WC_WIKIDATA_PERSON",
        ]
    for strtable in arrv2tables:
        # Un seul aller-retour par table : la langue demandee, puis l anglais, puis la
        # colonne scalaire. Chercher la langue dans les quatre tables avant de retenter
        # en anglais doublerait les requetes pour le meme resultat.
        strsql = f"""
SELECT COALESCE(
           JSON_UNQUOTE(JSON_EXTRACT(LABELS_JSON, %s)),
           JSON_UNQUOTE(JSON_EXTRACT(LABELS_JSON, '$.en')),
           NULLIF(LABEL_EN, '')
       ) AS LABEL
FROM   {strtable}
WHERE  ID_WIKIDATA = %s
LIMIT 1
"""
        cursor2.execute(strsql, (strjsonpath, strwikidataid))
        row = cursor2.fetchone()
        if row and row.get('LABEL'):
            return row['LABEL']
    # Fallback on the V1 row, to be removed with the V1 tables.
    strsqlv1 = """
SELECT LABEL
FROM   T_WC_WIKIDATA_ITEM_V1
WHERE  ID_WIKIDATA = %s
  AND  LANG = %s
  AND  LABEL IS NOT NULL
  AND  LABEL <> ''
LIMIT 1
"""
    cursor2.execute(strsqlv1, (strwikidataid, strlang))
    row = cursor2.fetchone()
    if row and row.get('LABEL'):
        return row['LABEL']
    return ""


def f_getwikidataimagepath(strwikidataid, strlang="en"):
    """Resolve an entity's Wikipedia lead image.

    WIKIDATA-CRAWLER-015 / WIKIPEDIA-CRAWLER-020, 2026-08-17. The image used to be read
    ONLY from the entity's V1 row, which is exactly what kept the V1 tables alive. It is
    now read FIRST from T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL, the home wikipedia-crawler
    writes to, keyed on (ID_WIKIDATA, LANG).

    The V1 tables stay as a FALLBACK, deliberately, and only until they are dropped: the
    new source covers more than the old one (141 424 films in English against 125 866),
    but a handful of entities crawled before the column existed have no value yet. Falling
    back costs one query on a miss and guarantees this change removes no image from any
    screen. Delete the fallback list when V1 goes.

    strlang matters now, where V1 could not carry it: V1 had ONE image column per entity
    while the crawler runs once per language, so the second language overwrote the first
    (collection 4845 lost its English image to a French portal banner). Callers that want
    a localized image pass strlang; the default keeps every existing caller unchanged.
    """
    if not strwikidataid:
        return ""
    cursor2 = cp.connectioncp.cursor()
    strsqlpagelang = """
SELECT MAIN_IMAGE_URL AS WIKIPEDIA_IMAGE_PATH
FROM   T_WC_WIKIPEDIA_PAGE_LANG
WHERE  ID_WIKIDATA = %s
  AND  LANG = %s
  AND  MAIN_IMAGE_URL IS NOT NULL
  AND  MAIN_IMAGE_URL <> ''
LIMIT 1
"""
    cursor2.execute(strsqlpagelang, (strwikidataid, strlang))
    row = cursor2.fetchone()
    if row and row.get('WIKIPEDIA_IMAGE_PATH'):
        return row['WIKIPEDIA_IMAGE_PATH']
    # Fallback on the V1 rows, to be removed with them.
    arrtables = [
        ("T_WC_WIKIDATA_ITEM_V1", "WIKIPEDIA_IMAGE_PATH"),
        ("T_WC_WIKIDATA_MOVIE_V1", "WIKIPEDIA_POSTER_PATH"),
        ("T_WC_WIKIDATA_SERIE_V1", "WIKIPEDIA_POSTER_PATH"),
        ("T_WC_WIKIDATA_PERSON_V1", "WIKIPEDIA_PROFILE_PATH"),
    ]
    for strtable, strfieldname in arrtables:
        strsql = f"""
SELECT {strfieldname} AS WIKIPEDIA_IMAGE_PATH
FROM {strtable}
WHERE ID_WIKIDATA = %s
  AND {strfieldname} IS NOT NULL
  AND {strfieldname} <> ''
LIMIT 1
"""
        cursor2.execute(strsql, (strwikidataid,))
        row = cursor2.fetchone()
        if row and row.get('WIKIPEDIA_IMAGE_PATH'):
            return row['WIKIPEDIA_IMAGE_PATH']
    return ""


def f_normalizewikidatalinkingtext(strtext):
    if not strtext:
        return ""
    strtext = html.unescape(str(strtext))
    strtext = unicodedata.normalize("NFKD", strtext)
    strtext = "".join(char for char in strtext if not unicodedata.combining(char))
    strtext = strtext.lower()
    strtext = re.sub(r"[_\-]+", " ", strtext)
    strtext = re.sub(r"[^\w\s]", " ", strtext)
    strtext = re.sub(r"\s+", " ", strtext).strip()
    return strtext


def f_topiclinkingvariants(strtext):
    strnormalized = f_normalizewikidatalinkingtext(strtext)
    if not strnormalized:
        return set()
    arrvariants = {strnormalized}
    arrwords = strnormalized.split()
    if arrwords:
        strlastword = arrwords[-1]
        if strlastword.endswith("ies") and len(strlastword) > 3:
            arrvariants.add(" ".join(arrwords[:-1] + [strlastword[:-3] + "y"]))
        if strlastword.endswith("es") and len(strlastword) > 2:
            arrvariants.add(" ".join(arrwords[:-1] + [strlastword[:-2]]))
        if strlastword.endswith("s") and not strlastword.endswith("ss") and len(strlastword) > 1:
            arrvariants.add(" ".join(arrwords[:-1] + [strlastword[:-1]]))
        elif len(strlastword) > 1:
            arrvariants.add(" ".join(arrwords[:-1] + [strlastword + "s"]))
    return {strvariant for strvariant in arrvariants if strvariant}


def f_topiclinkingtitlescore(strinput, strtitle, strsnippet):
    strnormalizedinput = f_normalizewikidatalinkingtext(strinput)
    strnormalizedtitle = f_normalizewikidatalinkingtext(strtitle)
    strnormalizedsnippet = f_normalizewikidatalinkingtext(strsnippet)
    if not strnormalizedinput or not strnormalizedtitle:
        return 0.0
    dblscore = SequenceMatcher(None, strnormalizedinput, strnormalizedtitle).ratio()
    arrinputtokens = set(strnormalizedinput.split())
    arrtitletokens = set(strnormalizedtitle.split())
    if arrinputtokens and arrtitletokens:
        dbltokenoverlap = len(arrinputtokens & arrtitletokens) / max(len(arrinputtokens), 1)
        dblscore = max(dblscore, dbltokenoverlap)
    if strnormalizedinput in strnormalizedtitle or strnormalizedtitle in strnormalizedinput:
        dblscore = max(dblscore, 0.9)
    if strnormalizedinput in strnormalizedsnippet:
        dblscore = min(1.0, dblscore + 0.05)
    if re.search(r"\((film|movie|album|song|tv series|television series|novel|book|video game)\)", str(strtitle).lower()):
        dblscore = max(0.0, dblscore - 0.1)
    return dblscore


def f_wikimediarequest(session, strurl, arrparams):
    dblrequestdelayseconds = float(getattr(session, "wikimedia_request_delay_seconds", 0.25))
    dblbackoffseconds = float(getattr(session, "wikimedia_backoff_seconds", 1.0))
    intmaxretries = int(getattr(session, "wikimedia_max_retries", 4))
    dbltimeoutseconds = float(getattr(session, "wikimedia_timeout_seconds", 20.0))
    dblnow = time.monotonic()
    dbllastrequesttimestamp = getattr(session, "wikimedia_last_request_timestamp", None)
    if dbllastrequesttimestamp is not None and dblrequestdelayseconds > 0:
        dblwaitseconds = dblrequestdelayseconds - (dblnow - dbllastrequesttimestamp)
        if dblwaitseconds > 0:
            time.sleep(dblwaitseconds)
    for intattempt in range(intmaxretries + 1):
        response = session.get(strurl, params=arrparams, timeout=dbltimeoutseconds)
        session.wikimedia_last_request_timestamp = time.monotonic()
        if response.status_code != 429:
            response.raise_for_status()
            return response
        if intattempt >= intmaxretries:
            response.raise_for_status()
        strretryafter = response.headers.get("Retry-After")
        try:
            dblretryafterseconds = float(strretryafter) if strretryafter else 0.0
        except (TypeError, ValueError):
            dblretryafterseconds = 0.0
        dblsleepseconds = max(dblretryafterseconds, dblbackoffseconds * (2 ** intattempt))
        print(f"Wikimedia API rate limit reached (429). Retrying in {dblsleepseconds:.2f} seconds (attempt {intattempt + 1}/{intmaxretries + 1}).")
        time.sleep(dblsleepseconds)
    raise RuntimeError("Wikimedia request retry loop exited unexpectedly")


def f_wikipediasearchcandidates(session, strquery, intlimit=5):
    strurl = "https://en.wikipedia.org/w/api.php"
    arrparams = {
        "action": "query",
        "list": "search",
        "srsearch": strquery,
        "srlimit": intlimit,
        "srprop": "snippet",
        "format": "json",
        "utf8": 1,
    }
    response = f_wikimediarequest(session, strurl, arrparams)
    arrdata = response.json()
    return arrdata.get("query", {}).get("search", [])


def f_wikipediaresolvepage(session, strtitle):
    strurl = "https://en.wikipedia.org/w/api.php"
    arrparams = {
        "action": "query",
        "titles": strtitle,
        "redirects": 1,
        "prop": "pageprops",
        "ppprop": "wikibase_item",
        "format": "json",
        "utf8": 1,
    }
    response = f_wikimediarequest(session, strurl, arrparams)
    arrdata = response.json()
    arrpages = arrdata.get("query", {}).get("pages", {})
    for arrpage in arrpages.values():
        if "missing" in arrpage:
            continue
        arrpageprops = arrpage.get("pageprops", {})
        return {
            "title": arrpage.get("title", ""),
            "wikibase_item": arrpageprops.get("wikibase_item", ""),
            "is_disambiguation": "disambiguation" in arrpageprops,
        }
    return None


def f_wikidataentitysummary(session, strwikidataid, arrentitytypecache, arracceptedtypes=None):
    # arracceptedtypes: optional set/list of Wikidata P31 ids. When provided, the
    # entity is accepted ONLY if it is an instance of one of those types
    # (allowlist mode, used for typed entities like companies). When None, the
    # legacy blocklist applies (keywords/technicals): accept unless the entity is
    # a work/medium that should never be a topic (film, book, song, album, ...).
    if not strwikidataid:
        return {"accepted": False, "label": ""}
    # The cache stores the raw instance-of (P31) ids + label, independent of the
    # acceptance mode, so one cache can serve both blocklist and allowlist callers
    # within a run without cross-contaminating their verdicts.
    if strwikidataid in arrentitytypecache:
        arrcached = arrentitytypecache[strwikidataid]
        arrinstanceofids = arrcached["instanceof"]
        strlabel = arrcached["label"]
    else:
        strurl = "https://www.wikidata.org/w/api.php"
        arrparams = {
            "action": "wbgetentities",
            "ids": strwikidataid,
            "props": "labels|claims",
            "format": "json",
            "languages": "en",
        }
        response = f_wikimediarequest(session, strurl, arrparams)
        arrdata = response.json()
        arrentity = arrdata.get("entities", {}).get(strwikidataid, {})
        arrclaims = arrentity.get("claims", {})
        arrp31 = arrclaims.get("P31", [])
        arrinstanceofids = set()
        for arrclaim in arrp31:
            arrmainsnak = arrclaim.get("mainsnak", {})
            arrdatavalue = arrmainsnak.get("datavalue", {})
            arrvalue = arrdatavalue.get("value", {})
            strinstanceofid = arrvalue.get("id")
            if strinstanceofid:
                arrinstanceofids.add(strinstanceofid)
        strlabel = arrentity.get("labels", {}).get("en", {}).get("value", "")
        arrentitytypecache[strwikidataid] = {"instanceof": arrinstanceofids, "label": strlabel}
    if arracceptedtypes:
        boolaccepted = bool(arrinstanceofids & set(arracceptedtypes))
    else:
        arrblockedtypes = {
            "Q571",
            "Q7725634",
            "Q11424",
            "Q15416",
            "Q5398426",
            "Q482994",
            "Q7366",
            "Q2188189",
            "Q7889",
        }
        # Removed "Q5" (human) from blocked types
        arrblockedtypes.discard("Q5")
        boolaccepted = not bool(arrinstanceofids & arrblockedtypes)
    return {"accepted": boolaccepted, "label": strlabel}


def f_linktmdbkeywordtowikidataquery(session, strsearchquery, strscoreinput, arrentitytypecache, arracceptedtypes=None):
    if not strsearchquery:
        return None
    arrinputvariants = f_topiclinkingvariants(strscoreinput)
    if not arrinputvariants:
        return None
    arrcandidates = f_wikipediasearchcandidates(session, strsearchquery)
    if not arrcandidates:
        return None
    for arrcandidate in arrcandidates:
        strcandidatetitle = arrcandidate.get("title", "")
        if f_normalizewikidatalinkingtext(strcandidatetitle) in arrinputvariants:
            arrresolved = f_wikipediaresolvepage(session, strcandidatetitle)
            if arrresolved and not arrresolved.get("is_disambiguation") and arrresolved.get("wikibase_item"):
                arrentitysummary = f_wikidataentitysummary(session, arrresolved["wikibase_item"], arrentitytypecache, arracceptedtypes)
                if arrentitysummary.get("accepted"):
                    arrresolved["wikidata_label"] = arrentitysummary.get("label", "")
                    arrresolved["confidence"] = 1.0
                    arrresolved["match_type"] = "exact_title"
                    return arrresolved
    arrtopcandidate = arrcandidates[0]
    arrresolvedtop = f_wikipediaresolvepage(session, arrtopcandidate.get("title", ""))
    if arrresolvedtop and not arrresolvedtop.get("is_disambiguation") and arrresolvedtop.get("wikibase_item"):
        if f_normalizewikidatalinkingtext(arrresolvedtop.get("title", "")) in arrinputvariants:
            arrentitysummary = f_wikidataentitysummary(session, arrresolvedtop["wikibase_item"], arrentitytypecache, arracceptedtypes)
            if arrentitysummary.get("accepted"):
                arrresolvedtop["wikidata_label"] = arrentitysummary.get("label", "")
                arrresolvedtop["confidence"] = 0.95
                arrresolvedtop["match_type"] = "top_candidate"
                return arrresolvedtop
    arrbestmatch = None
    dblbestscore = 0.0
    for arrcandidate in arrcandidates[:3]:
        strcandidatetitle = arrcandidate.get("title", "")
        strcandidatesnippet = re.sub(r"<[^>]+>", " ", arrcandidate.get("snippet", ""))
        dblscore = f_topiclinkingtitlescore(strscoreinput, strcandidatetitle, strcandidatesnippet)
        if dblscore < 0.92:
            continue
        arrresolved = f_wikipediaresolvepage(session, strcandidatetitle)
        if not arrresolved or arrresolved.get("is_disambiguation") or not arrresolved.get("wikibase_item"):
            continue
        arrentitysummary = f_wikidataentitysummary(session, arrresolved["wikibase_item"], arrentitytypecache, arracceptedtypes)
        if not arrentitysummary.get("accepted"):
            continue
        if dblscore > dblbestscore:
            dblbestscore = dblscore
            arrbestmatch = arrresolved
            arrbestmatch["wikidata_label"] = arrentitysummary.get("label", "")
            arrbestmatch["confidence"] = dblscore
            arrbestmatch["match_type"] = "fuzzy"
    return arrbestmatch


def f_linktmdbkeywordtowikidata(session, strkeywordname, arrentitytypecache, arracceptedtypes=None, arrtrustedtypes=None):
    # arrtrustedtypes: optional set of "high-trust" P31 ids (a subset of
    # arracceptedtypes). When provided, a REDIRECT-RESOLUTION fallback is enabled
    # after the search-based attempts fail: the raw name is resolved directly as a
    # Wikipedia page title, which follows Wikipedia's redirect graph. That graph
    # authoritatively maps historical / renamed company names to the current entity
    # ("20th Century Fox" -> "20th Century Studios", "RKO Radio Pictures" -> "RKO
    # Pictures", "Walt Disney Productions" -> "The Walt Disney Company"). The
    # fallback is gated on TRUSTED membership so a bare-brand collision whose page
    # resolves directly to a generic-typed company ("Allianz" -> the insurer) is
    # NOT auto-accepted. When arrtrustedtypes is None (keywords / technicals) the
    # fallback is skipped and behaviour is unchanged.
    if not strkeywordname:
        return None
    arrqueryattempts = [strkeywordname]
    if "," in strkeywordname:
        strbeforecomma = strkeywordname.split(",", 1)[0].strip()
        if strbeforecomma and strbeforecomma not in arrqueryattempts:
            arrqueryattempts.append(strbeforecomma)
    arrparenthesismatches = re.findall(r"\(([^()]+)\)", strkeywordname)
    for strparenthesismatch in arrparenthesismatches:
        strinsideparentheses = strparenthesismatch.strip()
        if strinsideparentheses and strinsideparentheses not in arrqueryattempts:
            arrqueryattempts.append(strinsideparentheses)
    for strqueryattempt in arrqueryattempts:
        arrmatch = f_linktmdbkeywordtowikidataquery(session, strqueryattempt, strqueryattempt, arrentitytypecache, arracceptedtypes)
        if arrmatch:
            return arrmatch
    if arrtrustedtypes:
        arrtrustedtypeset = set(arrtrustedtypes)
        for strqueryattempt in arrqueryattempts:
            arrresolved = f_wikipediaresolvepage(session, strqueryattempt)
            if not arrresolved or arrresolved.get("is_disambiguation") or not arrresolved.get("wikibase_item"):
                continue
            strwikibaseitem = arrresolved["wikibase_item"]
            arrentitysummary = f_wikidataentitysummary(session, strwikibaseitem, arrentitytypecache, arracceptedtypes)
            arrresolvedtypes = arrentitytypecache.get(strwikibaseitem, {}).get("instanceof", set())
            if arrentitysummary.get("accepted") and (arrresolvedtypes & arrtrustedtypeset):
                arrresolved["wikidata_label"] = arrentitysummary.get("label", "")
                arrresolved["confidence"] = 0.95
                arrresolved["match_type"] = "redirect"
                return arrresolved
    return None


def extract_color_technology(text):
    # Extract color technology information
    text_lower = text.lower()
    color_technologies = {
        'technicolor': ['technicolor'],
        'eastmancolor': ['eastmancolor', 'eastman color'],
        'kodachrome': ['kodachrome'],
        'agfacolor': ['agfacolor'],
        'kinemacolor': ['kinemacolor'],
        'metrocolor': ['metrocolor'],
        'deluxe': ['deluxe color', 'de luxe color', 'deluxe'],
        'pathécolor': ['pathécolor', 'pathecolor'],
        'warnercolor': ['warnercolor'],
        'trucolor': ['trucolor'],
        'anscocolor': ['anscocolor'],
        'cinecolor': ['cinecolor', 'cinécolor'],
        'colorfilm': ['colorfilm'],
        'gasparcolor': ['gasparcolor'],
        'sovcolor': ['sovcolor'],
        'gevacolor': ['gevacolor', 'geva color', 'gévacolor'],
        'fujicolor': ['fujicolor', 'fuji color', 'fujifilm']
    }
    found_technologies = []
    for tech, variants in color_technologies.items():
        if any(variant in text_lower for variant in variants):
            found_technologies.append(tech)
    # return '|'.join(sorted(found_technologies)) if found_technologies else ""
    if found_technologies:
        strcolortechnologies = '|'.join(sorted(found_technologies))
        strcolortechnologies = "|" + strcolortechnologies + "|"
    else:
        strcolortechnologies = ""
    return strcolortechnologies


def extract_film_technology(text):
    # Extract film technology information like Super 35, Panavision, etc
    text_lower = text.lower()
    technologies = {
        'super_35': ['super 35', 'super35'],
        'super_16': ['super 16', 'super16'],
        'panavision': ['panavision'],
        'panaflex': ['panaflex'],
        'ultra_panavision': ['ultra panavision'],
        'arriflex': ['arriflex'],
        'vistavision': ['vistavision'],
        'techniscope': ['techniscope'],
        'franscope': ['franscope'],
        'cinemascope': ['cinemascope', 'cinémascope'],
        'cinerama': ['cinerama'],
        'todd_ao': ['todd-ao', 'todd ao'],
        'd_cinema': ['d-cinema', 'd cinema'],
        'dynascreen': ['dynascreen'],
        'polyvision': ['polyvision'],
        'magnascope': ['magnascope'],
        'technirama': ['technirama'],
        'technovision': ['technovision'],
        'tohoscope': ['tohoscope'],
        'panoramique': ['panoramique'],  # French widescreen
        'is_3d': ['3d', '3-d']
    }
    found_tech = []
    for tech, variants in technologies.items():
        if any(variant in text_lower for variant in variants):
            if tech != 'is_3d':  # Handle is_3d separately
                found_tech.append(tech)
    # return ', '.join(sorted(found_tech)) if found_tech else None
    if found_tech:
        strfilmtechnologies = '|'.join(sorted(found_tech))
        strfilmtechnologies = "|" + strfilmtechnologies + "|"
    else:
        strfilmtechnologies = ""
    return strfilmtechnologies


def extract_sound_technology(text):
    """Extract detailed sound technology information"""
    text_lower = text.lower()

    # Track patterns
    track_pattern = r'(\d+)[\s-]track'
    track_match = re.search(track_pattern, text_lower)
    num_tracks = track_match.group(1) if track_match else None

    # Sound systems
    sound_technologies = {
        'western_electric': [
            'western electric recording',
            'western electric mirrophonic',
            'western electric noiseless',
            'western electric sound',
            'westrex'
        ],
        'tobis_klangfilm': ['tobis-klangfilm', 'tobis klangfilm'],
        'vitaphone': ['vitaphone'],
        'movietone': ['movietone'],
        'perspecta': ['perspecta sound', 'perspecta'],
        'sensurround': ['sensurround'],
        'fantasound': ['fantasound'],
        'photophone': ['photophone', 'rca photophone'],
        'westrex': ['westrex recording', 'westrex sound']
    }

    found_technologies = []
    for technology, variants in sound_technologies.items():
        if any(variant in text_lower for variant in variants):
            found_technologies.append(technology)

    if found_technologies:
        strsoundtechnologies = '|'.join(sorted(found_technologies))
        strsoundtechnologies = "|" + strsoundtechnologies + "|"
    else:
        strsoundtechnologies = ""
    return strsoundtechnologies


def load_closed_vocabularies():
    global CLOSED_VOCABULARIES_CACHE
    if CLOSED_VOCABULARIES_CACHE is not None:
        return CLOSED_VOCABULARIES_CACHE
    strbasepath = os.path.dirname(os.path.abspath(__file__))
    strvocabpath = os.path.join(strbasepath, 'data', 'closed_vocabularies.json')
    with open(strvocabpath, 'r', encoding='utf-8') as handle:
        CLOSED_VOCABULARIES_CACHE = json.load(handle)
    return CLOSED_VOCABULARIES_CACHE


def get_closed_vocabulary_aliases(strentityname):
    arrclosedvocabularies = load_closed_vocabularies()
    arrentity = arrclosedvocabularies.get(strentityname, {})
    arraliases = arrentity.get('aliases', {})
    if not isinstance(arraliases, dict):
        return {}
    return arraliases


def normalize_aspect_ratio(value):
    if value is None:
        return None
    strvalue = str(value).strip().lower()
    if strvalue == '':
        return None
    arraspectratioaliases = get_closed_vocabulary_aliases('Aspect_ratio')
    if strvalue in arraspectratioaliases:
        return arraspectratioaliases[strvalue]
    # Already a dot-decimal canonical (post-2026-05-20 convention).
    if re.fullmatch(r'\d+\.\d+', strvalue):
        return strvalue
    return None


def normalize_field_value(strfieldname, value):
    if strfieldname == 'ASPECT_RATIO':
        return normalize_aspect_ratio(value)
    return value


def normalize_component_dict(arrcomponents):
    if not isinstance(arrcomponents, dict):
        return arrcomponents
    arrnormalizedcomponents = dict(arrcomponents)
    for strfieldname, value in arrcomponents.items():
        arrnormalizedcomponents[strfieldname] = normalize_field_value(strfieldname, value)
    return arrnormalizedcomponents


def normalize_extracted_components(arrcomponents):
    return normalize_component_dict(arrcomponents)


_ASPECT_RATIO_LOOKUP_CACHE = None


def _build_aspect_ratio_lookup():
    """Lowercase alias -> dot-decimal canonical. Includes self-mappings."""
    global _ASPECT_RATIO_LOOKUP_CACHE
    if _ASPECT_RATIO_LOOKUP_CACHE is not None:
        return _ASPECT_RATIO_LOOKUP_CACHE
    arraliases = get_closed_vocabulary_aliases('Aspect_ratio')
    arrlookup = {}
    for stralias, strcanonical in arraliases.items():
        arrlookup[str(stralias).lower()] = str(strcanonical)
        arrlookup[str(strcanonical).lower()] = str(strcanonical)
    _ASPECT_RATIO_LOOKUP_CACHE = arrlookup
    return arrlookup


def extract_aspect_ratios_from_text(text):
    """Return an ordered, de-duplicated list of dot-decimal canonical aspect
    ratios found in the (already cleaned) format line. Scans for every alias
    in data/closed_vocabularies.json's Aspect_ratio block, longest-first to
    avoid substring conflicts, anchoring with word boundaries where the alias
    starts/ends with an alphanumeric character."""
    if not isinstance(text, str) or text == '':
        return []
    strtextlower = text.lower()
    arrlookup = _build_aspect_ratio_lookup()
    arrhits = []
    arrconsumedranges = []

    def overlaps(start, end):
        for cstart, cend in arrconsumedranges:
            if not (end <= cstart or start >= cend):
                return True
        return False

    arraliasessorted = sorted(arrlookup.keys(), key=len, reverse=True)
    for stralias in arraliasessorted:
        if stralias == '':
            continue
        strpattern = re.escape(stralias)
        if re.match(r'\w', stralias[0]):
            strpattern = r'\b' + strpattern
        if re.match(r'\w', stralias[-1]):
            strpattern = strpattern + r'\b'
        try:
            for objmatch in re.finditer(strpattern, strtextlower):
                intstart, intend = objmatch.span()
                if overlaps(intstart, intend):
                    continue
                arrhits.append((intstart, arrlookup[stralias]))
                arrconsumedranges.append((intstart, intend))
        except re.error:
            continue

    arrhits.sort(key=lambda x: x[0])
    arrfound = []
    arrseen = set()
    for _, strcanonical in arrhits:
        if strcanonical not in arrseen:
            arrseen.add(strcanonical)
            arrfound.append(strcanonical)
    return arrfound


def extract_format_components(text):
    """Extract format components from a format line."""
    components = {
        'SOUND_SYSTEM': None,
        'ASPECT_RATIO': None,
        'ASPECT_RATIO_LIST': [],
        'FILM_FORMAT': None,
        'IS_COLOR': False,
        'IS_BLACK_AND_WHITE': False,
        'IS_SILENT': False,
        'IS_3D': False,
        'COLOR_TECHNOLOGY': None,
        'FILM_TECHNOLOGY': None,
        'SOUND_TECHNOLOGY': None,
        'CAMERA_PROCESS': None,
        'NUM_AUDIO_TRACKS': None,
        'HAS_AUDIO': False
    }

    if not isinstance(text, str):
        return components

    text = text.lower()

    # Extract aspect ratios (multi-ratio aware, dot-decimal canonicals).
    arraspectratios = extract_aspect_ratios_from_text(text)
    components['ASPECT_RATIO_LIST'] = arraspectratios
    components['ASPECT_RATIO'] = arraspectratios[0] if arraspectratios else None
    
    # Extract sound systems
    sound_systems = []
    
    # Check for stereo variations
    stereo_patterns = [r'\bst[eéèê]r[eéèê]o\b', r'\bstereo\b']
    for pattern in stereo_patterns:
        if re.search(pattern, text):
            sound_systems.append('stereo')
            break
    
    # Check for other sound systems
    if 'dolby' in text:
        sound_systems.append('dolby')
    if 'dts' in text:
        sound_systems.append('dts')
    if 'sdds' in text:
        sound_systems.append('sdds')
    if 'imax' in text and ('track' in text or 'sound' in text):
        sound_systems.append('imax')
    if 'mono' in text or 'monophonique' in text:
        sound_systems.append('mono')
    if 'auro' in text:
        sound_systems.append('auro')
    if '5.1' in text:
        sound_systems.append('5.1')
    if '7.1' in text:
        sound_systems.append('7.1')
    
    strsoundsystems = '|'.join(sorted(sound_systems))
    strsoundsystems = "|" + strsoundsystems + "|"
    components['SOUND_SYSTEM'] = strsoundsystems
    
    # Extract sound technology
    components['SOUND_TECHNOLOGY'] = extract_sound_technology(text)
    
    # Extract film format
    if '35 mm' in text:
        components['FILM_FORMAT'] = '35 mm'
    elif '16 mm' in text:
        components['FILM_FORMAT'] = '16 mm'
    elif '70 mm' in text:
        components['FILM_FORMAT'] = '70 mm'
    elif '65 mm' in text:
        components['FILM_FORMAT'] = '65 mm'
    elif 'digital' in text:
        components['FILM_FORMAT'] = 'digital'
    elif 'dcp' in text:
        components['FILM_FORMAT'] = 'dcp'
    
    # Check for color/b&w
    # Also treat "colorisé/colorized" as *not* color when the line explicitly says B&W.
    is_bw = any(x in text for x in ['noir et blanc', 'black and white', 'b&w', 'b/w'])
    # word-boundary matching to avoid false positives like "colorisé" containing "color" is too restrictive
    #is_color = bool(re.search(r'\b(couleur|couleurs|color|colors|colour|colours)\b', text))
    is_color = any(x in text for x in ['couleur', 'color', 'colour'])
    is_colorized = bool(re.search(r'\b(coloris[ée]e?|colorized)\b', text))
    if is_bw and is_colorized:
        is_color = False
    components['IS_BLACK_AND_WHITE'] = is_bw
    components['IS_COLOR'] = is_color
    components['IS_SILENT'] = any(x in text for x in ['muet', 'silent', 'sans son', 'sans paroles', 'non sonore'])
    
    # Check for IMAX
    components['IS_3D'] = 'imax' in text
    
    # Extract color process
    components['COLOR_TECHNOLOGY'] = extract_color_technology(text)
    
    # Extract camera/projection process
    components['FILM_TECHNOLOGY'] = extract_film_technology(text)
    
    # Extract number of audio tracks if present
    track_match = re.search(r'(\d+)[\s-]*(track|piste)', text)
    if track_match:
        components['NUM_AUDIO_TRACKS'] = int(track_match.group(1))
    else:
        components['NUM_AUDIO_TRACKS'] = 0
    
    # Set has_audio based on presence of sound systems
    components['HAS_AUDIO'] = len(components['SOUND_SYSTEM']) > 0
    
    return components

def validate_format_line(text):
    """Validate if the format line contains required components"""
    # Convert dots to commas in aspect ratios for validation
    text = re.sub(r'(\d+)\.(\d+):1', r'\1,\2:1', text)
    
    required_components = [
        'couleur|noir et blanc',  # Color information
        r'\d+[,\.]\d+:\d+|\d+:\d+',  # Aspect ratio (supporting both dot and comma)
        r'\d+ mm|digital|dcp'    # Film format or digital
    ]
    
    # Additional format indicators that can validate the line
    additional_formats = [
        'panavision', 'cinemascope', 'cinémascope', 'cinerama', 'todd-ao',
        'vistavision', 'techniscope', 'super 35', 'super 16'
    ]
    
    # Check if any of the additional formats are present
    has_additional_format = any(format in text.lower() for format in additional_formats)
    
    # Either all required components must be present OR an additional format must be present
    basic_validation = all(re.search(pattern, text) for pattern in required_components)
    return basic_validation or has_additional_format

def clean_format_line(text):
    """Clean format line according to specific rules"""
    # Convert dots to commas in aspect ratios
    text = re.sub(r'(\d+)\.(\d+):1', r'\1,\2:1', text)
    
    # Remove spaces in aspect ratios (e.g., "2,39 : 1" -> "2,39:1")
    text = re.sub(r'(\d+,\d+)\s*:\s*(\d+)', r'\1:\2', text)
    
    # Remove brackets and their content
    text = re.sub(r'\[.*?\]', '', text)
    
    # Remove parentheses but keep their content
    text = re.sub(r'\((.*?)\)', r'\1', text)
    
    # Remove any kind of dash
    text = re.sub(r'[-–—]', ' ', text)
    
    # Handle slashes
    # First, replace all non-aspect-ratio slashes with spaces
    text = re.sub(r'(?<!\d)/(?!\d)', ' ', text)  # Replace slashes not between numbers with spaces
    
    # Remove Colorworks because it is not a format and it incorrectly flags the current movie as a color movie
    text = text.replace("colorworks", "")

    # Clean up extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

# Helper function to convert NaN to None and handle lists
def process_value(val, is_integer=False):
    # Handle None and NaN values
    if pd.isna(val) or val is None:
        return 0 if is_integer else ""
    
    # Convert to string if not already
    val_str = str(val)
    
    # For integer columns, handle immediately
    if is_integer:
        # Handle empty strings and empty lists
        if not val_str or val_str == '':
            return 0
        try:
            # Try to convert to integer
            return int(float(val_str)) if '.' in val_str else int(val_str)
        except (ValueError, TypeError):
            return 0
    
    # Handle empty strings and empty lists for non-integer columns
    if not val_str or val_str == '[]':
        return ""
    
    # Handle string representation of lists
    if val_str.startswith('[') and val_str.endswith(']'):
        # Remove brackets and split by comma
        val_str = val_str[1:-1]
        if not val_str:
            return ""
        # Clean up each element
        elements = [x.strip() for x in val_str.split(',') if x.strip()]
        if not elements:
            return ""
        return ', '.join(elements)
    
    return val_str

def batch_update_data(connection, df, batch_size=1000):
    """Update data in batches to improve performance"""
    cursor = connection.cursor()
        
    # Convert boolean values to integers (0 or 1)
    bool_columns = ['IS_COLOR', 'IS_BLACK_AND_WHITE', 'IS_SILENT', 'IS_3D', 'IS_VALID_FORMAT']
        
    # Create a copy of the dataframe to avoid modifying the original
    processed_df = df.copy()
    
    print("\nSample of processed data:")
    print(processed_df.head())
    
    print("\nProcess boolean columns")
    # Process boolean columns
    for col in bool_columns:
        processed_df[col] = processed_df[col].apply(lambda x: 1 if x else 0)
    
    print("\nProcess list columns")
    # Process list columns
    print("\nFILM_TECHNOLOGY")
    print(f"Type of FILM_TECHNOLOGY before processing: {type(processed_df['FILM_TECHNOLOGY'].iloc[0])}")
    # Convert list to string representation
    processed_df['FILM_TECHNOLOGY'] = processed_df['FILM_TECHNOLOGY'].astype(str)
    print(f"Type of FILM_TECHNOLOGY after conversion: {type(processed_df['FILM_TECHNOLOGY'].iloc[0])}")
    processed_df['FILM_TECHNOLOGY'] = processed_df['FILM_TECHNOLOGY'].apply(process_value)
    print(f"Type of FILM_TECHNOLOGY after processing: {type(processed_df['FILM_TECHNOLOGY'].iloc[0])}")
    
    print("\nSOUND_SYSTEM")
    print(f"Type of SOUND_SYSTEM before processing: {type(processed_df['SOUND_SYSTEM'].iloc[0])}")
    # Convert list to string representation
    processed_df['SOUND_SYSTEM'] = processed_df['SOUND_SYSTEM'].astype(str)
    print(f"Type of SOUND_SYSTEM after conversion: {type(processed_df['SOUND_SYSTEM'].iloc[0])}")
    processed_df['SOUND_SYSTEM'] = processed_df['SOUND_SYSTEM'].apply(process_value)
    print(f"Type of SOUND_SYSTEM after processing: {type(processed_df['SOUND_SYSTEM'].iloc[0])}")
    
    # Convert list to string representation
    processed_df['SOUND_TECHNOLOGY'] = processed_df['SOUND_TECHNOLOGY'].astype(str)
    print(f"Type of SOUND_TECHNOLOGY after conversion: {type(processed_df['SOUND_TECHNOLOGY'].iloc[0])}")
    processed_df['SOUND_TECHNOLOGY'] = processed_df['SOUND_TECHNOLOGY'].apply(process_value)
    print(f"Type of SOUND_TECHNOLOGY after processing: {type(processed_df['SOUND_TECHNOLOGY'].iloc[0])}")
    
    # Debug output for NUM_AUDIO_TRACKS
    print("\nNUM_AUDIO_TRACKS values before processing:")
    print(processed_df['NUM_AUDIO_TRACKS'].head())
    print(f"Type of NUM_AUDIO_TRACKS before processing: {type(processed_df['NUM_AUDIO_TRACKS'].iloc[0])}")
    
    # Total number of rows
    total_rows = len(processed_df)
    print("\nTotal number of rows to update: ",total_rows)
    rows_updated = 0
    rows_failed = 0
    
    # Process in batches
    for i in range(0, total_rows, batch_size):
        batch_df = processed_df.iloc[i:i+batch_size]
        
        # Process each row in the batch
        for _, row in batch_df.iterrows():
            # Prepare data for update, ensuring all values are properly processed
            update_data = (
                process_value(row['WIKIPEDIA_FORMAT_LINE']),
                process_value(row['IS_COLOR'], is_integer=True),
                process_value(row['IS_BLACK_AND_WHITE'], is_integer=True),
                process_value(row['IS_SILENT'], is_integer=True),
                process_value(row['IS_3D'], is_integer=True),
                process_value(row['COLOR_TECHNOLOGY']),
                process_value(row['FILM_TECHNOLOGY']),
                process_value(row['ASPECT_RATIO']),
                process_value(row['FILM_FORMAT']),
                process_value(row['SOUND_SYSTEM']),
                process_value(row['SOUND_TECHNOLOGY']),
                process_value(row['NUM_AUDIO_TRACKS'], is_integer=True),
                process_value(row['IS_VALID_FORMAT'], is_integer=True),
                row['ID_MOVIE']  # WHERE clause
            )
            
            # Display the produced UPDATE SQL query with parameter values
            #print("\nExecuting SQL query with parameters:")
            print(update_data)
            arrmoviecouples = {}
            #arrmoviecouples["WIKIPEDIA_FORMAT_LINE"] = row['WIKIPEDIA_FORMAT_LINE']
            arrmoviecouples["IS_COLOR"] = row['IS_COLOR']
            arrmoviecouples["IS_BLACK_AND_WHITE"] = row['IS_BLACK_AND_WHITE']
            arrmoviecouples["IS_SILENT"] = row['IS_SILENT']
            arrmoviecouples["IS_3D"] = row['IS_3D']
            arrmoviecouples["COLOR_TECHNOLOGY"] = row['COLOR_TECHNOLOGY']
            arrmoviecouples["FILM_TECHNOLOGY"] = row['FILM_TECHNOLOGY']
            arrmoviecouples["ASPECT_RATIO"] = row['ASPECT_RATIO']
            arrmoviecouples["FILM_FORMAT"] = row['FILM_FORMAT']
            arrmoviecouples["SOUND_SYSTEM"] = row['SOUND_SYSTEM']
            arrmoviecouples["SOUND_TECHNOLOGY"] = row['SOUND_TECHNOLOGY']
            
            if row['NUM_AUDIO_TRACKS'] > 0:
                arrmoviecouples["NUM_AUDIO_TRACKS"] = row['NUM_AUDIO_TRACKS']
            else:
                arrmoviecouples["NUM_AUDIO_TRACKS"] = 0
            
            arrmoviecouples["IS_VALID_FORMAT"] = row['IS_VALID_FORMAT']
            #print("\nArrmoviecouples:")
            #print(arrmoviecouples)
            #time.sleep(5)
            strsqltablename = "T_WC_TMDB_MOVIE"
            strsqlupdatecondition = f"ID_MOVIE = {row['ID_MOVIE']}"
            cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
            rows_updated += 1
            
        cp.f_setservervariable("strtmdbmoviepreprocesswikipedialineformatparsedcount",str(rows_updated),"Count of WIKIPEDIA_FORMAT_LINE row parsed",0)
        cp.f_setservervariable("strtmdbmoviepreprocesswikipedialineformatfailedcount",str(rows_failed),"Count of WIKIPEDIA_FORMAT_LINE row failed",0)
        # Commit batch
        connection.commit()
        
        # Update progress
        progress = ((i + len(batch_df)) / total_rows) * 100
        print(f"Progress: {progress:.2f}% - Updated {rows_updated} rows, Failed {rows_failed}", end='\r')

    print(f"\nData update completed: {rows_updated} rows updated successfully, {rows_failed} rows failed")


def load_technical_ids(cursor):
    """Resolve the medium_format + aspect_ratio rows in T_WC_T2S_TECHNICAL by
    (DESCRIPTION, TECHNICAL_TYPE) so IDs are never hardcoded. Returns
    (classification_id, aspect_ratio_id) as two dicts. Fails loudly if any of
    the 4 classification keys are missing — that signals the §12.2 migration
    is incomplete."""
    strsql = (
        "SELECT DESCRIPTION, ID_TECHNICAL, TECHNICAL_TYPE "
        "FROM T_WC_T2S_TECHNICAL "
        "WHERE TECHNICAL_TYPE IN ('medium_format', 'aspect_ratio') "
        "AND (DELETED = 0 OR DELETED IS NULL)"
    )
    cursor.execute(strsql)
    arrclassificationid = {}
    arraspectratioid = {}
    for row in cursor.fetchall():
        strdescription = row['DESCRIPTION']
        lngid = row['ID_TECHNICAL']
        strtype = row['TECHNICAL_TYPE']
        if not strdescription:
            continue
        if strtype == 'medium_format':
            arrclassificationid[strdescription] = lngid
        elif strtype == 'aspect_ratio':
            arraspectratioid[strdescription] = lngid

    arrrequired = ['color_movie', 'black_and_white_movie', 'silent_movie', '3d_movie']
    arrmissing = [strkey for strkey in arrrequired if strkey not in arrclassificationid]
    if arrmissing:
        raise RuntimeError(
            "T_WC_T2S_TECHNICAL is missing required medium_format rows: "
            + ", ".join(arrmissing)
            + ". Apply EXTEND_T2S_TECHNICAL.md §12.2 before running this pre-process step."
        )

    print(
        "Loaded technical IDs: medium_format=" + str(len(arrclassificationid))
        + " (color_movie=" + str(arrclassificationid['color_movie'])
        + ", black_and_white_movie=" + str(arrclassificationid['black_and_white_movie'])
        + ", silent_movie=" + str(arrclassificationid['silent_movie'])
        + ", 3d_movie=" + str(arrclassificationid['3d_movie']) + ")"
        + ", aspect_ratio=" + str(len(arraspectratioid))
    )
    arrniche = [s for s in ['1.77', '1.89', '1.90'] if s not in arraspectratioid]
    if arrniche:
        print(
            "Note: aspect_ratio canonicals not seeded (will be logged and skipped if seen in source data): "
            + ", ".join(arrniche)
        )
    return arrclassificationid, arraspectratioid


def _movie_technical_target_rows(row, arrclassificationid, arraspectratioid, arrunmapped):
    """Return list of (id_technical, display_order) tuples to write for one
    movie, given its parsed flags + ASPECT_RATIO_LIST. Aspect-ratio canonicals
    that have no matching DB row (e.g. 1.77/1.89/1.90 per §12.5.4) are appended
    to arrunmapped for caller-side logging and skipped."""
    arrrows = []

    arrclassifications = [
        ('IS_COLOR', 'color_movie', 1),
        ('IS_BLACK_AND_WHITE', 'black_and_white_movie', 2),
        ('IS_SILENT', 'silent_movie', 3),
        ('IS_3D', '3d_movie', 4),
    ]
    for strflag, strkey, intdisplayorder in arrclassifications:
        intvalue = row.get(strflag) if hasattr(row, 'get') else row[strflag]
        try:
            intvalue = int(intvalue) if intvalue is not None and not pd.isna(intvalue) else 0
        except (TypeError, ValueError):
            intvalue = 0
        if intvalue == 1:
            arrrows.append((arrclassificationid[strkey], intdisplayorder))

    arraspectlist = row.get('ASPECT_RATIO_LIST') if hasattr(row, 'get') else row['ASPECT_RATIO_LIST']
    if isinstance(arraspectlist, (list, tuple)):
        intaspectorder = 0
        arrseen = set()
        for strcanonical in arraspectlist:
            if not strcanonical or strcanonical in arrseen:
                continue
            arrseen.add(strcanonical)
            if strcanonical in arraspectratioid:
                intaspectorder += 1
                arrrows.append((arraspectratioid[strcanonical], intaspectorder))
            else:
                arrunmapped.append((row['ID_MOVIE'], strcanonical))

    return arrrows


def write_movie_technical_junction(connection, df, arrclassificationid, arraspectratioid):
    """Populate T_WC_T2S_MOVIE_TECHNICAL with medium_format and aspect_ratio
    rows for every movie in `df`. Uses scoped delete-then-insert (§12.5.5) so
    sibling-type rows (color_technology / film_technology / sound_system /
    sound_technology / film_format) owned by op 2 are not touched. Returns a
    summary dict for logging (§12.5.7)."""
    cursor = connection.cursor()
    arrsummary = {
        'color': 0,
        'bw': 0,
        'silent': 0,
        '3d': 0,
        'aspect_total': 0,
        'aspect_movies': 0,
        'multi_ratio_movies': 0,
        'unmapped': [],
        'movies_processed': 0,
    }
    strclassdelete = (
        "DELETE FROM T_WC_T2S_MOVIE_TECHNICAL "
        "WHERE ID_MOVIE = %s "
        "AND ID_TECHNICAL IN ("
        "  SELECT ID_TECHNICAL FROM T_WC_T2S_TECHNICAL "
        "  WHERE TECHNICAL_TYPE = 'medium_format'"
        ")"
    )
    strratiodelete = (
        "DELETE FROM T_WC_T2S_MOVIE_TECHNICAL "
        "WHERE ID_MOVIE = %s "
        "AND ID_TECHNICAL IN ("
        "  SELECT ID_TECHNICAL FROM T_WC_T2S_TECHNICAL "
        "  WHERE TECHNICAL_TYPE = 'aspect_ratio'"
        ")"
    )

    lngrowstotal = len(df)
    lngcommitevery = 200
    for inti, (_, row) in enumerate(df.iterrows()):
        lngmovieid = int(row['ID_MOVIE'])
        arrtargets = _movie_technical_target_rows(
            row, arrclassificationid, arraspectratioid, arrsummary['unmapped']
        )

        cursor.execute(strclassdelete, (lngmovieid,))
        cursor.execute(strratiodelete, (lngmovieid,))

        intaspectcountmovie = 0
        for lngidtechnical, intdisplayorder in arrtargets:
            arrcouples = {
                'ID_MOVIE': lngmovieid,
                'ID_TECHNICAL': lngidtechnical,
                'DISPLAY_ORDER': intdisplayorder,
            }
            strcondition = "ID_MOVIE = " + str(lngmovieid) + " AND ID_TECHNICAL = " + str(lngidtechnical)
            cp.f_sqlupdatearray("T_WC_T2S_MOVIE_TECHNICAL", arrcouples, strcondition, 1)
            if lngidtechnical == arrclassificationid['color_movie']:
                arrsummary['color'] += 1
            elif lngidtechnical == arrclassificationid['black_and_white_movie']:
                arrsummary['bw'] += 1
            elif lngidtechnical == arrclassificationid['silent_movie']:
                arrsummary['silent'] += 1
            elif lngidtechnical == arrclassificationid['3d_movie']:
                arrsummary['3d'] += 1
            else:
                intaspectcountmovie += 1
        if intaspectcountmovie > 0:
            arrsummary['aspect_total'] += intaspectcountmovie
            arrsummary['aspect_movies'] += 1
            if intaspectcountmovie >= 2:
                arrsummary['multi_ratio_movies'] += 1

        arrsummary['movies_processed'] += 1
        if (inti + 1) % lngcommitevery == 0:
            connection.commit()
            print(
                "  junction progress: " + str(inti + 1) + "/" + str(lngrowstotal)
                + " movies (multi-ratio so far: " + str(arrsummary['multi_ratio_movies']) + ")",
                end='\r'
            )
    connection.commit()
    arrsummary['unmapped_count'] = len(arrsummary['unmapped'])
    return arrsummary


def refresh_technical_movie_count(connection):
    """Recompute MOVIE_COUNT for every medium_format + aspect_ratio row in
    T_WC_T2S_TECHNICAL. Drives the /technicals/{id} siblings ordering."""
    cursor = connection.cursor()
    strsql = (
        "UPDATE T_WC_T2S_TECHNICAL t "
        "SET MOVIE_COUNT = ("
        "  SELECT COUNT(*) FROM T_WC_T2S_MOVIE_TECHNICAL mt "
        "  WHERE mt.ID_TECHNICAL = t.ID_TECHNICAL"
        ") "
        "WHERE TECHNICAL_TYPE IN ('medium_format', 'aspect_ratio')"
    )
    cursor.execute(strsql)
    connection.commit()


# ---------------------------------------------------------------------------
# TMDB-MOVIE-PREPROCESS-036 : ce qui est une recompense, et ce qui n'en est pas
# ---------------------------------------------------------------------------

STR_AWARD_CONE_TABLE = "T_WC_T2S_AWARD_CLASS"
LNG_AWARD_CONE_FLOOR = 10000


def f_buildawardconetable():
    """Rebuild the award class cone: the P279 transitive closure under Q618779.

    Returns the number of classes loaded.

    Why a materialised table rather than a CTE in the driving query: the closure is
    walked once per run instead of once per row, and the resulting table is tiny
    (14 260 classes measured 2026-08-19 against 5 227 784 subclass edges).

    The CAST in the anchor is not decorative. Without it MariaDB types the recursive
    column on the literal's length and rejects its own Q-ids with ERROR 1406.
    """
    connection = cp.connectioncp
    cursor = connection.cursor()
    try:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS `" + STR_AWARD_CONE_TABLE + "` ("
            "`ID_CLASS` VARCHAR(50) NOT NULL,"
            "`DAT_CREAT` DATETIME DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY (`ID_CLASS`)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
        cursor.execute("TRUNCATE TABLE `" + STR_AWARD_CONE_TABLE + "`")
        cursor.execute(
            "INSERT INTO `" + STR_AWARD_CONE_TABLE + "` (ID_CLASS) "
            "WITH RECURSIVE cone_award (qid) AS ( "
            "SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid "
            "FROM (SELECT 'Q618779' AS qid) AS r "
            "UNION "
            "SELECT sc.ID_CHILD FROM T_WC_WIKIDATA_SUBCLASS sc "
            "JOIN cone_award c ON c.qid = sc.ID_PARENT WHERE sc.DELETED = 0 "
            ") SELECT qid FROM cone_award"
        )
        connection.commit()
        cursor.execute("SELECT COUNT(*) AS COMPTE FROM `" + STR_AWARD_CONE_TABLE + "`")
        return int(cursor.fetchone()["COMPTE"])
    finally:
        cursor.close()


def f_awardconeguard(intprocess, lngfloor=LNG_AWARD_CONE_FLOOR):
    """Rebuild the cone and say whether the caller may filter with it.

    Returns (blnproceed, lngclasses).

    SKIPPING RATHER THAN RAISING, and it is deliberate. The process loop in
    tmdb-movie-preprocess.py carries no try around its body, so an exception here
    would cost the fifty processes that follow: movies, series, people, images,
    videos, seasons, episodes, assertion refresh. The preprocessing runs daily and
    nothing downstream reads T_WC_T2S_AWARD or T_WC_T2S_NOMINATION inside the same
    run, so a skipped day leaves both tables one day stale and costs nothing else.

    THE FLOOR TARGETS THE PARTIAL LOAD, NOT THE EMPTY TABLE. An empty
    T_WC_WIKIDATA_SUBCLASS is the loud failure and any floor catches it. The
    dangerous case is the crawler halfway through its reload: the cone then holds a
    plausible-looking few thousand classes, passes a low floor, and the build
    deletes every award whose class happened to sit in the missing half of the
    graph. A partial failure clears the sanity checks a total one would trip.

    SKIPPING QUIETLY IS NOT SKIPPING SILENTLY. The class count, the reason and a
    consecutive-skip streak all land in server variables. A guard that fires often
    and says nothing stops being read as a signal, and the tables would freeze for
    weeks without anyone noticing.
    """
    lngclasses = f_buildawardconetable()
    cp.f_setservervariable(
        "strtmdbmoviepreprocessawardconeclasses", str(lngclasses),
        "Classes in the P279 award cone at the last award/nomination build (-036)", 0)

    strstreak = cp.f_getservervariable("strtmdbmoviepreprocessawardconeskipstreak", 0)
    try:
        lngstreak = int(str(strstreak).strip() or "0")
    except ValueError:
        lngstreak = 0

    if lngclasses < lngfloor:
        lngstreak += 1
        strreason = (f"process {intprocess} skipped: award cone holds {lngclasses} classes, "
                     f"floor is {lngfloor}, T_WC_WIKIDATA_SUBCLASS looks incomplete "
                     f"(streak {lngstreak})")
        print(strreason)
        cp.f_setservervariable("strtmdbmoviepreprocessawardconeskipstreak", str(lngstreak),
                               "Consecutive runs where the award cone was too small to filter (-036)", 0)
        cp.f_setservervariable("strtmdbmoviepreprocessawardconeskipreason", strreason,
                               "Why the last award/nomination build was skipped (-036)", 0)
        return False, lngclasses

    # Ecrire 0 SYSTEMATIQUEMENT, et non seulement pour remettre une serie a zero.
    # Corrige le 2026-08-21 : la premiere execution reelle n'a rien ecrit du tout,
    # puisque la serie valait deja 0 et que la remise a zero etait conditionnelle. La
    # variable etait donc absente, et l'absence ne se distingue pas de « n'a jamais
    # tourne avec ce code ». Un garde-fou dont l'etat sain est invisible ne renseigne
    # que sur ses echecs, ce qui est precisement l'inverse du but.
    cp.f_setservervariable("strtmdbmoviepreprocessawardconeskipstreak", "0",
                           "Consecutive runs where the award cone was too small to filter (-036)", 0)
    cp.f_setservervariable("strtmdbmoviepreprocessawardconeskipreason", "",
                           "Why the last award/nomination build was skipped (-036)", 0)
    return True, lngclasses


# Les deux constantes STR_AWARD_CONE_FILTER_DRIVING et _PURGE vivaient ici. Elles
# lisaient T_WC_WIKIDATA_ITEM_PROPERTY, que les processus 44 et 47 n'interrogent
# plus depuis TMDB-MOVIE-PREPROCESS-039, et disaient la meme regle deux fois.
# Retirees le 2026-08-28 plutot que laissees en decoration : une constante morte
# qui contredit la fonction vivante est un piege pose pour le prochain lecteur.
# La regle unique est f_awardconefilter(), plus bas.


# ---- TMDB-MOVIE-PREPROCESS-043 : lire UNE valeur de statement V2 ---------------------
#
# V1 rangeait un fait par colonne, une valeur par entite. V2 range les faits en
# statements, et rien n'interdit qu'une entite en porte plusieurs pour la meme
# propriete. Les colonnes T2S visees sont scalaires : il faut donc CHOISIR, et le
# choix doit etre le meme d'une execution a l'autre, sinon la colonne change de
# valeur sans que rien n'ait bouge dans Wikidata.
#
# La regle de choix, corrigee le 2026-08-26 par la recette elle-meme. La premiere
# version triait sur IS_BEST_VALUE puis DISPLAY_ORDER : la section B a montre que
# ces deux colonnes sont VIDES pour les quatre proprietes, et l'ETL confirme
# pourquoi, il les ecrit en dur a None (wikidata_dump_etl.py:880-881). Deux criteres
# sur trois etaient donc morts, et le choix tombait en realite sur ID_STATEMENT seul.
#
# La colonne reellement remplie est RANK, et elle vaut mieux que les deux autres
# puisqu'elle porte le jugement de Wikidata elle-meme. D'ou la regle actuelle :
#   1. on ECARTE 'deprecated', un rang qui signifie « Wikidata tient cette valeur
#      pour fausse ». L'ancienne regle pouvait la choisir, et c'etait un defaut de
#      correction, pas un detail de tri.
#   2. 'preferred' passe devant le reste.
#   3. a egalite, ID_STATEMENT, qui suit l'ordre des claims dans le dump, c'est-a-dire
#      l'ordre d'affichage de Wikidata. C'est le remplacant naturel de DISPLAY_ORDER,
#      tant que celui-ci reste vide.
#
# Ce n'est PAS une reproduction de V1, et il faut le savoir avant de comparer. V1
# gardait la DERNIERE valeur renvoyee par SPARQL (sparql-crawler.py:1323 : une
# affectation dans une boucle, sans regle de tri), c'est-a-dire une valeur
# arbitraire. Il n'existe donc aucune valeur V1 a retrouver a l'identique : la
# recette porte sur la couverture, jamais sur l'egalite valeur par valeur.
#
# Pas de filtre DELETED, volontairement : f_awardconefilter() n'en pose
# pas non plus, et deux lectures V2 qui ne filtrent pas pareil finissent par
# diverger sans que personne ne s'en apercoive.
STR_WD_PROPERTY_INSTANCE_OF = "P31"
STR_WD_PROPERTY_PLEX = "P11460"
STR_WD_PROPERTY_CRITERION = "P9584"
STR_WD_PROPERTY_CRITERION_SPINE = "P12279"


def f_wikidatabestvaluesql(strpropertyid, strvaluetable, strvaluecolumn, strsubjectexpr,
                           blnnumeric=False, strprefix="sv", intmaxlength=0):
    """Sous-requete correlee rendant UNE valeur de statement V2, ou NULL.

    strsubjectexpr est l'expression SQL qui designe le sujet dans la requete
    englobante (par exemple "t2s.ID_WIKIDATA"). strprefix distingue les alias
    quand plusieurs appels cohabitent dans la meme requete : ils sont dans des
    sous-requetes disjointes, mais un alias unique reste plus lisible a l'EXPLAIN.
    """
    strvaluealias = strprefix + "v"
    strvalueexpr = f"{strvaluealias}.{strvaluecolumn}"
    strguard = ""
    if blnnumeric:
        # Un identifiant externe est du TEXTE. Le couler dans une colonne INT sans
        # garde rend 0 pour toute valeur non numerique, et un 0 se lit comme
        # « present » par un IS NOT NULL distrait : c'est exactement l'artefact qui
        # avait fait annoncer « 0 Criterion retrouve sur 19 924 » alors que le vrai
        # chiffre etait 1 673 sur 1 673. On ecarte la valeur plutot que de la
        # convertir en zero.
        # Le REGEXP accepte « 0 », et CAST en fait un zero. Or zero est PRECISEMENT
        # la valeur sentinelle que V1 utilisait pour dire « absent », celle que cette
        # migration remplace par NULL. La laisser passer recreerait l'ambiguite qu'on
        # vient de lever, et sur une seule ligne, ce qui est le pire des cas : trop
        # rare pour se voir, assez presente pour fausser un tri. Ajoute le 2026-08-27
        # apres avoir trouve un King Kong vs. Godzilla a numero de collection 0.
        strguard = f"AND {strvaluealias}.{strvaluecolumn} REGEXP '^[0-9]+$' "
        strguard += f"AND {strvaluealias}.{strvaluecolumn} <> '0' "
        strvalueexpr = f"CAST({strvalueexpr} AS UNSIGNED)"
    if intmaxlength:
        # La valeur externe V2 est en varchar(1200), les colonnes T2S visees sont bien
        # plus courtes. Une valeur trop longue ferait tronquer en silence, ou avorter le
        # processus si le serveur est en mode strict, et un processus de nuit qui
        # s'arrete sur une ligne aberrante coute la totalite des processus suivants. On
        # ecarte donc la valeur, comme on ecarte une valeur non numerique : la recette
        # (test-043-coverage.sql, section D3) compte ce qui est ecarte, pour que le choix
        # reste visible plutot que silencieux.
        strguard += f"AND CHAR_LENGTH({strvaluealias}.{strvaluecolumn}) <= {intmaxlength} "
    return (
        f"(SELECT {strvalueexpr} "
        f"FROM T_WC_WIKIDATA_STATEMENT {strprefix} "
        f"JOIN {strvaluetable} {strvaluealias} "
        f"ON {strvaluealias}.ID_STATEMENT = {strprefix}.ID_STATEMENT "
        f"WHERE {strprefix}.ID_WIKIDATA = {strsubjectexpr} "
        f"AND {strprefix}.ID_PROPERTY = '{strpropertyid}' "
        f"AND ({strprefix}.`RANK` IS NULL OR {strprefix}.`RANK` <> 'deprecated') "
        f"{strguard}"
        f"ORDER BY ({strprefix}.`RANK` = 'preferred') DESC, "
        f"{strprefix}.ID_STATEMENT ASC "
        f"LIMIT 1)"
    )


def f_wikidatainstanceofsql(strsubjectexpr, strprefix="sio"):
    """INSTANCE_OF (P31) : la classe principale de l'entite, ou NULL."""
    return f_wikidatabestvaluesql(STR_WD_PROPERTY_INSTANCE_OF,
                                  "T_WC_WIKIDATA_ITEM_VALUE", "ID_ITEM",
                                  strsubjectexpr, False, strprefix)


def f_wikidataexternalidsql(strpropertyid, strsubjectexpr, blnnumeric=False, strprefix="sx",
                            intmaxlength=0):
    """Un identifiant externe (Plex, Criterion, ...), ou NULL."""
    return f_wikidatabestvaluesql(strpropertyid,
                                  "T_WC_WIKIDATA_EXTERNAL_ID_VALUE", "VALUE_EXTERNAL_ID",
                                  strsubjectexpr, blnnumeric, strprefix, intmaxlength)


# ---- TMDB-MOVIE-PREPROCESS-039 : le cone, ecrit une seule fois -----------------------
#
# STR_AWARD_CONE_FILTER_DRIVING et STR_AWARD_CONE_FILTER_PURGE disaient la meme regle
# deux fois, et leur propre commentaire avertissait du danger : si l'une bouge sans
# l'autre, le processus recree chaque nuit ce qu'il vient d'effacer. Un avertissement
# n'est pas une protection. La regle s'ecrit desormais UNE fois, et les deux appelants
# ne fournissent que l'expression qui designe la valeur chez eux.
#
# ⚠ COLLISION D'ALIAS. Le filtre utilise 'st' et 'iv' a l'interieur de ses
# sous-requetes. La requete qui l'accueille ne doit donc PAS employer ces deux alias,
# sans quoi la sous-requete correlerait sur elle-meme au lieu de la requete englobante,
# en silence et sans erreur SQL. Les appelants V2 emploient 'sa'/'av' cote pilote et
# 'w'/'wv' cote purge.
def f_awardconefilter(strvalueexpr):
    """La condition qui distingue une recompense du reste, sur la VALEUR du statement.

    Deux termes : la classe de la valeur est dans le cone P279 sous Q618779, OU
    l'entite n'a aucune classe. Le second n'est pas un relachement, c'est la
    protection des 552 lignes sans P31 ou se trouvent de vraies recompenses absentes
    de V2 (Waldo Salt Screenwriting Award, prix Feneon, Gaudi Awards).
    """
    return (
        "AND ( "
        "EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st "
        "JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT "
        "JOIN " + STR_AWARD_CONE_TABLE + " ac ON ac.ID_CLASS = iv.ID_ITEM "
        f"WHERE st.ID_WIKIDATA = {strvalueexpr} AND st.ID_PROPERTY = 'P31') "
        "OR NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT st "
        f"WHERE st.ID_WIKIDATA = {strvalueexpr} AND st.ID_PROPERTY = 'P31') "
        ") "
    )


def f_awarddrivingsql():
    """L'ensemble pilote des prix et des nominations, lu dans les statements V2.

    V1 lisait T_WC_WIKIDATA_ITEM_PROPERTY, qui APLATIT la valeur principale et les
    valeurs de tous les qualificatifs sous le meme identifiant de propriete : la
    requete SPARQL laissait ?ps non contraint (sparql-crawler.py:316-318). Sous P166
    cohabitaient donc la recompense, la ceremonie qui l'a remise et l'oeuvre pour
    laquelle elle l'a ete, toutes trois rangees comme des prix. En V2 la valeur
    principale et les qualificatifs sont des colonnes distinctes : la confusion n'a
    plus lieu d'etre, elle ne se filtre plus, elle n'existe plus.

    Le pre-filtre sur les entites T2S liees est conserve tel quel, il est bon et sans
    rapport avec V1 : une recompense dont aucun laureat n'est suivi ne produit aucun
    lien et n'est que du bruit.
    """
    return (
        "SELECT DISTINCT av.ID_ITEM "
        "FROM T_WC_WIKIDATA_STATEMENT sa "
        "JOIN T_WC_WIKIDATA_ITEM_VALUE av ON av.ID_STATEMENT = sa.ID_STATEMENT "
        "WHERE sa.ID_PROPERTY = %s "
        "AND (sa.`RANK` IS NULL OR sa.`RANK` <> 'deprecated') "
        "AND ( "
        "EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE m WHERE m.ID_WIKIDATA = sa.ID_WIKIDATA AND m.ID_WIKIDATA <> '') "
        "OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE s WHERE s.ID_WIKIDATA = sa.ID_WIKIDATA AND s.ID_WIKIDATA <> '') "
        "OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = sa.ID_WIKIDATA AND pe.ID_WIKIDATA <> '') "
        ") "
        + f_awardconefilter("av.ID_ITEM")
        + "ORDER BY av.ID_ITEM ASC "
    )


def f_awardpurgesql(strtablename):
    """La purge, exact inverse du pilote ci-dessus.

    Les deux DOIVENT designer le meme ensemble. Elles partagent desormais le meme
    filtre de cone et la meme exclusion des rangs deprecies, ce qui rend l'ecart
    structurellement impossible plutot que seulement deconseille.
    """
    return (
        f"DELETE FROM {strtablename}\n"
        "WHERE NOT EXISTS (\n"
        "    SELECT 1\n"
        "    FROM T_WC_WIKIDATA_STATEMENT w\n"
        "    JOIN T_WC_WIKIDATA_ITEM_VALUE wv ON wv.ID_STATEMENT = w.ID_STATEMENT\n"
        f"    WHERE w.ID_PROPERTY = {strtablename}.AWARD_SOURCE\n"
        f"      AND wv.ID_ITEM = {strtablename}.ID_WIKIDATA\n"
        "      AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated')\n"
        "      AND (\n"
        "            EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE  m  WHERE m.ID_WIKIDATA  = w.ID_WIKIDATA AND m.ID_WIKIDATA  <> '')\n"
        "         OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE  s  WHERE s.ID_WIKIDATA  = w.ID_WIKIDATA AND s.ID_WIKIDATA  <> '')\n"
        "         OR EXISTS (SELECT 1 FROM T_WC_T2S_PERSON pe WHERE pe.ID_WIKIDATA = w.ID_WIKIDATA AND pe.ID_WIKIDATA <> '')\n"
        "      )\n"
        "      " + f_awardconefilter("wv.ID_ITEM") + "\n"
        ");"
    )


def f_awardlinksql(strt2stable, stralias, stridcolumn, strsortcolumn):
    """Qui a recu ce prix : les entites T2S liees a une recompense, lues en V2.

    TMDB-MOVIE-PREPROCESS-039. Migrer l'ensemble pilote sans migrer ces requetes
    laisserait le processus lire V2 pour decider ce QU'EST une recompense et V1 pour
    decider QUI l'a recue. Deux sources pour un meme fait, donc deux couvertures et
    des comptes qui ne se recoupent pas : c'est pire que l'un ou l'autre etat pur.

    L'ORDRE DES TABLES A CHANGE, ET C'EST DELIBERE. L'ancienne requete partait de
    T_WC_WIKIDATA_ITEM_PROPERTY, ou (ID_PROPERTY, ID_ITEM) menait droit au but. En V2
    ces deux colonnes vivent dans deux tables : la selectivite est du cote de la
    VALEUR (lv.ID_ITEM, indexe), la propriete seule ne filtrant presque rien sur des
    millions de statements. On part donc de la valeur, puis on remonte au statement
    par sa cle primaire.

    Le STRAIGHT_JOIN vers la table T2S est conserve : il existait pour empecher
    l'optimiseur de commencer par elle, correctif d'un plan errone constate en
    production, et cette raison n'a pas bouge. Relancer ANALYZE TABLE sur les tables
    V2 et verifier le temps des processus 44 et 47, pas seulement leur resultat.

    L'ordre des %s est inchange, (propriete, item), pour que les appelants n'aient
    pas a bouger : les marqueurs se lient dans l'ordre du texte, pas des jointures.
    """
    return (
        f"SELECT DISTINCT {stralias}.{stridcolumn}, {stralias}.{strsortcolumn} "
        "FROM T_WC_WIKIDATA_ITEM_VALUE lv "
        "JOIN T_WC_WIKIDATA_STATEMENT sl ON sl.ID_STATEMENT = lv.ID_STATEMENT "
        f"STRAIGHT_JOIN {strt2stable} {stralias} ON {stralias}.ID_WIKIDATA = sl.ID_WIKIDATA "
        "WHERE sl.ID_PROPERTY = %s AND lv.ID_ITEM = %s "
        "AND (sl.`RANK` IS NULL OR sl.`RANK` <> 'deprecated') "
        f"AND {stralias}.ID_WIKIDATA IS NOT NULL AND {stralias}.ID_WIKIDATA <> '' "
        f"ORDER BY {stralias}.{strsortcolumn} DESC, {stralias}.{stridcolumn} ASC "
    )
