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
            # TMDB-MOVIE-PREPROCESS-049 : provenance of the colour flags. The parse
            # signs 'format_line'; process 64 signs 'wikidata' on its own and never
            # touches a movie carrying this signature or a non-empty Format line.
            if 'COLOR_SOURCE' in processed_df.columns:
                arrmoviecouples["COLOR_SOURCE"] = row['COLOR_SOURCE']
                arrmoviecouples["TIM_COLOR_SOURCE"] = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
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


# ---- TMDB-MOVIE-PREPROCESS-044 : le 4e mecanisme des listes personnalisees ------------
#
# Les trois mecanismes historiques de T_WC_CUSTOM_LIST savent dire "ces films-la"
# (ID_IMDB_LIST), "les films dont la propriete P vaut l'item Q" (WIKIDATA_PROPERTIES) et
# "les films portant ce mot-cle TMDb" (TMDB_ELEMENTS). Aucun ne sait dire "les films qui
# PORTENT telle propriete", quelle que soit sa valeur. Or c'est ainsi que se definit un
# catalogue d'editeur : un film est chez Criterion parce qu'il a un identifiant Criterion
# (P9584), pas parce qu'une propriete pointe vers l'item Criterion.
#
# Le mecanisme 2 ne pouvait pas etre etendu pour le faire : il lit
# T_WC_WIKIDATA_ITEM_PROPERTY, la table V1 qui ne contient QUE des statements a valeur
# d'item. Un identifiant externe est une chaine ; il n'y a jamais eu de ligne a y trouver.
# Celui-ci lit donc V2, comme l'enrichissement du processus 4.
#
# LA PROPRIETE D'ORDRE est le second apport, et elle ne filtre rien. Sa valeur numerique
# est versee dans ORIGINAL_ORDER, la colonne que SORT_BY = 1 trie deja en releguant les
# NULL en fin de liste. Un catalogue numerote (Criterion et ses spine numbers, P12279) se
# range donc dans l'ordre de son editeur sans une seule ligne de code de tri nouvelle, et
# les titres sans numero tombent a la fin au lieu de disparaitre. DISPLAY_ORDER, qui n'est
# qu'un compteur suivant cet ORDER BY, en decoule.
#
# SYNTAXE dans WIKIDATA_PROPERTIES, volontairement EXPLICITE :
#     ID:P9584 ORDER:P12279 Q1204187
# Les marqueurs sont obligatoires. Declencher sur "un P sans Q" aurait ete plus court, et
# c'etait un piege : une ligne existante portant un P orphelin, aujourd'hui inerte, serait
# devenue du jour au lendemain une collection de plusieurs milliers de titres que personne
# n'a demandee. Les marqueurs laissent en outre le Q libre pour ce a quoi il sert ici,
# illustrer la collection (ID_WIKIDATA et image Wikipedia), sans reveiller le mecanisme 2.
#
# Pas de filtre DELETED sur le statement, par alignement delibere avec
# f_wikidatabestvaluesql et f_awardconefilter : deux lectures V2 qui ne filtrent pas
# pareil finissent par diverger sans que personne ne s'en apercoive.
#
# LECTURE DIRECTE DE T2S, la ou le mecanisme 2 passe par la table TMDb source. Ce n'est
# pas une divergence de style : T_WC_T2S_MOVIE porte deja ID_WIKIDATA, ADULT, la date et
# le score, et les autres mecanismes doivent de toute facon la joindre pour le score. Le
# detour par T_WC_TMDB_MOVIE serait une jointure pour rien, sur une population que la
# collection ne peut pas contenir de toute maniere.
ARR_CUSTOM_EXTERNAL_ID_ENTITY = {
    "movie": ("T_WC_T2S_MOVIE", "ID_MOVIE", "IMDB_RATING_WEIGHTED", "DAT_RELEASE"),
    "serie": ("T_WC_T2S_SERIE", "ID_SERIE", "IMDB_RATING_WEIGHTED", "DAT_FIRST_AIR"),
}


def f_parsecustomexternalidproperties(strwikidataproperties):
    """Lit les marqueurs ID: et ORDER:, rend (appartenance, ordre, reste de la chaine).

    Le troisieme element est la chaine PRIVEE des tokens consommes, et c'est lui que le
    mecanisme 2 doit parser ensuite. Sans cela il verrait P9584 comme sa propre propriete
    de filtre et fabriquerait un couple (P9584, Q1204187) qui ne designe rien : une
    collection vide, sans erreur SQL, donc sans rien pour alerter.
    """
    strremainder = strwikidataproperties or ""
    strmembershipproperty = ""
    strorderproperty = ""
    objmatch = re.search(r"\bID\s*:\s*(P\d+)", strremainder, re.IGNORECASE)
    if objmatch:
        strmembershipproperty = objmatch.group(1)
        strremainder = strremainder.replace(objmatch.group(0), " ")
    objmatch = re.search(r"\bORDER\s*:\s*(P\d+)", strremainder, re.IGNORECASE)
    if objmatch:
        strorderproperty = objmatch.group(1)
        strremainder = strremainder.replace(objmatch.group(0), " ")
    return strmembershipproperty, strorderproperty, strremainder


def f_customexternalidsourcesql(strmembershipproperty, strorderproperty, strentitykind):
    """Une source du mecanisme 4, ou la chaine vide si la ligne ne le demande pas.

    Meme contrat que les trois autres sources : quatre colonnes dans le meme ordre
    (identifiant, ORIGINAL_ORDER, score, SORT_DATE), aucun ORDER BY, une espace finale,
    puisque f_buildcustomaggregatequery les assemble en UNION ALL et pose le tri.
    """
    if strentitykind not in ARR_CUSTOM_EXTERNAL_ID_ENTITY:
        return ""
    if not re.fullmatch(r"P\d+", strmembershipproperty or ""):
        return ""
    (strt2stable, stridfield, strscorefield,
     strdatefield) = ARR_CUSTOM_EXTERNAL_ID_ENTITY[strentitykind]
    strorderexpr = "NULL"
    if re.fullmatch(r"P\d+", strorderproperty or ""):
        # Exactement la lecture du processus 4 : rang preferred d'abord, valeur numerique
        # seulement, et '0' ecarte parce que c'etait la sentinelle "absent" de V1.
        strorderexpr = f_wikidataexternalidsql(strorderproperty, "t.ID_WIKIDATA", True, "sco")
    return (
        f"SELECT DISTINCT t.{stridfield}, {strorderexpr} AS ORIGINAL_ORDER, "
        f"t.{strscorefield}, t.{strdatefield} AS SORT_DATE "
        "FROM T_WC_WIKIDATA_STATEMENT sme "
        f"STRAIGHT_JOIN {strt2stable} t ON t.ID_WIKIDATA = sme.ID_WIKIDATA "
        "INNER JOIN T_WC_WIKIDATA_EXTERNAL_ID_VALUE smv ON smv.ID_STATEMENT = sme.ID_STATEMENT "
        f"WHERE sme.ID_PROPERTY = '{strmembershipproperty}' "
        "AND (sme.`RANK` IS NULL OR sme.`RANK` <> 'deprecated') "
        "AND smv.VALUE_EXTERNAL_ID <> '' "
        "AND t.ADULT = 0 "
        "AND t.ID_WIKIDATA IS NOT NULL AND t.ID_WIKIDATA <> '' "
    )


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


def f_awardpurgesql(strtablename, strsourcecolumn):
    """La purge, exact inverse du pilote ci-dessus.

    ⚠ La colonne qui porte la propriete source N'A PAS le meme nom dans les deux
    tables : AWARD_SOURCE pour les prix, NOMINATION_SOURCE pour les nominations. Elle
    est donc passee en parametre et non deduite du nom de la table. Deduire aurait
    produit un SQL valide en apparence et faux au premier passage du processus 47.

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
        f"    WHERE w.ID_PROPERTY = {strtablename}.{strsourcecolumn}\n"
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


# ---- TMDB-MOVIE-PREPROCESS-040 et -041 : les processus 43 (groupes) et 46 (deces) ----
#
# ⚠ -040 PARTAIT D'UNE LECTURE FAUSSE, corrigee ici. Le ticket decrivait
# T_WC_WIKIDATA_PERSON_V1 comme un « pont TMDb vers Wikidata ». C'en est pas un :
# T_WC_TMDB_PERSON porte deja ID_WIKIDATA, le pont existe sans elle. La jointure est
# une PORTE : elle exige que Wikidata connaisse l'entite comme une personne. Le
# ticket demandait donc de mesurer la couverture de P4985 avant de basculer, ce qui
# n'a aucun objet. L'equivalent V2 est la table d'entite T_WC_WIKIDATA_PERSON, meme
# jointure, meme role. Ce qu'il faut mesurer, c'est sa couverture en personnes.
def f_persondrivingsql(strpropertyid, strextraclause=""):
    """L'ensemble pilote des groupes et des deces : les items qu'au moins DEUX
    personnes suivies partagent.

    Le seuil de deux n'est pas cosmetique. Sans lui, le processus itererait les
    centaines de milliers d'items de P463, P108 et P54 pour n'en supprimer que des
    singletons ; la purge en fin de processus applique le meme seuil, et les deux
    doivent rester d'accord.
    """
    return (
        "SELECT pv.ID_ITEM "
        "FROM T_WC_WIKIDATA_STATEMENT sp "
        "JOIN T_WC_WIKIDATA_ITEM_VALUE pv ON pv.ID_STATEMENT = sp.ID_STATEMENT "
        "INNER JOIN T_WC_TMDB_PERSON ON T_WC_TMDB_PERSON.ID_WIKIDATA = sp.ID_WIKIDATA "
        "INNER JOIN T_WC_WIKIDATA_PERSON ON T_WC_TMDB_PERSON.ID_WIKIDATA = T_WC_WIKIDATA_PERSON.ID_WIKIDATA "
        f"WHERE sp.ID_PROPERTY = '{strpropertyid}' "
        "AND (sp.`RANK` IS NULL OR sp.`RANK` <> 'deprecated') "
        + strextraclause
        + "GROUP BY pv.ID_ITEM "
        "HAVING COUNT(DISTINCT T_WC_TMDB_PERSON.ID_PERSON) >= 2 "
        "ORDER BY pv.ID_ITEM ASC "
    )


def f_personlinkfromsql():
    """Le FROM et le WHERE des liaisons personne, la liste des colonnes restant a
    l'appelant qui en choisit une dizaine.

    Meme inversion d'ordre que dans f_awardlinksql, et pour la meme raison : en V2 la
    selectivite est sur la VALEUR, indexee, la propriete seule ne filtrant presque
    rien. Les deux STRAIGHT_JOIN sont conserves, leur raison n'ayant pas bouge.
    """
    return (
        "FROM T_WC_WIKIDATA_ITEM_VALUE pv "
        "JOIN T_WC_WIKIDATA_STATEMENT sp ON sp.ID_STATEMENT = pv.ID_STATEMENT "
        "STRAIGHT_JOIN T_WC_TMDB_PERSON ON T_WC_TMDB_PERSON.ID_WIKIDATA = sp.ID_WIKIDATA "
        "STRAIGHT_JOIN T_WC_WIKIDATA_PERSON ON T_WC_TMDB_PERSON.ID_WIKIDATA = T_WC_WIKIDATA_PERSON.ID_WIKIDATA "
        "WHERE sp.ID_PROPERTY = %s "
        "AND pv.ID_ITEM = %s "
        "AND (sp.`RANK` IS NULL OR sp.`RANK` <> 'deprecated') "
        "ORDER BY T_WC_TMDB_PERSON.POPULARITY DESC "
    )


def f_persongrouppurgesql(strtablename, strsourcecolumn, blncustomguard=True, strextraclause=""):
    """La purge des groupes et des deces : exact inverse du pilote, meme seuil de deux.

    ⚠ La colonne source n'a pas le meme nom d'une table a l'autre, GROUP_SOURCE et
    DEATH_SOURCE. Elle est passee, jamais deduite : l'avoir deduite avait produit un
    SQL valide en apparence et faux au premier passage, sur la purge des nominations
    (-039, corrige le 2026-08-29).

    Les deux processus ne purgent pas tout a fait pareil : 43 protege les groupes
    'custom', 46 ne le fait pas et porte une exclusion P1196 qui lui est propre. D'ou
    les deux derniers parametres, plutot qu'une deuxieme fonction presque identique.
    """
    strhead = f"DELETE FROM {strtablename}\n"
    if blncustomguard:
        strhead += f"WHERE {strsourcecolumn} <> 'custom'\n  AND (\n"
    else:
        strhead += "WHERE (\n"
    return (
        strhead
        + "    SELECT COUNT(DISTINCT p.ID_PERSON)\n"
        "    FROM T_WC_WIKIDATA_ITEM_VALUE wv\n"
        "    JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT\n"
        "    INNER JOIN T_WC_TMDB_PERSON p ON p.ID_WIKIDATA = w.ID_WIKIDATA\n"
        "    INNER JOIN T_WC_WIKIDATA_PERSON wp ON p.ID_WIKIDATA = wp.ID_WIKIDATA\n"
        f"    WHERE w.ID_PROPERTY = {strtablename}.{strsourcecolumn}\n"
        f"      AND wv.ID_ITEM = {strtablename}.ID_WIKIDATA\n"
        "      AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated')\n"
        + strextraclause
        + "  ) < 2;"
    )


# ---------------------------------------------------------------------------
# TMDB-MOVIE-PREPROCESS-014 : le read-model des lieux
# ---------------------------------------------------------------------------

STR_LOCATION_CLASS_TABLE = "T_WC_T2S_LOCATION_CLASS"

# Les cones de classes qui donnent LOCATION_TYPE, DANS L'ORDRE DE PRIORITE.
#
# ⚠ LES RACINES SONT RELEVEES, PAS DEVINEES. Chaque QID ci-dessous vient de la section 1
# de doc/sql/test-014-location-classes.sql, lancee le 2026-09-11, qui a liste les classes
# que les lieux portent REELLEMENT avec leur libelle et leur frequence. Ecrire un QID de
# memoire est la faute du 2026-08-31, ou un cas temoin pointait un identifiant reconstruit
# de tete et rendait zero ligne en se lisant comme un succes.
#
# ⚠ L'ORDRE EST LE COEUR DU SUJET, parce que l'appartenance multiple est la norme et non
# l'exception : Paris porte QUINZE classes, Prague quinze, Singapour dix dont "big city",
# "country", "port city" et "border city" ensemble. Le premier cone qui correspond gagne.
#
# Les trois choix d'ordre qui se discutent, et pourquoi ils sont ainsi :
#   - fiction EN PREMIER : une ville fictive est de la fiction avant d'etre une ville.
#     Bikini Bottom, cite 780 fois, ne doit pas se ranger avec Los Angeles.
#   - country AVANT city : Singapour devient un pays. Une question sur les pays est plus
#     souvent posee qu'une question sur les villes-Etats, et le pays est la categorie la
#     plus grossiere, donc la plus decisive.
#   - city AVANT region : et celui-la protege les cas les plus cites. Berlin est un
#     "federated state of Germany", Hambourg aussi, Paris une "territorial collectivity
#     of France with special status". Ranger la region avant la ville les ferait tous
#     basculer en region, ce qui serait faux pour l'usage.
#
# Un type NULL n'est pas un echec : 0,5 % des lieux n'ont aucun P31 (85 sur 16 299), et
# ceux dont aucune classe ne tombe dans un cone restent sans type plutot que mal classes.
# Le compte des non classes est publie en variable serveur, pour qu'il se voie.
LOCATION_TYPE_CONES = (
    # ---------------------------------------------------------------------
    # DEUXIEME ITERATION, 2026-09-12, sur la mesure de
    # doc/sql/test-014-location-type-drivers.sql. La premiere avait bon l'ORDRE, les
    # huit temoins de priorite etant tous justes, et faux les RACINES.
    #
    # ⚠ LA CORRECTION PRINCIPALE EST UN DEPLACEMENT, PAS UN AJOUT, et je m'etais
    # trompe de diagnostic. J'avais accuse la TAILLE du cone region, 19 482 classes.
    # Le probleme etait son RANG. La priorite se resout a la construction par un
    # INSERT IGNORE sur la cle : la premiere insertion gagne. region etant inseree
    # avant structure, toute classe presente dans les deux cones etait estampillee
    # region, et les deux cones se recouvrent massivement. Mesure : 274 rues, 162
    # chateaux, 108 places, 64 chaines de montagnes, 63 parcs, 44 lacs, 43 rivieres
    # et 29 zoos etaient classes « region ». Aucun n'est une region.
    #
    # structure et nature passent donc AVANT region, qui redevient ce qu'elle doit
    # etre : le fourre-tout des vraies entites territoriales, en avant-dernier.
    # ---------------------------------------------------------------------
    ("fiction",   ("Q1964689",    # fictional city
                   "Q2775969",    # fictional planet
                   # Quatre racines relevees dans les lieux SANS type, gains mesures
                   # au regard de ce qu'elles prennent aux deja classes :
                   "Q1145276",    # fictional country      51 gagnes /  7 pris
                   "Q106921111",  # fictional town         41 gagnes /  0 pris
                   "Q14637321",   # fictional spacecraft   40 gagnes /  1 pris
                   "Q6619693")),  # fictional island       25 gagnes /  2 pris
    ("country",   ("Q6256",)),                            # country
    ("city",      ("Q486972",)),                          # human settlement
    ("island",    ("Q23442",)),                           # island
    ("structure", ("Q41176",      # building
                   "Q23413",      # castle
                   "Q33506",      # museum
                   "Q24354",      # theatre building
                   "Q55488",      # railway station
                   "Q79007",      # street
                   "Q174782",     # square
                   "Q375336",     # film studio            77 gagnes / 16 pris
                   "Q40357",      # prison                 43 gagnes / 21 pris
                   "Q22698",      # park
                   "Q22746",      # urban park
                   "Q43501")),    # zoo
    # Le relief, famille qui manquait entierement : une montagne n'est ni une
    # structure ni une region, et elle etait rangee dans la seconde.
    ("nature",    ("Q8502",       # mountain               56 gagnes / 12 pris
                   "Q46831",      # mountain range
                   "Q40080",      # beach                  29 gagnes / 14 pris
                   "Q23397",      # lake
                   "Q4022")),     # river
    ("region",    ("Q82794",)),                            # region
)

# ⚠ DEUX RACINES DELIBEREMENT ECARTEES, ET LA RAISON EST CHIFFREE.
#
# Q811979 « architectural structure » recupererait 233 lieux sans type, mais en
# prendrait 6 747 aux deja classes, soit 57 % du corpus, pour un cone de 25 047
# classes. Ma propre regle de recette dit que la seconde colonne doit rester petite :
# une racine qui reclasse massivement n'est pas un ajout, c'est un changement de
# conception. A remesurer APRES le deplacement ci-dessus, parce qu'une partie de ces
# 6 747 sont justement les rues et les chateaux que region volait, et qu'ils seront
# deja chez structure : le chiffre devrait donc fondre, et la racine redevenir un
# simple ajout.
#
# Q15284 et Q484170, les classes administratives peuplees, restent a mesurer. Elles
# sont la cause directe du seul defaut de conception encore ouvert : MADRID ne porte
# qu'UNE classe, Q2074737, et elle n'est dans aucun cone city. Madrid sort donc en
# region, ce qui est faux. Q2074737, Q747074 et Q856076 partagent un parent P279,
# Q15284, qui est probablement la racine a poser, mais son libelle est absent de la
# base et je ne le nommerai pas de memoire : c'est la faute du 2026-08-31. A mesurer
# comme les autres avant de l'ajouter.



# TMDB-MOVIE-PREPROCESS-063 : les etapes de wikidata-crawler qui ecrivent les tables cibles
# Wikidata que lit le processus 72 (110 chargement, 111 controle, 112 medias, 113 controle,
# 114 purge des anciens lots). 108 et 109 ne touchent que le staging, 115 aussi.
INT_WIKIDATA_LOAD_STEPS = range(110, 115)
# Au-dela, une variable RUNNING est un reste d'arret brutal (kill, reboot) et non un
# chargement : l'etape 110 la plus longue mesuree a dure 12 h (2026-10-15 16:57 au
# 2026-10-16 05:01). Sans ce plafond, un crawler tue laisserait 72 saute pour toujours.
INT_WIKIDATA_LOAD_STALE_HOURS = 48


def f_wikidataloadbusy(strprocess="72"):
    """Return why a Wikidata-reading rebuild must wait, or "" when it can run (-063).

    Used by process 72 (locations) and 73 (source works); strprocess only labels the log line.

    Process 72 reads T_WC_WIKIDATA_STATEMENT. While wikidata-crawler upserts it (step 110)
    and then deletes the rows of the previous batch (step 114), a rebuild reads a partial
    snapshot: a place missing from it goes DELETED = 1 for a day, embedding-update drops its
    document, and both come back the next night. The 2026-10-16 01:00 run fell in the
    middle of step 110. The crawler's own server variables say where it is; no lock to add
    on its side.
    """
    strstatus = (cp.f_getservervariable("strwikidatacrawlerstatus", 0) or "").strip()
    if strstatus != "RUNNING":
        return ""
    strcurrent = (cp.f_getservervariable("strwikidatacrawlercurrentprocess", 0) or "").strip()
    strcode = strcurrent.split(":", 1)[0].strip()
    if not strcode.isdigit() or int(strcode) not in INT_WIKIDATA_LOAD_STEPS:
        return ""
    strstarted = (cp.f_getservervariable("strwikidatacrawlerstep" + strcode + "startedat", 0) or "").strip()
    try:
        dtstarted = datetime.strptime(strstarted, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        dtstarted = None
    if dtstarted is not None:
        fltage = (datetime.now(cp.paris_tz).replace(tzinfo=None) - dtstarted).total_seconds() / 3600
        if fltage > INT_WIKIDATA_LOAD_STALE_HOURS:
            print(f"{strprocess}: wikidata-crawler says RUNNING step {strcurrent} since {strstarted}, "
                  f"{fltage:.0f} h ago: treated as a stale flag, the rebuild runs")
            return ""
    return f"wikidata-crawler is loading the Wikidata tables (step {strcurrent}, started {strstarted or 'at an unknown time'})"


def f_buildlocationclasstable():
    """Construit la table de classement des classes de lieux, un cone par type.

    Meme mecanisme que f_buildawardconetable() : la fermeture transitive P279 est
    materialisee une fois par passage plutot que parcourue une fois par ligne.

    Le CAST de l'ancre n'est pas decoratif. Sans lui MariaDB type la colonne recursive
    sur la longueur du litteral et rejette ses propres Q-ids avec l'erreur 1406.

    La priorite est resolue A LA CONSTRUCTION, pas a la lecture : une classe presente
    dans deux cones ne garde que le type du premier, si bien que la table rendue associe
    a chaque classe UN seul type. La requete de classement devient une jointure simple,
    ce qui evite d'apprendre l'ordre au modele de langage comme aux lecteurs futurs.

    Rend le nombre de classes chargees, par type.
    """
    connection = cp.connectioncp
    cursor = connection.cursor()
    arrcounts = {}
    try:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS `" + STR_LOCATION_CLASS_TABLE + "` ("
            "`ID_CLASS` VARCHAR(50) NOT NULL,"
            "`LOCATION_TYPE` VARCHAR(20) NOT NULL,"
            "`DAT_CREAT` DATETIME DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY (`ID_CLASS`),"
            "KEY `LOCATION_TYPE` (`LOCATION_TYPE`)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
        cursor.execute("TRUNCATE TABLE `" + STR_LOCATION_CLASS_TABLE + "`")
        for strtype, arrroots in LOCATION_TYPE_CONES:
            strroots = " UNION ALL ".join(
                "SELECT '" + strroot + "' AS qid" for strroot in arrroots
            )
            # INSERT IGNORE et non INSERT : la cle primaire sur ID_CLASS fait que la
            # premiere insertion gagne, ce qui EST la priorite. Les cones se recouvrent
            # largement et ce recouvrement n'est pas une anomalie a corriger.
            cursor.execute(
                "INSERT IGNORE INTO `" + STR_LOCATION_CLASS_TABLE + "` (ID_CLASS, LOCATION_TYPE) "
                "WITH RECURSIVE cone_location (qid) AS ( "
                "SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid "
                "FROM (" + strroots + ") AS r "
                "UNION "
                "SELECT sc.ID_CHILD FROM T_WC_WIKIDATA_SUBCLASS sc "
                "JOIN cone_location c ON c.qid = sc.ID_PARENT WHERE sc.DELETED = 0 "
                ") SELECT qid, %s FROM cone_location",
                (strtype,),
            )
            connection.commit()
            cursor.execute(
                "SELECT COUNT(*) AS COMPTE FROM `" + STR_LOCATION_CLASS_TABLE + "` "
                "WHERE LOCATION_TYPE = %s", (strtype,)
            )
            arrcounts[strtype] = int(cursor.fetchone()["COMPTE"])
        return arrcounts
    finally:
        cursor.close()


# L'ensemble pilote : tout item cite en valeur de P840 ou P915 par un film ou une serie
# QUE T2S CONNAIT. La definition n'est pas inventee ici, c'est celle du processus 209
# d'embedding-update, portee sur les statements V2 plutot que sur la table plate V1.
#
# La restriction aux oeuvres T2S retire 4 377 lieux sur 16 299, et la mesure du
# 2026-09-11 dit pourquoi : 2 665 sont cites par des films et des series que Wikidata
# connait et que TMDb ignore, 1 636 par des episodes, et seulement 102 par des romans,
# jeux ou bandes dessinees. Ce n'est donc PAS une affaire de nature d'oeuvre, c'est le
# defaut de rapprochement que selenium-tmdb ferme un CSV a la fois.
STR_LOCATION_DRIVING = (
    "  SELECT DISTINCT iv.ID_ITEM AS ID_WIKIDATA\n"
    "  FROM T_WC_WIKIDATA_STATEMENT st\n"
    "  INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT\n"
    "  WHERE st.ID_PROPERTY IN ('P840', 'P915')\n"
    "    AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')\n"
    "    AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE m WHERE m.ID_WIKIDATA = st.ID_WIKIDATA)\n"
    "       OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE s WHERE s.ID_WIKIDATA = st.ID_WIKIDATA) )\n"
)

# Le meme perimetre, episodes compris (TMDB-MOVIE-PREPROCESS-062, 2026-10-17). Mesure du
# jour (doc/sql/test-062-episode-locations-20261017.txt) : 4 494 citations sur 3 370
# episodes T2S, 440 lieux que ni un film ni une serie ne citent. Sert seulement quand
# T_WC_T2S_EPISODE_LOCATION existe, voir f_buildlocationtables().
STR_LOCATION_DRIVING_WITH_EPISODES = (
    "  SELECT DISTINCT iv.ID_ITEM AS ID_WIKIDATA\n"
    "  FROM T_WC_WIKIDATA_STATEMENT st\n"
    "  INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT\n"
    "  WHERE st.ID_PROPERTY IN ('P840', 'P915')\n"
    "    AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')\n"
    "    AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE m WHERE m.ID_WIKIDATA = st.ID_WIKIDATA)\n"
    "       OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE s WHERE s.ID_WIKIDATA = st.ID_WIKIDATA)\n"
    "       OR EXISTS (SELECT 1 FROM T_WC_T2S_EPISODE e WHERE e.ID_WIKIDATA = st.ID_WIKIDATA) )\n"
)


def f_locationassociationsql(strmajor):
    """Le peuplement d'une table d'association, film, serie ou episode.

    strmajor vaut 'MOVIE', 'SERIE' ou 'EPISODE' (TMDB-MOVIE-PREPROCESS-062). Les deux requetes sont rigoureusement paralleles,
    et c'est la raison de les generer plutot que de les ecrire deux fois : le defaut
    classique de ces paires est qu'une des deux derive lors d'une correction.

    LOCATION_ROLE traduit la propriete en mot, 'filming' pour P915 et 'narrative' pour
    P840. Le modele de langage ecrit du SQL contre cette colonne ; lui faire retenir des
    codes Wikidata est l'erreur que FASTAPI-TEXT2SQL-238 a corrigee.

    DISPLAY_ORDER n'est pas rempli ici : l'API trie par IMDB_RATING_WEIGHTED de l'oeuvre,
    qui est une donnee vivante, et figer un rang le jour du calcul le rendrait faux des
    le lendemain.
    """
    strid = "ID_" + strmajor
    strt2s = "T_WC_T2S_" + strmajor
    return (
        f"INSERT INTO T_WC_T2S_{strmajor}_LOCATION_BUILD\n"
        f"    ({strid}, ID_LOCATION, LOCATION_ROLE, DELETED, DAT_CREAT, TIM_UPDATED)\n"
        f"SELECT DISTINCT w.{strid},\n"
        "       loc.ID_LOCATION,\n"
        "       CASE st.ID_PROPERTY WHEN 'P915' THEN 'filming' ELSE 'narrative' END,\n"
        "       0, CURDATE(), NOW()\n"
        "FROM T_WC_WIKIDATA_STATEMENT st\n"
        "INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT\n"
        f"INNER JOIN {strt2s} w ON w.ID_WIKIDATA = st.ID_WIKIDATA\n"
        "INNER JOIN T_WC_T2S_LOCATION_BUILD loc ON loc.ID_WIKIDATA = iv.ID_ITEM\n"
        "WHERE st.ID_PROPERTY IN ('P840', 'P915')\n"
        "  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')\n"
    )


# TMDB-MOVIE-PREPROCESS-048 : la hierarchie des lieux, « en France » rend les films tournes
# a Paris. Fermeture transitive materialisee de P131 (entite administrative contenante) et
# P17 (pays), calculee chaque nuit avec les lieux et basculee dans le meme RENAME.
STR_LOCATION_ANCESTOR_TABLE = "T_WC_T2S_LOCATION_ANCESTOR"
# La mesure du 2026-10-19 (doc/sql/test-048-location-hierarchy-20261019.txt) s'arrete a la
# profondeur 7 (Angleterre). 12 laisse de la marge ; la borne n'est la que pour les cycles,
# que Wikidata n'interdit pas.
INT_LOCATION_ANCESTOR_MAX_DEPTH = 12
# TMDB-MOVIE-PREPROCESS-067 : un lieu a cheval sur plusieurs pays (les Appalaches, la
# Mediterranee) n'est dans aucun. Mesure et regles : doc/sql/archive/test-067-cross-border-places.sql
# et ses check-067-*, resultats du 2026-10-21.
# Classes P31 d'un parent passe : l'arete qui y monte tombe. Wikidata ne date presque jamais
# ces aretes (2 qualificatifs P582 sur 510 974) et la base ne tient que 135 P576.
TUPLE_LOCATION_ENDED_CLASSES = (
    "Q3024240",   # historical country (622 des 1 944 parents P17, l'URSS comprise)
    "Q11514315",  # historical period
    "Q19953632",  # former administrative territorial entity
    "Q50068795",  # historical Chinese state
    "Q12857432",  # Chinese dynasty
    "Q182547",    # Roman province (la Judee de Massada)
)
# historical unrecognized state, au rang preferred seulement : la Premiere Republique de
# Hongrie et les Etats confederes la portent preferred, les Etats-Unis et l'Indonesie normal.
# Prise a tout rang, la fiche des Etats-Unis tombait de 15 556 films a 420.
STR_LOCATION_ENDED_IF_PREFERRED = "Q99541706"
# occupied territory : un parent passe (le Gouvernement general du ghetto de Varsovie), sauf
# s'il est aussi disputed territory (la Cisjordanie, Gaza restent des parents vivants).
STR_LOCATION_OCCUPIED = "Q2577883"
STR_LOCATION_DISPUTED = "Q15239622"
# Decision de Philippe du 2026-10-21 : un lieu dispute va sous chacun des pays revendiques.
# Est dispute un lieu (D1) de classe disputed ou occupied territory (Jerusalem, la Crimee,
# la Cisjordanie, le Golan) ; (D2) au meme ensemble P17 qu'un lieu T2S de D1 (Yalta,
# Sebastopol : leurs parents P131 manquent presque tous a la base) ; (D3) a deux P17 dont
# l'un est un regime de facto ou un territoire dispute (Laayoune, Savur-Mohyla). Pas
# Q10711424, state with limited recognition : la Chine la porte, et l'Everest passait dispute.
TUPLE_LOCATION_DISPUTED_CLASSES = (STR_LOCATION_DISPUTED, STR_LOCATION_OCCUPIED)
TUPLE_LOCATION_DISPUTED_PARENT_CLASSES = ("Q1179466", STR_LOCATION_DISPUTED)
# Decision de Philippe du 2026-10-21 : la Normandie (Q15878) reste sous la France. Wikidata
# lui donne aussi Guernesey et Jersey (la Normandie historique) ; seule exception nommee.
TUPLE_LOCATION_P17_DROP = (("Q15878", "Q25230"), ("Q15878", "Q785"))


def _f_buildlocationancestor(cursor, connection):
    """Remplit T_WC_T2S_LOCATION_ANCESTOR_BUILD a partir de T_WC_T2S_LOCATION_BUILD (-048).

    UNE LIGNE PAR (LIEU, ANCETRE), LE LIEU LUI-MEME A LA PROFONDEUR 0. C'est le detail qui
    porte tout : « les films en X » se lit d'une seule jointure, que le film soit rattache a
    X ou a un lieu que X contient. Sans la ligne 0 il faudrait une UNION, et le modele de
    langage n'en ecrirait qu'une branche. La Lune, Gotham, la Terre du Milieu n'ont que
    cette ligne, et c'est juste.

    LE MEILLEUR RANG, PAR (ITEM, PROPRIETE). Paris porte six P131 et sept P17 (le royaume de
    France, l'Empire, l'administration militaire allemande) : si l'item a au moins une
    declaration 'preferred' pour la propriete, seules celles-la comptent, sinon toutes les
    non 'deprecated'. Mesure : Paris garde France (P17) et Grand Paris (P131). Le chainage
    date par les qualificatifs (Berlin-Est et la RDA) est ignore, c'est assume.

    LE PARCOURS TRAVERSE DES ITEMS QUI NE SONT PAS DES LIEUX T2S (« metropolitan France »,
    les departements) : la table ne garde que les paires dont les deux bouts sont des lieux
    vivants, mais la profondeur compte les sauts Wikidata. Ne pas lire DEPTH comme un niveau
    administratif.

    JAMAIS LOCATION_TYPE. La classification des lieux est fausse par endroits (Marseille est
    une region) et la hierarchie ne doit pas en dependre.

    UN LIEU A CHEVAL SUR PLUSIEURS PAYS N'EST DANS AUCUN (-067). Les Appalaches ont pour pays
    les Etats-Unis, le Canada et la France (Saint-Pierre-et-Miquelon) et apportaient 51 films
    a la fiche France. Deux temps, etapes 3d-0a a 3d-0e : (a) l'arete qui monte vers un parent
    passe tombe (qualificatif P582, P576, ou les classes TUPLE_LOCATION_ENDED_CLASSES : sans
    elles Edo, Bangkok, Sobibor passaient transfrontaliers) ; (b) un item qui garde deux P17
    ou plus, parents connus de la base seulement, ne monte plus du tout, P131 compris (sinon
    Saint-Pierre-et-Miquelon, en P131, ramene la France). Il garde sa ligne 0 et ses enfants.
    Les lieux disputes y echappent (decision de Philippe, voir TUPLE_LOCATION_DISPUTED_CLASSES)
    et vont sous chaque pays revendique ; la Normandie reste sous la France
    (TUPLE_LOCATION_P17_DROP). Mesure du 2026-10-21 : 1 268 paires sur 37 155 tombent, aucune
    n'apparait ; France 1 797 a 1 792 films tournes, 3 753 a 3 608 qui s'y passent.

    Parcours en largeur, une instruction par profondeur : INSERT IGNORE sur la cle
    (lieu, ancetre) garde la premiere profondeur atteinte, la plus petite, et coupe les
    cycles. Rend (lignes, lignes au-dessus de 0, profondeur atteinte).
    """
    strcharset = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    strended = ", ".join("'" + strclass + "'" for strclass in TUPLE_LOCATION_ENDED_CLASSES)
    strdisputed = ", ".join("'" + strclass + "'" for strclass in TUPLE_LOCATION_DISPUTED_CLASSES)
    strdisputedparent = ", ".join("'" + strclass + "'" for strclass in TUPLE_LOCATION_DISPUTED_PARENT_CLASSES)
    strdrop = " OR ".join(f"(ID_CHILD = '{strchild}' AND ID_PARENT = '{strparent}')"
                          for strchild, strparent in TUPLE_LOCATION_P17_DROP)
    # Une arete ouverte : ni datee, ni vers un parent passe, ni retiree par nom.
    stropen = "HAS_END = 0 AND PARENT_ENDED = 0 AND EDITORIAL_DROP = 0"
    for strtable in ("tmp_lieu_arete", "tmp_lieu_arete_e", "tmp_lieu_passe", "tmp_lieu_multi",
                     "tmp_lieu_dispute", "tmp_lieu_dispute_set", "tmp_lieu_transfrontalier"):
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS " + strtable)
    cursor.execute("CREATE TEMPORARY TABLE tmp_lieu_arete_e ("
                   "ID_CHILD VARCHAR(50) NOT NULL, ID_PARENT VARCHAR(50) NOT NULL, PROP VARCHAR(10) NOT NULL, "
                   "HAS_END TINYINT NOT NULL, PARENT_ENDED TINYINT NOT NULL DEFAULT 0, "
                   "EDITORIAL_DROP TINYINT NOT NULL DEFAULT 0, PARENT_KNOWN TINYINT NOT NULL DEFAULT 0, "
                   "PRIMARY KEY (ID_CHILD, ID_PARENT, PROP), KEY (ID_PARENT))" + strcharset)
    _f_locationstep(cursor, "3d-0a. INSERT des aretes P131 / P17 au meilleur rang (tmp_lieu_arete_e)", (
        "INSERT INTO tmp_lieu_arete_e (ID_CHILD, ID_PARENT, PROP, HAS_END)\n"
        "SELECT st.ID_WIKIDATA, iv.ID_ITEM, st.ID_PROPERTY,\n"
        "       MAX(EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER q\n"
        "                   WHERE q.ID_STATEMENT = st.ID_STATEMENT AND q.ID_QUALIFIER_PROPERTY = 'P582'\n"
        "                     AND COALESCE(q.DELETED, 0) = 0))\n"
        "FROM T_WC_WIKIDATA_STATEMENT st\n"
        "INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT\n"
        "WHERE st.ID_PROPERTY IN ('P131', 'P17')\n"
        "  AND st.DELETED = 0\n"
        "  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')\n"
        "  AND (st.`RANK` = 'preferred'\n"
        "       OR NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT p\n"
        "                      WHERE p.ID_WIKIDATA = st.ID_WIKIDATA AND p.ID_PROPERTY = st.ID_PROPERTY\n"
        "                        AND p.`RANK` = 'preferred' AND p.DELETED = 0))\n"
        "  AND iv.ID_ITEM <> st.ID_WIKIDATA\n"
        "GROUP BY st.ID_WIKIDATA, iv.ID_ITEM, st.ID_PROPERTY"))
    connection.commit()
    cursor.execute("CREATE TEMPORARY TABLE tmp_lieu_passe (ID_ITEM VARCHAR(50) NOT NULL PRIMARY KEY)" + strcharset)
    _f_locationstep(cursor, "3d-0b. INSERT des parents passes (tmp_lieu_passe)", (
        "INSERT IGNORE INTO tmp_lieu_passe\n"
        "SELECT DISTINCT st.ID_WIKIDATA FROM T_WC_WIKIDATA_STATEMENT st\n"
        "WHERE st.ID_PROPERTY = 'P576' AND st.DELETED = 0 AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')\n"
        "  AND st.ID_WIKIDATA IN (SELECT ID_PARENT FROM tmp_lieu_arete_e)"))
    _f_locationstep(cursor, "3d-0b. INSERT des parents de classe passee (tmp_lieu_passe)", (
        "INSERT IGNORE INTO tmp_lieu_passe\n"
        "SELECT DISTINCT st.ID_WIKIDATA FROM T_WC_WIKIDATA_STATEMENT st\n"
        "INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT\n"
        "WHERE st.ID_PROPERTY = 'P31' AND st.DELETED = 0 AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')\n"
        f"  AND (iv.ID_ITEM IN ({strended})\n"
        f"       OR (iv.ID_ITEM = '{STR_LOCATION_ENDED_IF_PREFERRED}' AND st.`RANK` = 'preferred')\n"
        f"       OR (iv.ID_ITEM = '{STR_LOCATION_OCCUPIED}'\n"
        "           AND NOT EXISTS (SELECT 1 FROM T_WC_WIKIDATA_STATEMENT s2\n"
        "                           INNER JOIN T_WC_WIKIDATA_ITEM_VALUE v2 ON v2.ID_STATEMENT = s2.ID_STATEMENT\n"
        "                           WHERE s2.ID_WIKIDATA = st.ID_WIKIDATA AND s2.ID_PROPERTY = 'P31' AND s2.DELETED = 0\n"
        "                             AND (s2.`RANK` IS NULL OR s2.`RANK` <> 'deprecated')\n"
        f"                             AND v2.ID_ITEM = '{STR_LOCATION_DISPUTED}')))\n"
        "  AND st.ID_WIKIDATA IN (SELECT ID_PARENT FROM tmp_lieu_arete_e)"))
    _f_locationstep(cursor, "3d-0b. UPDATE des drapeaux d'arete (passe, connu, retrait nomme)", (
        "UPDATE tmp_lieu_arete_e e INNER JOIN tmp_lieu_passe d ON d.ID_ITEM = e.ID_PARENT SET e.PARENT_ENDED = 1"))
    cursor.execute("UPDATE tmp_lieu_arete_e e INNER JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = e.ID_PARENT "
                   "SET e.PARENT_KNOWN = 1")
    cursor.execute(f"UPDATE tmp_lieu_arete_e SET EDITORIAL_DROP = 1 WHERE PROP = 'P17' AND ({strdrop})")
    connection.commit()
    # L'ensemble P17 ouvert de chaque item qui en garde deux ou plus, parents connus seulement
    # (decision de Philippe : un parent sans ligne T_WC_WIKIDATA_ITEM ne compte pas).
    cursor.execute("CREATE TEMPORARY TABLE tmp_lieu_multi (ID_CHILD VARCHAR(50) NOT NULL PRIMARY KEY, "
                   "N_P17 INT NOT NULL, P17_SET VARCHAR(2000) NOT NULL, KEY (P17_SET(200)))" + strcharset)
    _f_locationstep(cursor, "3d-0c. INSERT des items a deux P17 ou plus (tmp_lieu_multi)", (
        "INSERT INTO tmp_lieu_multi (ID_CHILD, N_P17, P17_SET)\n"
        "SELECT ID_CHILD, COUNT(DISTINCT ID_PARENT),\n"
        "       GROUP_CONCAT(DISTINCT ID_PARENT ORDER BY ID_PARENT SEPARATOR ' ')\n"
        "FROM tmp_lieu_arete_e\n"
        f"WHERE PROP = 'P17' AND {stropen} AND PARENT_KNOWN = 1\n"
        "GROUP BY ID_CHILD HAVING COUNT(DISTINCT ID_PARENT) >= 2"))
    cursor.execute("CREATE TEMPORARY TABLE tmp_lieu_dispute (ID_CHILD VARCHAR(50) NOT NULL PRIMARY KEY)" + strcharset)
    _f_locationstep(cursor, "3d-0d. INSERT des items disputes, D1 par la classe (tmp_lieu_dispute)", (
        "INSERT IGNORE INTO tmp_lieu_dispute\n"
        "SELECT DISTINCT m.ID_CHILD FROM tmp_lieu_multi m\n"
        "INNER JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA = m.ID_CHILD AND s.ID_PROPERTY = 'P31' AND s.DELETED = 0\n"
        "  AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated')\n"
        "INNER JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT\n"
        f"WHERE v.ID_ITEM IN ({strdisputed})"))
    # MariaDB ne rouvre pas une table temporaire dans la meme instruction : les ensembles P17
    # des lieux D1 passent par leur propre table avant D2.
    cursor.execute("CREATE TEMPORARY TABLE tmp_lieu_dispute_set (P17_SET VARCHAR(2000) NOT NULL, "
                   "KEY (P17_SET(200)))" + strcharset)
    cursor.execute(
        "INSERT INTO tmp_lieu_dispute_set\n"
        "SELECT DISTINCT m.P17_SET FROM tmp_lieu_dispute d\n"
        "INNER JOIN tmp_lieu_multi m ON m.ID_CHILD = d.ID_CHILD\n"
        "WHERE EXISTS (SELECT 1 FROM T_WC_T2S_LOCATION_BUILD l WHERE l.ID_WIKIDATA = d.ID_CHILD AND l.DELETED = 0)")
    _f_locationstep(cursor, "3d-0d. INSERT des items disputes, D2 par l'ensemble P17", (
        "INSERT IGNORE INTO tmp_lieu_dispute\n"
        "SELECT m.ID_CHILD FROM tmp_lieu_multi m\n"
        "INNER JOIN tmp_lieu_dispute_set x ON x.P17_SET = m.P17_SET"))
    _f_locationstep(cursor, "3d-0d. INSERT des items disputes, D3 par le parent", (
        "INSERT IGNORE INTO tmp_lieu_dispute\n"
        "SELECT DISTINCT m.ID_CHILD FROM tmp_lieu_multi m\n"
        "INNER JOIN tmp_lieu_arete_e e ON e.ID_CHILD = m.ID_CHILD AND e.PROP = 'P17'\n"
        f"  AND e.{stropen.replace(' AND ', ' AND e.')} AND e.PARENT_KNOWN = 1\n"
        "INNER JOIN T_WC_WIKIDATA_STATEMENT s ON s.ID_WIKIDATA = e.ID_PARENT AND s.ID_PROPERTY = 'P31' AND s.DELETED = 0\n"
        "  AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated')\n"
        "INNER JOIN T_WC_WIKIDATA_ITEM_VALUE v ON v.ID_STATEMENT = s.ID_STATEMENT\n"
        f"WHERE m.N_P17 = 2 AND v.ID_ITEM IN ({strdisputedparent})"))
    cursor.execute("CREATE TEMPORARY TABLE tmp_lieu_transfrontalier (ID_CHILD VARCHAR(50) NOT NULL PRIMARY KEY)"
                   + strcharset)
    _f_locationstep(cursor, "3d-0e. INSERT des items transfrontaliers (tmp_lieu_transfrontalier)", (
        "INSERT INTO tmp_lieu_transfrontalier\n"
        "SELECT m.ID_CHILD FROM tmp_lieu_multi m\n"
        "WHERE NOT EXISTS (SELECT 1 FROM tmp_lieu_dispute d WHERE d.ID_CHILD = m.ID_CHILD)"))
    connection.commit()
    cursor.execute("CREATE TEMPORARY TABLE tmp_lieu_arete ("
                   "ID_CHILD VARCHAR(50) NOT NULL, ID_PARENT VARCHAR(50) NOT NULL, "
                   "PRIMARY KEY (ID_CHILD, ID_PARENT))" + strcharset)
    _f_locationstep(cursor, "3d-0f. INSERT des aretes gardees (tmp_lieu_arete)", (
        "INSERT IGNORE INTO tmp_lieu_arete (ID_CHILD, ID_PARENT)\n"
        "SELECT e.ID_CHILD, e.ID_PARENT FROM tmp_lieu_arete_e e\n"
        f"WHERE e.{stropen.replace(' AND ', ' AND e.')}\n"
        "  AND NOT EXISTS (SELECT 1 FROM tmp_lieu_transfrontalier c WHERE c.ID_CHILD = e.ID_CHILD)"))
    connection.commit()
    for strtable in ("tmp_lieu_arete_e", "tmp_lieu_passe", "tmp_lieu_multi", "tmp_lieu_dispute",
                     "tmp_lieu_dispute_set", "tmp_lieu_transfrontalier"):
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS " + strtable)
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_lieu_ancetre")
    cursor.execute("CREATE TEMPORARY TABLE tmp_lieu_ancetre ("
                   "ID_LIEU VARCHAR(50) NOT NULL, ID_ANCETRE VARCHAR(50) NOT NULL, DEPTH INT NOT NULL, "
                   "PRIMARY KEY (ID_LIEU, ID_ANCETRE), KEY (DEPTH)) "
                   "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
    _f_locationstep(cursor, "3d-1. INSERT de la profondeur 0", (
        "INSERT INTO tmp_lieu_ancetre (ID_LIEU, ID_ANCETRE, DEPTH)\n"
        "SELECT ID_WIKIDATA, ID_WIKIDATA, 0 FROM T_WC_T2S_LOCATION_BUILD\n"
        "WHERE DELETED = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> ''"))
    intdepth = 0
    for intnext in range(1, INT_LOCATION_ANCESTOR_MAX_DEPTH + 1):
        # Pas de _f_locationstep : douze lignes de journal pour une boucle, c'est trop ; la
        # ligne 3d-2 ci-dessous dit jusqu'ou la remontee est allee.
        cursor.execute(
            "INSERT IGNORE INTO tmp_lieu_ancetre (ID_LIEU, ID_ANCETRE, DEPTH)\n"
            "SELECT t.ID_LIEU, a.ID_PARENT, %s\n"
            "FROM tmp_lieu_ancetre t\n"
            "INNER JOIN tmp_lieu_arete a ON a.ID_CHILD = t.ID_ANCETRE\n"
            "WHERE t.DEPTH = %s", (intnext, intnext - 1))
        if cursor.rowcount <= 0:
            break
        intdepth = intnext
    if intdepth == INT_LOCATION_ANCESTOR_MAX_DEPTH:
        print(f"72:   3d-2. ATTENTION : la remontee touche encore la profondeur {intdepth}, "
              "la borne. Un cycle ou une chaine anormale, a regarder (doc/sql/test-048-location-hierarchy.sql).")
    print(f"72:   3d-2. remontee jusqu'a la profondeur {intdepth}", flush=True)
    connection.commit()
    introws = _f_locationstep(cursor, "3d-3. INSERT " + STR_LOCATION_ANCESTOR_TABLE + "_BUILD", (
        "INSERT INTO " + STR_LOCATION_ANCESTOR_TABLE + "_BUILD (ID_LOCATION, ID_ANCESTOR, DEPTH)\n"
        "SELECT loc.ID_LOCATION, anc.ID_LOCATION, t.DEPTH\n"
        "FROM tmp_lieu_ancetre t\n"
        "INNER JOIN T_WC_T2S_LOCATION_BUILD loc ON loc.ID_WIKIDATA = t.ID_LIEU AND loc.DELETED = 0\n"
        "INNER JOIN T_WC_T2S_LOCATION_BUILD anc ON anc.ID_WIKIDATA = t.ID_ANCETRE AND anc.DELETED = 0\n"
        "ORDER BY loc.ID_LOCATION, t.DEPTH, anc.ID_LOCATION"))
    connection.commit()
    cursor.execute("SELECT COUNT(*) AS C FROM " + STR_LOCATION_ANCESTOR_TABLE + "_BUILD WHERE DEPTH > 0")
    intabove = int(cursor.fetchone()["C"])
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_lieu_ancetre")
    cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_lieu_arete")
    return introws, intabove, intdepth


def _f_locationstep(cursor, strstep, strsql):
    """Execute une etape de la reconstruction des lieux en la NOMMANT d'abord.

    ⚠ ECRIT LE 2026-09-12 APRES UN ECHEC MUET. Le passage s'est arrete sur
    « MySQL Error: (1205, Lock wait timeout exceeded) » juste apres la construction des
    cones, et rien dans le journal ne disait a quelle instruction : il fallait deviner
    entre deux DROP, deux CREATE, trois INSERT, trois UPDATE correles et un RENAME de six
    tables. Une reconstruction en onze instructions qui echoue sans dire laquelle oblige
    a refaire le diagnostic de zero.

    Le nom part AVANT l'execution et non apres, sinon la derniere ligne du journal est
    celle qui a REUSSI, ce qui envoie chercher au mauvais endroit.

    Rend le nombre de lignes touchees, utile aux etapes qui comptent.
    """
    print(f"72:   {strstep} ...", flush=True)
    cursor.execute(strsql)
    return cursor.rowcount


def f_buildlocationtables():
    """Reconstruit les tables des lieux (quatre, cinq avec les episodes), en entier, par du SQL seul.

    Depuis 1.1.19 (TMDB-MOVIE-PREPROCESS-048), la quatrieme est la hierarchie,
    T_WC_T2S_LOCATION_ANCESTOR, voir _f_buildlocationancestor().

    RECONSTRUCTION TOTALE ET NON INCREMENTALE, et c'est la volumetrie qui l'a decide :
    11 922 lieux et 95 766 associations mesures le 2026-09-11. A cette taille, un
    curseur, un filigrane et une reprise coutent plus en complexite qu'ils ne font gagner
    en temps. Le patron BUILD puis RENAME est celui du processus 40 : la table servie
    n'est jamais vide, meme une seconde, parce que le remplacement est atomique.

    L'ORDRE DES ETAPES EST CONTRAINT. Les associations ont besoin de ID_LOCATION, donc
    de la table d'entite construite ; les compteurs et les agregats ont besoin des
    associations. D'ou entite, associations, agregats, puis les trois RENAME ensemble.

    Rend un dictionnaire de comptes, pour la telemetrie et pour la recette.
    """
    connection = cp.connectioncp
    cursor = connection.cursor()
    arrresult = {}
    try:
        # ⚠ ATTENTE DE VERROU BORNEE A DEUX MINUTES, sur la session seulement.
        # Par defaut MariaDB attend 50 secondes, ce qui est court pour un RENAME de six
        # tables que l'API lit en continu, et le 2026-09-12 le passage est mort la.
        # Deux minutes laissent passer une lecture longue sans bloquer le traitement de
        # nuit derriere une transaction oubliee : au-dela, l'echec est le BON
        # comportement, il signale qu'une session tient un verrou qu'elle ne devrait plus
        # tenir. Ne pas monter ce chiffre pour faire disparaitre le symptome.
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 120")

        # ⚠ LECTURE NON VERROUILLANTE QUAND LE SERVEUR LE PERMET, et la condition est
        # verifiee A L'EXECUTION plutot qu'ecrite en dur.
        #
        # Le probleme : l'etape 1c est un INSERT ... SELECT sur T_WC_WIKIDATA_STATEMENT,
        # et ce type d'instruction ne lit PAS en simple lecture. Il pose des verrous
        # PARTAGES sur les lignes source, pour que le journal binaire rejoue le meme
        # resultat. Le 2026-09-12, pendant que wikidata-crawler inserait dans cette table
        # depuis 4 425 secondes, l'etape a attendu puis rendu 1205 : la table _BUILD
        # etait creee a 17:22:08 et vide, ce qui localise l'echec sans ambiguite.
        #
        # La sortie : en READ COMMITTED la lecture redevient un instantane et ne verrouille
        # plus rien. Mais ce n'est licite que s'il n'y a pas de journal a rejouer.
        # Mesure du 2026-09-12 sur ce serveur : log_bin = 0, journal binaire inactif,
        # donc rien a preserver et binlog_format = MIXED est sans effet.
        #
        # POURQUOI UNE VERIFICATION ET NON UNE LIGNE EN DUR. Le correctif depend d'un
        # reglage serveur qui peut changer sans que ce code le sache : activer la
        # journalisation binaire, pour de la replication ou une restauration a l'instant,
        # invaliderait le raisonnement. La verification garde le code juste dans les deux
        # mondes, et elle dit dans le journal ce qu'elle a decide, ce qu'une ligne en dur
        # ne ferait pas.
        try:
            cursor.execute("SELECT @@GLOBAL.log_bin AS LOGBIN, @@GLOBAL.binlog_format AS FORMAT")
            arrbinlog = cursor.fetchone()
            blnbinlog = int(arrbinlog["LOGBIN"] or 0) == 1
            strbinlogformat = (arrbinlog["FORMAT"] or "").upper()
            if (not blnbinlog) or strbinlogformat == "ROW":
                cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                strraison = "journal binaire inactif" if not blnbinlog else "binlog_format = ROW"
                print(f"72: lecture en READ COMMITTED ({strraison}), l'INSERT ... SELECT ne verrouillera pas la source.")
            else:
                print(f"72: lecture laissee en REPEATABLE READ (binlog_format = {strbinlogformat}, journalisation active) :")
                print("72:   l'INSERT ... SELECT verrouillera la source, donc NE PAS lancer pendant un chargement de wikidata-crawler.")
        except Exception as isolationerror:
            # Ne jamais faire echouer la reconstruction sur l'optimisation qui devait la
            # proteger : sans le reglage, le comportement est celui d'avant, verrouillant
            # mais correct.
            print(f"72: isolation laissee par defaut ({type(isolationerror).__name__}: {isolationerror}).")

        # --- 0. Les episodes, seulement si leur table existe (TMDB-MOVIE-PREPROCESS-062) --
        # La table T_WC_T2S_EPISODE_LOCATION et la colonne EPISODE_COUNT viennent d'une
        # migration (doc/sql/migration-062-episode-location.sql) qui ne passe pas par ce
        # code. Sans cette verification, un deploiement du code avant la migration ferait
        # echouer le CREATE ... LIKE de l'etape 3 chaque nuit, et les lieux cesseraient de
        # se rafraichir. Absentes, le passage reste celui d'avant, films et series.
        cursor.execute("SHOW TABLES LIKE 'T_WC_T2S_EPISODE_LOCATION'")
        blnepisodes = cursor.fetchone() is not None
        if blnepisodes:
            cursor.execute("SHOW COLUMNS FROM T_WC_T2S_LOCATION LIKE 'EPISODE_COUNT'")
            blnepisodes = cursor.fetchone() is not None
        arrmajors = ("MOVIE", "SERIE", "EPISODE") if blnepisodes else ("MOVIE", "SERIE")
        arrresult["episodes_actifs"] = 1 if blnepisodes else 0
        print("72: episodes " + ("inclus (T_WC_T2S_EPISODE_LOCATION presente)." if blnepisodes else
                                 "exclus : T_WC_T2S_EPISODE_LOCATION ou EPISODE_COUNT absente, migration 062 a passer."))

        # --- 1. L'entite -----------------------------------------------------
        # Le libelle et la description viennent de V2 seul, jamais de T_WC_T2S_ITEM :
        # la population de cette derniere est pilotee par V1 deliberement, et elle part
        # en TMDB-MOVIE-PREPROCESS-047. S'y adosser ajouterait un lecteur V1 au moment
        # ou l'on cherche a en retirer.
        #
        # OVERVIEW porte la description Wikidata, et ce n'est pas decoratif : c'est elle
        # qui desambiguise l'embedding. "Paris, capital and largest city of France" et
        # "Paris, city in Texas" se distinguent par le vecteur, la ou "Paris" et "Paris"
        # produisent deux documents identiques que rien ne peut departager. Le type seul
        # n'y suffirait pas, les deux sont des villes.
        # OVERVIEW_FR (TMDB-MOVIE-PREPROCESS-064) est ajoutee ici a la table servie, AVANT que
        # 1b ne cree la table de construction LIKE elle : sinon la colonne manquerait a la
        # construction et les INSERT qui la nomment echoueraient. Meme geste que IMDB_VOTES
        # sur T_WC_T2S_EPISODE (processus 26) ; une seule nuit l'execute, les suivantes
        # trouvent la colonne et ne font rien. Pas de migration en voie 2 a passer.
        _f_locationstep(cursor, "1-0. ALTER T_WC_T2S_LOCATION ADD OVERVIEW_FR IF NOT EXISTS",
                        "ALTER TABLE T_WC_T2S_LOCATION ADD COLUMN IF NOT EXISTS OVERVIEW_FR MEDIUMTEXT NULL AFTER OVERVIEW")
        connection.commit()
        _f_locationstep(cursor, "1a. DROP T_WC_T2S_LOCATION_BUILD",
                        "DROP TABLE IF EXISTS T_WC_T2S_LOCATION_BUILD")
        _f_locationstep(cursor, "1b. CREATE T_WC_T2S_LOCATION_BUILD",
                        "CREATE TABLE T_WC_T2S_LOCATION_BUILD LIKE T_WC_T2S_LOCATION")
        connection.commit()
        # --- 1c. Les lieux, en trois insertions, POUR QUE ID_LOCATION NE BOUGE PAS ----
        # ⚠ CORRIGE LE 2026-09-13. La premiere version inserait sans ID_LOCATION dans une
        # table creee LIKE la table servie : l'AUTO_INCREMENT renumerotait les lieux a
        # chaque nuit. Un identifiant qui change chaque nuit n'en est pas un : un lien
        # /locations/{id}, une assertion d'evaluation, un document Chroma cle dessus
        # pointaient ailleurs le lendemain, sans erreur. ID_LOCATION se comporte
        # desormais comme ID_GROUP ou ID_AWARD : attribue une fois, jamais reattribue.
        #
        #   (0)   le perimetre est materialise une fois : la requete pilote est la partie
        #         couteuse, et elle sert trois fois ;
        #   (i)   un lieu deja connu garde son ID_LOCATION et sa DAT_CREAT ;
        #   (ii)  un lieu sorti du perimetre reste, DELETED = 1, pour que son numero ne
        #         soit jamais reattribue et qu'un consommateur lise « supprime » plutot
        #         que rien, la convention DELETED des tables T2S ; ses associations et
        #         ses compteurs tombent a zero d'eux-memes, le perimetre ne le cite plus ;
        #   (iii) un lieu nouveau recoit un numero au-dela de tout numero deja attribue,
        #         le compteur etant aligne explicitement sur MAX(ID_LOCATION) de la table
        #         servie, parce que CREATE TABLE ... LIKE ne reporte pas le compteur, et
        #         parce qu'aucune ligne n'etant plus jamais retiree, ce MAX est le vrai
        #         plus haut numero jamais attribue.
        # Trois insertions et non une seule melant ID explicites et NULL : dans un meme
        # INSERT ... SELECT, l'AUTO_INCREMENT peut attribuer a une ligne NULL un numero
        # qu'une ligne explicite apporte plus loin, et c'est une cle dupliquee.
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_lieux")
        cursor.execute("CREATE TEMPORARY TABLE tmp_lieux (ID_WIKIDATA VARCHAR(20) NOT NULL PRIMARY KEY) "
                       "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
        arrresult["perimetre"] = _f_locationstep(cursor, "1c-0. INSERT du perimetre (tmp_lieux)", (
            "INSERT INTO tmp_lieux (ID_WIKIDATA)\n"
            "SELECT lieux.ID_WIKIDATA\n"
            "FROM (\n" + (STR_LOCATION_DRIVING_WITH_EPISODES if blnepisodes else STR_LOCATION_DRIVING) + ") lieux\n"
            "WHERE lieux.ID_WIKIDATA IS NOT NULL AND lieux.ID_WIKIDATA <> ''"))
        connection.commit()
        # Les libelles (TMDB-MOVIE-PREPROCESS-064). LOCATION_NAME : l'anglais, le 'mul' que
        # LABEL_EN porte deja (WIKIDATA-CRAWLER-026), les variantes en-gb et en-us, le
        # francais, puis le premier libelle de n'importe quelle langue : le 2026-10-19,
        # 172 lieux vivants n'avaient aucun nom, et tmdb-front ne lie pas un lieu sans nom
        # (Unterfuhrung Messedamm n'existait qu'en allemand). Seul un lieu sans aucune
        # ligne Wikidata reste sans nom.
        # LOCATION_NAME_FR : le francais, puis 'mul', puis le nom ci-dessus. Pas de drapeau
        # de repli : un LOCATION_NAME_FR egal a LOCATION_NAME dit deja « pas de libelle
        # francais propre », la convention qu'embedding-update applique (pas de document FR
        # quand les deux noms sont egaux). Le prompt de l'API qui compare LOCATION_NAME_FR
        # a « Londres » y gagne : « Oslo » repond en francais aussi (2 214 lieux sans
        # libelle francais le 2026-10-19).
        # OVERVIEW_FR : la description francaise seule, sans repli sur l'anglais ; une
        # description anglaise sur une page francaise est un choix du consommateur.
        strnameexpr = ("COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')),\n"
                       "                NULLIF(wi.LABEL_EN, ''),\n"
                       "                JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.\"en-gb\"')),\n"
                       "                JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.\"en-us\"')),\n"
                       "                JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.fr')),\n"
                       "                JSON_UNQUOTE(JSON_EXTRACT(JSON_EXTRACT(wi.LABELS_JSON, '$.*'), '$[0]')))")
        strlabels = (
            "       " + strnameexpr + ",\n"
            "       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.fr')),\n"
            "                JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.mul')),\n"
            "                " + strnameexpr + "),\n"
            "       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(wi.DESCRIPTIONS_JSON, '$.en')),\n"
            "                NULLIF(wi.DESCRIPTION_EN, '')),\n"
            "       JSON_UNQUOTE(JSON_EXTRACT(wi.DESCRIPTIONS_JSON, '$.fr')),\n"
        )
        # (i) reporte aussi tout ce que le passage ne recalcule pas : les images (71), le
        # tampon TIM_WIKIDATA_COMPLETED, l'ordre d'affichage et les colonnes de
        # proprietaire. Sans ce report, chaque nuit les remettait a NULL pour tous les
        # lieux vivants, et seule la branche (ii) les gardait. Le type, les compteurs et
        # les agregats sont recalcules plus bas, inutile de les reporter.
        arrresult["lieux_conserves"] = _f_locationstep(cursor, "1c-i. INSERT des lieux connus (ID_LOCATION conserve)", (
            "INSERT INTO T_WC_T2S_LOCATION_BUILD\n"
            "    (ID_LOCATION, ID_WIKIDATA, LOCATION_NAME, LOCATION_NAME_FR, OVERVIEW, OVERVIEW_FR,\n"
            "     LOCATION_SOURCE, DELETED, DISPLAY_ORDER, ID_CREATOR, DAT_CREAT,\n"
            "     ID_OWNER, TIM_UPDATED, ID_USER_UPDATED, POSTER_PATH, WIKIPEDIA_IMAGE_PATH,\n"
            "     TIM_WIKIDATA_COMPLETED, WIKIPEDIA_MAIN_IMAGE_URL, WIKIPEDIA_MAIN_IMAGE_URL_FR)\n"
            "SELECT old.ID_LOCATION, lieux.ID_WIKIDATA,\n" + strlabels +
            "       'wikidata', 0, old.DISPLAY_ORDER, old.ID_CREATOR, COALESCE(old.DAT_CREAT, CURDATE()),\n"
            "       old.ID_OWNER, NOW(), old.ID_USER_UPDATED, old.POSTER_PATH, old.WIKIPEDIA_IMAGE_PATH,\n"
            "       old.TIM_WIKIDATA_COMPLETED, old.WIKIPEDIA_MAIN_IMAGE_URL, old.WIKIPEDIA_MAIN_IMAGE_URL_FR\n"
            "FROM tmp_lieux lieux\n"
            "INNER JOIN T_WC_T2S_LOCATION old ON old.ID_WIKIDATA = lieux.ID_WIKIDATA\n"
            "LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = lieux.ID_WIKIDATA"))
        connection.commit()
        arrresult["lieux_supprimes"] = _f_locationstep(cursor, "1c-ii. INSERT des lieux sortis du perimetre (DELETED = 1)", (
            "INSERT INTO T_WC_T2S_LOCATION_BUILD\n"
            "    (ID_LOCATION, ID_WIKIDATA, LOCATION_NAME, LOCATION_NAME_FR, OVERVIEW, OVERVIEW_FR,\n"
            "     LOCATION_SOURCE, LOCATION_TYPE, DELETED, DISPLAY_ORDER, ID_CREATOR, DAT_CREAT,\n"
            "     ID_OWNER, TIM_UPDATED, ID_USER_UPDATED, POSTER_PATH, WIKIPEDIA_IMAGE_PATH,\n"
            "     TIM_WIKIDATA_COMPLETED, WIKIPEDIA_MAIN_IMAGE_URL, WIKIPEDIA_MAIN_IMAGE_URL_FR)\n"
            "SELECT old.ID_LOCATION, old.ID_WIKIDATA, old.LOCATION_NAME, old.LOCATION_NAME_FR, old.OVERVIEW, old.OVERVIEW_FR,\n"
            "       old.LOCATION_SOURCE, old.LOCATION_TYPE, 1, old.DISPLAY_ORDER, old.ID_CREATOR, old.DAT_CREAT,\n"
            "       old.ID_OWNER, CASE WHEN old.DELETED = 1 THEN old.TIM_UPDATED ELSE NOW() END, old.ID_USER_UPDATED,\n"
            "       old.POSTER_PATH, old.WIKIPEDIA_IMAGE_PATH,\n"
            "       old.TIM_WIKIDATA_COMPLETED, old.WIKIPEDIA_MAIN_IMAGE_URL, old.WIKIPEDIA_MAIN_IMAGE_URL_FR\n"
            "FROM T_WC_T2S_LOCATION old\n"
            "WHERE NOT EXISTS (SELECT 1 FROM tmp_lieux lieux WHERE lieux.ID_WIKIDATA = old.ID_WIKIDATA)"))
        connection.commit()
        cursor.execute("SELECT COALESCE(MAX(ID_LOCATION), 0) AS M FROM T_WC_T2S_LOCATION")
        lngnextlocationid = int(cursor.fetchone()["M"]) + 1
        _f_locationstep(cursor, f"1c-iii-0. ALTER AUTO_INCREMENT = {lngnextlocationid}",
                        f"ALTER TABLE T_WC_T2S_LOCATION_BUILD AUTO_INCREMENT = {lngnextlocationid}")
        arrresult["lieux_nouveaux"] = _f_locationstep(cursor, "1c-iii. INSERT des lieux nouveaux (ID_LOCATION attribue)", (
            "INSERT INTO T_WC_T2S_LOCATION_BUILD\n"
            "    (ID_WIKIDATA, LOCATION_NAME, LOCATION_NAME_FR, OVERVIEW, OVERVIEW_FR,\n"
            "     LOCATION_SOURCE, DELETED, DAT_CREAT, TIM_UPDATED)\n"
            "SELECT lieux.ID_WIKIDATA,\n" + strlabels +
            "       'wikidata', 0, CURDATE(), NOW()\n"
            "FROM tmp_lieux lieux\n"
            "LEFT JOIN T_WC_T2S_LOCATION old ON old.ID_WIKIDATA = lieux.ID_WIKIDATA\n"
            "LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = lieux.ID_WIKIDATA\n"
            "WHERE old.ID_LOCATION IS NULL"))
        connection.commit()
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_lieux")
        arrresult["lieux"] = arrresult["lieux_conserves"] + arrresult["lieux_nouveaux"]
        print(f"72:   1c. {arrresult['lieux_conserves']} lieux conserves, {arrresult['lieux_nouveaux']} nouveaux "
              f"(a partir de {lngnextlocationid}), {arrresult['lieux_supprimes']} sortis du perimetre gardes en DELETED = 1")

        # --- 2. Le type, par jointure sur les cones --------------------------
        # DEUX NIVEAUX DE PRIORITE, et il faut les deux.
        #   Le premier est resolu a la construction des cones : une CLASSE presente dans
        #   deux cones ne garde que le premier type, par l'INSERT IGNORE sur la cle.
        #   Le second est ici : un LIEU porte souvent plusieurs classes de types
        #   differents, et Paris en porte quinze. Le ORDER BY FIELD() rejoue alors le
        #   meme ordre que LOCATION_TYPE_CONES, si bien qu'un tri alphabetique accidentel
        #   ne peut pas s'y substituer.
        # Sans le second niveau, le type rendu dependrait de l'ordre physique des lignes,
        # donc du hasard, et changerait d'un passage a l'autre sans que rien ne bouge.
        _f_locationstep(cursor, "2. UPDATE LOCATION_TYPE", (
            "UPDATE T_WC_T2S_LOCATION_BUILD loc\n"
            "SET loc.LOCATION_TYPE = (\n"
            "    SELECT lc.LOCATION_TYPE\n"
            "    FROM T_WC_WIKIDATA_STATEMENT st\n"
            "    INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT\n"
            "    INNER JOIN `" + STR_LOCATION_CLASS_TABLE + "` lc ON lc.ID_CLASS = iv.ID_ITEM\n"
            "    WHERE st.ID_WIKIDATA = loc.ID_WIKIDATA AND st.ID_PROPERTY = 'P31'\n"
            "      AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')\n"
            "    ORDER BY FIELD(lc.LOCATION_TYPE, " +
            ", ".join("'" + t + "'" for t, _ in LOCATION_TYPE_CONES) + ")\n"
            "    LIMIT 1)"))
        connection.commit()
        cursor.execute("SELECT COUNT(*) AS C FROM T_WC_T2S_LOCATION_BUILD WHERE LOCATION_TYPE IS NULL")
        arrresult["sans_type"] = int(cursor.fetchone()["C"])

        # --- 3. Les associations, deux ou trois selon les episodes ----------
        for strmajor in arrmajors:
            strtable = "T_WC_T2S_" + strmajor + "_LOCATION"
            _f_locationstep(cursor, "3a. DROP " + strtable + "_BUILD",
                            "DROP TABLE IF EXISTS " + strtable + "_BUILD")
            _f_locationstep(cursor, "3b. CREATE " + strtable + "_BUILD",
                            "CREATE TABLE " + strtable + "_BUILD LIKE " + strtable)
            connection.commit()
            arrresult[strmajor.lower()] = _f_locationstep(
                cursor, "3c. INSERT " + strtable + "_BUILD", f_locationassociationsql(strmajor))
            connection.commit()

        # --- 3d. La hierarchie (TMDB-MOVIE-PREPROCESS-048) -------------------
        # La table servie est creee ici si elle manque, comme T_WC_T2S_LOCATION_CLASS :
        # elle ne touche aucune table existante, une migration a part n'apporterait qu'une
        # etape de deploiement de plus. Elle bascule avec les autres a l'etape 5.
        _f_locationstep(cursor, "3d. CREATE " + STR_LOCATION_ANCESTOR_TABLE + " IF NOT EXISTS", (
            "CREATE TABLE IF NOT EXISTS " + STR_LOCATION_ANCESTOR_TABLE + " (\n"
            "  ID_ROW INT NOT NULL AUTO_INCREMENT,\n"
            "  ID_LOCATION INT NOT NULL,\n"
            "  ID_ANCESTOR INT NOT NULL,\n"
            "  DEPTH INT NOT NULL,\n"
            "  PRIMARY KEY (ID_ROW),\n"
            "  UNIQUE KEY UK_LOCATION_ANCESTOR (ID_LOCATION, ID_ANCESTOR),\n"
            "  KEY IDX_ANCESTOR_DEPTH (ID_ANCESTOR, DEPTH)\n"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"))
        _f_locationstep(cursor, "3d. DROP " + STR_LOCATION_ANCESTOR_TABLE + "_BUILD",
                        "DROP TABLE IF EXISTS " + STR_LOCATION_ANCESTOR_TABLE + "_BUILD")
        _f_locationstep(cursor, "3d. CREATE " + STR_LOCATION_ANCESTOR_TABLE + "_BUILD",
                        "CREATE TABLE " + STR_LOCATION_ANCESTOR_TABLE + "_BUILD LIKE " + STR_LOCATION_ANCESTOR_TABLE)
        connection.commit()
        arrresult["ancetres"], arrresult["ancetres_au_dessus"], arrresult["ancetres_profondeur"] = \
            _f_buildlocationancestor(cursor, connection)

        # --- 4. Compteurs et agregats ----------------------------------------
        # Les formules sont celles des collections et des mouvements : la note du lieu
        # est la moyenne des notes des oeuvres qui s'y rattachent, sa popularite la
        # moyenne de leur popularite. Un lieu sans oeuvre notee garde NULL plutot que
        # zero, un zero se trierait comme une mauvaise note alors qu'il signifie
        # l'absence de mesure. C'est le piege du sentinelle zero, paye trois fois dans
        # cette migration.
        _f_locationstep(cursor, "4a. UPDATE des compteurs", (
            "UPDATE T_WC_T2S_LOCATION_BUILD loc SET\n"
            "  loc.MOVIE_COUNT = (SELECT COUNT(DISTINCT ml.ID_MOVIE)\n"
            "                     FROM T_WC_T2S_MOVIE_LOCATION_BUILD ml\n"
            "                     WHERE ml.ID_LOCATION = loc.ID_LOCATION),\n"
            "  loc.SERIE_COUNT = (SELECT COUNT(DISTINCT sl.ID_SERIE)\n"
            "                     FROM T_WC_T2S_SERIE_LOCATION_BUILD sl\n"
            "                     WHERE sl.ID_LOCATION = loc.ID_LOCATION)"))
        connection.commit()
        if blnepisodes:
            _f_locationstep(cursor, "4a-bis. UPDATE de EPISODE_COUNT", (
                "UPDATE T_WC_T2S_LOCATION_BUILD loc SET\n"
                "  loc.EPISODE_COUNT = (SELECT COUNT(DISTINCT el.ID_EPISODE)\n"
                "                       FROM T_WC_T2S_EPISODE_LOCATION_BUILD el\n"
                "                       WHERE el.ID_LOCATION = loc.ID_LOCATION)"))
            connection.commit()
        # 4b, les notes et la popularite (TMDB-MOVIE-PREPROCESS-064) : films ET series,
        # une voix par oeuvre. Jusqu'en 1.1.19 la moyenne ne lisait que les films, et
        # chaque ligne d'association : un film tourne ET situe au meme lieu comptait deux
        # fois, et les 2 117 lieux que seules des series citent restaient sans note. Les
        # oeuvres passent par une table temporaire cle (lieu, type, oeuvre), ou l'INSERT
        # IGNORE retire le second role ; MariaDB refuse une reference exterieure dans une
        # table derivee, d'ou l'UPDATE par jointure plutot que des sous-requetes correlees.
        # Les episodes n'y entrent pas : leur serie y est deja, ou y entrera par ses
        # propres liens. Le zero reste exclu (CASE ... > 0), NULL reste « pas de mesure ».
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_lieu_oeuvre")
        cursor.execute("CREATE TEMPORARY TABLE tmp_lieu_oeuvre (\n"
                       "  ID_LOCATION INT NOT NULL, KIND CHAR(1) NOT NULL, ID_WORK INT NOT NULL,\n"
                       "  IMDB_RATING DOUBLE NULL, IMDB_RATING_WEIGHTED DOUBLE NULL, POPULARITY DOUBLE NULL,\n"
                       "  PRIMARY KEY (ID_LOCATION, KIND, ID_WORK)\n"
                       ") ENGINE=InnoDB")
        _f_locationstep(cursor, "4b-1. INSERT des films dans tmp_lieu_oeuvre", (
            "INSERT IGNORE INTO tmp_lieu_oeuvre\n"
            "SELECT ml.ID_LOCATION, 'm', m.ID_MOVIE, m.IMDB_RATING, m.IMDB_RATING_WEIGHTED, m.POPULARITY\n"
            "FROM T_WC_T2S_MOVIE_LOCATION_BUILD ml\n"
            "JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = ml.ID_MOVIE"))
        _f_locationstep(cursor, "4b-2. INSERT des series dans tmp_lieu_oeuvre", (
            "INSERT IGNORE INTO tmp_lieu_oeuvre\n"
            "SELECT sl.ID_LOCATION, 's', s.ID_SERIE, s.IMDB_RATING, s.IMDB_RATING_WEIGHTED, s.POPULARITY\n"
            "FROM T_WC_T2S_SERIE_LOCATION_BUILD sl\n"
            "JOIN T_WC_T2S_SERIE s ON s.ID_SERIE = sl.ID_SERIE"))
        _f_locationstep(cursor, "4b-3. UPDATE des notes et de la popularite (films et series)", (
            "UPDATE T_WC_T2S_LOCATION_BUILD loc\n"
            "LEFT JOIN (\n"
            "    SELECT ID_LOCATION,\n"
            "           AVG(CASE WHEN IMDB_RATING > 0 THEN IMDB_RATING END) AS R,\n"
            "           AVG(CASE WHEN IMDB_RATING_WEIGHTED > 0 THEN IMDB_RATING_WEIGHTED END) AS RW,\n"
            "           AVG(CASE WHEN POPULARITY > 0 THEN POPULARITY END) AS P\n"
            "    FROM tmp_lieu_oeuvre GROUP BY ID_LOCATION) agg ON agg.ID_LOCATION = loc.ID_LOCATION\n"
            "SET loc.IMDB_RATING = agg.R, loc.IMDB_RATING_WEIGHTED = agg.RW, loc.POPULARITY = agg.P"))
        connection.commit()
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_lieu_oeuvre")
        cursor.execute("SELECT SUM(LOCATION_NAME IS NULL OR LOCATION_NAME = '') AS N, "
                       "SUM(OVERVIEW_FR IS NOT NULL AND OVERVIEW_FR <> '') AS F "
                       "FROM T_WC_T2S_LOCATION_BUILD WHERE DELETED = 0")
        row = cursor.fetchone()
        arrresult["sans_nom"] = int(row["N"] or 0)
        arrresult["avec_overview_fr"] = int(row["F"] or 0)

        # --- 4c. L'image du lieu (TMDB-MOVIE-PREPROCESS-065) ----------------
        # tmdb-front et voice-agent lisent POSTER_PATH puis WIKIPEDIA_IMAGE_PATH, comme pour
        # les mouvements ou les collections, que f_getwikidataimagepath() remplit avec
        # l'image principale de la page Wikipedia. Ici personne ne l'ecrivait : le
        # 2026-10-18, 0 lieu sur 12 977 l'avait, alors que 9 883 portaient l'URL anglaise
        # dans WIKIPEDIA_MAIN_IMAGE_URL (processus 71), Los Angeles le panneau Hollywood.
        # Meme source que 71, T_WC_WIKIPEDIA_PAGE_LANG, lue directement parce que 72 passe
        # AVANT 71 : un lieu nouveau a son image des sa premiere nuit.
        # L'anglais d'abord, le francais a defaut (555 lieux n'ont que lui, Londres entre
        # autres, dont l'image anglaise a ete effacee comme bandeau partage). '' est une
        # reponse (« pas d'image en tete », WIKIPEDIA-CRAWLER-019) et fait passer a l'autre
        # langue ; NULL dans les deux langues est une inconnue (crawl rate) et garde la
        # valeur recopiee de la veille par 1c-i.
        # ⚠ La source est en varchar(1000), la cible en varchar(500) : le premier passage,
        # le 2026-10-18, est tombe sur « Data too long for column » et 72 a tout annule.
        # Une URL de plus de 500 caracteres est sautee (l'autre langue, sinon rien) plutot
        # que tronquee : une URL coupee est une image cassee a l'ecran.
        _f_locationstep(cursor, "4c. UPDATE de WIKIPEDIA_IMAGE_PATH", (
            "UPDATE T_WC_T2S_LOCATION_BUILD loc\n"
            "LEFT JOIN T_WC_WIKIPEDIA_PAGE_LANG pen ON pen.ID_WIKIDATA = loc.ID_WIKIDATA AND pen.LANG = 'en'\n"
            "LEFT JOIN T_WC_WIKIPEDIA_PAGE_LANG pfr ON pfr.ID_WIKIDATA = loc.ID_WIKIDATA AND pfr.LANG = 'fr'\n"
            "SET loc.WIKIPEDIA_IMAGE_PATH = CASE\n"
            "    WHEN pen.MAIN_IMAGE_URL IS NULL AND pfr.MAIN_IMAGE_URL IS NULL THEN loc.WIKIPEDIA_IMAGE_PATH\n"
            "    ELSE COALESCE(IF(CHAR_LENGTH(pen.MAIN_IMAGE_URL) BETWEEN 1 AND 500, pen.MAIN_IMAGE_URL, NULL),\n"
            "                  IF(CHAR_LENGTH(pfr.MAIN_IMAGE_URL) BETWEEN 1 AND 500, pfr.MAIN_IMAGE_URL, NULL)) END"))
        connection.commit()
        cursor.execute("SELECT COUNT(*) AS C FROM T_WC_T2S_LOCATION_BUILD "
                       "WHERE DELETED = 0 AND WIKIPEDIA_IMAGE_PATH IS NOT NULL AND WIKIPEDIA_IMAGE_PATH <> ''")
        arrresult["avec_image"] = int(cursor.fetchone()["C"])

        # --- 5. Les bascules ------------------------------------------------
        # RENAME TABLE d'un seul tenant : MariaDB le traite comme une operation atomique,
        # donc aucune requete ne peut voir une table nouvelle et une autre ancienne.
        #
        # ⚠ C'EST L'ETAPE LA PLUS EXPOSEE AU VERROU, parce qu'elle demande un verrou de
        # metadonnees exclusif sur les tables que l'API lit en permanence. Une session
        # qui a laisse une transaction ouverte sur l'une d'elles, un onglet phpMyAdmin
        # par exemple, suffit a la faire echouer en 1205. Si c'est ici que le passage
        # meurt, chercher la transaction fautive plutot que rejouer.
        # Avec les episodes, quatre tables basculent dans le meme RENAME : un lieu nouveau
        # que seul un episode cite ne doit jamais etre servi sans son association.
        # La hierarchie bascule avec eux : un lieu nouveau ne doit pas etre servi sans sa
        # ligne de profondeur 0, sinon « les films a X » ne le trouverait pas (-048).
        arrswaptables = (["T_WC_T2S_LOCATION"] + ["T_WC_T2S_" + strmajor + "_LOCATION" for strmajor in arrmajors]
                         + [STR_LOCATION_ANCESTOR_TABLE])
        for strtable in arrswaptables:
            cursor.execute("DROP TABLE IF EXISTS " + strtable + "_OLD")
        connection.commit()
        _f_locationstep(cursor, f"5. RENAME des {len(arrswaptables)} tables", (
            "RENAME TABLE\n" +
            ",\n".join(f"  {strtable} TO {strtable}_OLD,\n  {strtable}_BUILD TO {strtable}"
                       for strtable in arrswaptables)))
        connection.commit()
        for strtable in arrswaptables:
            cursor.execute("DROP TABLE IF EXISTS " + strtable + "_OLD")
        connection.commit()
        return arrresult
    finally:
        cursor.close()


# ===========================================================================
# T2S_SOURCE_WORK : "based on" (Wikidata P144), Process 73
# TMDB-MOVIE-PREPROCESS-055, shape decided 2026-10-07 and 2026-10-09.
# ===========================================================================
#
# The cones that type a source work, in PRIORITY ORDER. The rank of an entry is its
# index: a class reached by several cones keeps the first one (INSERT IGNORE on the
# class key), and a source carrying several classes takes its smallest rank. The rank
# therefore encodes both the type priority and, inside a type, the form precedence
# ('musical' before 'play': an opera is also a dramatic work).
#
# Every root is a P31 class MEASURED on the sources (doc/sql/test-055-*.sql, runs of
# 2026-10-07), plus Q95074 (CHARACTER_ROOTS of wikidata-crawler), Q11424 (film) and
# Q5398426 (television series). Deliberately NOT roots: Q17537576 "creative work" and
# Q1190554 "occurrence", top classes whose cones (2.6 million classes for the second)
# would decide nothing. Rule from that run: read a cone's size before adding a root.
#
# Order, measured on run 2: what is not a work first (character, franchise: 'other',
# decision of 2026-10-07), then game, comic, stage, folklore, screen, and literary
# LAST, because "written work" (53,962 classes) and "literary work" (6,116) are the
# broadest cones and would otherwise swallow manga, plays and visual novels. A cached
# source no cone catches goes to 'other' (112 sources on run 2: real events, brands,
# toys, bands).
SOURCE_WORK_TYPE_CONES = (
    ("other", None, ("Q95074",       # character
                     "Q15632617",    # fictional human
                     "Q15773347",    # film character
                     "Q1114461",     # comics character
                     "Q15773317",    # television character
                     "Q15711870",    # animated character
                     "Q3658341",     # literary character
                     "Q14514600",    # group of fictional characters
                     "Q196600")),    # media franchise
    ("game", None, ("Q7889",         # video game
                    "Q7058673",      # video game series
                    "Q112144412",    # esports discipline
                    "Q1643932",      # tabletop role-playing game
                    "Q131436",       # board game
                    "Q734698",       # collectible card game
                    "Q116774927",    # (unlabelled) the Pokemon game pairs
                    "Q116774997")),  # (unlabelled) Pokemon remakes
    ("comic", "manga", ("Q21198342",   # manga series
                        "Q865484")),   # yonkoma
    ("comic", None, ("Q14406742",    # comic book series
                     "Q838795",      # comic strip
                     "Q1004",        # comic
                     "Q1760610",     # comic book
                     "Q2831984",     # comic book album
                     "Q725377",      # graphic novel
                     "Q3297186",     # limited series
                     "Q115378877",   # comic book storyline
                     "Q7978994",     # webtoon
                     "Q213369",      # webcomic
                     "Q74262765",    # manhwa series
                     "Q137637896")), # manhua series
    ("stage", "musical", ("Q58483083",)),  # dramatico-musical work (musical, opera, ballet)
    ("stage", "play", ("Q116476516",   # dramatic work
                       "Q25372")),     # drama
    ("stage", None, ("Q7777570",     # theatrical production
                     "Q110013395")), # theatrical work
    ("folklore", None, ("Q699",      # fairy tale
                        "Q1221280",  # folk tale
                        "Q47451145", # tale type
                        "Q4400636",  # Russian folktale
                        "Q19718870")),  # mythology by ethnic group
    ("screen", None, ("Q24856",      # film series
                      "Q11424",      # film
                      "Q5398426",    # television series
                      "Q21191270",   # television series episode
                      "Q3464665")),  # television series season
    ("literary", "light_novel", ("Q104213567",)),  # light novel series
    ("literary", None, ("Q7725634",  # literary work
                        "Q47461344", # written work
                        "Q571",      # book
                        "Q277759",   # book series
                        "Q1667921",  # novel series
                        "Q13593966", # literary trilogy
                        "Q867335",   # literary cycle
                        "Q12799318", # short novel
                        "Q59126",    # xiaoshuo
                        "Q8275050",  # children's book
                        "Q47068459", # children's book series
                        "Q3331189",  # version, edition or translation
                        "Q29154430")),  # book of the Bible
)
# 'novel' / 'short_story' come from P7937 (form of creative work) once wikidata-crawler
# caches it for items (WIKIDATA-CRAWLER-021, code shipped 2026-10-09). The mapping of its
# values is NOT written here: the form classes are to be measured on the first run that
# carries P7937, not named from memory.

STR_SOURCE_WORK_CLASS_TABLE = "T_WC_T2S_SOURCE_WORK_CLASS"


def f_buildsourceworkclasstable():
    """Materialise the P279 cones of SOURCE_WORK_TYPE_CONES, one row per class.

    Same mechanism as f_buildlocationclasstable(): the transitive closure is walked
    once per pass, not once per row, and the priority is resolved at construction
    (INSERT IGNORE on the class key, first cone wins). CONE_RANK carries the index of
    the winning cone, so a source with several classes picks MIN(CONE_RANK).

    The CAST of the anchor is not decorative: without it MariaDB types the recursive
    column on the literal's length and rejects its own Q-ids with error 1406.

    Returns the number of classes per cone rank.
    """
    connection = cp.connectioncp
    cursor = connection.cursor()
    arrcounts = {}
    try:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS `" + STR_SOURCE_WORK_CLASS_TABLE + "` ("
            "`ID_CLASS` VARCHAR(50) NOT NULL,"
            "`CONE_RANK` INT NOT NULL,"
            "`SOURCE_WORK_TYPE` VARCHAR(20) NOT NULL,"
            "`SOURCE_WORK_FORM` VARCHAR(20) NULL,"
            "`DAT_CREAT` DATETIME DEFAULT CURRENT_TIMESTAMP,"
            "PRIMARY KEY (`ID_CLASS`),"
            "KEY `CONE_RANK` (`CONE_RANK`)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
        cursor.execute("TRUNCATE TABLE `" + STR_SOURCE_WORK_CLASS_TABLE + "`")
        for lngrank, (strtype, strform, arrroots) in enumerate(SOURCE_WORK_TYPE_CONES):
            strroots = " UNION ALL ".join(
                "SELECT '" + strroot + "' AS qid" for strroot in arrroots
            )
            cursor.execute(
                "INSERT IGNORE INTO `" + STR_SOURCE_WORK_CLASS_TABLE + "` "
                "(ID_CLASS, CONE_RANK, SOURCE_WORK_TYPE, SOURCE_WORK_FORM) "
                "WITH RECURSIVE cone_source_work (qid) AS ( "
                "SELECT CAST(r.qid AS CHAR(50)) COLLATE utf8mb4_unicode_ci AS qid "
                "FROM (" + strroots + ") AS r "
                "UNION "
                "SELECT sc.ID_CHILD FROM T_WC_WIKIDATA_SUBCLASS sc "
                "JOIN cone_source_work c ON c.qid = sc.ID_PARENT WHERE sc.DELETED = 0 "
                ") SELECT qid, %s, %s, %s FROM cone_source_work",
                (lngrank, strtype, strform),
            )
            connection.commit()
            cursor.execute(
                "SELECT COUNT(*) AS COMPTE FROM `" + STR_SOURCE_WORK_CLASS_TABLE + "` "
                "WHERE CONE_RANK = %s", (lngrank,)
            )
            strlabel = f"{lngrank}:{strtype}" + (f"/{strform}" if strform else "")
            arrcounts[strlabel] = int(cursor.fetchone()["COMPTE"])
        return arrcounts
    finally:
        cursor.close()


# The driving filter: a live P144 statement. 17,454 distinct sources for 25,222 links
# from T2S works on 2026-10-07.
STR_SOURCE_WORK_P144_FILTER = (
    "st.ID_PROPERTY = 'P144'\n"
    "  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')\n"
    "  AND COALESCE(st.DELETED, 0) = 0\n"
)


def _f_sourceworkstep(cursor, strstep, strsql):
    """Run one step of the source-work rebuild, NAMING it first (lesson of process 72,
    2026-09-12: a rebuild that fails without saying which statement is a rebuild to
    diagnose from zero). Returns the row count."""
    print(f"73:   {strstep} ...", flush=True)
    cursor.execute(strsql)
    return cursor.rowcount


def f_sourceworkassociationsql(strmajor):
    """Fill one link table, film or series. The two queries come from one template so
    that a correction cannot reach one and miss the other.

    A work is never linked to itself (a source row pointing back to the same T2S work):
    Wikidata holds a few such loops, and they would read as "X is based on X".
    """
    strid = "ID_" + strmajor
    return (
        f"INSERT IGNORE INTO T_WC_T2S_{strmajor}_SOURCE_WORK_BUILD\n"
        f"    ({strid}, ID_SOURCE_WORK, DELETED, DAT_CREAT, TIM_UPDATED)\n"
        f"SELECT DISTINCT w.{strid}, sw.ID_SOURCE_WORK, 0, CURDATE(), NOW()\n"
        "FROM T_WC_WIKIDATA_STATEMENT st\n"
        "INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT\n"
        f"INNER JOIN T_WC_T2S_{strmajor} w ON w.ID_WIKIDATA = st.ID_WIKIDATA\n"
        "INNER JOIN T_WC_T2S_SOURCE_WORK_BUILD sw ON sw.ID_WIKIDATA = iv.ID_ITEM AND sw.DELETED = 0\n"
        "WHERE " + STR_SOURCE_WORK_P144_FILTER +
        f"  AND NOT (sw.{strid} <=> w.{strid})\n"
    )


def _f_readwithoutlocking(cursor, strtag):
    """READ COMMITTED for this session when no binary log needs the source rows locked.

    Same reasoning as process 72 (2026-09-12): an INSERT ... SELECT on
    T_WC_WIKIDATA_STATEMENT takes shared locks on the source under REPEATABLE READ and
    collides with a wikidata-crawler load. READ COMMITTED is safe only without a binary
    log to replay (or with binlog_format = ROW), so the condition is checked at run time
    rather than assumed, and the decision is printed.
    """
    try:
        cursor.execute("SELECT @@GLOBAL.log_bin AS LOGBIN, @@GLOBAL.binlog_format AS FORMAT")
        arrbinlog = cursor.fetchone()
        blnbinlog = int(arrbinlog["LOGBIN"] or 0) == 1
        strbinlogformat = (arrbinlog["FORMAT"] or "").upper()
        if (not blnbinlog) or strbinlogformat == "ROW":
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            print(f"{strtag}: READ COMMITTED, the INSERT ... SELECT will not lock the Wikidata source rows.")
        else:
            print(f"{strtag}: left in REPEATABLE READ (binlog_format = {strbinlogformat}): "
                  "do NOT run during a wikidata-crawler load.")
    except Exception as isolationerror:
        print(f"{strtag}: isolation left at default ({type(isolationerror).__name__}: {isolationerror}).")


def f_buildsourceworktables():
    """Rebuild T_WC_T2S_SOURCE_WORK and its two link tables, in full, in SQL only.

    Same pattern as f_buildlocationtables(), on the same reasoning: about 17,500 sources
    and 25,000 links, small enough for a full rebuild in _BUILD tables swapped by one
    atomic RENAME, so the served tables are never empty.

    THE SHAPE (method A, 2026-10-09). The left side, the work studied, is carried by the
    link table (MOVIE or SERIE). The right side is always a SOURCE_WORK row; when the
    source is itself a T2S film or series, the row points to it by ID_MOVIE / ID_SERIE.

    ID_SOURCE_WORK IS STABLE, in three inserts as for ID_LOCATION (fix of 2026-09-13):
    a known source keeps its id; a source that left the perimeter stays with
    DELETED = 1 so that its number is never given again; a new one gets a number above
    the previous maximum.

    Returns a dictionary of counts, for the telemetry and the acceptance.
    """
    connection = cp.connectioncp
    cursor = connection.cursor()
    arrresult = {}
    try:
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 120")
        _f_readwithoutlocking(cursor, "73")

        # --- 0. The perimeter, and everything known about each source ---------
        # Materialised once in a temporary table, the driving query being the costly
        # part. Names, year and home are resolved by one UPDATE per question rather than
        # in a single join, whose duplicates (two T2S films sharing a Q-id) would break
        # the unique key of the build table.
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_source_work")
        cursor.execute(
            "CREATE TEMPORARY TABLE tmp_source_work ("
            " ID_WIKIDATA VARCHAR(20) NOT NULL PRIMARY KEY,"
            " ID_MOVIE INT NULL, ID_SERIE INT NULL,"
            " IS_CORE_SCREEN TINYINT NOT NULL DEFAULT 0,"
            " IS_PERSON_OR_CHARACTER TINYINT NOT NULL DEFAULT 0,"
            " SOURCE_NAME VARCHAR(250) NULL, SOURCE_NAME_FR VARCHAR(250) NULL,"
            " OVERVIEW MEDIUMTEXT NULL, SOURCE_YEAR INT NULL, CONE_RANK INT NULL"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci")
        arrresult["perimeter"] = _f_sourceworkstep(cursor, "0a. INSERT the perimeter (tmp_source_work)", (
            "INSERT IGNORE INTO tmp_source_work (ID_WIKIDATA)\n"
            "SELECT DISTINCT iv.ID_ITEM\n"
            "FROM T_WC_WIKIDATA_STATEMENT st\n"
            "INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT\n"
            "WHERE " + STR_SOURCE_WORK_P144_FILTER +
            "  AND iv.ID_ITEM IS NOT NULL AND iv.ID_ITEM <> ''\n"
            "  AND ( EXISTS (SELECT 1 FROM T_WC_T2S_MOVIE m WHERE m.ID_WIKIDATA = st.ID_WIKIDATA)\n"
            "     OR EXISTS (SELECT 1 FROM T_WC_T2S_SERIE s WHERE s.ID_WIKIDATA = st.ID_WIKIDATA) )"))
        connection.commit()
        _f_sourceworkstep(cursor, "0b. UPDATE the T2S hops (ID_MOVIE, ID_SERIE)", (
            "UPDATE tmp_source_work s SET\n"
            "  s.ID_MOVIE = (SELECT MIN(m.ID_MOVIE) FROM T_WC_T2S_MOVIE m WHERE m.ID_WIKIDATA = s.ID_WIKIDATA),\n"
            "  s.ID_SERIE = (SELECT MIN(se.ID_SERIE) FROM T_WC_T2S_SERIE se WHERE se.ID_WIKIDATA = s.ID_WIKIDATA)"))
        _f_sourceworkstep(cursor, "0c. UPDATE the homes (core film or series, person, character)", (
            "UPDATE tmp_source_work s SET\n"
            "  s.IS_CORE_SCREEN = (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_MOVIE wm WHERE wm.ID_WIKIDATA = s.ID_WIKIDATA)\n"
            "                   OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_SERIE ws WHERE ws.ID_WIKIDATA = s.ID_WIKIDATA)),\n"
            "  s.IS_PERSON_OR_CHARACTER = (EXISTS (SELECT 1 FROM T_WC_WIKIDATA_PERSON wp WHERE wp.ID_WIKIDATA = s.ID_WIKIDATA)\n"
            "                           OR EXISTS (SELECT 1 FROM T_WC_WIKIDATA_CHARACTER wc WHERE wc.ID_WIKIDATA = s.ID_WIKIDATA))"))
        # The type: the smallest cone rank among the source's P31 classes. Computed BEFORE
        # the names and the year, because it decides whether the T2S hop is kept.
        _f_sourceworkstep(cursor, "0d. UPDATE the cone rank", (
            "UPDATE tmp_source_work s SET s.CONE_RANK = (\n"
            "    SELECT MIN(c.CONE_RANK)\n"
            "    FROM T_WC_WIKIDATA_STATEMENT st\n"
            "    INNER JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_STATEMENT = st.ID_STATEMENT\n"
            "    INNER JOIN `" + STR_SOURCE_WORK_CLASS_TABLE + "` c ON c.ID_CLASS = iv.ID_ITEM\n"
            "    WHERE st.ID_WIKIDATA = s.ID_WIKIDATA AND st.ID_PROPERTY = 'P31'\n"
            "      AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated'))"))
        # A T2S hop only for a source that IS a film or a series (fix of 2026-10-09).
        # Measured that day (doc/sql/test-055-diagnostics-20261009.txt): about 70 sources
        # had a T2S sheet while their own classes said manga, novel or play, and Wikidata
        # did not hold them as films or series. The T2S work carries the source's Q-id by
        # a matching error upstream (anime series on the manga's Q-id, National Theatre
        # Live recordings on the play's, the 2026 "Heart of Darkness" on Conrad's novel).
        # Trusting the hop typed the novel 'screen' with the year 2026. The hop is
        # therefore kept only when the source is a core film or series, or when its own
        # classes reach the screen cone, or reach no cone at all; otherwise the classes
        # decide the type and P577 the year, and the source keeps no T2S sheet. The
        # upstream error itself is TMDB-MOVIE-PREPROCESS-056.
        lngscreenrank = next(r for r, (t, f, _) in enumerate(SOURCE_WORK_TYPE_CONES) if t == "screen")
        arrresult["unhooked"] = _f_sourceworkstep(cursor, "0e. UPDATE drop the T2S hop of sources that are not films or series", (
            "UPDATE tmp_source_work s SET s.ID_MOVIE = NULL, s.ID_SERIE = NULL\n"
            "WHERE (s.ID_MOVIE IS NOT NULL OR s.ID_SERIE IS NOT NULL)\n"
            "  AND s.IS_CORE_SCREEN = 0\n"
            f"  AND s.CONE_RANK IS NOT NULL AND s.CONE_RANK <> {lngscreenrank}"))
        # Names: the T2S title first for a T2S work (the name the base already shows),
        # then the English label wherever the entity lives, then a fallback in an ordered
        # list of languages (fix of 2026-10-09: 1,718 of the 1,743 unnamed sources had a
        # label in another language, but the first key of LABELS_JSON gave Chinese for Jane
        # Eyre and Arabic for Street Fighter).
        #
        # LATIN-SCRIPT LANGUAGES ONLY, and no "any language" step (second fix of
        # 2026-10-09). The first version ended on the first key of LABELS_JSON, which
        # named The Last of Us game in Persian, in English and French alike. A name the
        # user cannot read is worse than an honest NULL: the count of unnamed sources is
        # printed by the pass, and the real gap (cached items with dozens of labels but
        # neither en nor fr) is WIKIDATA-CRAWLER-026.
        # 'mul' comes first: Wikidata's multilingual default label, used for names that
        # are the same across languages; an item labelled in 'mul' may carry no 'en' at
        # all. Same in the French name, right after 'fr'.
        arrlabelfallback = ("mul", "en-gb", "en-us", "en-ca", "fr", "de", "es", "it", "pt", "nl",
                            "sv", "nb", "nn", "da", "fi", "is", "pl", "cs", "sk", "sl", "hr",
                            "ro", "hu", "et", "lv", "lt", "ca", "gl", "eu", "ga", "cy", "sq",
                            "tr", "id", "ms", "vi", "tl", "sw", "af", "eo", "la")
        strfallback = ",\n".join(
            f"      JSON_UNQUOTE(JSON_EXTRACT({a}.LABELS_JSON, '$.\"{l}\"'))"
            for a in ("wi", "wm", "ws") for l in arrlabelfallback)
        _f_sourceworkstep(cursor, "0f. UPDATE names and description", (
            "UPDATE tmp_source_work s\n"
            "LEFT JOIN T_WC_T2S_MOVIE tm ON tm.ID_MOVIE = s.ID_MOVIE\n"
            "LEFT JOIN T_WC_T2S_SERIE ts ON ts.ID_SERIE = s.ID_SERIE\n"
            "LEFT JOIN T_WC_WIKIDATA_ITEM wi ON wi.ID_WIKIDATA = s.ID_WIKIDATA\n"
            "LEFT JOIN T_WC_WIKIDATA_MOVIE wm ON wm.ID_WIKIDATA = s.ID_WIKIDATA\n"
            "LEFT JOIN T_WC_WIKIDATA_SERIE ws ON ws.ID_WIKIDATA = s.ID_WIKIDATA\n"
            "LEFT JOIN T_WC_WIKIDATA_PERSON wp ON wp.ID_WIKIDATA = s.ID_WIKIDATA\n"
            "LEFT JOIN T_WC_WIKIDATA_CHARACTER wc ON wc.ID_WIKIDATA = s.ID_WIKIDATA\n"
            "SET s.SOURCE_NAME = LEFT(COALESCE(NULLIF(tm.MOVIE_TITLE, ''), NULLIF(ts.SERIE_TITLE, ''),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.en')), NULLIF(wi.LABEL_EN, ''),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wm.LABELS_JSON, '$.en')), NULLIF(wm.LABEL_EN, ''),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(ws.LABELS_JSON, '$.en')), NULLIF(ws.LABEL_EN, ''),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wp.LABELS_JSON, '$.en')), NULLIF(wp.LABEL_EN, ''),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wc.LABELS_JSON, '$.en')), NULLIF(wc.LABEL_EN, ''),\n"
            + strfallback + "), 250),\n"
            "    s.SOURCE_NAME_FR = LEFT(COALESCE(NULLIF(tm.MOVIE_TITLE_FR, ''), NULLIF(ts.SERIE_TITLE_FR, ''),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.fr')),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wm.LABELS_JSON, '$.fr')),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(ws.LABELS_JSON, '$.fr')),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wp.LABELS_JSON, '$.fr')),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wc.LABELS_JSON, '$.fr')),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wi.LABELS_JSON, '$.mul')),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wm.LABELS_JSON, '$.mul')),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(ws.LABELS_JSON, '$.mul'))), 250),\n"
            "    s.OVERVIEW = COALESCE(\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wi.DESCRIPTIONS_JSON, '$.en')), NULLIF(wi.DESCRIPTION_EN, ''),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wm.DESCRIPTIONS_JSON, '$.en')), NULLIF(wm.DESCRIPTION_EN, ''),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(ws.DESCRIPTIONS_JSON, '$.en')), NULLIF(ws.DESCRIPTION_EN, ''),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wp.DESCRIPTIONS_JSON, '$.en')), NULLIF(wp.DESCRIPTION_EN, ''),\n"
            "      JSON_UNQUOTE(JSON_EXTRACT(wc.DESCRIPTIONS_JSON, '$.en')), NULLIF(wc.DESCRIPTION_EN, ''))"))
        # The year: the T2S year for a T2S work, else the earliest live P577.
        _f_sourceworkstep(cursor, "0g. UPDATE the year", (
            "UPDATE tmp_source_work s\n"
            "LEFT JOIN T_WC_T2S_MOVIE tm ON tm.ID_MOVIE = s.ID_MOVIE\n"
            "LEFT JOIN T_WC_T2S_SERIE ts ON ts.ID_SERIE = s.ID_SERIE\n"
            "SET s.SOURCE_YEAR = COALESCE(tm.RELEASE_YEAR, ts.FIRST_AIR_YEAR, (\n"
            "    SELECT MIN(tv.YEAR_VALUE)\n"
            "    FROM T_WC_WIKIDATA_STATEMENT st\n"
            "    INNER JOIN T_WC_WIKIDATA_TIME_VALUE tv ON tv.ID_STATEMENT = st.ID_STATEMENT\n"
            "    WHERE st.ID_WIKIDATA = s.ID_WIKIDATA AND st.ID_PROPERTY = 'P577'\n"
            "      AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')\n"
            "      AND tv.YEAR_VALUE BETWEEN -5000 AND 2200))"))
        connection.commit()

        # The type and form as SQL CASE expressions generated from the cone list, so the
        # mapping rank -> (type, form) lives in one place. Precedence: a film or series
        # (T2S or core) is 'screen' whatever its classes; a person or a core character is
        # 'other'; then the cone; nothing caught gives 'other'.
        strtypecase = " ".join(f"WHEN {r} THEN '{t}'" for r, (t, f, _) in enumerate(SOURCE_WORK_TYPE_CONES))
        strformcase = " ".join(f"WHEN {r} THEN '{f}'" for r, (t, f, _) in enumerate(SOURCE_WORK_TYPE_CONES) if f)
        strscreen = "s.ID_MOVIE IS NOT NULL OR s.ID_SERIE IS NOT NULL OR s.IS_CORE_SCREEN = 1"
        strtypeexpr = (
            "CASE WHEN " + strscreen + " THEN 'screen'\n"
            "     WHEN s.IS_PERSON_OR_CHARACTER = 1 THEN 'other'\n"
            "     ELSE COALESCE(CASE s.CONE_RANK " + strtypecase + " END, 'other') END")
        strformexpr = (
            "CASE WHEN " + strscreen + " OR s.IS_PERSON_OR_CHARACTER = 1 THEN NULL\n"
            "     ELSE CASE s.CONE_RANK " + strformcase + " ELSE NULL END END")

        # --- 1. The entity, in three inserts so that ID_SOURCE_WORK never moves ---
        _f_sourceworkstep(cursor, "1a. DROP T_WC_T2S_SOURCE_WORK_BUILD",
                          "DROP TABLE IF EXISTS T_WC_T2S_SOURCE_WORK_BUILD")
        _f_sourceworkstep(cursor, "1b. CREATE T_WC_T2S_SOURCE_WORK_BUILD",
                          "CREATE TABLE T_WC_T2S_SOURCE_WORK_BUILD LIKE T_WC_T2S_SOURCE_WORK")
        connection.commit()
        strcolumns = ("ID_WIKIDATA, SOURCE_WORK_NAME, SOURCE_WORK_NAME_FR, OVERVIEW, SOURCE_WORK_TYPE,\n"
                      "     SOURCE_WORK_FORM, SOURCE_WORK_YEAR, ID_MOVIE, ID_SERIE, SOURCE_WORK_SOURCE, DELETED,\n"
                      "     TIM_UPDATED")
        strvalues = ("s.ID_WIKIDATA, s.SOURCE_NAME, COALESCE(s.SOURCE_NAME_FR, s.SOURCE_NAME), s.OVERVIEW,\n"
                     "       " + strtypeexpr + ",\n"
                     "       " + strformexpr + ",\n"
                     "       s.SOURCE_YEAR, s.ID_MOVIE, s.ID_SERIE, 'wikidata', 0, NOW()")
        # (i) known sources keep their id, creation date and images. The images are
        # rewritten by process 71; carrying them over avoids a night without images when
        # this process runs alone, in the source-works scope.
        arrresult["kept"] = _f_sourceworkstep(cursor, "1c-i. INSERT the known sources (ID_SOURCE_WORK kept)", (
            "INSERT INTO T_WC_T2S_SOURCE_WORK_BUILD\n"
            "    (ID_SOURCE_WORK, " + strcolumns + ", DAT_CREAT,\n"
            "     WIKIPEDIA_MAIN_IMAGE_URL, WIKIPEDIA_MAIN_IMAGE_URL_FR, WIKIPEDIA_IMAGE_PATH)\n"
            "SELECT old.ID_SOURCE_WORK, " + strvalues + ", COALESCE(old.DAT_CREAT, CURDATE()),\n"
            "       old.WIKIPEDIA_MAIN_IMAGE_URL, old.WIKIPEDIA_MAIN_IMAGE_URL_FR, old.WIKIPEDIA_IMAGE_PATH\n"
            "FROM tmp_source_work s\n"
            "INNER JOIN T_WC_T2S_SOURCE_WORK old ON old.ID_WIKIDATA = s.ID_WIKIDATA"))
        connection.commit()
        # (ii) sources that left the perimeter stay, DELETED = 1: their number is never
        # given again, and a consumer reads "deleted" rather than nothing. Columns are
        # named, never positional, so a column added later cannot shift them silently.
        strallcolumns = ("ID_SOURCE_WORK, ID_WIKIDATA, SOURCE_WORK_NAME, SOURCE_WORK_NAME_FR, OVERVIEW,\n"
                         "     SOURCE_WORK_TYPE, SOURCE_WORK_FORM, SOURCE_WORK_YEAR, ID_MOVIE, ID_SERIE,\n"
                         "     SOURCE_WORK_SOURCE, DISPLAY_ORDER, ID_CREATOR, DAT_CREAT, ID_OWNER, ID_USER_UPDATED,\n"
                         "     POSTER_PATH, WIKIPEDIA_IMAGE_PATH, IMDB_RATING, IMDB_RATING_WEIGHTED, POPULARITY,\n"
                         "     TIM_WIKIDATA_COMPLETED, WIKIPEDIA_MAIN_IMAGE_URL, WIKIPEDIA_MAIN_IMAGE_URL_FR")
        arrresult["deleted"] = _f_sourceworkstep(cursor, "1c-ii. INSERT the sources that left the perimeter (DELETED = 1)", (
            "INSERT INTO T_WC_T2S_SOURCE_WORK_BUILD\n"
            "    (" + strallcolumns + ", DELETED, TIM_UPDATED, MOVIE_COUNT, SERIE_COUNT)\n"
            "SELECT " + ", ".join("old." + c.strip() for c in strallcolumns.split(",")) + ",\n"
            "       1, CASE WHEN old.DELETED = 1 THEN old.TIM_UPDATED ELSE NOW() END, 0, 0\n"
            "FROM T_WC_T2S_SOURCE_WORK old\n"
            "WHERE NOT EXISTS (SELECT 1 FROM tmp_source_work s WHERE s.ID_WIKIDATA = old.ID_WIKIDATA)"))
        connection.commit()
        # (iii) new sources get a number above every number ever given: CREATE ... LIKE
        # does not carry the counter, and no row is ever removed, so MAX is the true high.
        cursor.execute("SELECT COALESCE(MAX(ID_SOURCE_WORK), 0) AS M FROM T_WC_T2S_SOURCE_WORK")
        lngnextid = int(cursor.fetchone()["M"]) + 1
        _f_sourceworkstep(cursor, f"1c-iii-0. ALTER AUTO_INCREMENT = {lngnextid}",
                          f"ALTER TABLE T_WC_T2S_SOURCE_WORK_BUILD AUTO_INCREMENT = {lngnextid}")
        arrresult["new"] = _f_sourceworkstep(cursor, "1c-iii. INSERT the new sources (ID_SOURCE_WORK assigned)", (
            "INSERT INTO T_WC_T2S_SOURCE_WORK_BUILD\n"
            "    (" + strcolumns + ", DAT_CREAT)\n"
            "SELECT " + strvalues + ", CURDATE()\n"
            "FROM tmp_source_work s\n"
            "LEFT JOIN T_WC_T2S_SOURCE_WORK old ON old.ID_WIKIDATA = s.ID_WIKIDATA\n"
            "WHERE old.ID_SOURCE_WORK IS NULL"))
        connection.commit()
        arrresult["sources"] = arrresult["kept"] + arrresult["new"]
        cursor.execute("SELECT COUNT(*) AS C FROM tmp_source_work s\n"
                       "WHERE NOT (" + strscreen + ") AND s.IS_PERSON_OR_CHARACTER = 0 AND s.CONE_RANK IS NULL")
        arrresult["other_by_default"] = int(cursor.fetchone()["C"])
        cursor.execute("SELECT COUNT(*) AS C FROM tmp_source_work WHERE SOURCE_NAME IS NULL")
        arrresult["without_name"] = int(cursor.fetchone()["C"])
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_source_work")
        print(f"73:   1c. {arrresult['kept']} sources kept, {arrresult['new']} new (from {lngnextid}), "
              f"{arrresult['deleted']} left the perimeter kept as DELETED = 1")

        # --- 2. The two link tables --------------------------------------------
        for strmajor in ("MOVIE", "SERIE"):
            strtable = "T_WC_T2S_" + strmajor + "_SOURCE_WORK"
            _f_sourceworkstep(cursor, "2a. DROP " + strtable + "_BUILD",
                              "DROP TABLE IF EXISTS " + strtable + "_BUILD")
            _f_sourceworkstep(cursor, "2b. CREATE " + strtable + "_BUILD",
                              "CREATE TABLE " + strtable + "_BUILD LIKE " + strtable)
            connection.commit()
            arrresult[strmajor.lower()] = _f_sourceworkstep(
                cursor, "2c. INSERT " + strtable + "_BUILD", f_sourceworkassociationsql(strmajor))
            connection.commit()

        # --- 3. Counts and aggregates --------------------------------------------
        _f_sourceworkstep(cursor, "3a. UPDATE the counts", (
            "UPDATE T_WC_T2S_SOURCE_WORK_BUILD sw SET\n"
            "  sw.MOVIE_COUNT = (SELECT COUNT(DISTINCT ms.ID_MOVIE) FROM T_WC_T2S_MOVIE_SOURCE_WORK_BUILD ms\n"
            "                    WHERE ms.ID_SOURCE_WORK = sw.ID_SOURCE_WORK),\n"
            "  sw.SERIE_COUNT = (SELECT COUNT(DISTINCT ss.ID_SERIE) FROM T_WC_T2S_SERIE_SOURCE_WORK_BUILD ss\n"
            "                    WHERE ss.ID_SOURCE_WORK = sw.ID_SOURCE_WORK)\n"
            "WHERE sw.DELETED = 0"))
        connection.commit()
        # A source that is a T2S work keeps its own rating and popularity; any other takes
        # the average over its adapted films, the convention of collections and locations.
        # NULL rather than zero when nothing is rated: a zero would sort as a bad rating
        # when it means no measure.
        strratingsql = ""
        for strcolumn in ("IMDB_RATING", "IMDB_RATING_WEIGHTED", "POPULARITY"):
            strratingsql += (
                ("" if not strratingsql else ",\n    ") +
                f"sw.{strcolumn} = COALESCE(NULLIF(tm.{strcolumn}, 0), NULLIF(ts.{strcolumn}, 0),\n"
                f"      (SELECT AVG(m.{strcolumn}) FROM T_WC_T2S_MOVIE_SOURCE_WORK_BUILD ms\n"
                "       JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = ms.ID_MOVIE\n"
                f"       WHERE ms.ID_SOURCE_WORK = sw.ID_SOURCE_WORK AND m.{strcolumn} > 0))")
        _f_sourceworkstep(cursor, "3b. UPDATE ratings and popularity", (
            "UPDATE T_WC_T2S_SOURCE_WORK_BUILD sw\n"
            "LEFT JOIN T_WC_T2S_MOVIE tm ON tm.ID_MOVIE = sw.ID_MOVIE\n"
            "LEFT JOIN T_WC_T2S_SERIE ts ON ts.ID_SERIE = sw.ID_SERIE\n"
            "SET " + strratingsql + "\n"
            "WHERE sw.DELETED = 0"))
        connection.commit()
        cursor.execute("SELECT SOURCE_WORK_TYPE AS T, COALESCE(SOURCE_WORK_FORM, '') AS F, COUNT(*) AS C\n"
                       "FROM T_WC_T2S_SOURCE_WORK_BUILD WHERE DELETED = 0 GROUP BY T, F ORDER BY C DESC")
        arrresult["types"] = {(row["T"] + ("/" + row["F"] if row["F"] else "")): int(row["C"])
                              for row in cursor.fetchall()}

        # --- 4. The swap, one atomic RENAME of the three tables ------------------
        # The step most exposed to a metadata lock: a session with an open transaction on
        # one of the served tables (a phpMyAdmin tab) is enough to fail it in 1205. If the
        # pass dies here, look for that transaction rather than replaying.
        arrtables = ("T_WC_T2S_SOURCE_WORK", "T_WC_T2S_MOVIE_SOURCE_WORK", "T_WC_T2S_SERIE_SOURCE_WORK")
        for strtable in arrtables:
            cursor.execute("DROP TABLE IF EXISTS " + strtable + "_OLD")
        connection.commit()
        _f_sourceworkstep(cursor, "4. RENAME the three tables", (
            "RENAME TABLE\n" + ",\n".join(
                f"  {t} TO {t}_OLD,\n  {t}_BUILD TO {t}" for t in arrtables)))
        connection.commit()
        for strtable in arrtables:
            cursor.execute("DROP TABLE IF EXISTS " + strtable + "_OLD")
        connection.commit()
        return arrresult
    finally:
        cursor.close()



# ============================================================================
# Process 74, T2S_GROUP_WORK: a group's works (TMDB-MOVIE-PREPROCESS-059)
# ============================================================================
#
# A group (process 43) holds members and nothing else. This process attaches films and
# series to it, in T_WC_T2S_GROUP_MOVIE and T_WC_T2S_GROUP_SERIE, from two signals:
#   - the members' credits: how many distinct members are credited on the work
#     (MEMBER_COUNT), and that count over the group's PERSON_COUNT (MEMBER_SHARE);
#   - the group's own Wikidata item credited on the work (IS_GROUP_CREDITED): The
#     Beatles as P175 performer of "A Hard Day's Night", Monty Python as P161 cast
#     member of "Life of Brian".
#
# THE RULE (decided by Philippe on 2026-10-11). A work attaches when the group itself
# is credited, OR when at least INT_GROUP_WORK_MIN_MEMBERS members are credited AND
# they are at least DBL_GROUP_WORK_MIN_SHARE of the group. Only attached rows are
# stored, with their counts, so the API can tighten the rule without a rebuild (it
# cannot loosen it, the rows below the threshold are not kept).
#
# THE GROUP FAMILIES. Only P463 (member of) and the custom groups. P108 (employer: a
# studio, a newspaper, a university) and P54 (sports team) make groups where two
# members sharing a film is coincidence; they are left out on purpose. The values are
# the ones process 43 writes in GROUP_SOURCE: 'P463', 'P108', 'P54', 'custom'.
#
# SERIES. A series' aggregate cast (T_WC_T2S_PERSON_SERIE) is known to hold the latest
# season only (TMDB-CRAWLER-033), so the season and episode credits are rolled up to
# their series (T_WC_T2S_PERSON_SEASON and T_WC_T2S_PERSON_EPISODE both carry ID_SERIE).
#
# SET-BASED, IN SQL ONLY. Temporary tables per major (scope, member credits, group
# credits, result, rank), then one upsert and one delete on the served table. No _BUILD
# and RENAME here: the table has no business id to keep stable (ID_ROW is a row
# number, the key is the pair), and an upsert keeps TIM_UPDATED meaningful.

INT_GROUP_WORK_MIN_MEMBERS = 2
DBL_GROUP_WORK_MIN_SHARE = 0.4

# The GROUP_SOURCE values kept, as process 43 writes them.
ARR_GROUP_WORK_SOURCES = ("P463", "custom")

# The Wikidata properties that credit an item on a work. P161 cast member, P175
# performer, P58 screenwriter, P86 composer, P170 creator, P162 producer, P57
# director, P725 voice actor.
ARR_GROUP_WORK_CREDIT_PROPERTIES = ("P161", "P175", "P58", "P86", "P170", "P162", "P57", "P725")

# CORRECTIONS AFTER THE FIRST RUN (2026-10-11, doc/sql/test-059-group-work-20261011.txt).
# 1. P175 performer is how Wikidata says a group's SONG is heard in a work: The Beatles
#    "performer" of One Crazy Night, with no Beatle in the credits (676 movies and 76
#    series were credited with no member at all). A P175 credit alone attaches nothing;
#    it counts only when at least one member is credited too. The other properties
#    (cast, writing, composing, directing, producing, creating, voicing) are taking part
#    and attach on their own: Laxmikant-Pyarelal, a composing duo TMDb often credits as
#    one person, keeps its films.
ARR_GROUP_WORK_PARTICIPATION_PROPERTIES = ("P161", "P58", "P86", "P170", "P162", "P57", "P725")
# 2. Archive footage is not taking part: a Beatles clip in an Ali documentary attached
#    the film to the band. TMDb writes it in the character ("Self (archive footage)").
STR_GROUP_WORK_ARCHIVE_PATTERN = "%archive%"
# 3. Talk, news and reality series gather guests episode after episode: three Pythons
#    over the years on Wogan or Conan attached the show to Monty Python. TMDb genre ids
#    10767 Talk, 10763 News, 10764 Reality: such a series never attaches by its members
#    (a group credited on it still does).
ARR_GROUP_WORK_EXCLUDED_SERIE_GENRES = (10767, 10763, 10764)

# One entry per major: the served table, the work id column, the TMDb table that
# carries ID_WIKIDATA, the T2S table that decides presence, its date column, and the
# member credit tables rolled up to the work.
ARR_GROUP_WORK_MAJORS = {
    "movie": {
        "target": "T_WC_T2S_GROUP_MOVIE",
        "id": "ID_MOVIE",
        "tmdb": "T_WC_TMDB_MOVIE",
        "t2s": "T_WC_T2S_MOVIE",
        "date": "DAT_RELEASE",
        "credits": ("T_WC_T2S_PERSON_MOVIE",),
    },
    "serie": {
        "target": "T_WC_T2S_GROUP_SERIE",
        "id": "ID_SERIE",
        "tmdb": "T_WC_TMDB_SERIE",
        "t2s": "T_WC_T2S_SERIE",
        "date": "DAT_FIRST_AIR",
        "credits": ("T_WC_T2S_PERSON_SERIE", "T_WC_T2S_PERSON_SEASON", "T_WC_T2S_PERSON_EPISODE"),
    },
}


def _f_groupworkstep(cursor, strstep, strsql, params=None):
    """Run one step of the group-work pass, NAMING it first (lesson of process 72:
    a failure that does not say which statement failed is diagnosed from zero).
    Returns the row count."""
    print(f"74:   {strstep} ...", flush=True)
    cursor.execute(strsql, params)
    return cursor.rowcount


def _f_groupworkcount(cursor, strsql):
    """One COUNT(*) AS COMPTE, as an int."""
    cursor.execute(strsql)
    arrrow = cursor.fetchone()
    return int((arrrow or {}).get("COMPTE") or 0)


def f_buildgroupworktables():
    """Rebuild T_WC_T2S_GROUP_MOVIE and T_WC_T2S_GROUP_SERIE from the members' credits
    and the group's own Wikidata credits. Set-based, idempotent, safe to re-run.

    Returns a dictionary of counts for the telemetry: 'groups' (groups in scope), and
    per major ('movie', 'serie') a dictionary with 'rows', 'created', 'updated',
    'deleted', 'credited' (IS_GROUP_CREDITED = 1 rows) and the step counts.
    """
    connection = cp.connectioncp
    cursor = connection.cursor()
    arrresult = {}
    strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    strtoday = strnow[:10]
    strsources = ", ".join(f"'{s}'" for s in ARR_GROUP_WORK_SOURCES)
    strproperties = ", ".join(f"'{p}'" for p in ARR_GROUP_WORK_CREDIT_PROPERTIES)
    strparticipation = ", ".join(f"'{p}'" for p in ARR_GROUP_WORK_PARTICIPATION_PROPERTIES)
    strexcludedgenres = ", ".join(str(i) for i in ARR_GROUP_WORK_EXCLUDED_SERIE_GENRES)
    strtablecharset = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    try:
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 120")
        # The group-credit step reads T_WC_WIKIDATA_STATEMENT in an INSERT ... SELECT:
        # same reasoning as processes 72 and 73.
        _f_readwithoutlocking(cursor, "74")

        # --- 0. The groups in scope ------------------------------------------------
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_group_work_scope")
        cursor.execute(
            "CREATE TEMPORARY TABLE tmp_group_work_scope ("
            " ID_GROUP INT NOT NULL PRIMARY KEY,"
            " ID_WIKIDATA VARCHAR(20) NULL,"
            " PERSON_COUNT INT NOT NULL,"
            " KEY ID_WIKIDATA (ID_WIKIDATA)"
            ") " + strtablecharset)
        arrresult["groups"] = _f_groupworkstep(cursor, "0. INSERT the groups in scope (tmp_group_work_scope)", (
            "INSERT IGNORE INTO tmp_group_work_scope (ID_GROUP, ID_WIKIDATA, PERSON_COUNT)\n"
            "SELECT g.ID_GROUP, g.ID_WIKIDATA, g.PERSON_COUNT\n"
            "FROM `T_WC_T2S_GROUP` g\n"
            f"WHERE g.GROUP_SOURCE IN ({strsources})\n"
            "  AND COALESCE(g.DELETED, 0) = 0\n"
            "  AND g.PERSON_COUNT >= 2"))
        connection.commit()

        for strmajor, arrmajor in ARR_GROUP_WORK_MAJORS.items():
            strtarget = arrmajor["target"]
            strid = arrmajor["id"]
            strtmdb = arrmajor["tmdb"]
            strt2s = arrmajor["t2s"]
            strdate = arrmajor["date"]
            arrcounts = {}
            cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess", f"Group works: {strmajor}", "Current sub process in the TMDb database movie preprocess", 0)

            # --- 1. The member credits, one row per (group, work, member, kind) ----
            # Driven from the small scope table, then PERSON_GROUP by ID_GROUP, then
            # the credit table by its ID_PERSON index (STRAIGHT_JOIN pins that order).
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_group_work_credit")
            cursor.execute(
                "CREATE TEMPORARY TABLE tmp_group_work_credit ("
                " ID_GROUP INT NOT NULL, ID_WORK INT NOT NULL, ID_PERSON INT NOT NULL,"
                " CREDIT_TYPE VARCHAR(10) NOT NULL,"
                " PRIMARY KEY (ID_GROUP, ID_WORK, ID_PERSON, CREDIT_TYPE)"
                ") " + strtablecharset)
            for strcredittable in arrmajor["credits"]:
                arrcounts["credits_" + strcredittable] = _f_groupworkstep(cursor, f"1. INSERT the member credits from {strcredittable}", (
                    "INSERT IGNORE INTO tmp_group_work_credit (ID_GROUP, ID_WORK, ID_PERSON, CREDIT_TYPE)\n"
                    f"SELECT g.ID_GROUP, c.{strid}, c.ID_PERSON, COALESCE(c.CREDIT_TYPE, '')\n"
                    "FROM tmp_group_work_scope g\n"
                    "STRAIGHT_JOIN T_WC_T2S_PERSON_GROUP pg ON pg.ID_GROUP = g.ID_GROUP\n"
                    f"STRAIGHT_JOIN {strcredittable} c ON c.ID_PERSON = pg.ID_PERSON\n"
                    "WHERE COALESCE(pg.DELETED, 0) = 0\n"
                    "  AND COALESCE(c.DELETED, 0) = 0\n"
                    "  AND COALESCE(c.CAST_CHARACTER, '') NOT LIKE %s"),
                    (STR_GROUP_WORK_ARCHIVE_PATTERN,))
                connection.commit()

            # --- 2. The group's own item credited on the work (Wikidata V2) --------
            # Driven from the group's Q-id: ITEM_VALUE by its ID_ITEM index, then the
            # statement by its primary key, then the work by ID_WIKIDATA (join plan of
            # groups-multi-repo-management.md section 9.17: the selective side drives).
            # Custom groups carry ID_CUSTOM_LIST in ID_WIKIDATA, not a Q-id: the REGEXP
            # leaves them out of this step, and they attach by their members only.
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_group_work_credited")
            cursor.execute(
                "CREATE TEMPORARY TABLE tmp_group_work_credited ("
                " ID_GROUP INT NOT NULL, ID_WORK INT NOT NULL,"
                " IS_PARTICIPATION INT NOT NULL DEFAULT 0,"
                " PRIMARY KEY (ID_GROUP, ID_WORK)"
                ") " + strtablecharset)
            arrcounts["credited_pairs"] = _f_groupworkstep(cursor, "2. INSERT the works crediting the group's own item", (
                "INSERT INTO tmp_group_work_credited (ID_GROUP, ID_WORK, IS_PARTICIPATION)\n"
                f"SELECT g.ID_GROUP, w.{strid}, MAX(st.ID_PROPERTY IN ({strparticipation}))\n"
                "FROM tmp_group_work_scope g\n"
                "STRAIGHT_JOIN T_WC_WIKIDATA_ITEM_VALUE iv ON iv.ID_ITEM = g.ID_WIKIDATA\n"
                "STRAIGHT_JOIN T_WC_WIKIDATA_STATEMENT st ON st.ID_STATEMENT = iv.ID_STATEMENT\n"
                f"STRAIGHT_JOIN {strtmdb} w ON w.ID_WIKIDATA = st.ID_WIKIDATA\n"
                f"STRAIGHT_JOIN {strt2s} t ON t.{strid} = w.{strid}\n"
                "WHERE g.ID_WIKIDATA REGEXP '^Q[0-9]+$'\n"
                f"  AND st.ID_PROPERTY IN ({strproperties})\n"
                "  AND (st.`RANK` IS NULL OR st.`RANK` <> 'deprecated')\n"
                "  AND COALESCE(st.DELETED, 0) = 0\n"
                f"GROUP BY g.ID_GROUP, w.{strid}"))
            connection.commit()

            # --- 3. The result: attached works only -------------------------------
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_group_work_result")
            cursor.execute(
                "CREATE TEMPORARY TABLE tmp_group_work_result ("
                " ID_GROUP INT NOT NULL, ID_WORK INT NOT NULL,"
                " MEMBER_COUNT INT NOT NULL DEFAULT 0,"
                " MEMBER_SHARE DOUBLE NOT NULL DEFAULT 0,"
                " IS_GROUP_CREDITED INT NOT NULL DEFAULT 0,"
                " CREDIT_KINDS VARCHAR(20) NULL,"
                " SORT_DATE DATE NULL,"
                " DISPLAY_ORDER INT NULL,"
                " PRIMARY KEY (ID_GROUP, ID_WORK)"
                ") " + strtablecharset)
            # The share is rounded to 4 decimals so that two passes compute the same
            # value and an unchanged row is not counted as updated. LEAST(1, ...) guards
            # a PERSON_COUNT that lags behind PERSON_GROUP.
            strkinds = ("NULLIF(CONCAT_WS(',',\n"
                        "    IF(MAX(c.CREDIT_TYPE = 'cast'), 'cast', NULL),\n"
                        "    IF(MAX(c.CREDIT_TYPE = 'crew'), 'crew', NULL)), '')")
            strshare = "ROUND(LEAST(1, COUNT(DISTINCT c.ID_PERSON) / g.PERSON_COUNT), 4)"
            # 3a. By the members: at least N members AND at least the share.
            arrcounts["by_members"] = _f_groupworkstep(cursor, "3a. INSERT the works attached by their members", (
                "INSERT INTO tmp_group_work_result (ID_GROUP, ID_WORK, MEMBER_COUNT, MEMBER_SHARE, IS_GROUP_CREDITED, CREDIT_KINDS)\n"
                f"SELECT c.ID_GROUP, c.ID_WORK, COUNT(DISTINCT c.ID_PERSON), {strshare}, 0,\n"
                f"  {strkinds}\n"
                "FROM tmp_group_work_credit c\n"
                "INNER JOIN tmp_group_work_scope g ON g.ID_GROUP = c.ID_GROUP\n"
                + ("WHERE NOT EXISTS (SELECT 1 FROM T_WC_T2S_SERIE_GENRE sg\n"
                   f"  WHERE sg.ID_SERIE = c.ID_WORK AND sg.ID_GENRE IN ({strexcludedgenres})\n"
                   "  AND COALESCE(sg.DELETED, 0) = 0)\n"
                   if strmajor == "serie" else "") +
                "GROUP BY c.ID_GROUP, c.ID_WORK, g.PERSON_COUNT\n"
                "HAVING COUNT(DISTINCT c.ID_PERSON) >= %s\n"
                "   AND COUNT(DISTINCT c.ID_PERSON) / g.PERSON_COUNT >= %s"),
                (INT_GROUP_WORK_MIN_MEMBERS, DBL_GROUP_WORK_MIN_SHARE))
            # 3b. By the group's own credit, whatever the member count (0 included).
            _f_groupworkstep(cursor, "3b. UPSERT the works crediting the group itself", (
                "INSERT INTO tmp_group_work_result (ID_GROUP, ID_WORK, MEMBER_COUNT, MEMBER_SHARE, IS_GROUP_CREDITED, CREDIT_KINDS)\n"
                f"SELECT cr.ID_GROUP, cr.ID_WORK, COUNT(DISTINCT c.ID_PERSON), {strshare}, 1,\n"
                f"  {strkinds}\n"
                "FROM tmp_group_work_credited cr\n"
                "INNER JOIN tmp_group_work_scope g ON g.ID_GROUP = cr.ID_GROUP\n"
                "LEFT JOIN tmp_group_work_credit c ON c.ID_GROUP = cr.ID_GROUP AND c.ID_WORK = cr.ID_WORK\n"
                "GROUP BY cr.ID_GROUP, cr.ID_WORK, g.PERSON_COUNT, cr.IS_PARTICIPATION\n"
                "HAVING cr.IS_PARTICIPATION = 1 OR COUNT(DISTINCT c.ID_PERSON) >= 1\n"
                "ON DUPLICATE KEY UPDATE IS_GROUP_CREDITED = 1"))
            # 3c. Only works present in the T2S read model (the credit tables already
            # are, but the check costs nothing and guards against a stale credit row).
            arrcounts["outside_t2s"] = _f_groupworkstep(cursor, f"3c. DELETE the works absent from {strt2s}", (
                "DELETE r FROM tmp_group_work_result r\n"
                f"LEFT JOIN {strt2s} t ON t.{strid} = r.ID_WORK\n"
                f"WHERE t.{strid} IS NULL"))
            _f_groupworkstep(cursor, f"3d. UPDATE the sort date from {strt2s}.{strdate}", (
                "UPDATE tmp_group_work_result r\n"
                f"INNER JOIN {strt2s} t ON t.{strid} = r.ID_WORK\n"
                f"SET r.SORT_DATE = t.{strdate}"))
            # 3e. DISPLAY_ORDER per group: the group credited first, then the most
            # members, then the oldest work (undated works last), the id as tie-break.
            # Computed in its own table: MariaDB does not take a window function in an
            # UPDATE, and a temporary table cannot be opened twice in one statement.
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_group_work_rank")
            cursor.execute(
                "CREATE TEMPORARY TABLE tmp_group_work_rank ("
                " ID_GROUP INT NOT NULL, ID_WORK INT NOT NULL, DISPLAY_ORDER INT NOT NULL,"
                " PRIMARY KEY (ID_GROUP, ID_WORK)"
                ") " + strtablecharset)
            _f_groupworkstep(cursor, "3e. INSERT the display order (tmp_group_work_rank)", (
                "INSERT INTO tmp_group_work_rank (ID_GROUP, ID_WORK, DISPLAY_ORDER)\n"
                "SELECT r.ID_GROUP, r.ID_WORK,\n"
                "  ROW_NUMBER() OVER (PARTITION BY r.ID_GROUP\n"
                "    ORDER BY r.IS_GROUP_CREDITED DESC, r.MEMBER_COUNT DESC,\n"
                "             (r.SORT_DATE IS NULL) ASC, r.SORT_DATE ASC, r.ID_WORK ASC)\n"
                "FROM tmp_group_work_result r"))
            _f_groupworkstep(cursor, "3f. UPDATE the display order", (
                "UPDATE tmp_group_work_result r\n"
                "INNER JOIN tmp_group_work_rank k ON k.ID_GROUP = r.ID_GROUP AND k.ID_WORK = r.ID_WORK\n"
                "SET r.DISPLAY_ORDER = k.DISPLAY_ORDER"))
            connection.commit()

            # --- 4. The counts, measured BEFORE the writes --------------------------
            # ON DUPLICATE KEY UPDATE reports 1 per insert and 2 per changed row in one
            # rowcount, which cannot be split afterwards; the three counts are read here.
            arrcounts["created"] = _f_groupworkcount(cursor, (
                "SELECT COUNT(*) AS COMPTE FROM tmp_group_work_result r\n"
                f"LEFT JOIN {strtarget} x ON x.ID_GROUP = r.ID_GROUP AND x.{strid} = r.ID_WORK\n"
                "WHERE x.ID_ROW IS NULL"))
            arrcounts["updated"] = _f_groupworkcount(cursor, (
                "SELECT COUNT(*) AS COMPTE FROM tmp_group_work_result r\n"
                f"INNER JOIN {strtarget} x ON x.ID_GROUP = r.ID_GROUP AND x.{strid} = r.ID_WORK\n"
                "WHERE NOT (x.MEMBER_COUNT <=> r.MEMBER_COUNT AND x.MEMBER_SHARE <=> r.MEMBER_SHARE\n"
                "     AND x.IS_GROUP_CREDITED <=> r.IS_GROUP_CREDITED AND x.CREDIT_KINDS <=> r.CREDIT_KINDS\n"
                "     AND x.DISPLAY_ORDER <=> r.DISPLAY_ORDER AND x.DELETED <=> 0)"))
            arrcounts["deleted"] = _f_groupworkcount(cursor, (
                f"SELECT COUNT(*) AS COMPTE FROM {strtarget} x\n"
                f"LEFT JOIN tmp_group_work_result r ON r.ID_GROUP = x.ID_GROUP AND r.ID_WORK = x.{strid}\n"
                "WHERE r.ID_GROUP IS NULL"))

            # --- 5. The writes: one upsert, one delete -----------------------------
            # TIM_UPDATED is assigned FIRST in the UPDATE clause, while the other
            # columns still hold their old values (MariaDB evaluates the assignments
            # left to right), so it moves only when the row really changed. Dates in
            # Paris time, passed from Python (DEPLOYMENT-015, TMDB-MOVIE-PREPROCESS-060).
            _f_groupworkstep(cursor, f"5a. UPSERT {strtarget}", (
                f"INSERT INTO {strtarget}\n"
                f"    (ID_GROUP, {strid}, MEMBER_COUNT, MEMBER_SHARE, IS_GROUP_CREDITED, CREDIT_KINDS,\n"
                "     DELETED, DISPLAY_ORDER, DAT_CREAT, TIM_UPDATED)\n"
                "SELECT r.ID_GROUP, r.ID_WORK, r.MEMBER_COUNT, r.MEMBER_SHARE, r.IS_GROUP_CREDITED, r.CREDIT_KINDS,\n"
                "       0, r.DISPLAY_ORDER, %s, %s\n"
                "FROM tmp_group_work_result r\n"
                "ON DUPLICATE KEY UPDATE\n"
                f"  TIM_UPDATED = IF({strtarget}.MEMBER_COUNT <=> VALUES(MEMBER_COUNT)\n"
                f"                   AND {strtarget}.MEMBER_SHARE <=> VALUES(MEMBER_SHARE)\n"
                f"                   AND {strtarget}.IS_GROUP_CREDITED <=> VALUES(IS_GROUP_CREDITED)\n"
                f"                   AND {strtarget}.CREDIT_KINDS <=> VALUES(CREDIT_KINDS)\n"
                f"                   AND {strtarget}.DISPLAY_ORDER <=> VALUES(DISPLAY_ORDER)\n"
                f"                   AND {strtarget}.DELETED <=> 0,\n"
                f"                   {strtarget}.TIM_UPDATED, VALUES(TIM_UPDATED)),\n"
                "  MEMBER_COUNT = VALUES(MEMBER_COUNT),\n"
                "  MEMBER_SHARE = VALUES(MEMBER_SHARE),\n"
                "  IS_GROUP_CREDITED = VALUES(IS_GROUP_CREDITED),\n"
                "  CREDIT_KINDS = VALUES(CREDIT_KINDS),\n"
                "  DISPLAY_ORDER = VALUES(DISPLAY_ORDER),\n"
                "  DELETED = 0,\n"
                f"  DAT_CREAT = COALESCE({strtarget}.DAT_CREAT, VALUES(DAT_CREAT))"),
                (strtoday, strnow))
            # A row that no longer attaches is removed, not flagged: the table stores
            # attached works only, and ID_ROW is a row number, not an id anyone keeps.
            # This also removes the works of groups that left the scope or vanished.
            _f_groupworkstep(cursor, f"5b. DELETE from {strtarget} the works no longer attached", (
                f"DELETE x FROM {strtarget} x\n"
                f"LEFT JOIN tmp_group_work_result r ON r.ID_GROUP = x.ID_GROUP AND r.ID_WORK = x.{strid}\n"
                "WHERE r.ID_GROUP IS NULL"))
            connection.commit()

            arrcounts["rows"] = _f_groupworkcount(cursor, f"SELECT COUNT(*) AS COMPTE FROM {strtarget}")
            arrcounts["credited"] = _f_groupworkcount(cursor, f"SELECT COUNT(*) AS COMPTE FROM {strtarget} WHERE IS_GROUP_CREDITED = 1")
            arrresult[strmajor] = arrcounts

        for strtemp in ("tmp_group_work_rank", "tmp_group_work_result", "tmp_group_work_credited",
                        "tmp_group_work_credit", "tmp_group_work_scope"):
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS " + strtemp)
        connection.commit()
        return arrresult
    finally:
        cursor.close()


# ---------------------------------------------------------------------------------------
# Process 75, T2S_WORK_FINANCE (TMDB-MOVIE-PREPROCESS-066): budgets (P2130) and box office
# (P2142) of the T2S movies and series, from the Wikidata V2 statements, one row per
# statement with its currency and qualifiers. Census of 2026-10-20
# (wikidata-crawler/doc/sql/check-029-external-id-census-20261020.txt): 7 213 budget and
# 7 858 box office statements on movies; the place is almost always P3005 (valid in place,
# 4 679 box office statements, Q13780930 = worldwide), the date P585 (797), the period P1264
# (41), the rank on a chart P1352 (247), the method P459 (1 529 budgets).
# Audit of 2026-10-21 (1.1.24): one row per statement AND work (a Q-id shared by two T2S
# works lost the second under INSERT IGNORE), the dates only when their parts are valid,
# each with its precision, the amount at the source scale, and N_PLACES / N_POINTS to show
# what the first-qualifier pick leaves out. Requests: doc/sql/check-066-audit-*.sql.
# ---------------------------------------------------------------------------------------

STR_WORK_FINANCE_TABLE = "T_WC_T2S_WORK_FINANCE"
STR_WORK_FINANCE_WORLDWIDE = "Q13780930"
# Place qualifiers, in order of preference when a statement carries several.
ARR_WORK_FINANCE_PLACE_PROPERTIES = ("P3005", "P17", "P1001", "P276", "P291")
# ISO 4217 codes of the most used currencies of the census; the others keep their Q-id only.
ARR_WORK_FINANCE_CURRENCY_CODES = {
    "Q4917": "USD", "Q4916": "EUR", "Q25224": "GBP", "Q8146": "JPY", "Q41044": "RUB",
    "Q1104069": "CAD", "Q259502": "AUD", "Q39099": "CNY",
}

STR_WORK_FINANCE_DDL = (
    "CREATE TABLE IF NOT EXISTS `" + STR_WORK_FINANCE_TABLE + "` (\n"
    "  `ID_STATEMENT` bigint(20) NOT NULL,\n"
    "  `ITEM_TYPE` varchar(10) NOT NULL,\n"
    "  `ID_ITEM` int(11) NOT NULL,\n"
    "  `ID_WIKIDATA` varchar(20) NOT NULL,\n"
    "  `FINANCE_KIND` varchar(20) NOT NULL,\n"
    "  `AMOUNT` decimal(30,10) DEFAULT NULL,\n"
    "  `ID_CURRENCY` varchar(20) DEFAULT NULL,\n"
    "  `CURRENCY_CODE` char(3) DEFAULT NULL,\n"
    "  `ID_PLACE_WIKIDATA` varchar(20) DEFAULT NULL,\n"
    "  `ID_LOCATION` int(11) DEFAULT NULL,\n"
    "  `IS_WORLDWIDE` tinyint(1) NOT NULL DEFAULT 0,\n"
    "  `N_PLACES` int(11) NOT NULL DEFAULT 0,\n"
    "  `DAT_POINT` date DEFAULT NULL,\n"
    "  `POINT_PRECISION` int(11) DEFAULT NULL,\n"
    "  `N_POINTS` int(11) NOT NULL DEFAULT 0,\n"
    "  `DAT_START` date DEFAULT NULL,\n"
    "  `START_PRECISION` int(11) DEFAULT NULL,\n"
    "  `DAT_END` date DEFAULT NULL,\n"
    "  `END_PRECISION` int(11) DEFAULT NULL,\n"
    "  `ID_PERIOD_WIKIDATA` varchar(20) DEFAULT NULL,\n"
    "  `CHART_RANK` int(11) DEFAULT NULL,\n"
    "  `ID_METHOD_WIKIDATA` varchar(20) DEFAULT NULL,\n"
    "  `WIKIDATA_RANK` varchar(20) DEFAULT NULL,\n"
    "  `DISPLAY_ORDER` int(11) DEFAULT NULL,\n"
    "  `DELETED` int(5) DEFAULT NULL,\n"
    "  `ID_CREATOR` int(5) DEFAULT NULL,\n"
    "  `DAT_CREAT` date DEFAULT NULL,\n"
    "  `ID_OWNER` int(5) DEFAULT NULL,\n"
    "  `TIM_UPDATED` datetime DEFAULT NULL,\n"
    "  `ID_USER_UPDATED` int(5) DEFAULT NULL,\n"
    "  PRIMARY KEY (`ID_STATEMENT`, `ITEM_TYPE`, `ID_ITEM`),\n"
    "  KEY `ITEM` (`ITEM_TYPE`, `ID_ITEM`),\n"
    "  KEY `ID_WIKIDATA` (`ID_WIKIDATA`),\n"
    "  KEY `FINANCE_KIND` (`FINANCE_KIND`),\n"
    "  KEY `AMOUNT` (`AMOUNT`),\n"
    "  KEY `CURRENCY_CODE` (`CURRENCY_CODE`),\n"
    "  KEY `ID_LOCATION` (`ID_LOCATION`),\n"
    "  KEY `IS_WORLDWIDE` (`IS_WORLDWIDE`),\n"
    "  KEY `DAT_POINT` (`DAT_POINT`),\n"
    "  KEY `TIM_UPDATED` (`TIM_UPDATED`)\n"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
)

ARR_WORK_FINANCE_COLUMNS = (
    "ITEM_TYPE", "ID_ITEM", "ID_WIKIDATA", "FINANCE_KIND", "AMOUNT", "ID_CURRENCY", "CURRENCY_CODE",
    "ID_PLACE_WIKIDATA", "ID_LOCATION", "IS_WORLDWIDE", "N_PLACES", "DAT_POINT", "POINT_PRECISION",
    "N_POINTS", "DAT_START", "START_PRECISION", "DAT_END", "END_PRECISION", "ID_PERIOD_WIKIDATA",
    "CHART_RANK", "ID_METHOD_WIKIDATA", "WIKIDATA_RANK", "DISPLAY_ORDER",
)

# The qualifier child table of each value kind, and the VALUE_TYPE its parent must carry.
ARR_WORK_FINANCE_QUALIFIER_TABLES = {
    "item": ("T_WC_WIKIDATA_QUALIFIER_ITEM_VALUE", "item"),
    "time": ("T_WC_WIKIDATA_QUALIFIER_TIME_VALUE", "time"),
    "quantity": ("T_WC_WIKIDATA_QUALIFIER_QUANTITY_VALUE", "quantity"),
}

# A qualifier date, only when its parts make a real Gregorian day at precision 9 (year),
# 10 (month) or 11 (day), years 1000 to 9999, no time zone: the first day of the year or
# month at precisions 9 and 10, to be read with its precision column. Anything else
# (31 February, month 13, Julian calendar, decade, a zone) gives NULL rather than a date
# shifted by MAKEDATE + INTERVAL. DATE_START of the qualifier rows is empty (dry run of
# 2026-10-20) and is not read.
STR_WORK_FINANCE_DATE = (
    "CASE WHEN qv.TIME_PRECISION IN (9, 10, 11) AND qv.YEAR_VALUE BETWEEN 1000 AND 9999"
    " AND SUBSTRING_INDEX(qv.CALENDAR_MODEL, '/', -1) = 'Q1985727' AND COALESCE(qv.TIMEZONE_OFFSET, 0) = 0"
    " AND (qv.TIME_PRECISION = 9 OR qv.MONTH_VALUE BETWEEN 1 AND 12)"
    " AND (qv.TIME_PRECISION < 11 OR qv.DAY_VALUE BETWEEN 1 AND"
    " DAY(LAST_DAY(MAKEDATE(qv.YEAR_VALUE, 1) + INTERVAL (qv.MONTH_VALUE - 1) MONTH)))"
    " THEN MAKEDATE(qv.YEAR_VALUE, 1) + INTERVAL IF(qv.TIME_PRECISION = 9, 0, qv.MONTH_VALUE - 1) MONTH"
    " + INTERVAL IF(qv.TIME_PRECISION = 11, qv.DAY_VALUE - 1, 0) DAY END"
)


def _f_workfinancestep(cursor, strstep, strsql, params=None):
    """One step of process 75, named before it runs (lesson of process 72)."""
    print(f"75:   {strstep} ...", flush=True)
    cursor.execute(strsql, params)
    return cursor.rowcount


def _f_workfinancequalifier(strproperty, strvalue):
    """Correlated subquery: the first value of one qualifier of statement s."""
    strkind, strexpr = {
        "item": ("item", "qv.ID_ITEM"),
        "date": ("time", STR_WORK_FINANCE_DATE),
        "precision": ("time", "qv.TIME_PRECISION"),
        "amount": ("quantity", "qv.AMOUNT"),
    }[strvalue]
    strtable, strtype = ARR_WORK_FINANCE_QUALIFIER_TABLES[strkind]
    return (
        f"(SELECT {strexpr} FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER sq "
        f"JOIN {strtable} qv ON qv.ID_STATEMENT_QUALIFIER = sq.ID_STATEMENT_QUALIFIER "
        f"WHERE sq.ID_STATEMENT = s.ID_STATEMENT AND sq.ID_QUALIFIER_PROPERTY = '{strproperty}' "
        f"AND sq.VALUE_TYPE = '{strtype}' "
        "ORDER BY sq.DISPLAY_ORDER, sq.ID_STATEMENT_QUALIFIER LIMIT 1)"
    )


def _f_workfinancequalifiercount(arrproperties, strkind):
    """Correlated subquery: how many qualifiers of these properties statement s carries
    (with a value), so that a row whose first pick hides others can be found."""
    strtable, strtype = ARR_WORK_FINANCE_QUALIFIER_TABLES[strkind]
    strin = ", ".join(f"'{p}'" for p in arrproperties)
    return (
        f"(SELECT COUNT(*) FROM T_WC_WIKIDATA_STATEMENT_QUALIFIER sq "
        f"JOIN {strtable} qv ON qv.ID_STATEMENT_QUALIFIER = sq.ID_STATEMENT_QUALIFIER "
        f"WHERE sq.ID_STATEMENT = s.ID_STATEMENT AND sq.ID_QUALIFIER_PROPERTY IN ({strin}) "
        f"AND sq.VALUE_TYPE = '{strtype}')"
    )


def f_buildworkfinancetable():
    """Rebuild T_WC_T2S_WORK_FINANCE from the V2 statements P2130 and P2142 of the T2S
    movies and series. Set-based, idempotent: a full recompute, an upsert that moves
    TIM_UPDATED only when a row changes, and the removal of the statements gone from
    Wikidata. One row per statement and T2S work. Deprecated statements are left out;
    no DELETED filter on the V2 reads (house rule, see f_wikidatabestvaluesql, kept
    after the audit of 2026-10-21).

    Returns the counts: 'rows', 'budget', 'box_office', 'worldwide', 'dated',
    'upserted', 'deleted'.
    """
    connection = cp.connectioncp
    cursor = connection.cursor()
    arrresult = {}
    strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    strtoday = strnow[:10]
    strtablecharset = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    strcurrency = "CASE q.UNIT_ID_WIKIDATA " + " ".join(
        f"WHEN '{strqid}' THEN '{strcode}'" for strqid, strcode in ARR_WORK_FINANCE_CURRENCY_CODES.items()) + " END"
    strplace = "COALESCE(" + ", ".join(_f_workfinancequalifier(p, "item") for p in ARR_WORK_FINANCE_PLACE_PROPERTIES) + ")"
    try:
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 120")
        _f_readwithoutlocking(cursor, "75")
        # A table created by 1.1.22 or 1.1.23 (key on ID_STATEMENT alone, no precision or
        # count columns) is dropped and recreated: it is fully recomputed below and nothing
        # points to its rows.
        cursor.execute(
            "SELECT COUNT(*) AS N_COLUMNS, SUM(COLUMN_NAME = 'END_PRECISION') AS HAS_PRECISIONS "
            "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            (STR_WORK_FINANCE_TABLE,))
        arrshape = cursor.fetchone() or {}
        if int(arrshape.get("N_COLUMNS") or 0) > 0 and not int(arrshape.get("HAS_PRECISIONS") or 0):
            _f_workfinancestep(cursor, "0. DROP the 1.1.22 shape of " + STR_WORK_FINANCE_TABLE,
                               "DROP TABLE `" + STR_WORK_FINANCE_TABLE + "`")
        _f_workfinancestep(cursor, "0. CREATE TABLE IF NOT EXISTS " + STR_WORK_FINANCE_TABLE, STR_WORK_FINANCE_DDL)

        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_work_finance")
        cursor.execute(
            "CREATE TEMPORARY TABLE tmp_work_finance ("
            " ID_STATEMENT BIGINT NOT NULL,"
            " ITEM_TYPE VARCHAR(10) NOT NULL, ID_ITEM INT NOT NULL, ID_WIKIDATA VARCHAR(20) NOT NULL,"
            " FINANCE_KIND VARCHAR(20) NOT NULL, AMOUNT DECIMAL(30,10) NULL,"
            " ID_CURRENCY VARCHAR(20) NULL, CURRENCY_CODE CHAR(3) NULL,"
            " ID_PLACE_WIKIDATA VARCHAR(20) NULL, ID_LOCATION INT NULL, IS_WORLDWIDE TINYINT(1) NOT NULL DEFAULT 0,"
            " N_PLACES INT NOT NULL DEFAULT 0, DAT_POINT DATE NULL, POINT_PRECISION INT NULL,"
            " N_POINTS INT NOT NULL DEFAULT 0, DAT_START DATE NULL, START_PRECISION INT NULL,"
            " DAT_END DATE NULL, END_PRECISION INT NULL,"
            " ID_PERIOD_WIKIDATA VARCHAR(20) NULL, CHART_RANK INT NULL, ID_METHOD_WIKIDATA VARCHAR(20) NULL,"
            " WIKIDATA_RANK VARCHAR(20) NULL, DISPLAY_ORDER INT NULL,"
            " PRIMARY KEY (ID_STATEMENT, ITEM_TYPE, ID_ITEM)"
            ") " + strtablecharset)
        lngread = 0
        for stritemtype, strt2s, strid in (("movie", "T_WC_T2S_MOVIE", "ID_MOVIE"), ("serie", "T_WC_T2S_SERIE", "ID_SERIE")):
            # A plain INSERT, no IGNORE: the key is the statement AND the work, so a
            # duplicate can only be a bug, and it must stop the step rather than vanish.
            lngread += _f_workfinancestep(cursor, f"1. INSERT the {stritemtype} statements (tmp_work_finance)", (
                "INSERT INTO tmp_work_finance (ID_STATEMENT, ITEM_TYPE, ID_ITEM, ID_WIKIDATA, FINANCE_KIND,"
                " AMOUNT, ID_CURRENCY, CURRENCY_CODE, ID_PLACE_WIKIDATA, N_PLACES, DAT_POINT, POINT_PRECISION,"
                " N_POINTS, DAT_START, START_PRECISION, DAT_END, END_PRECISION,"
                " ID_PERIOD_WIKIDATA, CHART_RANK, ID_METHOD_WIKIDATA, WIKIDATA_RANK, DISPLAY_ORDER)\n"
                f"SELECT s.ID_STATEMENT, '{stritemtype}' AS ITEM_TYPE, w.{strid} AS ID_ITEM, s.ID_WIKIDATA,\n"
                "       IF(s.ID_PROPERTY = 'P2130', 'budget', 'box_office') AS FINANCE_KIND,\n"
                f"       q.AMOUNT, q.UNIT_ID_WIKIDATA AS ID_CURRENCY, {strcurrency} AS CURRENCY_CODE,\n"
                f"       {strplace} AS ID_PLACE_WIKIDATA,\n"
                f"       {_f_workfinancequalifiercount(ARR_WORK_FINANCE_PLACE_PROPERTIES, 'item')} AS N_PLACES,\n"
                f"       {_f_workfinancequalifier('P585', 'date')} AS DAT_POINT, {_f_workfinancequalifier('P585', 'precision')} AS POINT_PRECISION,\n"
                f"       {_f_workfinancequalifiercount(('P585',), 'time')} AS N_POINTS,\n"
                f"       {_f_workfinancequalifier('P580', 'date')} AS DAT_START, {_f_workfinancequalifier('P580', 'precision')} AS START_PRECISION,\n"
                f"       {_f_workfinancequalifier('P582', 'date')} AS DAT_END, {_f_workfinancequalifier('P582', 'precision')} AS END_PRECISION,\n"
                f"       {_f_workfinancequalifier('P1264', 'item')} AS ID_PERIOD_WIKIDATA, {_f_workfinancequalifier('P1352', 'amount')} AS CHART_RANK,\n"
                f"       {_f_workfinancequalifier('P459', 'item')} AS ID_METHOD_WIKIDATA,\n"
                "       s.`RANK` AS WIKIDATA_RANK, s.DISPLAY_ORDER\n"
                "FROM T_WC_WIKIDATA_STATEMENT s\n"
                "JOIN T_WC_WIKIDATA_QUANTITY_VALUE q ON q.ID_STATEMENT = s.ID_STATEMENT\n"
                f"JOIN `{strt2s}` w ON w.ID_WIKIDATA = s.ID_WIKIDATA AND COALESCE(w.DELETED, 0) = 0\n"
                "WHERE s.ID_PROPERTY IN ('P2130', 'P2142') AND s.VALUE_TYPE = 'quantity'\n"
                "  AND (s.`RANK` IS NULL OR s.`RANK` <> 'deprecated')\n"
                "  AND q.AMOUNT IS NOT NULL"))
        _f_workfinancestep(cursor, "2. UPDATE the place to its T2S location and the worldwide flag", (
            "UPDATE tmp_work_finance t\n"
            "LEFT JOIN T_WC_T2S_LOCATION l ON l.ID_WIKIDATA = t.ID_PLACE_WIKIDATA\n"
            "SET t.ID_LOCATION = l.ID_LOCATION,\n"
            "    t.IS_WORLDWIDE = (t.ID_PLACE_WIKIDATA <=> %s)"), (STR_WORK_FINANCE_WORLDWIDE,))
        connection.commit()
        print(f"75:   {lngread} statement(s) read")

        strcolumns = ", ".join(ARR_WORK_FINANCE_COLUMNS)
        strunchanged = " AND ".join(f"{STR_WORK_FINANCE_TABLE}.{c} <=> VALUES({c})" for c in ARR_WORK_FINANCE_COLUMNS)
        strupdates = ",\n".join(f"  {c} = VALUES({c})" for c in ARR_WORK_FINANCE_COLUMNS)
        # TIM_UPDATED first: it must compare the OLD values, before the assignments below.
        arrresult["upserted"] = _f_workfinancestep(cursor, "3. UPSERT into " + STR_WORK_FINANCE_TABLE, (
            f"INSERT INTO {STR_WORK_FINANCE_TABLE} (ID_STATEMENT, {strcolumns}, DELETED, ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED)\n"
            f"SELECT ID_STATEMENT, {strcolumns}, 0, %s, %s, %s, %s, %s FROM tmp_work_finance\n"
            "ON DUPLICATE KEY UPDATE\n"
            f"  TIM_UPDATED = IF({strunchanged} AND {STR_WORK_FINANCE_TABLE}.DELETED <=> 0,\n"
            f"                   {STR_WORK_FINANCE_TABLE}.TIM_UPDATED, VALUES(TIM_UPDATED)),\n"
            f"{strupdates},\n"
            "  DELETED = 0,\n"
            f"  DAT_CREAT = COALESCE({STR_WORK_FINANCE_TABLE}.DAT_CREAT, VALUES(DAT_CREAT))"),
            (cp.lnguseridsession, strtoday, cp.lnguseridsession, strnow, cp.lnguseridsession))
        # A statement gone from Wikidata (or turned deprecated, or whose work left T2S) is
        # removed: the key is the Wikidata statement and its work, nobody keeps a reference to it.
        arrresult["deleted"] = _f_workfinancestep(cursor, "4. DELETE the statements no longer read", (
            f"DELETE x FROM {STR_WORK_FINANCE_TABLE} x\n"
            "LEFT JOIN tmp_work_finance t ON t.ID_STATEMENT = x.ID_STATEMENT\n"
            "  AND t.ITEM_TYPE = x.ITEM_TYPE AND t.ID_ITEM = x.ID_ITEM\n"
            "WHERE t.ID_STATEMENT IS NULL"))
        connection.commit()

        cursor.execute(
            "SELECT COUNT(*) AS ROWS_, SUM(FINANCE_KIND = 'budget') AS BUDGET, SUM(FINANCE_KIND = 'box_office') AS BOX_OFFICE, "
            "SUM(IS_WORLDWIDE = 1) AS WORLDWIDE, SUM(DAT_POINT IS NOT NULL OR DAT_START IS NOT NULL OR ID_PERIOD_WIKIDATA IS NOT NULL) AS DATED "
            f"FROM {STR_WORK_FINANCE_TABLE}")
        arrrow = cursor.fetchone() or {}
        for strkey, strcol in (("rows", "ROWS_"), ("budget", "BUDGET"), ("box_office", "BOX_OFFICE"),
                               ("worldwide", "WORLDWIDE"), ("dated", "DATED")):
            arrresult[strkey] = int(arrrow.get(strcol) or 0)
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_work_finance")
        connection.commit()
        return arrresult
    finally:
        cursor.close()
