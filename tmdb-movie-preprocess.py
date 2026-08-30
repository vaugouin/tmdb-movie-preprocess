import time
import os
import requests
import pymysql.cursors
#from pymysql import Error
import json
import html
import citizenphil as cp
from datetime import datetime, timedelta
import gzip
import shutil
import numpy as np
import pandas as pd
import psutil
import re
import sys

from tmdb_preprocess_helpers import (
    EntityTelemetry,
    batch_update_data,
    check_memory,
    clean_format_line,
    execute_sql_with_retry,
    extract_color_technology,
    extract_film_technology,
    extract_format_components,
    extract_sound_technology,
    f_buildcustomaggregatequery,
    f_buildcustomorderbyclause,
    f_customexternalidsourcesql,
    f_getcustomsortby,
    f_getwikidataimagepath,
    f_parsecustomexternalidproperties,
    f_awardconeguard,
    f_awarddrivingsql,
    f_awardlinksql,
    f_persondrivingsql,
    f_personlinkfromsql,
    f_persongrouppurgesql,
    f_awardpurgesql,
    f_getwikidatalabel,
    f_wikidataexternalidsql,
    f_wikidatainstanceofsql,
    STR_WD_PROPERTY_CRITERION,
    STR_WD_PROPERTY_CRITERION_SPINE,
    STR_WD_PROPERTY_PLEX,
    f_linktmdbkeywordtowikidata,
    f_tmdbpersonsetusedfortags,
    f_wikidataentitysummary,
    f_wikidataitemproperties,
    load_technical_ids,
    normalize_extracted_components,
    process_value,
    refresh_technical_movie_count,
    validate_format_line,
    write_movie_technical_junction,
)

# Global settings for pre processing
# Test settings 2024-11-29
lngimdbweightedratingm = 20900.0 # Target closeness score: 90.97 / 100

lngmaxlengthkeywords = 3000
lngmaxlengthcompanies = 3000
lngmaxcast = 20
lngmaxcrews = 20
lngmaxdirectors = 10
lngmaxwriters = 10
lngmaxproducers = 10
lngmaxeditors = 10
lngmaxart = 10
lngmaxcamera = 10
lngmaxlightning = 10
lngmaxsound = 10
lngmaxcostumemakeup = 10
lngmaxvisualeffects = 10
lngmaxlengthcastwithaliases = 3000
lngmaxlengthcrewswithaliases = 3000
lngmaxlengthdirectors = 800
lngmaxlengthwriters = 800
lngmaxlengthproducers = 800
lngmaxlengtheditors = 800
lngmaxlengthart = 800
lngmaxlengthcamera = 800
lngmaxlengthlightning = 800
lngmaxlengthsound = 800
lngmaxlengthcostumemakeup = 800
lngmaxlengthvisualeffects = 800
intallowpersonmultiplecredit = True
intincludepersonaliases = False

strdattoday = datetime.now(cp.paris_tz).strftime("%Y-%m-%d")

try:
    conn = cp.f_getconnection()
    with conn:
        with conn.cursor() as cursor:
            cursor2 = conn.cursor()
            cursor3 = conn.cursor()
            cursor4 = conn.cursor()
            cursor5 = conn.cursor()
            # L'horloge du RUN porte un nom qui lui est propre depuis le 2026-08-21. Elle
            # s'appelait start_time, comme les quatre chronometres locaux des processus qui
            # l'ecrasaient au passage : le calcul de duree totale, en fin de fichier, mesurait
            # donc le dernier processus a l'avoir reaffectee. Le run du 2026-08-20, long d'1 h 27,
            # s'est ainsi declare long de 58 secondes. Meme raison pour dblrunendtime.
            dblrunstarttime = time.time()
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strtmdbmoviepreprocessstartdatetime",strnow,"Date and time of the last start of the TMDb database preprocess",0)
            strprocessesexecutedprevious = cp.f_getservervariable("strtmdbmoviepreprocessprocessesexecuted",0)
            strprocessesexecuteddesc = "List of processes executed in the TMDb movie preprocess"
            cp.f_setservervariable("strtmdbmoviepreprocessprocessesexecutedprevious",strprocessesexecutedprevious,strprocessesexecuteddesc + " (previous execution)",0)
            strprocessesexecuted = ""
            cp.f_setservervariable("strtmdbmoviepreprocessprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
            strtotalruntimedesc = "Total runtime of the TMDb movie preprocess"
            strtotalruntimeprevious = cp.f_getservervariable("strtmdbmoviepreprocesstotalruntime",0)
            cp.f_setservervariable("strtmdbmoviepreprocesstotalruntimeprevious",strtotalruntimeprevious,strtotalruntimedesc + " (previous execution)",0)
            strtotalruntime = "RUNNING"
            cp.f_setservervariable("strtmdbmoviepreprocesstotalruntime",strtotalruntime,strtotalruntimedesc,0)

            #arrprocessscope = {2: 'T2S_MOVIE_TECHNICAL'}
            #arrprocessscope = {20: 'TMDB_KEYWORD'}
            #arrprocessscope = {6: 'T2S_PERSON'}
            #arrprocessscope = {5: 'T2S_SERIE'}
            # --- Process scope selection (env-driven) --------------------------------------
            # The network-bound, rate-limited Wikidata keyword linker (Process 60, ~3h45m) is
            # DECOUPLED from the main DB ETL so it can run on its own schedule (its own cron /
            # `docker run` with TMDB_PREPROCESS_SCOPE=wikidata-topics) instead of blocking it.
            # Process 3 (T2S_TOPIC) only reads the ID_WIKIDATA that 60 stamps on
            # T_WC_TMDB_KEYWORD and is itself a rolling idempotent batch, so the two need not
            # run in the same invocation. The default scope ("main") excludes Process 60.
            arrprocessscopemain = {0: 'T_WC_CUSTOM_LIST_UNESCAPE', 1: 'WIKIPEDIA_FORMAT_LINE', 2: 'T2S_MOVIE_TECHNICAL', 62: 'Link Wikidata items to T2S technical', 3: 'T2S_TOPIC', 41: 'T2S_COLLECTION', 61: 'Link Wikidata items to collections', 42: 'T2S_LIST', 43: 'T2S_GROUP', 44: 'T2S_AWARD', 47: 'T2S_NOMINATION', 45: 'T2S_MOVEMENT', 46: 'T2S_DEATH', 4: 'T2S_MOVIE', 5: 'T2S_SERIE', 6: 'T2S_PERSON', 7: 'T2S_COMPANY', 8: 'T2S_NETWORK', 9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE', 11: 'T2S_MOVIE_GENRE', 12: 'T2S_SERIE_GENRE', 36: 'T2S_MOVIE_SIMILAR', 37: 'T2S_MOVIE_RECOMMENDATION', 38: 'T2S_SERIE_SIMILAR', 39: 'T2S_SERIE_RECOMMENDATION', 13: 'T2S_MOVIE_COMPANY', 14: 'T2S_SERIE_COMPANY', 15: 'T2S_SERIE_NETWORK', 16: 'T2S_MOVIE_PRODUCTION_COUNTRY', 17: 'T2S_SERIE_PRODUCTION_COUNTRY', 18: 'T2S_MOVIE_SPOKEN_LANGUAGE', 19: 'T2S_SERIE_SPOKEN_LANGUAGE', 20: 'T2S_COMPANY_IMAGE', 21: 'T2S_MOVIE_IMAGE', 22: 'T2S_NETWORK_IMAGE', 23: 'T2S_PERSON_IMAGE', 24: 'T2S_SERIE_IMAGE', 25: 'T2S_MOVIE_VIDEO', 26: 'T2S_SERIE_VIDEO', 27: 'T2S_SEASON', 28: 'T2S_EPISODE', 29: 'T2S_PERSON_SEASON', 31: 'T2S_PERSON_EPISODE', 32: 'T2S_SEASON_IMAGE', 33: 'T2S_EPISODE_IMAGE', 34: 'T2S_SEASON_VIDEO', 35: 'T2S_EPISODE_VIDEO', 40: 'T2S_ITEM', 70: 'T2S_EVALUATION_ASSERTION_REFRESH', 71: 'T2S_WIKIPEDIA_MAIN_IMAGE'}
            arrprocessscopewikidatatopics = {60: 'Link Wikidata items to topics'}
            # Pilot: the same decoupled, rate-limited pattern as Process 60, for
            # companies (Process 63). Run with TMDB_PREPROCESS_SCOPE=wikidata-companies.
            arrprocessscopewikidatacompany = {63: 'Link Wikidata items to companies'}
            # Combined scope: run EVERY Wikidata linker SEQUENTIALLY in a single
            # container (dict insertion order = execution order). This is the
            # preferred scope to schedule: one process means one Wikimedia request
            # stream, so the linkers never contend for the rate limit (unlike firing
            # several decoupled containers at once). To add a future linker
            # (network / genre / character), define its own scope dict above and
            # merge it in here -- it then runs as part of `wikidata-all` automatically.
            arrprocessscopewikidataall = {
                **arrprocessscopewikidatatopics,
                # PILOT VALIDATED (2026-06-29): Process 63 (companies) re-enabled in
                # the scheduled wikidata-all run after the tuning pass (Poverty Row
                # allowlist type, redirect-resolution fallback for renamed studios,
                # match_type-based quarantine). The review batch confirmed the 0.50
                # quarantine band shrank 584->208, exact-title studios moved to 1.0,
                # and brand collisions (Apple, Spirit) stayed quarantined.
                **arrprocessscopewikidatacompany,
            }
            # Assertion-refresh only (Process 70): rebuild ASSERTIONS_QUERY_RESULT for the
            # living evals from ASSERTION_REFRESH_SQL, on demand (e.g. right after seeding
            # new showcase samples). Run with TMDB_PREPROCESS_SCOPE=assertion-refresh.
            arrprocessscopeassertionrefresh = {70: 'T2S_EVALUATION_ASSERTION_REFRESH'}
            # Wikipedia main image only (Process 71): re-copy the lead image from
            # T_WC_WIKIPEDIA_PAGE_LANG into the T2S serving columns, on demand, which is
            # useful right after a wikipedia-crawler pass without replaying the whole ETL.
            # Run with TMDB_PREPROCESS_SCOPE=wikipedia-main-image.
            arrprocessscopewikipediamainimage = {71: 'T2S_WIKIPEDIA_MAIN_IMAGE'}
            # Grounded-neighbour build only (Processes 36-39): rebuild the T2S similar /
            # recommendation tables from the raw T_WC_TMDB_* neighbours, on demand -- handy as a
            # unit test right after creating the four T2S tables, without the whole pipeline.
            # Run with TMDB_PREPROCESS_SCOPE=neighbours. Order matters: T2S_MOVIE/SERIE (4/5) must
            # already exist, which they do in a normal DB (this scope only rebuilds the neighbour twins).
            arrprocessscopeneighbours = {36: 'T2S_MOVIE_SIMILAR', 37: 'T2S_MOVIE_RECOMMENDATION', 38: 'T2S_SERIE_SIMILAR', 39: 'T2S_SERIE_RECOMMENDATION'}
            strprocessscope = os.getenv("TMDB_PREPROCESS_SCOPE", "main").strip().lower()
            if strprocessscope == "wikidata-topics":
                arrprocessscope = arrprocessscopewikidatatopics
            elif strprocessscope == "wikidata-companies":
                arrprocessscope = arrprocessscopewikidatacompany
            elif strprocessscope in ("wikidata-all", "wikidata"):
                arrprocessscope = arrprocessscopewikidataall
            elif strprocessscope in ("assertion-refresh", "assertions"):
                arrprocessscope = arrprocessscopeassertionrefresh
            elif strprocessscope in ("wikipedia-main-image", "wikipedia-image"):
                arrprocessscope = arrprocessscopewikipediamainimage
            elif strprocessscope in ("neighbours", "neighbors", "similar-recommendations"):
                arrprocessscope = arrprocessscopeneighbours
            else:
                strprocessscope = "main"
                arrprocessscope = arrprocessscopemain
            if strnow.startswith("2026-07-18"):
                arrprocessscope = {4: 'T2S_MOVIE'}
                arrprocessscope = {5: 'T2S_SERIE'}
            cp.f_setservervariable("strtmdbmoviepreprocessscope", strprocessscope, "Selected process scope for this run (main | wikidata-topics | wikidata-companies | wikidata-all | assertion-refresh | neighbours)", 0)
            print(f"Process scope: {strprocessscope} ({len(arrprocessscope)} process(es))")
            #arrprocessscope = {48: 'TMDB_CHARACTER', 49: 'TMDB_CHARACTER_ALT'}
            #arrprocessscope = {10: 'T2S_PERSON_SERIE'}
            #arrprocessscope = {9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE'}
            #arrprocessscope = {4: 'T2S_MOVIE', 5: 'T2S_SERIE'}
            #arrprocessscope = {7: 'T2S_COMPANY'}
            #arrprocessscope = {8: 'T2S_NETWORK'}
            #arrprocessscope = {3: 'T2S_TOPIC', 4: 'T2S_MOVIE', 5: 'T2S_SERIE', 6: 'T2S_PERSON', 7: 'T2S_COMPANY', 8: 'T2S_NETWORK', 9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE'}
            #arrprocessscope = {1: 'WIKIPEDIA_FORMAT_LINE'}
            #arrprocessscope = {40: 'T2S_ITEM'}
            #arrprocessscope = {41: 'T2S_COLLECTION'}
            #arrprocessscope = {41: 'T2S_COLLECTION', 42: 'T2S_LIST'}
            #arrprocessscope = {3: 'T2S_TOPIC'}
            #arrprocessscope = {43: 'T2S_GROUP'}
            #if strnow.startswith("2026-05-31"):
            #    arrprocessscope = {0: 'T_WC_CUSTOM_LIST_UNESCAPE'}
            # Per-process run window (startdatetime/enddatetime/processedseconds)
            # published via the shared EntityTelemetry helper (kind="copy") here in
            # the loop wrapper (telcopy.begin() below, telcopy.finish() at the end of
            # the loop body). Covers the bulk copy/rebuild steps (4-40) AND the
            # utility / Wikidata-linking steps that have no bespoke telemetry of their
            # own (0,1,2, the linking steps 60/61/62, and the alternate-character
            # source 49). The richer dimension derivations (Topic 3, Collection 41,
            # List 42, Group 43, Award 44, Movement 45, Death 46, Nomination 47,
            # Character 48) self-instrument inside their own blocks (EntityTelemetry,
            # or bespoke vars for 43/46) and are intentionally absent here so prefixes
            # don't clash.
            arrcopytelemetry = {
                0: ("customlistunescape", "custom list HTML unescape"),
                1: ("wikipediaformatline", "Wikipedia format-line cleanup"),
                2: ("movietechnical", "movie technical"),
                4: ("movie", "movie"),
                5: ("serie", "serie"),
                6: ("person", "person"),
                7: ("company", "company"),
                8: ("network", "network"),
                9: ("personmovie", "person-movie link"),
                10: ("personserie", "person-serie link"),
                11: ("moviegenre", "movie-genre link"),
                12: ("seriegenre", "serie-genre link"),
                13: ("moviecompany", "movie-company link"),
                14: ("seriecompany", "serie-company link"),
                15: ("serienetwork", "serie-network link"),
                16: ("movieproductioncountry", "movie production country"),
                17: ("serieproductioncountry", "serie production country"),
                18: ("moviespokenlanguage", "movie spoken language"),
                19: ("seriespokenlanguage", "serie spoken language"),
                20: ("companyimage", "company image"),
                21: ("movieimage", "movie image"),
                22: ("networkimage", "network image"),
                23: ("personimage", "person image"),
                24: ("serieimage", "serie image"),
                25: ("movievideo", "movie video"),
                26: ("serievideo", "serie video"),
                27: ("season", "season"),
                28: ("episode", "episode"),
                29: ("personseason", "person-season link"),
                31: ("personepisode", "person-episode link"),
                32: ("seasonimage", "season image"),
                33: ("episodeimage", "episode image"),
                34: ("seasonvideo", "season video"),
                35: ("episodevideo", "episode video"),
                40: ("item", "item"),
                49: ("characteralt", "character (alt) source build"),
                60: ("topicwikidatalink", "topic Wikidata linking"),
                61: ("collectionwikidatalink", "collection Wikidata linking"),
                62: ("technicalwikidatalink", "technical Wikidata linking"),
            }
            # Uniform per-process wall-clock, measured centrally here in the loop
            # wrapper so EVERY process is timed the same way regardless of whatever
            # bespoke telemetry it emits internally. Drives the "longest first"
            # ranking printed/published after the loop (optimization candidates).
            arrprocessdurations = {}
            # Refresh optimizer statistics on the bulk-loaded tables before any per-record
            # Wikidata-link query plans its joins. After a bulk rebuild the InnoDB stats go
            # stale and the optimizer can pick a full-table-scan plan instead of driving from
            # the selective side -- which turned the GROUP/AWARD derivations into ~16h runs,
            # and slows the text-to-SQL API's location/award/etc. queries the same way.
            # ANALYZE only samples index pages, so it is cheap.
            #
            # The whole T2S read model (every T_WC_T2S_* table the text-to-SQL prompt can
            # query, doc tmdb-front/.../groups-multi-repo-management.md §9.17) is discovered
            # dynamically so the list never drifts from the prompt schema; the shared
            # Wikidata/TMDB join tables are appended explicitly. Belt-and-suspenders: the
            # preprocess link queries also pin their join order with STRAIGHT_JOIN.
            try:
                cursoranalyze = cp.connectioncp.cursor()
                cursoranalyze.execute(
                    "SELECT TABLE_NAME FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' "
                    "AND TABLE_NAME LIKE 'T\\_WC\\_T2S\\_%'"
                )
                arranalyzetables = [row["TABLE_NAME"] for row in cursoranalyze.fetchall()]
                arranalyzetables += [
                    # Les statistiques nourrissent des plans que le code force par
                    # STRAIGHT_JOIN : elles doivent porter sur les tables reellement
                    # lues. Les trois tables V1 ont ete remplacees par leurs
                    # equivalents V2 le 2026-08-30 (-040, -041) ; ANALYZE sur une
                    # table que plus personne n'interroge ne coute rien mais ne sert
                    # a rien, et son absence sur les nouvelles coute un mauvais plan.
                    "T_WC_WIKIDATA_STATEMENT", "T_WC_WIKIDATA_ITEM_VALUE",
                    "T_WC_WIKIDATA_PERSON", "T_WC_WIKIDATA_ITEM_V1",
                    "T_WC_WIKIDATA_PERSON_V1", "T_WC_TMDB_MOVIE",
                    "T_WC_TMDB_SERIE", "T_WC_TMDB_PERSON", "T_WC_TMDB_GENRE",
                ]
                # Backtick every name (T_WC_T2S_GROUP collides with the reserved word GROUP);
                # chunk so one bad/locked table cannot abort the rest.
                for intanalyzestart in range(0, len(arranalyzetables), 25):
                    arranalyzechunk = arranalyzetables[intanalyzestart:intanalyzestart + 25]
                    cursoranalyze.execute("ANALYZE TABLE " + ", ".join("`" + t + "`" for t in arranalyzechunk))
                    cursoranalyze.fetchall()
                cursoranalyze.close()
                print(f"ANALYZE TABLE: optimizer statistics refreshed on {len(arranalyzetables)} read-model + join tables.")
            except Exception as excanalyze:
                print(f"ANALYZE TABLE refresh skipped: {excanalyze}")
            for intindex, strdesc in arrprocessscope.items():
                strprocessesexecuted += str(intindex) + ", "
                cp.f_setservervariable("strtmdbmoviepreprocessprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
                cp.f_setservervariable("strtmdbmoviepreprocesscurrentprocess",strdesc,"Current process in the TMDb database preprocess",0)
                cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","","Current sub process in the TMDb database preprocess",0)
                dblprocessstarttime = time.time()
                telcopy = None
                if intindex in arrcopytelemetry:
                    strcopyentity, strcopylabel = arrcopytelemetry[intindex]
                    telcopy = EntityTelemetry(strcopyentity, intindex, strcopylabel, kind="copy")
                    telcopy.begin()
                if intindex == 0:
                    #----------------------------------------------------
                    # Decode HTML-escaped characters (e.g. &#039;, &amp;, &quot;)
                    # in the T_WC_CUSTOM_LIST source table so that downstream
                    # copies to T2S collections, lists, movements, groups, etc.
                    # display clean labels (e.g. "Time Magazine's All-TIME Movies"
                    # instead of "Time Magazine&#039;s All-TIME Movies").
                    print("T_WC_CUSTOM_LIST_UNESCAPE processing")
                    start_time = time.time()
                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","T_WC_CUSTOM_LIST HTML unescape","Current sub process in the TMDb database preprocess",0)

                    # Text columns of T_WC_CUSTOM_LIST that may hold HTML-escaped
                    # characters; html.unescape() is a no-op on already-clean values.
                    arrcustomlisttextcolumns = [
                        "LIST_NAME",
                        "LIST_NAME_FR",
                        "OVERVIEW",
                        "WIKIDATA_PROPERTIES",
                        "ID_IMDB_LIST",
                        "TMDB_ELEMENTS",
                        "TMDB_TARGET_RECORD",
                        "POSTER_PATH",
                        "WIKIPEDIA_IMAGE_PATH",
                    ]
                    strcolumns = ", ".join(arrcustomlisttextcolumns)
                    query = f"SELECT ID_CUSTOM_LIST, {strcolumns} FROM T_WC_CUSTOM_LIST "
                    print(query)
                    cursor2.execute(query)
                    results = cursor2.fetchall()
                    print(f"Loaded {len(results)} rows of T_WC_CUSTOM_LIST")

                    lngrowsupdated = 0
                    for row in results:
                        arrchanged = {}
                        for strcolumn in arrcustomlisttextcolumns:
                            strvalue = row[strcolumn]
                            if strvalue is None:
                                continue
                            strunescaped = html.unescape(strvalue)
                            if strunescaped != strvalue:
                                arrchanged[strcolumn] = strunescaped
                        if arrchanged:
                            strsetclause = ", ".join(f"{strcolumn} = %s" for strcolumn in arrchanged)
                            arrparams = list(arrchanged.values())
                            arrparams.append(row["ID_CUSTOM_LIST"])
                            strupdate = f"UPDATE T_WC_CUSTOM_LIST SET {strsetclause} WHERE ID_CUSTOM_LIST = %s "
                            cursor2.execute(strupdate, arrparams)
                            lngrowsupdated += 1
                            print(f"  ID_CUSTOM_LIST {row['ID_CUSTOM_LIST']}: unescaped {list(arrchanged.keys())}")
                    cp.connectioncp.commit()
                    print(f"T_WC_CUSTOM_LIST_UNESCAPE complete: {lngrowsupdated} row(s) updated")
                    print(f"Elapsed time: {time.time() - start_time:.2f} seconds")
                elif intindex == 1:
                    #----------------------------------------------------
                    print("WIKIPEDIA_FORMAT_LINE processing")
                    start_time = time.time()
                    
                    # Check memory
                    dblavailableram = check_memory()
                    
                    lngformatlinelookbackminutes = 60
                    strrunstart = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                    strlastrun = cp.f_getservervariable("strtmdbmoviepreprocesswikipediaformatlinelastrun", 0)
                    # Only (re)parse movies whose WIKIPEDIA_FORMAT_LINE was stamped since the last
                    # SUCCESSFUL run. The upstream crawler sets DAT_WIKIPEDIA_FORMAT_LINE (datetime,
                    # indexed) whenever it writes WIKIPEDIA_FORMAT_LINE, so that column is the change
                    # marker. The watermark is the previous run start time, persisted only after this
                    # run completes; the first run (empty watermark) falls back to a full scan. A
                    # look-back buffer absorbs clock skew between the crawler host and this process.
                    strincrementalfilter = ""
                    if strlastrun:
                        strincrementalfilter = (
                            "AND DAT_WIKIPEDIA_FORMAT_LINE >= "
                            "DATE_SUB('" + strlastrun + "', INTERVAL "
                            + str(lngformatlinelookbackminutes) + " MINUTE) "
                        )
                        print(f"Incremental run: rows changed since {strlastrun} (minus {lngformatlinelookbackminutes} min buffer)")
                    else:
                        print("First run (no watermark): full scan of all WIKIPEDIA_FORMAT_LINE rows")
                    
                    # Read data from database using fetchall()
                    query = (
                        "SELECT ID_MOVIE, WIKIPEDIA_FORMAT_LINE "
                        "FROM T_WC_TMDB_MOVIE "
                        "WHERE WIKIPEDIA_FORMAT_LINE IS NOT NULL "
                        "AND WIKIPEDIA_FORMAT_LINE <> '' "
                        + strincrementalfilter +
                        "ORDER BY ID_MOVIE ASC "
                    )
                    print(query)
                    cursor2.execute(query)
                    result = cursor2.fetchall()
                    # Convert the result to a pandas DataFrame
                    data = pd.DataFrame(result)
                    print(f"Loaded {len(data)} rows of data")
                    if telcopy is not None:
                        telcopy.set_processed(len(data))
                    if len(data) == 0:
                        print("No changed WIKIPEDIA_FORMAT_LINE rows since last run; skipping parse/junction work.")
                    else:
                        print(data.head())
                        #time.sleep(5)

                        # Create backup of original data
                        data['WIKIPEDIA_FORMAT_LINE'] = data['WIKIPEDIA_FORMAT_LINE'].astype(str)

                        # Convert to lowercase and apply cleaning
                        data['WIKIPEDIA_FORMAT_LINE'] = data['WIKIPEDIA_FORMAT_LINE'].str.lower()
                        data['WIKIPEDIA_FORMAT_LINE'] = data['WIKIPEDIA_FORMAT_LINE'].apply(clean_format_line)
                        #print(data['WIKIPEDIA_FORMAT_LINE'])

                        # Apply transformations and extract components
                        # Enumerate through dataframe and display WIKIPEDIA_FORMAT_LINE for each row
                        #for index, row in data.iterrows():
                        #    print(f"Row {index}: {row['WIKIPEDIA_FORMAT_LINE']}")

                        format_components = data['WIKIPEDIA_FORMAT_LINE'].apply(extract_format_components)
                        format_components = format_components.apply(normalize_extracted_components)

                        print("\nAfter extract_format_components()")
                        print(format_components.head())
                        #time.sleep(5)

                        format_df = pd.DataFrame(format_components.tolist())

                        # Add the extracted components to the main DataFrame
                        data['IS_COLOR'] = format_df['IS_COLOR']
                        data['IS_BLACK_AND_WHITE'] = format_df['IS_BLACK_AND_WHITE']
                        data['IS_SILENT'] = format_df['IS_SILENT']
                        data['IS_3D'] = format_df['IS_3D']
                        data['COLOR_TECHNOLOGY'] = format_df['COLOR_TECHNOLOGY']
                        data['FILM_TECHNOLOGY'] = format_df['FILM_TECHNOLOGY']
                        data['ASPECT_RATIO'] = format_df['ASPECT_RATIO']
                        data['ASPECT_RATIO_LIST'] = format_df['ASPECT_RATIO_LIST']
                        data['FILM_FORMAT'] = format_df['FILM_FORMAT']
                        data['SOUND_SYSTEM'] = format_df['SOUND_SYSTEM']
                        data['SOUND_TECHNOLOGY'] = format_df['SOUND_TECHNOLOGY']
                        data['NUM_AUDIO_TRACKS'] = format_df['NUM_AUDIO_TRACKS']

                        # Validate format lines
                        data['IS_VALID_FORMAT'] = data['WIKIPEDIA_FORMAT_LINE'].apply(validate_format_line)

                        # Display sample of processed data
                        print("\nSample of processed data:")
                        print(data.head())
                        #time.sleep(5)

                        # Update data in batches
                        print("Updating data in MariaDB...")
                        batch_update_data(cp.connectioncp, data, 1)

                        # Junction-table enrichment for medium_format + aspect_ratio (EXTEND_T2S_TECHNICAL.md §12.5).
                        print("\nPopulating T_WC_T2S_MOVIE_TECHNICAL (medium_format + aspect_ratio)...")
                        arrclassificationid, arraspectratioid = load_technical_ids(cursor)
                        arrjunctionsummary = write_movie_technical_junction(
                            cp.connectioncp, data, arrclassificationid, arraspectratioid
                        )
                        refresh_technical_movie_count(cp.connectioncp)
                        print(
                            "  medium_format rows: color=" + str(arrjunctionsummary['color'])
                            + " bw=" + str(arrjunctionsummary['bw'])
                            + " silent=" + str(arrjunctionsummary['silent'])
                            + " 3d=" + str(arrjunctionsummary['3d'])
                        )
                        print(
                            "  aspect_ratio rows: " + str(arrjunctionsummary['aspect_total'])
                            + " across " + str(arrjunctionsummary['aspect_movies']) + " movies"
                            + " (" + str(arrjunctionsummary['multi_ratio_movies']) + " with 2+ ratios)"
                        )
                        print(
                            "  unmapped aspect-ratio canonicals (no T_WC_T2S_TECHNICAL row): "
                            + str(arrjunctionsummary['unmapped_count'])
                        )
                        if arrjunctionsummary['unmapped_count'] > 0:
                            arrsamples = arrjunctionsummary['unmapped'][:20]
                            for lngmovieid, strcanonical in arrsamples:
                                print("    ID_MOVIE=" + str(lngmovieid) + " -> " + str(strcanonical))
                        cp.f_setservervariable(
                            "strtmdbmoviepreprocessmediumformatrowscount",
                            str(arrjunctionsummary['color'] + arrjunctionsummary['bw']
                                + arrjunctionsummary['silent'] + arrjunctionsummary['3d']),
                            "Count of medium_format junction rows written in WIKIPEDIA_FORMAT_LINE step",
                            0
                        )
                        cp.f_setservervariable(
                            "strtmdbmoviepreprocessaspectratiorowscount",
                            str(arrjunctionsummary['aspect_total']),
                            "Count of aspect_ratio junction rows written in WIKIPEDIA_FORMAT_LINE step",
                            0
                        )
                        cp.f_setservervariable(
                            "strtmdbmoviepreprocessmultiratiomoviescount",
                            str(arrjunctionsummary['multi_ratio_movies']),
                            "Count of movies that received 2+ aspect_ratio junction rows",
                            0
                        )
                    # Persist the watermark only after a successful run (full, incremental, or even a
                    # no-op run with zero changed rows) so the next run starts here. An exception
                    # earlier aborts before this line, leaving the previous watermark for retry.
                    cp.f_setservervariable(
                        "strtmdbmoviepreprocesswikipediaformatlinelastrun",
                        strrunstart,
                        "Start datetime of the last successful WIKIPEDIA_FORMAT_LINE run; incremental watermark on DAT_WIKIPEDIA_FORMAT_LINE",
                        0,
                    )

                    # Calculate and display execution time
                    end_time = time.time()
                    execution_time = end_time - start_time
                    print(f"Execution time: {execution_time:.2f} seconds")
                elif intindex == 2:
                    #----------------------------------------------------
                    print("WIKIPEDIA_FORMAT_LINE -> T2S_MOVIE_TECHNICAL")
                    
                    # Read T_WC_T2S_TECHNICAL table into array mapping DESCRIPTION to ID_TECHNICAL
                    print("Reading T_WC_T2S_TECHNICAL table...")
                    strsql_technical = "SELECT ID_TECHNICAL, DESCRIPTION FROM T_WC_T2S_TECHNICAL ORDER BY ID_TECHNICAL"
                    cursor.execute(strsql_technical)
                    technical_results = cursor.fetchall()
                    
                    # Create dictionary mapping DESCRIPTION to ID_TECHNICAL
                    technical_lookup = {}
                    for row in technical_results:
                        id_technical = row['ID_TECHNICAL']
                        description = row['DESCRIPTION']
                        if description:  # Only add if description is not null/empty
                            technical_lookup[description] = id_technical
                    
                    print(f"Loaded {len(technical_lookup)} technical descriptions:")
                    for desc, tech_id in list(technical_lookup.items())[:10]:  # Show first 10 entries
                        print(f"  '{desc}' -> {tech_id}")
                    if len(technical_lookup) > 10:
                        print(f"  ... and {len(technical_lookup) - 10} more entries")
                    print("")
                    
                    strsql = """SELECT ID_MOVIE, COLOR_TECHNOLOGY, FILM_TECHNOLOGY, SOUND_SYSTEM, SOUND_TECHNOLOGY, FILM_FORMAT 
FROM T_WC_TMDB_MOVIE 
WHERE WIKIPEDIA_FORMAT_LINE IS NOT NULL """
                    cursor.execute(strsql)
                    lngrowcount = cursor.rowcount
                    print(f"{lngrowcount} lines")
                    results = cursor.fetchall()
                    lnglinesprocessed = 0
                    for row in results:
                        # print("------------------------------------------")
                        lnglinesprocessed += 1
                        lngmovieid = row['ID_MOVIE']
                        strtechidlist = ""
                        print(f"{lnglinesprocessed}: ID_MOVIE={lngmovieid}")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovieid",str(lngmovieid),"Current movie ID in the TMDb database movie preprocess",0)
                        strmoviecolortech = row['COLOR_TECHNOLOGY']
                        strmoviefilmtech = row['FILM_TECHNOLOGY']
                        strmoviesoundtech = row['SOUND_TECHNOLOGY']
                        strmoviesoundsystem = row['SOUND_SYSTEM']
                        strmoviefilmformat = row['FILM_FORMAT']
                        arrtechtype = {1: 'color_technology', 2: 'film_technology', 3: 'sound_technology', 4: 'sound_system', 5: 'film_format'}
                        for inttechtype, strtechtype in arrtechtype.items():
                            strtechvalue = ""
                            if inttechtype == 1:
                                strtechvalue = strmoviecolortech
                            elif inttechtype == 2:
                                strtechvalue = strmoviefilmtech
                            elif inttechtype == 3:
                                strtechvalue = strmoviesoundtech
                            elif inttechtype == 4:
                                strtechvalue = strmoviesoundsystem
                            elif inttechtype == 5:
                                strtechvalue = strmoviefilmformat
                            if strtechvalue:
                                if strtechvalue != "":
                                    # Remove leading and trailing pipes, then split
                                    values = strtechvalue.strip('|').split('|')
                                    for index, value in enumerate(values):
                                        #print(f"{strtechtype} {index}: {value}")

                                        # Case-insensitive lookup in technical_lookup
                                        tech_id = None
                                        value_clean = value.strip()
                                        
                                        if value_clean == "":
                                            continue
                                        # First try exact match
                                        if value_clean in technical_lookup:
                                            tech_id = technical_lookup[value_clean]
                                            #print(f"  Found exact match: {value_clean} -> {tech_id}")
                                        else:
                                            # Try case-insensitive search
                                            for desc, desc_id in technical_lookup.items():
                                                if desc.lower() == value_clean.lower():
                                                    tech_id = desc_id
                                                    #print(f"  Found case-insensitive match: {value_clean} -> {tech_id}")
                                                    break
                                        
                                        # If not found, add to T_WC_T2S_TECHNICAL table
                                        if tech_id is None and value_clean != "":
                                            #print(f"  Value '{value_clean}' not found in technical_lookup, adding to database...")
                                            
                                            # Prepare data for insertion
                                            arrtechnicalcouples = {
                                                'DESCRIPTION': value_clean,
                                                'TECHNICAL_TYPE': strtechtype
                                            }
                                            
                                            strsqltablename = "T_WC_T2S_TECHNICAL"
                                            strsqlupdatecondition = f"DESCRIPTION = '{value_clean}' AND TECHNICAL_TYPE = '{strtechtype}'"
                                            
                                            # Insert using cp.f_sqlupdatearray()
                                            new_tech_id = cp.f_sqlupdatearray(strsqltablename, arrtechnicalcouples, strsqlupdatecondition, 1)
                                            
                                            if new_tech_id:
                                                # Add to local lookup dictionary for future use
                                                technical_lookup[value_clean] = new_tech_id
                                                #print(f"  Successfully added '{value_clean}' with ID: {new_tech_id}")
                                                tech_id = new_tech_id
                                            #else:
                                                #print(f"  Failed to add '{value_clean}' to database")
                                        if tech_id > 0:
                                            # Insert into T2S_MOVIE_TECHNICAL table
                                            if strtechidlist != "":
                                                strtechidlist += ","
                                            strtechidlist += str(tech_id)
                                            arrtechnicalcouples = {
                                                'ID_MOVIE': lngmovieid,
                                                'ID_TECHNICAL': tech_id
                                            }
                                            strsqlupdatecondition = "ID_MOVIE = " + str(lngmovieid) + " AND ID_TECHNICAL = " + str(tech_id)
                                            cp.f_sqlupdatearray("T_WC_T2S_MOVIE_TECHNICAL", arrtechnicalcouples, strsqlupdatecondition, 1)
                        if strtechidlist != "":
                            # Scoped delete (EXTEND_T2S_TECHNICAL.md §12.5.5): only clear stale rows
                            # whose ID_TECHNICAL belongs to one of the five TECHNICAL_TYPEs this op
                            # owns. Medium_format / aspect_ratio rows (written by op 1) are out of
                            # this scope and must survive a re-run of op 2.
                            strsqldelete = (
                                "DELETE FROM " + cp.strsqlns + "T2S_MOVIE_TECHNICAL "
                                "WHERE ID_MOVIE = " + str(lngmovieid) + " "
                                "AND ID_TECHNICAL NOT IN (" + strtechidlist + ") "
                                "AND ID_TECHNICAL IN ("
                                "  SELECT ID_TECHNICAL FROM T_WC_T2S_TECHNICAL "
                                "  WHERE TECHNICAL_TYPE IN ("
                                "    'color_technology','film_technology','sound_system',"
                                "    'sound_technology','film_format'"
                                "  )"
                                ") "
                            )
                            cursor2.execute(strsqldelete)
                            cp.connectioncp.commit()

                elif intindex == 62:
                    print("Link Wikidata items to T2S technical processing")
                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Wikipedia entity linking for T2S technical","Current sub process in the TMDb database movie preprocess",0)
                    strsqltechnical = ""
                    strsqltechnical += "SELECT ID_TECHNICAL, DESCRIPTION, ID_WIKIDATA "
                    strsqltechnical += "FROM T_WC_T2S_TECHNICAL "
                    strsqltechnical += "WHERE DESCRIPTION IS NOT NULL AND DESCRIPTION <> '' "
                    strsqltechnical += "AND (DELETED IS NULL OR DELETED = 0) "
                    strsqltechnical += "AND ( "
                    strsqltechnical += "    (ID_WIKIDATA IS NULL OR ID_WIKIDATA = '') "
                    strsqltechnical += "    OR (ID_WIKIDATA <> '' AND (WIKIDATA_LABEL IS NULL OR WIKIDATA_LABEL = '')) "
                    strsqltechnical += ") "
                    strsqltechnical += "ORDER BY TIM_WIKIPEDIA_SEARCH ASC, ID_TECHNICAL ASC "
                    print(strsqltechnical)
                    cursor2.execute(strsqltechnical)
                    print("Number of rows: " + str(cursor2.rowcount))
                    results = cursor2.fetchall()
                    session = requests.Session()
                    strwikimediauseragent = os.getenv("WIKIMEDIA_USER_AGENT", "tmdb-movie-preprocess/1.0")
                    session.headers.update({"User-Agent": strwikimediauseragent})
                    session.wikimedia_request_delay_seconds = float(os.getenv("WIKIMEDIA_REQUEST_DELAY_SECONDS", "0.25"))
                    session.wikimedia_backoff_seconds = float(os.getenv("WIKIMEDIA_BACKOFF_SECONDS", "1.0"))
                    session.wikimedia_max_retries = int(os.getenv("WIKIMEDIA_MAX_RETRIES", "4"))
                    session.wikimedia_timeout_seconds = float(os.getenv("WIKIMEDIA_TIMEOUT_SECONDS", "20"))
                    arrentitytypecache = {}
                    for row in results:
                        lngtechnicalid = row['ID_TECHNICAL']
                        strtechnicaldescription = row['DESCRIPTION'] or ''
                        strexistingwikidataid = (row['ID_WIKIDATA'] or '').strip()
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrenttechnicalid",str(lngtechnicalid),"Current technical ID in the TMDb database movie preprocess",0)
                        if strexistingwikidataid:
                            # Enrichment branch: ID_WIKIDATA already set (manual or legacy).
                            # Trust it and fetch the English label from Wikidata directly.
                            print("Enriching technical: " + str(lngtechnicalid) + ": " + strtechnicaldescription + " (existing ID_WIKIDATA=" + strexistingwikidataid + ")")
                            try:
                                arrsummary = f_wikidataentitysummary(session, strexistingwikidataid, arrentitytypecache)
                            except Exception as exc:
                                print("Wikidata enrichment error for technical " + str(lngtechnicalid) + " (" + strexistingwikidataid + "): " + str(exc))
                                arrtechnicalcouples = {
                                    "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                cp.f_sqlupdatearray("T_WC_T2S_TECHNICAL",arrtechnicalcouples,"ID_TECHNICAL = " + str(lngtechnicalid),0)
                                continue
                            strwikidatalabel = (arrsummary or {}).get("label", "") or ""
                            if strwikidatalabel:
                                print("Enriched technical '" + strtechnicaldescription + "' (" + strexistingwikidataid + ") with label '" + strwikidatalabel + "'")
                                arrtechnicalcouples = {
                                    "WIKIDATA_LABEL": strwikidatalabel,
                                    "CONFIDENCE": 1.0,
                                    "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                            else:
                                print("No Wikidata label resolved for technical '" + strtechnicaldescription + "' (" + strexistingwikidataid + ")")
                                arrtechnicalcouples = {
                                    "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                            cp.f_sqlupdatearray("T_WC_T2S_TECHNICAL",arrtechnicalcouples,"ID_TECHNICAL = " + str(lngtechnicalid),0)
                            continue
                        print("Processing technical: " + str(lngtechnicalid) + ": " + strtechnicaldescription)
                        try:
                            arrmatch = f_linktmdbkeywordtowikidata(session, strtechnicaldescription, arrentitytypecache)
                        except Exception as exc:
                            print("Wikipedia/Wikidata linking error for technical " + str(lngtechnicalid) + ": " + str(exc))
                            arrtechnicalcouples = {
                                "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            cp.f_sqlupdatearray("T_WC_T2S_TECHNICAL",arrtechnicalcouples,"ID_TECHNICAL = " + str(lngtechnicalid),0)
                            continue
                        if arrmatch and arrmatch.get("wikibase_item"):
                            print("Matched technical '" + strtechnicaldescription + "' to " + arrmatch.get("title", "") + " (" + arrmatch["wikibase_item"] + ") with confidence " + str(round(arrmatch.get("confidence", 0.0), 4)))
                            arrtechnicalcouples = {
                                "ID_WIKIDATA": arrmatch["wikibase_item"],
                                "WIKIDATA_LABEL": arrmatch.get("wikidata_label", ""),
                                "CONFIDENCE": arrmatch.get("confidence", 0.0),
                                "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            cp.f_sqlupdatearray("T_WC_T2S_TECHNICAL",arrtechnicalcouples,"ID_TECHNICAL = " + str(lngtechnicalid),0)
                        else:
                            print("No match found for technical '" + strtechnicaldescription + "'")
                            arrtechnicalcouples = {
                                "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            cp.f_sqlupdatearray("T_WC_T2S_TECHNICAL",arrtechnicalcouples,"ID_TECHNICAL = " + str(lngtechnicalid),0)

                elif intindex == 60:
                    print("Link Wikidata items to topics processing")
                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Wikipedia entity linking for TMDb keywords","Current sub process in the TMDb database movie preprocess",0)
                    strsqlkeywords = ""
                    strsqlkeywords += "SELECT ID_KEYWORD, NAME "
                    strsqlkeywords += "FROM T_WC_TMDB_KEYWORD "
                    strsqlkeywords += "WHERE NAME IS NOT NULL AND NAME <> '' "
                    #strsqlkeywords += "AND (ID_WIKIDATA IS NULL OR ID_WIKIDATA = '') "
                    # I may include the following condition so that only keywords that are topics will be processed
                    strsqlkeywords += "AND USED_FOR_T2S_TOPIC = 1 "
                    strsqlkeywords += "ORDER BY TIM_WIKIPEDIA_SEARCH ASC, ID_KEYWORD ASC "
                    strsqlkeywords += "LIMIT 3000 "
                    # The LIMIT is for processing 3000 keywords a day so 90000 for 30 days (full keyword list)
                    print(strsqlkeywords)
                    cursor2.execute(strsqlkeywords)
                    print("Number of rows: " + str(cursor2.rowcount))
                    results = cursor2.fetchall()
                    session = requests.Session()
                    strwikimediauseragent = os.getenv("WIKIMEDIA_USER_AGENT", "tmdb-movie-preprocess/1.0")
                    session.headers.update({"User-Agent": strwikimediauseragent})
                    session.wikimedia_request_delay_seconds = float(os.getenv("WIKIMEDIA_REQUEST_DELAY_SECONDS", "0.25"))
                    session.wikimedia_backoff_seconds = float(os.getenv("WIKIMEDIA_BACKOFF_SECONDS", "1.0"))
                    session.wikimedia_max_retries = int(os.getenv("WIKIMEDIA_MAX_RETRIES", "4"))
                    session.wikimedia_timeout_seconds = float(os.getenv("WIKIMEDIA_TIMEOUT_SECONDS", "20"))
                    arrentitytypecache = {}
                    for row in results:
                        lngkeywordid = row['ID_KEYWORD']
                        strkeywordname = row['NAME'] or ''
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentkeywordid",str(lngkeywordid),"Current keyword ID in the TMDb database movie preprocess",0)
                        print("Processing keyword: " + str(lngkeywordid) + ": " + strkeywordname)
                        try:
                            arrmatch = f_linktmdbkeywordtowikidata(session, strkeywordname, arrentitytypecache)
                        except Exception as exc:
                            print("Wikipedia/Wikidata linking error for keyword " + str(lngkeywordid) + ": " + str(exc))
                            arrkeywordcouples = {
                                "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            cp.f_sqlupdatearray("T_WC_TMDB_KEYWORD",arrkeywordcouples,"ID_KEYWORD = " + str(lngkeywordid),0)
                            continue
                        if arrmatch and arrmatch.get("wikibase_item"):
                            print("Matched keyword '" + strkeywordname + "' to " + arrmatch.get("title", "") + " (" + arrmatch["wikibase_item"] + ") with confidence " + str(round(arrmatch.get("confidence", 0.0), 4)))
                            arrkeywordcouples = {
                                "ID_WIKIDATA": arrmatch["wikibase_item"],
                                "WIKIDATA_LABEL": arrmatch.get("wikidata_label", ""),
                                "CONFIDENCE": arrmatch.get("confidence", 0.0),
                                "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            cp.f_sqlupdatearray("T_WC_TMDB_KEYWORD",arrkeywordcouples,"ID_KEYWORD = " + str(lngkeywordid),0)
                        else:
                            print("No match found for keyword '" + strkeywordname + "'")
                            arrkeywordcouples = {
                                "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            cp.f_sqlupdatearray("T_WC_TMDB_KEYWORD",arrkeywordcouples,"ID_KEYWORD = " + str(lngkeywordid),0)
                elif intindex == 63:
                    # Wikidata entity linking for TMDb companies (pilot). Mirrors the
                    # keyword linker (Process 60) but passes a per-entity ALLOWLIST of
                    # accepted P31 types, so only genuine companies/organizations match
                    # (not a person or a film sharing the name). Tune the allowlist
                    # after reviewing real match results, then replicate to
                    # network / genre / character.
                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Wikipedia entity linking for TMDb companies","Current sub process in the TMDb database movie preprocess",0)
                    # Tiered P31 allowlist. TRUSTED = unambiguously a media company
                    # (film OR broadcast); a match on one of these is trusted at face
                    # value. GENERIC = business/organization types SHARED with
                    # airlines, insurers, retailers, telecoms... A match that qualifies
                    # ONLY through a generic type (no media-specific type) is a
                    # brand-collision risk: the first pilot run produced Spirit ->
                    # Spirit Airlines, Allianz -> Allianz SE, American Eagle ->
                    # American Eagle Outfitters, pro-ject -> Pro-Ject Audio -- some at
                    # confidence 1.0 (exact name match on a famous non-film brand).
                    # Such generic-only matches are QUARANTINED below (CONFIDENCE
                    # capped to a sentinel) so the review query surfaces them and
                    # downstream consumers thresholding on CONFIDENCE skip them,
                    # instead of a wrong link entering silently.
                    arrcompanystrongtypes = {
                        "Q1762059",   # film production company
                        "Q375336",    # film studio
                        "Q1107679",   # animation studio
                        "Q735427",    # Poverty Row (low-budget studio classification --
                                      # the sole P31 of Monogram, Republic, PRC, Grand
                                      # National...; unambiguously film-industry, no
                                      # collision risk). Added 2026-06-29 after the pilot
                                      # review found exact-title studios rejected on type.
                    }
                    # Broadcast / TV media types -- added 2026-06-24 after the pilot
                    # review showed the film-only allowlist gated out high-MOVIE_COUNT
                    # broadcasters and distributors (BBC, ZDF, NBC, ITV, RAI, Canal+,
                    # Lifetime, History...). These are media-specific and collide
                    # essentially never with non-media brands, so they are TRUSTED like
                    # the strong film types, NOT quarantined. QIDs label-verified
                    # against Wikidata; derived from the missed-P31 review query
                    # (doc/sql/wikidata-company-missed-p31.sql).
                    arrcompanybroadcasttypes = {
                        "Q11396960",  # production company
                        "Q10689397",  # television production company
                        "Q368290",    # film distributor
                        "Q1616075",   # television station
                        "Q1254874",   # television network
                        "Q1126006",   # public broadcaster
                        "Q15265344",  # broadcaster
                        "Q26398",     # public broadcasting
                        "Q561068",    # specialty channel
                        "Q5009242",   # cable channel
                    }
                    # Trusted = film + broadcast media types (kept at real confidence).
                    arrcompanytrustedtypes = arrcompanystrongtypes | arrcompanybroadcasttypes
                    arrcompanygenerictypes = {
                        "Q4830453",   # business
                        "Q783794",    # company
                        "Q6881511",   # enterprise
                        "Q891723",    # public company
                        "Q43229",     # organization
                        "Q161726",    # multinational corporation
                    }
                    arrcompanyacceptedtypes = arrcompanytrustedtypes | arrcompanygenerictypes
                    # Sentinel confidence for generic-only (brand-collision-risk)
                    # matches. Kept below the linker's real-match floor (0.92) so a
                    # CONFIDENCE >= 0.9 downstream filter cleanly excludes them.
                    dblgenericonlyconfidencecap = 0.50
                    strsqlcompanies = ""
                    strsqlcompanies += "SELECT ID_COMPANY, NAME "
                    strsqlcompanies += "FROM T_WC_TMDB_COMPANY "
                    strsqlcompanies += "WHERE NAME IS NOT NULL AND NAME <> '' "
                    strsqlcompanies += "AND (DELETED IS NULL OR DELETED = 0) "
                    strsqlcompanies += "ORDER BY TIM_WIKIPEDIA_SEARCH ASC, ID_COMPANY ASC "
                    strsqlcompanies += "LIMIT 3000 "
                    # The LIMIT processes 3000 companies a day (rolling, TIM-ordered so
                    # never-searched rows come first). Re-runs keep refreshing the oldest.
                    print(strsqlcompanies)
                    cursor2.execute(strsqlcompanies)
                    print("Number of rows: " + str(cursor2.rowcount))
                    results = cursor2.fetchall()
                    session = requests.Session()
                    strwikimediauseragent = os.getenv("WIKIMEDIA_USER_AGENT", "tmdb-movie-preprocess/1.0")
                    session.headers.update({"User-Agent": strwikimediauseragent})
                    session.wikimedia_request_delay_seconds = float(os.getenv("WIKIMEDIA_REQUEST_DELAY_SECONDS", "0.25"))
                    session.wikimedia_backoff_seconds = float(os.getenv("WIKIMEDIA_BACKOFF_SECONDS", "1.0"))
                    session.wikimedia_max_retries = int(os.getenv("WIKIMEDIA_MAX_RETRIES", "4"))
                    session.wikimedia_timeout_seconds = float(os.getenv("WIKIMEDIA_TIMEOUT_SECONDS", "20"))
                    arrentitytypecache = {}
                    for row in results:
                        lngcompanyid = row['ID_COMPANY']
                        strcompanyname = row['NAME'] or ''
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentcompanyid",str(lngcompanyid),"Current company ID in the TMDb database movie preprocess",0)
                        print("Processing company: " + str(lngcompanyid) + ": " + strcompanyname)
                        try:
                            arrmatch = f_linktmdbkeywordtowikidata(session, strcompanyname, arrentitytypecache, arrcompanyacceptedtypes, arrcompanytrustedtypes)
                        except Exception as exc:
                            print("Wikipedia/Wikidata linking error for company " + str(lngcompanyid) + ": " + str(exc))
                            arrcompanycouples = {
                                "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            cp.f_sqlupdatearray("T_WC_TMDB_COMPANY",arrcompanycouples,"ID_COMPANY = " + str(lngcompanyid),0)
                            continue
                        if arrmatch and arrmatch.get("wikibase_item"):
                            strwikibaseitem = arrmatch["wikibase_item"]
                            dblconfidence = arrmatch.get("confidence", 0.0)
                            # Gate on the matched entity's P31 tier. The linker cached
                            # the entity's instance-of ids during acceptance, so no
                            # extra Wikidata call is needed here. A match that has NO
                            # media-specific type (passed only via a generic business
                            # type) is a brand-collision risk -> cap its confidence to
                            # quarantine it for review rather than trust it.
                            arrmatchinstanceof = arrentitytypecache.get(strwikibaseitem, {}).get("instanceof", set())
                            booltrustedtype = bool(arrmatchinstanceof & arrcompanytrustedtypes)
                            # An exact-TITLE match (the resolved Wikipedia page is
                            # literally titled the company name) is a strong correctness
                            # signal even when Wikidata types the entity with only a
                            # generic corporate P31 (many legit studios are typed
                            # `business`/`enterprise`, not `film studio`: Toei, Shochiku,
                            # Disney Television Animation...), so it is trusted at face
                            # value. Gate on the explicit match_type, NOT on the
                            # confidence value: a FUZZY match can also score 1.0 via
                            # token overlap (single-word brand fully contained in a longer
                            # title -- "Spirit" -> "Spirit Airlines", "Apple" -> "Apple
                            # Inc."), and those generic-only fuzzy hits must stay
                            # quarantined as brand-collision risks.
                            boolexacttitlematch = arrmatch.get("match_type") == "exact_title"
                            if not booltrustedtype and not boolexacttitlematch and dblconfidence > dblgenericonlyconfidencecap:
                                print("  Generic-only fuzzy P31 match for '" + strcompanyname + "' -> " + strwikibaseitem + " (types " + str(sorted(arrmatchinstanceof)) + "); quarantining confidence " + str(round(dblconfidence, 4)) + " -> " + str(dblgenericonlyconfidencecap))
                                dblconfidence = dblgenericonlyconfidencecap
                            print("Matched company '" + strcompanyname + "' to " + arrmatch.get("title", "") + " (" + strwikibaseitem + ") with confidence " + str(round(dblconfidence, 4)))
                            arrcompanycouples = {
                                "ID_WIKIDATA": strwikibaseitem,
                                "WIKIDATA_LABEL": arrmatch.get("wikidata_label", ""),
                                "CONFIDENCE": dblconfidence,
                                "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            cp.f_sqlupdatearray("T_WC_TMDB_COMPANY",arrcompanycouples,"ID_COMPANY = " + str(lngcompanyid),0)
                        else:
                            print("No match found for company '" + strcompanyname + "'")
                            arrcompanycouples = {
                                "TIM_WIKIPEDIA_SEARCH": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            cp.f_sqlupdatearray("T_WC_TMDB_COMPANY",arrcompanycouples,"ID_COMPANY = " + str(lngcompanyid),0)
                elif intindex == 61:
                    print("Link Wikidata items to collections processing")
                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Link Wikidata items to T2S collections","Current sub process in the TMDb database movie preprocess",0)
                    strsqlcollections = ""
                    strsqlcollections += "SELECT ID_T2S_COLLECTION "
                    strsqlcollections += "FROM T_WC_T2S_COLLECTION "
                    strsqlcollections += "WHERE ID_WIKIDATA IS NULL OR ID_WIKIDATA = '' "
                    strsqlcollections += "ORDER BY ID_T2S_COLLECTION ASC "
                    print(strsqlcollections)
                    cursor2.execute(strsqlcollections)
                    print("Number of rows: " + str(cursor2.rowcount))
                    arrcollections = cursor2.fetchall()
                    for row in arrcollections:
                        lngcollectionid = row['ID_T2S_COLLECTION']
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentcollectionid",str(lngcollectionid),"Current collection ID in the TMDb database movie preprocess",0)
                        print("Processing collection: " + str(lngcollectionid))
                        strsqlmovies = ""
                        strsqlmovies += "SELECT T_WC_T2S_MOVIE.ID_WIKIDATA "
                        strsqlmovies += "FROM T_WC_T2S_MOVIE "
                        strsqlmovies += "INNER JOIN T_WC_T2S_MOVIE_COLLECTION ON T_WC_T2S_MOVIE.ID_MOVIE = T_WC_T2S_MOVIE_COLLECTION.ID_MOVIE "
                        strsqlmovies += "WHERE T_WC_T2S_MOVIE_COLLECTION.ID_T2S_COLLECTION = " + str(lngcollectionid) + " "
                        strsqlmovies += "AND T_WC_T2S_MOVIE.ID_WIKIDATA IS NOT NULL AND T_WC_T2S_MOVIE.ID_WIKIDATA <> '' "
                        print(strsqlmovies)
                        cursor3.execute(strsqlmovies)
                        arrmovies = cursor3.fetchall()
                        arrmoviewikidataids = sorted({row3['ID_WIKIDATA'] for row3 in arrmovies if row3.get('ID_WIKIDATA')})
                        intmoviecount = len(arrmoviewikidataids)
                        if intmoviecount == 0:
                            continue
                        strwikidataidlist = "'" + "','".join(arrmoviewikidataids) + "'"
                        strsqlitem = ""
                        strsqlitem += "SELECT wv.ID_ITEM "
                        strsqlitem += "FROM T_WC_WIKIDATA_ITEM_VALUE wv "
                        strsqlitem += "JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT "
                        strsqlitem += "WHERE w.ID_WIKIDATA IN (" + strwikidataidlist + ") "
                        strsqlitem += "AND w.ID_PROPERTY = 'P179' "
                        strsqlitem += "AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated') "
                        strsqlitem += "GROUP BY wv.ID_ITEM "
                        strsqlitem += "HAVING COUNT(DISTINCT w.ID_WIKIDATA) = " + str(intmoviecount) + " "
                        strsqlitem += "AND (SELECT COUNT(DISTINCT w2.ID_WIKIDATA) "
                        strsqlitem += "FROM T_WC_WIKIDATA_ITEM_VALUE wv2 "
                        strsqlitem += "JOIN T_WC_WIKIDATA_STATEMENT w2 ON w2.ID_STATEMENT = wv2.ID_STATEMENT "
                        strsqlitem += "WHERE wv2.ID_ITEM = wv.ID_ITEM "
                        strsqlitem += "AND w2.ID_PROPERTY = 'P179' "
                        strsqlitem += "AND (w2.`RANK` IS NULL OR w2.`RANK` <> 'deprecated')) = " + str(intmoviecount) + " "
                        strsqlitem += "ORDER BY wv.ID_ITEM ASC "
                        print(strsqlitem)
                        cursor4.execute(strsqlitem)
                        arritems = cursor4.fetchall()
                        if arritems:
                            strwikidataitemid = arritems[0]['ID_ITEM']
                            print("Matched collection " + str(lngcollectionid) + " to Wikidata item " + strwikidataitemid)
                            arrcollectioncouples = {
                                "ID_WIKIDATA": strwikidataitemid
                            }
                            cp.f_sqlupdatearray("T_WC_T2S_COLLECTION",arrcollectioncouples,"ID_T2S_COLLECTION = " + str(lngcollectionid),0)
                elif intindex == 3:
                    #----------------------------------------------------
                    print("T2S_TOPIC processing")
                    teltopic = EntityTelemetry("topic", 3, "topic")
                    teltopic.begin()
                    # ------------------------------------------------------------------
                    # Rolling refresh batch selection.
                    # Instead of reprocessing every qualifying keyword on every run, we
                    # rotate through them over a ~30-day cycle using TIM_T2S_TOPIC_REFRESH
                    # (same pattern as the Wikidata linker in processes 60/62). The
                    # MOVIE_COUNT / SERIE_COUNT / KPI / topic-build passes below are all
                    # scoped to this single batch so each keyword's counts, KPIs and topic
                    # rows are rebuilt together and stay mutually consistent.
                    # ------------------------------------------------------------------
                    lngrefreshcycledays = 30
                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Select keyword refresh batch","Current sub process in the TMDb database movie preprocess",0)
                    cursor.execute("SELECT COUNT(*) AS N FROM T_WC_TMDB_KEYWORD WHERE USED_FOR_T2S_TOPIC > 0 OR USE_FOR_TAGGING > 0")
                    lngqualifying = cursor.fetchone()['N']
                    # Size the daily batch so the whole qualifying set rotates within the
                    # cycle, with +30% headroom for growth and missed runs. ceil(N*1.3/days).
                    lngtopicrefreshbatchsize = max(1, -(-(lngqualifying * 13) // (lngrefreshcycledays * 10)))
                    strsqlbatch = ""
                    strsqlbatch += "SELECT ID_KEYWORD FROM T_WC_TMDB_KEYWORD "
                    strsqlbatch += "WHERE (USED_FOR_T2S_TOPIC > 0 OR USE_FOR_TAGGING > 0) "
                    strsqlbatch += "AND (TIM_T2S_TOPIC_REFRESH IS NULL OR TIM_T2S_TOPIC_REFRESH < (NOW() - INTERVAL " + str(lngrefreshcycledays) + " DAY)) "
                    strsqlbatch += "ORDER BY CASE WHEN TIM_T2S_TOPIC_REFRESH IS NULL THEN 0 ELSE 1 END, TIM_T2S_TOPIC_REFRESH ASC, ID_KEYWORD ASC "
                    strsqlbatch += "LIMIT " + str(lngtopicrefreshbatchsize) + " "
                    print(strsqlbatch)
                    cursor.execute(strsqlbatch)
                    arrbatchkeywordids = [row['ID_KEYWORD'] for row in cursor.fetchall()]
                    if arrbatchkeywordids:
                        strkeywordinclause = "(" + ",".join(str(lngid) for lngid in arrbatchkeywordids) + ")"
                    else:
                        # No keyword is due: a sentinel that matches no real row keeps every
                        # downstream "ID_KEYWORD IN <clause>" valid and turns the run into a no-op.
                        strkeywordinclause = "(-1)"
                    print(f"Keyword refresh batch: {len(arrbatchkeywordids)} of {lngqualifying} qualifying keyword(s) (cycle {lngrefreshcycledays} days, batch size {lngtopicrefreshbatchsize})")
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Compute MOVIE_COUNT for KEYWORD","Current sub process in the TMDb database movie preprocess",0)
                        # Reset counts for the batch first, so keywords that have lost all of
                        # their movie/serie links settle to 0 (otherwise the GROUP-BY passes
                        # below, which only return keywords WITH links, would leave stale counts
                        # and a wrong IS_EMPTY KPI).
                        cursor2.execute("UPDATE T_WC_TMDB_KEYWORD SET MOVIE_COUNT = 0, SERIE_COUNT = 0 WHERE ID_KEYWORD IN " + strkeywordinclause)
                        # Compute MOVIE_COUNT for KEYWORD
                        print("Compute MOVIE_COUNT for KEYWORD")
                        strsqlcompanies = f"""
SELECT COUNT(DISTINCT T_WC_T2S_MOVIE.ID_MOVIE) AS COMPTE, T_WC_TMDB_KEYWORD.NAME, T_WC_TMDB_KEYWORD.ID_KEYWORD
FROM T_WC_T2S_MOVIE
JOIN T_WC_TMDB_MOVIE_KEYWORD ON T_WC_T2S_MOVIE.ID_MOVIE = T_WC_TMDB_MOVIE_KEYWORD.ID_MOVIE
JOIN T_WC_TMDB_KEYWORD ON T_WC_TMDB_MOVIE_KEYWORD.ID_KEYWORD = T_WC_TMDB_KEYWORD.ID_KEYWORD
WHERE T_WC_TMDB_KEYWORD.ID_KEYWORD IN {strkeywordinclause}
GROUP BY T_WC_TMDB_KEYWORD.NAME
ORDER BY COMPTE DESC """
                        print(strsqlcompanies)
                        cursor2.execute(strsqlcompanies)
                        print("Number of rows: " + str(cursor2.rowcount))
                        results = cursor2.fetchall()
                        for row in results:
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentkeywordid",str(row['ID_KEYWORD']),"Current keyword ID in the TMDb database movie preprocess",0)
                            #print(row)
                            arrcompanycouples = {}
                            arrcompanycouples["MOVIE_COUNT"] = row['COMPTE']
                            cp.f_sqlupdatearray("T_WC_TMDB_KEYWORD",arrcompanycouples,"ID_KEYWORD = " + str(row['ID_KEYWORD']),0)
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Compute SERIE_COUNT for KEYWORD","Current sub process in the TMDb database movie preprocess",0)
                        # Compute SERIE_COUNT for KEYWORD
                        print("Compute SERIE_COUNT for KEYWORD")
                        strsqlcompanies = f"""
SELECT COUNT(DISTINCT T_WC_T2S_SERIE.ID_SERIE) AS COMPTE, T_WC_TMDB_KEYWORD.NAME, T_WC_TMDB_KEYWORD.ID_KEYWORD
FROM T_WC_T2S_SERIE
JOIN T_WC_TMDB_SERIE_KEYWORD ON T_WC_T2S_SERIE.ID_SERIE = T_WC_TMDB_SERIE_KEYWORD.ID_SERIE
JOIN T_WC_TMDB_KEYWORD ON T_WC_TMDB_SERIE_KEYWORD.ID_KEYWORD = T_WC_TMDB_KEYWORD.ID_KEYWORD
WHERE T_WC_TMDB_KEYWORD.ID_KEYWORD IN {strkeywordinclause}
GROUP BY T_WC_TMDB_KEYWORD.NAME
ORDER BY COMPTE DESC """
                        print(strsqlcompanies)
                        cursor2.execute(strsqlcompanies)
                        print("Number of rows: " + str(cursor2.rowcount))
                        results = cursor2.fetchall()
                        for row in results:
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentkeywordid",str(row['ID_KEYWORD']),"Current keyword ID in the TMDb database movie preprocess",0)
                            #print(row)
                            arrcompanycouples = {}
                            arrcompanycouples["SERIE_COUNT"] = row['COMPTE']
                            cp.f_sqlupdatearray("T_WC_TMDB_KEYWORD",arrcompanycouples,"ID_KEYWORD = " + str(row['ID_KEYWORD']),0)
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Compute KPI for KEYWORD","Current sub process in the TMDb database movie preprocess",0)
                        print("Compute KPI for KEYWORD")
                        strsqlkeywords = ""
                        strsqlkeywords += "SELECT * FROM T_WC_TMDB_KEYWORD "
                        strsqlkeywords += "WHERE ID_KEYWORD IN " + strkeywordinclause + " "
                        strsqlkeywords += "ORDER BY ID_KEYWORD ASC "
                        cursor2.execute(strsqlkeywords)
                        print("Number of rows: " + str(cursor2.rowcount))
                        results = cursor2.fetchall()
                        for row in results:
                            lngkeywordid = row['ID_KEYWORD']
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentkeywordid",str(lngkeywordid),"Current keyword ID in the TMDb database movie preprocess",0)
                            strkeywordname = row['NAME']
                            # Compute word count using space, comma, and other punctuation as separators
                            lngnamewordcount = 0
                            try:
                                lngnamewordcount = len(re.findall(r'\b\w+\b', strkeywordname))
                            except:
                                pass
                            print(f"Keyword: '{strkeywordname}' - Word count: {lngnamewordcount}")
                            
                            # Check if strkeywordname exists in T_WC_TMDB_PERSON.NAME
                            strsqlperson = "SELECT NAME FROM T_WC_TMDB_PERSON WHERE NAME = %s"
                            cursor3.execute(strsqlperson, (strkeywordname,))
                            person_result = cursor3.fetchall()
                            intisperson = 0
                            for row3 in person_result:
                                strpersonname = row3['NAME']
                                if strpersonname == strkeywordname:
                                    intisperson = 1
                                    break
                            
                            lngmoviecount = 0
                            if row['MOVIE_COUNT'] is not None:
                                lngmoviecount = row['MOVIE_COUNT']
                            lngseriecount = 0
                            if row['SERIE_COUNT'] is not None:
                                lngseriecount = row['SERIE_COUNT']
                            lngtotalcount = lngmoviecount + lngseriecount
                            if lngtotalcount >= 2:
                                intisempty = 0
                            else:
                                intisempty = 1
                            #print(row)
                            
                            arrkeywordcouples = {}
                            arrkeywordcouples["IS_EMPTY"] = intisempty
                            arrkeywordcouples["IS_PERSON"] = intisperson
                            arrkeywordcouples["NAME_WORD_COUNT"] = lngnamewordcount
                            cp.f_sqlupdatearray("T_WC_TMDB_KEYWORD",arrkeywordcouples,"ID_KEYWORD = " + str(lngkeywordid),0)
                    if 1:
                        #arrtopics = {1: 'en-list', 2: 'fr-list', 3: 'en-collection', 4: 'fr-collection', 5: 'en-keyword'}
                        # Lists and collections are not copied to the Topic table anymore!
                        arrtopics = {5: 'en-keyword'}
                        for inttopic, strtopic in arrtopics.items():
                            strsql = ""
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess",strtopic,"Current sub process in the TMDb database movie preprocess",0)
                            if inttopic == 5:
                                strcurrentprocess = f"{inttopic}: Copying from TMDB_KEYWORD to T2S_TOPIC"
                                strsql += "SELECT 'keyword' AS TOPIC_SOURCE, 'keyword' AS TOPIC_TYPE, T_WC_TMDB_KEYWORD.ID_KEYWORD AS ID_RECORD, T_WC_TMDB_KEYWORD.NAME, '' AS OVERVIEW, 'en' AS LANG, '' AS POSTER_PATH, T_WC_TMDB_KEYWORD.ID_WIKIDATA "
                                strsql += "FROM T_WC_TMDB_KEYWORD "
                                strsql += "WHERE T_WC_TMDB_KEYWORD.ID_KEYWORD IN " + strkeywordinclause + " "
                                strsql += "ORDER BY ID_RECORD ASC "
                                #strsql += "LIMIT 10 "
                                #strsql += "LIMIT 1000 "
                                target_field_name = "TOPIC_NAME"
                            if strsql != "":
                                # Now we process the SELECT query
                                print(strsql)
                                cursor.execute(strsql)
                                lngrowcount = cursor.rowcount
                                print(f"{lngrowcount} lines")
                                lnglinesprocessed = 0
                                # Fetching all rows from the last executed statement
                                results = cursor.fetchall()
                                # Iterating through the results and printing
                                for row in results:
                                    # print("------------------------------------------")
                                    lnglinesprocessed += 1
                                    lngrecordid = row['ID_RECORD']
                                    # Stamp the refresh timestamp up-front (stamp-then-skip): even if
                                    # this keyword errors out below and we `continue`, it has rotated
                                    # out of the batch and will not be retried until the next cycle.
                                    cp.f_sqlupdatearray("T_WC_TMDB_KEYWORD", {"TIM_T2S_TOPIC_REFRESH": datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")}, "ID_KEYWORD = " + str(lngrecordid), 0)
                                    strrecordname = row['NAME']
                                    strrecordoverview = row['OVERVIEW']
                                    strrecordlang = row['LANG']
                                    strrecordtopicsource = row['TOPIC_SOURCE']
                                    strrecordtopictype = row['TOPIC_TYPE']
                                    strrecordposterpath = row['POSTER_PATH']
                                    strrecordidwikidata = row['ID_WIKIDATA'] if 'ID_WIKIDATA' in row else None
                                    strrecordwikipediaimagepath = f_getwikidataimagepath(strrecordidwikidata)
                                    print("Processing record: " + str(lngrecordid) + ": " + strrecordname + " (" + strrecordtopicsource + ")")
                                    teltopic.position(recordid=lngrecordid, currentvalue=strrecordname, currentprocess=strcurrentprocess)
                                    if target_field_name == "TOPIC_NAME":
                                        arrtopiccouples = {
                                            'ID_RECORD': lngrecordid,
                                            'TOPIC_NAME': strrecordname,
                                            'OVERVIEW': strrecordoverview,
                                            'TOPIC_SOURCE': strrecordtopicsource,
                                            'TOPIC_TYPE': strrecordtopictype,
                                            'LANG': strrecordlang,
                                            'POSTER_PATH': strrecordposterpath,
                                            'ID_WIKIDATA': strrecordidwikidata,
                                            'WIKIPEDIA_IMAGE_PATH': strrecordwikipediaimagepath
                                        }
                                    elif target_field_name == "TOPIC_NAME_FR":
                                        arrtopiccouples = {
                                            'ID_RECORD': lngrecordid,
                                            'TOPIC_NAME_FR': strrecordname,
                                            'TOPIC_SOURCE': strrecordtopicsource,
                                            'TOPIC_TYPE': strrecordtopictype,
                                            'ID_WIKIDATA': strrecordidwikidata,
                                            'WIKIPEDIA_IMAGE_PATH': strrecordwikipediaimagepath
                                        }
                                    strsqltablename = "T_WC_T2S_TOPIC"
                                    strsqlupdatecondition = f"ID_RECORD = '{lngrecordid}' AND TOPIC_SOURCE = '{strrecordtopicsource}'"
                                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(lngrecordid),"Current record in the TMDb database movie preprocess",0)
                                    
                                    strsqlmovies = ""
                                    strsqlseries = ""
                                    if inttopic == 5:
                                        # Retrieving movies for this keyword by excluding adult movies and movies without Wikidata ID
                                        strsqlmovies += "SELECT T_WC_TMDB_MOVIE_KEYWORD.ID_MOVIE, T_WC_T2S_MOVIE.IMDB_RATING_WEIGHTED "
                                        strsqlmovies += "FROM T_WC_TMDB_MOVIE_KEYWORD "
                                        strsqlmovies += "INNER JOIN T_WC_T2S_MOVIE ON T_WC_TMDB_MOVIE_KEYWORD.ID_MOVIE = T_WC_T2S_MOVIE.ID_MOVIE "
                                        strsqlmovies += "WHERE ID_KEYWORD = " + str(lngrecordid) + " "
                                        strsqlmovies += "AND T_WC_TMDB_MOVIE_KEYWORD.DELETED = 0 "
                                        strsqlmovies += "AND T_WC_TMDB_MOVIE_KEYWORD.ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_TMDB_MOVIE WHERE ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '') "
                                        strsqlmovies += "ORDER BY CASE WHEN T_WC_T2S_MOVIE.IMDB_RATING_WEIGHTED IS NULL THEN 1 ELSE 0 END, T_WC_T2S_MOVIE.IMDB_RATING_WEIGHTED DESC, T_WC_TMDB_MOVIE_KEYWORD.ID_MOVIE ASC "
                                        # Retrieving series for this keyword by excluding adult series and series without Wikidata ID
                                        strsqlseries += "SELECT T_WC_TMDB_SERIE_KEYWORD.ID_SERIE, T_WC_T2S_SERIE.IMDB_RATING_WEIGHTED "
                                        strsqlseries += "FROM T_WC_TMDB_SERIE_KEYWORD "
                                        strsqlseries += "INNER JOIN T_WC_T2S_SERIE ON T_WC_TMDB_SERIE_KEYWORD.ID_SERIE = T_WC_T2S_SERIE.ID_SERIE "
                                        strsqlseries += "WHERE ID_KEYWORD = " + str(lngrecordid) + " "
                                        strsqlseries += "AND T_WC_TMDB_SERIE_KEYWORD.DELETED = 0 "
                                        strsqlseries += "AND T_WC_TMDB_SERIE_KEYWORD.ID_SERIE IN (SELECT ID_SERIE FROM T_WC_TMDB_SERIE WHERE ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '') "
                                        strsqlseries += "ORDER BY CASE WHEN T_WC_T2S_SERIE.IMDB_RATING_WEIGHTED IS NULL THEN 1 ELSE 0 END, T_WC_T2S_SERIE.IMDB_RATING_WEIGHTED DESC, T_WC_TMDB_SERIE_KEYWORD.ID_SERIE ASC "
                                    if strsqlmovies != "":
                                        # Retrieving elements for this topic (list/collection/keyword)
                                        cursor2.execute(strsqlmovies)
                                        lngmoviecount = cursor2.rowcount
                                        resultsmovies = cursor2.fetchall()
                                        lngseriescount = 0
                                        resultsseries = []
                                        #print(f"{lngmoviecount} lines")
                                        if strsqlseries != "":
                                            cursor4.execute(strsqlseries)
                                            lngseriescount = cursor4.rowcount
                                            resultsseries = cursor4.fetchall()
                                            #print(f"{lngseriescount} lines")
                                        if lngmoviecount + lngseriescount > 1:
                                            # This topic has more than one element (movie or serie)
                                            # So we create/update this topic
                                            lngtopicid = cp.f_sqlupdatearray(strsqltablename, arrtopiccouples, strsqlupdatecondition, 1)
                                            if lngtopicid is None:
                                                strsqltopic = "SELECT ID_TOPIC FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                                cursor3.execute(strsqltopic)
                                                lngrowcount = cursor3.rowcount
                                                if lngrowcount == 0:
                                                    print("Error: Failed to create/update topic - lngtopicid is None")
                                                    continue
                                                lngtopicid = cursor3.fetchone()["ID_TOPIC"]
                                            teltopic.created()
                                            teltopic.set_entity_id(lngtopicid)
                                            if inttopic == 5:
                                                strsqldelete = "DELETE FROM T_WC_T2S_MOVIE_TOPIC WHERE ID_TOPIC = " + str(lngtopicid)
                                                cursor2.execute(strsqldelete)
                                                if strsqlseries != "":
                                                    strsqldelete = "DELETE FROM T_WC_T2S_SERIE_TOPIC WHERE ID_TOPIC = " + str(lngtopicid)
                                                    cursor2.execute(strsqldelete)
                                                # Retrieve all movies for this topic
                                                # Only processing when handling original English (records from T_WC_TMDB_LIST or T_WC_TMDB_COLLECTION or T_WC_TMDB_KEYWORD) to avoid duplicates with the translated versions
                                                lngdisplayorder = 0
                                                for row in resultsmovies:
                                                    lngmovieid = row["ID_MOVIE"]
                                                    lngdisplayorder += 1
                                                    arrmovietopiccouples = {
                                                        'ID_MOVIE': lngmovieid,
                                                        'ID_TOPIC': lngtopicid,
                                                        'DISPLAY_ORDER': lngdisplayorder
                                                    }
                                                    strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND ID_TOPIC = " + str(lngtopicid)
                                                    #print(strsqlupdatecondition2)
                                                    cp.f_sqlupdatearray("T_WC_T2S_MOVIE_TOPIC", arrmovietopiccouples, strsqlupdatecondition2, 1)
                                                if strsqlseries != "":
                                                    # Retrieve all series for this topic
                                                    lngdisplayorder = 0
                                                    for row in resultsseries:
                                                        lngseriesid = row["ID_SERIE"]
                                                        lngdisplayorder += 1
                                                        arrserietopiccouples = {
                                                            'ID_SERIE': lngseriesid,
                                                            'ID_TOPIC': lngtopicid,
                                                            'DISPLAY_ORDER': lngdisplayorder
                                                        }
                                                        strsqlupdatecondition2 = "ID_SERIE = " + str(lngseriesid) + " AND ID_TOPIC = " + str(lngtopicid)
                                                        #print(strsqlupdatecondition2)
                                                        cp.f_sqlupdatearray("T_WC_T2S_SERIE_TOPIC", arrserietopiccouples, strsqlupdatecondition2, 1)
                                                arrtopiccouples = {
                                                    'MOVIE_COUNT': lngmoviecount,
                                                    'SERIE_COUNT': lngseriescount
                                                }
                                                cp.f_sqlupdatearray(strsqltablename, arrtopiccouples, strsqlupdatecondition, 1)
                                        else:
                                            # This topic has only one element or none
                                            # So we delete this topic if it already exists
                                            strsqltablename = "T_WC_T2S_TOPIC"
                                            strsqldelete = "DELETE FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                            print(strsqldelete)
                                            cursor2.execute(strsqldelete)
                                            teltopic.deleted(cursor2.rowcount)
                                            #cursor2.commit()
                    if 1:
                        strsqltablename = "T_WC_T2S_TOPIC"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE TOPIC_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        teltopic.deleted(cursor2.rowcount)

                        strsqldelete = ""
                        strsqldelete += "DELETE FROM T_WC_T2S_TOPIC "
                        strsqldelete += "WHERE TOPIC_SOURCE = 'list' "
                        #strsqldelete += "AND ID_RECORD NOT IN (SELECT ID_LIST FROM T_WC_TMDB_LIST WHERE USE_FOR_TAGGING > 0) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqldelete = ""
                        strsqldelete += "DELETE FROM T_WC_T2S_TOPIC "
                        strsqldelete += "WHERE TOPIC_SOURCE = 'collection' "
                        #strsqldelete += "AND ID_RECORD NOT IN (SELECT ID_COLLECTION FROM T_WC_TMDB_COLLECTION) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqldelete = ""
                        strsqldelete += "DELETE FROM T_WC_T2S_TOPIC "
                        strsqldelete += "WHERE TOPIC_SOURCE = 'keyword' "
                        strsqldelete += "AND ID_RECORD NOT IN (SELECT ID_KEYWORD FROM T_WC_TMDB_KEYWORD WHERE USED_FOR_T2S_TOPIC > 0 OR USE_FOR_TAGGING > 0) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        teltopic.deleted(cursor2.rowcount)
                        
                        # Update T_WC_T2S_TOPIC ratings and popularity from movies
                        strsql = """UPDATE T_WC_T2S_TOPIC t
JOIN (
    SELECT
        mt.ID_TOPIC,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_MOVIE_TOPIC mt
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mt.ID_MOVIE
    INNER JOIN T_WC_T2S_TOPIC t2
        ON t2.ID_TOPIC = mt.ID_TOPIC
       AND t2.TOPIC_TYPE = 'keyword'
    GROUP BY mt.ID_TOPIC
) x
    ON x.ID_TOPIC = t.ID_TOPIC
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED,
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_TOPIC ratings and popularity from series
                        strsql = """UPDATE T_WC_T2S_TOPIC t
JOIN (
    SELECT
        st.ID_TOPIC,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(s.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_SERIE_TOPIC st
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = st.ID_SERIE
    INNER JOIN T_WC_T2S_TOPIC t2
        ON t2.ID_TOPIC = st.ID_TOPIC
       AND t2.TOPIC_TYPE = 'keyword'
    GROUP BY st.ID_TOPIC
) x
    ON x.ID_TOPIC = t.ID_TOPIC
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED),
    t.POPULARITY = COALESCE(t.POPULARITY, x.AVG_POPULARITY);
                        """
                        print(strsql)
                        cursor2.execute(strsql)
                    if 1:
                        # Update T_WC_T2S_COLLECTION ratings and popularity from movies
                        strsql = """UPDATE T_WC_T2S_COLLECTION t
JOIN (
    SELECT
        mc.ID_T2S_COLLECTION,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_MOVIE_COLLECTION mc
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mc.ID_MOVIE
    INNER JOIN T_WC_T2S_COLLECTION t2
        ON t2.ID_T2S_COLLECTION = mc.ID_T2S_COLLECTION
       AND t2.COLLECTION_TYPE = 'collection'
    GROUP BY mc.ID_T2S_COLLECTION
) x
    ON x.ID_T2S_COLLECTION = t.ID_T2S_COLLECTION
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED,
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_COLLECTION ratings and popularity from series
                        strsql = """UPDATE T_WC_T2S_COLLECTION t
JOIN (
    SELECT
        sc.ID_T2S_COLLECTION,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(s.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_SERIE_COLLECTION sc
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = sc.ID_SERIE
    INNER JOIN T_WC_T2S_COLLECTION t2
        ON t2.ID_T2S_COLLECTION = sc.ID_T2S_COLLECTION
       AND t2.COLLECTION_TYPE = 'collection'
    GROUP BY sc.ID_T2S_COLLECTION
) x
    ON x.ID_T2S_COLLECTION = t.ID_T2S_COLLECTION
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED),
    t.POPULARITY = COALESCE(t.POPULARITY, x.AVG_POPULARITY);
                        """
                        print(strsql)
                        cursor2.execute(strsql)
                    if 1:
                        # Update T_WC_T2S_LIST ratings and popularity from movies
                        strsql = """UPDATE T_WC_T2S_LIST t
JOIN (
    SELECT
        ml.ID_T2S_LIST,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_MOVIE_LIST ml
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = ml.ID_MOVIE
    INNER JOIN T_WC_T2S_LIST t2
        ON t2.ID_T2S_LIST = ml.ID_T2S_LIST
    GROUP BY ml.ID_T2S_LIST
) x
    ON x.ID_T2S_LIST = t.ID_T2S_LIST
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED,
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_LIST ratings and popularity from series
                        strsql = """UPDATE T_WC_T2S_LIST t
JOIN (
    SELECT
        sl.ID_T2S_LIST,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(s.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_SERIE_LIST sl
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = sl.ID_SERIE
    INNER JOIN T_WC_T2S_LIST t2
        ON t2.ID_T2S_LIST = sl.ID_T2S_LIST
    GROUP BY sl.ID_T2S_LIST
) x
    ON x.ID_T2S_LIST = t.ID_T2S_LIST
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED),
    t.POPULARITY = COALESCE(t.POPULARITY, x.AVG_POPULARITY);
                        """
                        print(strsql)
                        cursor2.execute(strsql)
                    if 1:
                        # Update T_WC_T2S_MOVEMENT ratings and popularity from movies
                        strsql = """UPDATE T_WC_T2S_MOVEMENT t
JOIN (
    SELECT
        mm.ID_MOVEMENT,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_MOVIE_MOVEMENT mm
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mm.ID_MOVIE
    INNER JOIN T_WC_T2S_MOVEMENT t2
        ON t2.ID_MOVEMENT = mm.ID_MOVEMENT
    GROUP BY mm.ID_MOVEMENT
) x
    ON x.ID_MOVEMENT = t.ID_MOVEMENT
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED,
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_MOVEMENT ratings and popularity from series
                        strsql = """UPDATE T_WC_T2S_MOVEMENT t
JOIN (
    SELECT
        sm.ID_MOVEMENT,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(s.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_SERIE_MOVEMENT sm
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = sm.ID_SERIE
    INNER JOIN T_WC_T2S_MOVEMENT t2
        ON t2.ID_MOVEMENT = sm.ID_MOVEMENT
    GROUP BY sm.ID_MOVEMENT
) x
    ON x.ID_MOVEMENT = t.ID_MOVEMENT
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED),
    t.POPULARITY = COALESCE(t.POPULARITY, x.AVG_POPULARITY);
                        """
                        print(strsql)
                        cursor2.execute(strsql)
                    if 1:
                        # Update T_WC_T2S_COMPANY ratings and popularity from movies
                        strsql = """UPDATE T_WC_T2S_COMPANY t
JOIN (
    SELECT
        mc.ID_COMPANY,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_MOVIE_COMPANY mc
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mc.ID_MOVIE
    INNER JOIN T_WC_T2S_COMPANY t2
        ON t2.ID_COMPANY = mc.ID_COMPANY
    GROUP BY mc.ID_COMPANY
) x
    ON x.ID_COMPANY = t.ID_COMPANY
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED,
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_COMPANY ratings and popularity from series
                        strsql = """UPDATE T_WC_T2S_COMPANY t
JOIN (
    SELECT
        sc.ID_COMPANY,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(s.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_SERIE_COMPANY sc
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = sc.ID_SERIE
    INNER JOIN T_WC_T2S_COMPANY t2
        ON t2.ID_COMPANY = sc.ID_COMPANY
    GROUP BY sc.ID_COMPANY
) x
    ON x.ID_COMPANY = t.ID_COMPANY
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED),
    t.POPULARITY = COALESCE(t.POPULARITY, x.AVG_POPULARITY);
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        strsqltablename = "T_WC_T2S_MOVIE_TOPIC"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_TOPIC NOT IN (SELECT ID_TOPIC FROM T_WC_T2S_TOPIC) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        #cursor2.commit()
                        strsqltablename = "T_WC_T2S_SERIE_TOPIC"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_TOPIC NOT IN (SELECT ID_TOPIC FROM T_WC_T2S_TOPIC) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        #cursor2.commit()

                    teltopic.finish()

                elif intindex == 41:
                    #----------------------------------------------------
                    print("T2S_COLLECTION processing")
                    telcollection = EntityTelemetry("collection", 41, "collection")
                    telcollection.begin()

                    arrcollections = {1: 'en-list', 2: 'fr-list', 3: 'en-collection', 4: 'fr-collection', 5: 'custom-collection'}
                    #arrcollections = {1: 'en-list', 2: 'fr-list'}    
                    for intcollection, strcollection in arrcollections.items():
                        strsql = ""
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess",strcollection,"Current sub process in the TMDb database movie preprocess",0)
                        if intcollection == 1:
                            strcurrentprocess = f"{intcollection}: Copying from TMDB_LIST to T2S_COLLECTION"
                            strsql += "SELECT 'list' AS COLLECTION_SOURCE, 'collection' AS COLLECTION_TYPE, T_WC_TMDB_LIST.ID_LIST AS ID_RECORD, T_WC_TMDB_LIST.NAME, T_WC_TMDB_LIST.DESCRIPTION AS OVERVIEW, 'en' AS LANG, T_WC_TMDB_LIST.POSTER_PATH, NULL AS ID_WIKIDATA "
                            strsql += "FROM T_WC_TMDB_LIST WHERE USED_FOR_T2S_COLLECTION > 0 "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "COLLECTION_NAME"
                        elif intcollection == 2:
                            strcurrentprocess = f"{intcollection}: Copying from T_WC_TMDB_LIST_LANG to T2S_COLLECTION"
                            strsql += "SELECT 'list' AS COLLECTION_SOURCE, 'collection' AS COLLECTION_TYPE, T_WC_TMDB_LIST.ID_LIST AS ID_RECORD, T_WC_TMDB_LIST_LANG.SHORT_NAME AS NAME, '' AS OVERVIEW, T_WC_TMDB_LIST_LANG.LANG, '' AS POSTER_PATH, NULL AS ID_WIKIDATA "
                            strsql += "FROM T_WC_TMDB_LIST "
                            strsql += "INNER JOIN T_WC_TMDB_LIST_LANG ON T_WC_TMDB_LIST.ID_LIST = T_WC_TMDB_LIST_LANG.ID_LIST "
                            strsql += "WHERE T_WC_TMDB_LIST.USED_FOR_T2S_COLLECTION > 0 "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "COLLECTION_NAME_FR"
                        elif intcollection == 3:
                            strcurrentprocess = f"{intcollection}: Copying from TMDB_COLLECTION to T2S_COLLECTION"
                            strsql += "SELECT 'collection' AS COLLECTION_SOURCE, 'collection' AS COLLECTION_TYPE, T_WC_TMDB_COLLECTION.ID_COLLECTION AS ID_RECORD, T_WC_TMDB_COLLECTION.NAME, T_WC_TMDB_COLLECTION.OVERVIEW, 'en' AS LANG, T_WC_TMDB_COLLECTION.POSTER_PATH, NULL AS ID_WIKIDATA "
                            strsql += "FROM T_WC_TMDB_COLLECTION "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "COLLECTION_NAME"
                        elif intcollection == 4:
                            strcurrentprocess = f"{intcollection}: Copying from T_WC_TMDB_COLLECTION_LANG to T2S_COLLECTION"
                            strsql += "SELECT 'collection' AS COLLECTION_SOURCE, 'collection' AS COLLECTION_TYPE, T_WC_TMDB_COLLECTION.ID_COLLECTION AS ID_RECORD, T_WC_TMDB_COLLECTION_LANG.NAME, T_WC_TMDB_COLLECTION_LANG.OVERVIEW, T_WC_TMDB_COLLECTION_LANG.LANG, T_WC_TMDB_COLLECTION_LANG.POSTER_PATH, NULL AS ID_WIKIDATA "
                            strsql += "FROM T_WC_TMDB_COLLECTION "
                            strsql += "INNER JOIN T_WC_TMDB_COLLECTION_LANG ON T_WC_TMDB_COLLECTION.ID_COLLECTION = T_WC_TMDB_COLLECTION_LANG.ID_COLLECTION "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "COLLECTION_NAME_FR"
                        elif intcollection == 5:
                            strcurrentprocess = f"{intcollection}: Copying from T_WC_CUSTOM_LIST to T2S_COLLECTION"
                            strsql += "SELECT 'custom' AS COLLECTION_SOURCE, 'collection' AS COLLECTION_TYPE, T_WC_CUSTOM_LIST.ID_CUSTOM_LIST AS ID_RECORD, T_WC_CUSTOM_LIST.LIST_NAME AS NAME, T_WC_CUSTOM_LIST.LIST_NAME_FR AS NAME_FR, T_WC_CUSTOM_LIST.OVERVIEW AS OVERVIEW, 'en' AS LANG, T_WC_CUSTOM_LIST.POSTER_PATH, NULL AS ID_WIKIDATA, T_WC_CUSTOM_LIST.ID_IMDB_LIST, T_WC_CUSTOM_LIST.WIKIDATA_PROPERTIES, T_WC_CUSTOM_LIST.TMDB_ELEMENTS, T_WC_CUSTOM_LIST.SORT_BY, T_WC_CUSTOM_LIST.TMDB_TARGET_RECORD "
                            strsql += "FROM T_WC_CUSTOM_LIST WHERE DELETED = 0 "
                            # Only processing custom collections targeting the T_WC_T2S_COLLECTION table
                            strsql += "AND TARGET_TABLE = 2 "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "COLLECTION_NAME"
                        if strsql != "":
                            # Now we process the SELECT query
                            print(strsql)
                            cursor.execute(strsql)
                            lngrowcount = cursor.rowcount
                            print(f"{lngrowcount} lines")
                            lnglinesprocessed = 0
                            # Fetching all rows from the last executed statement
                            results = cursor.fetchall()
                            # Iterating through the results and printing
                            for row in results:
                                # print("------------------------------------------")
                                lnglinesprocessed += 1
                                lngrecordid = row['ID_RECORD']
                                strrecordname = row['NAME']
                                strrecordoverview = row['OVERVIEW']
                                strrecordcollectionsource = row['COLLECTION_SOURCE']
                                strrecordcollectiontype = row['COLLECTION_TYPE']
                                strrecordposterpath = row['POSTER_PATH']
                                strrecordidwikidata = row['ID_WIKIDATA'] if 'ID_WIKIDATA' in row else None
                                print("Processing record: " + str(lngrecordid) + ": " + strrecordname + " (" + strrecordcollectionsource + ")")
                                telcollection.position(recordid=lngrecordid, currentvalue=strrecordname, currentprocess=strcurrentprocess)
                                if target_field_name == "COLLECTION_NAME":
                                    arrcollectioncouples = {
                                        'ID_RECORD': lngrecordid,
                                        'COLLECTION_NAME': strrecordname,
                                        'OVERVIEW': strrecordoverview,
                                        'COLLECTION_SOURCE': strrecordcollectionsource,
                                        'COLLECTION_TYPE': strrecordcollectiontype,
                                        'POSTER_PATH': strrecordposterpath,
                                    }
                                    if intcollection == 5:
                                        arrcollectioncouples['COLLECTION_NAME_FR'] = row['NAME_FR'] or ''
                                elif target_field_name == "COLLECTION_NAME_FR":
                                    arrcollectioncouples = {
                                        'ID_RECORD': lngrecordid,
                                        'COLLECTION_NAME_FR': strrecordname,
                                        'COLLECTION_SOURCE': strrecordcollectionsource,
                                        'COLLECTION_TYPE': strrecordcollectiontype,
                                    }
                                if strrecordidwikidata:
                                    arrcollectioncouples['ID_WIKIDATA'] = strrecordidwikidata
                                    arrcollectioncouples['WIKIPEDIA_IMAGE_PATH'] = f_getwikidataimagepath(strrecordidwikidata)
                                strsqltablename = "T_WC_T2S_COLLECTION"
                                strsqlupdatecondition = f"ID_RECORD = '{lngrecordid}' AND COLLECTION_SOURCE = '{strrecordcollectionsource}'"
                                cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(lngrecordid),"Current record in the TMDb database movie preprocess",0)
                                strtmdbtargetrecord = row['TMDB_TARGET_RECORD'] if intcollection == 5 and 'TMDB_TARGET_RECORD' in row and row['TMDB_TARGET_RECORD'] else ''
                                strtmdbtargetfieldname = ''
                                lngtmdbtargetrecordid = 0
                                lngt2stargetrecordid = 0
                                if strtmdbtargetrecord:
                                    objtmdbtargetrecordmatch = re.match(r'^\s*(ID_COLLECTION|ID_LIST)\s*=\s*(\d+)\s*$', strtmdbtargetrecord)
                                    if objtmdbtargetrecordmatch:
                                        strtmdbtargetfieldname = objtmdbtargetrecordmatch.group(1)
                                        lngtmdbtargetrecordid = int(objtmdbtargetrecordmatch.group(2))
                                        if strtmdbtargetfieldname == 'ID_COLLECTION':
                                            strsqltargetrecord = "SELECT c.ID_T2S_COLLECTION FROM T_WC_T2S_COLLECTION c INNER JOIN T_WC_TMDB_COLLECTION tc ON tc.ID_COLLECTION = c.ID_RECORD WHERE c.COLLECTION_SOURCE = 'collection' AND c.ID_RECORD = " + str(lngtmdbtargetrecordid)
                                            cursor3.execute(strsqltargetrecord)
                                            if cursor3.rowcount > 0:
                                                lngt2stargetrecordid = cursor3.fetchone()["ID_T2S_COLLECTION"]
                                        elif strtmdbtargetfieldname == 'ID_LIST':
                                            strsqltargetrecord = "SELECT c.ID_T2S_COLLECTION FROM T_WC_T2S_COLLECTION c INNER JOIN T_WC_TMDB_LIST tl ON tl.ID_LIST = c.ID_RECORD WHERE c.COLLECTION_SOURCE = 'list' AND c.ID_RECORD = " + str(lngtmdbtargetrecordid)
                                            cursor3.execute(strsqltargetrecord)
                                            if cursor3.rowcount > 0:
                                                lngt2stargetrecordid = cursor3.fetchone()["ID_T2S_COLLECTION"]
                                    else:
                                        print("Invalid TMDB_TARGET_RECORD value for custom collection " + str(lngrecordid) + ": " + strtmdbtargetrecord)
                                    if lngt2stargetrecordid == 0:
                                        print("TMDB_TARGET_RECORD target not found for custom collection " + str(lngrecordid) + ": " + strtmdbtargetrecord)
                                
                                strsqlmovies = ""
                                strsqlseries = ""
                                if intcollection == 1 or intcollection == 2:
                                    # Retrieving movies for this list by excluding adult movies and movies without Wikidata ID
                                    strsqlmovies += "SELECT ml.ID_MOVIE, m.DAT_RELEASE "
                                    strsqlmovies += "FROM T_WC_TMDB_MOVIE_LIST ml "
                                    strsqlmovies += "INNER JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = ml.ID_MOVIE "
                                    strsqlmovies += "WHERE ml.ID_LIST = " + str(lngrecordid) + " "
                                    strsqlmovies += "AND ml.DELETED = 0 "
                                    strsqlmovies += "AND m.ADULT = 0 "
                                    strsqlmovies += "AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                    strsqlmovies += "ORDER BY CASE WHEN m.DAT_RELEASE IS NULL THEN 1 ELSE 0 END, m.DAT_RELEASE ASC, ml.ID_MOVIE ASC "
                                    # Retrieving series for this list by excluding adult series and series without Wikidata ID
                                    strsqlseries += "SELECT sl.ID_SERIE, s.DAT_FIRST_AIR "
                                    strsqlseries += "FROM T_WC_TMDB_SERIE_LIST sl "
                                    strsqlseries += "INNER JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = sl.ID_SERIE "
                                    strsqlseries += "WHERE sl.ID_LIST = " + str(lngrecordid) + " "
                                    strsqlseries += "AND sl.DELETED = 0 "
                                    strsqlseries += "AND s.ADULT = 0 "
                                    strsqlseries += "AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    strsqlseries += "ORDER BY CASE WHEN s.DAT_FIRST_AIR IS NULL THEN 1 ELSE 0 END, s.DAT_FIRST_AIR DESC, sl.ID_SERIE ASC "
                                elif intcollection == 3 or intcollection == 4:
                                    # Retrieving movies for this collection by excluding adult movies and movies without Wikidata ID
                                    strsqlmovies += "SELECT ID_MOVIE, DAT_RELEASE "
                                    strsqlmovies += "FROM T_WC_TMDB_MOVIE "
                                    strsqlmovies += "WHERE ID_COLLECTION = " + str(lngrecordid) + " "
                                    strsqlmovies += "AND DELETED = 0 "
                                    strsqlmovies += "AND ADULT = 0 "
                                    strsqlmovies += "AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
                                    strsqlmovies += "ORDER BY CASE WHEN DAT_RELEASE IS NULL THEN 1 ELSE 0 END, DAT_RELEASE ASC, ID_MOVIE ASC "
                                elif intcollection == 5:
                                    # Retrieving elements for this custom collection (movies/series)
                                    intsortby = f_getcustomsortby(row, 4)
                                    # Mechanism 1: parse IMDb IDs/URLs from ID_IMDB_LIST (newline-separated)
                                    strimdblist = row['ID_IMDB_LIST'] or ''
                                    arrimdbids = re.findall(r'(tt\d+)', strimdblist)
                                    strsqlmovies_imdb = ""
                                    strsqlseries_imdb = ""
                                    if arrimdbids:
                                        strimdbidlist = "'" + "','".join(arrimdbids) + "'"
                                        strfieldorder = "'" + "','".join(arrimdbids) + "'"
                                        strsqlmovies_imdb = "SELECT m.ID_MOVIE, FIELD(m.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE m.ID_IMDB IN (" + strimdbidlist + ") AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_imdb = "SELECT s.ID_SERIE, FIELD(s.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE s.ID_IMDB IN (" + strimdbidlist + ") AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Mechanism 2: Wikidata property/item filter from WIKIDATA_PROPERTIES
                                    strwikidataproperties = row['WIKIDATA_PROPERTIES'] or ''
                                    # TMDB-MOVIE-PREPROCESS-044: the mechanism-4 markers are read FIRST and
                                    # REMOVED from the string. Mechanism 2 then parses the remainder only, so it
                                    # never mistakes an external-id property for its own filter property, while
                                    # the Q left in place keeps its role here, illustrating the collection
                                    # (ID_WIKIDATA and the Wikipedia image).
                                    strwdmembershipproperty, strwdorderproperty, strwikidataremainder = f_parsecustomexternalidproperties(strwikidataproperties)
                                    arrwdtokens = re.findall(r'[PQ]\d+', strwikidataremainder)
                                    strwdpropertyid = next((t for t in arrwdtokens if t.startswith('P')), '')
                                    strwditemid = next((t for t in arrwdtokens if t.startswith('Q')), '')
                                    if strwditemid:
                                        arrcollectioncouples['ID_WIKIDATA'] = strwditemid
                                        arrcollectioncouples['WIKIPEDIA_IMAGE_PATH'] = f_getwikidataimagepath(strwditemid)
                                    strsqlmovies_wikidata = ""
                                    strsqlseries_wikidata = ""
                                    if strwdpropertyid and strwditemid:
                                        strsqlmovies_wikidata = "SELECT DISTINCT m.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_WIKIDATA_ITEM_VALUE wv JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT STRAIGHT_JOIN T_WC_TMDB_MOVIE m ON m.ID_WIKIDATA = w.ID_WIKIDATA INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE w.ID_PROPERTY ='" + strwdpropertyid + "' AND wv.ID_ITEM = '" + strwditemid + "' AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated') AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_wikidata = "SELECT DISTINCT s.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_WIKIDATA_ITEM_VALUE wv JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT STRAIGHT_JOIN T_WC_TMDB_SERIE s ON s.ID_WIKIDATA = w.ID_WIKIDATA INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE w.ID_PROPERTY ='" + strwdpropertyid + "' AND wv.ID_ITEM = '" + strwditemid + "' AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated') AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Mechanism 3: TMDb keyword filter from TMDB_ELEMENTS
                                    strtmdbelements = row['TMDB_ELEMENTS'] or ''
                                    strsqlmovies_keyword = ""
                                    strsqlseries_keyword = ""
                                    strkeywordmatch = re.search(r"T_WC_TMDB_KEYWORD\.NAME\s*=\s*'([^']+)'", strtmdbelements.replace('&#039;', "'"))
                                    if strkeywordmatch:
                                        strkeywordname = strkeywordmatch.group(1).strip().replace("'", "''")
                                        strsqlmovies_keyword = "SELECT mk.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE_KEYWORD mk INNER JOIN T_WC_TMDB_KEYWORD k ON mk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = mk.ID_MOVIE INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE k.NAME = '" + strkeywordname + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_keyword = "SELECT sk.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE_KEYWORD sk INNER JOIN T_WC_TMDB_KEYWORD k ON sk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = sk.ID_SERIE INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE k.NAME = '" + strkeywordname + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Mechanism 4: membership by Wikidata EXTERNAL ID, read in the V2
                                    # statements (TMDB-MOVIE-PREPROCESS-044). 'ID:P9584' means "every title
                                    # carrying a Criterion film id", which is how a publisher catalogue is
                                    # actually defined: by the identifier itself, not by a property pointing at
                                    # the publisher's item. 'ORDER:P12279' pours the spine number into
                                    # ORIGINAL_ORDER, which SORT_BY = 1 then sorts, spineless titles last.
                                    strsqlmovies_externalid = f_customexternalidsourcesql(strwdmembershipproperty, strwdorderproperty, "movie")
                                    strsqlseries_externalid = f_customexternalidsourcesql(strwdmembershipproperty, strwdorderproperty, "serie")
                                    # Combine mechanisms cumulatively
                                    arrsqlmovies_sources = [s for s in [strsqlmovies_imdb, strsqlmovies_wikidata, strsqlmovies_keyword, strsqlmovies_externalid] if s]
                                    arrsqlseries_sources = [s for s in [strsqlseries_imdb, strsqlseries_wikidata, strsqlseries_keyword, strsqlseries_externalid] if s]
                                    strsqlmovies = f_buildcustomaggregatequery(arrsqlmovies_sources, "ID_MOVIE", "IMDB_RATING_WEIGHTED", intsortby)
                                    strsqlseries = f_buildcustomaggregatequery(arrsqlseries_sources, "ID_SERIE", "IMDB_RATING_WEIGHTED", intsortby)
                                if strsqlmovies != "":
                                    # Retrieving elements for this collection (list/collection)
                                    cursor2.execute(strsqlmovies)
                                    lngmoviecount = cursor2.rowcount
                                    lngseriescount = 0
                                    #print(f"{lngmoviecount} lines")
                                    if strsqlseries != "":
                                        cursor4.execute(strsqlseries)
                                        lngseriescount = cursor4.rowcount
                                        #print(f"{lngseriescount} lines")
                                    if lngmoviecount + lngseriescount > 1:
                                        if strtmdbtargetrecord and lngt2stargetrecordid == 0:
                                            continue
                                        # This collection has more than one element (movie or serie)
                                        # So we create/update this collection
                                        if lngt2stargetrecordid > 0:
                                            lngcollectionid = lngt2stargetrecordid
                                        else:
                                            lngcollectionid = cp.f_sqlupdatearray(strsqltablename, arrcollectioncouples, strsqlupdatecondition, 1)
                                        if lngcollectionid is None:
                                            strsqlcollection = "SELECT ID_T2S_COLLECTION FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                            cursor3.execute(strsqlcollection)
                                            lngrowcount = cursor3.rowcount
                                            if lngrowcount == 0:
                                                print("Error: Failed to create/update collection - lngcollectionid is None")
                                                continue
                                            lngcollectionid = cursor3.fetchone()["ID_T2S_COLLECTION"]
                                        telcollection.created()
                                        telcollection.set_entity_id(lngcollectionid)
                                        if intcollection == 1 or intcollection == 3 or intcollection == 5:
                                            # Retrieve all movies for this collection
                                            # Only processing when handling original English (records from T_WC_TMDB_LIST or T_WC_TMDB_COLLECTION) to avoid duplicates with the translated versions
                                            results = cursor2.fetchall()
                                            lngdisplayorder = 0
                                            if lngt2stargetrecordid > 0:
                                                strsqlmaxdisplayorder = "SELECT COALESCE(MAX(DISPLAY_ORDER), 0) AS MAX_DISPLAY_ORDER FROM T_WC_T2S_MOVIE_COLLECTION WHERE ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                cursor3.execute(strsqlmaxdisplayorder)
                                                if cursor3.rowcount > 0:
                                                    lngdisplayorder = cursor3.fetchone()["MAX_DISPLAY_ORDER"]
                                            arrcurrentmovieids = []
                                            for row in results:
                                                lngmovieid = row["ID_MOVIE"]
                                                arrcurrentmovieids.append(str(lngmovieid))
                                                if lngt2stargetrecordid > 0:
                                                    strsqlcheckassociation = "SELECT 1 FROM T_WC_T2S_MOVIE_COLLECTION WHERE ID_MOVIE = " + str(lngmovieid) + " AND ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                    cursor3.execute(strsqlcheckassociation)
                                                    if cursor3.rowcount == 0:
                                                        lngdisplayorder += 1
                                                        arrmoviecollectioncouples = {
                                                            'ID_MOVIE': lngmovieid,
                                                            'ID_T2S_COLLECTION': lngcollectionid,
                                                            'DISPLAY_ORDER': lngdisplayorder
                                                        }
                                                        strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                        cp.f_sqlupdatearray("T_WC_T2S_MOVIE_COLLECTION", arrmoviecollectioncouples, strsqlupdatecondition2, 1)
                                                else:
                                                    lngdisplayorder += 1
                                                    arrmoviecollectioncouples = {
                                                        'ID_MOVIE': lngmovieid,
                                                        'ID_T2S_COLLECTION': lngcollectionid,
                                                        'DISPLAY_ORDER': lngdisplayorder
                                                    }
                                                    strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                    #print(strsqlupdatecondition2)
                                                    cp.f_sqlupdatearray("T_WC_T2S_MOVIE_COLLECTION", arrmoviecollectioncouples, strsqlupdatecondition2, 1)
                                            if arrcurrentmovieids and lngt2stargetrecordid == 0:
                                                strsqldelete = "DELETE FROM T_WC_T2S_MOVIE_COLLECTION WHERE ID_T2S_COLLECTION = " + str(lngcollectionid) + " AND ID_MOVIE NOT IN (" + ",".join(arrcurrentmovieids) + ") "
                                                print(strsqldelete)
                                                cursor2.execute(strsqldelete)
                                                strsqldelete = "DELETE mc1 FROM T_WC_T2S_MOVIE_COLLECTION mc1 INNER JOIN T_WC_T2S_MOVIE_COLLECTION mc2 ON mc1.ID_T2S_COLLECTION = mc2.ID_T2S_COLLECTION AND mc1.ID_MOVIE = mc2.ID_MOVIE AND mc1.ID_ROW > mc2.ID_ROW WHERE mc1.ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                print(strsqldelete)
                                                cursor2.execute(strsqldelete)
                                            if strsqlseries != "":
                                                # Retrieve all series for this collection
                                                results = cursor4.fetchall()
                                                lngdisplayorder = 0
                                                if lngt2stargetrecordid > 0:
                                                    strsqlmaxdisplayorder = "SELECT COALESCE(MAX(DISPLAY_ORDER), 0) AS MAX_DISPLAY_ORDER FROM T_WC_T2S_SERIE_COLLECTION WHERE ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                    cursor3.execute(strsqlmaxdisplayorder)
                                                    if cursor3.rowcount > 0:
                                                        lngdisplayorder = cursor3.fetchone()["MAX_DISPLAY_ORDER"]
                                                arrcurrentserieids = []
                                                for row in results:
                                                    lngseriesid = row["ID_SERIE"]
                                                    arrcurrentserieids.append(str(lngseriesid))
                                                    if lngt2stargetrecordid > 0:
                                                        strsqlcheckassociation = "SELECT 1 FROM T_WC_T2S_SERIE_COLLECTION WHERE ID_SERIE = " + str(lngseriesid) + " AND ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                        cursor3.execute(strsqlcheckassociation)
                                                        if cursor3.rowcount == 0:
                                                            lngdisplayorder += 1
                                                            arrseriecollectioncouples = {
                                                                'ID_SERIE': lngseriesid,
                                                                'ID_T2S_COLLECTION': lngcollectionid,
                                                                'DISPLAY_ORDER': lngdisplayorder
                                                            }
                                                            strsqlupdatecondition2 = "ID_SERIE = " + str(lngseriesid) + " AND ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                            cp.f_sqlupdatearray("T_WC_T2S_SERIE_COLLECTION", arrseriecollectioncouples, strsqlupdatecondition2, 1)
                                                    else:
                                                        lngdisplayorder += 1
                                                        arrseriecollectioncouples = {
                                                            'ID_SERIE': lngseriesid,
                                                            'ID_T2S_COLLECTION': lngcollectionid,
                                                            'DISPLAY_ORDER': lngdisplayorder
                                                        }
                                                        strsqlupdatecondition2 = "ID_SERIE = " + str(lngseriesid) + " AND ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                        #print(strsqlupdatecondition2)
                                                        cp.f_sqlupdatearray("T_WC_T2S_SERIE_COLLECTION", arrseriecollectioncouples, strsqlupdatecondition2, 1)
                                                if arrcurrentserieids and lngt2stargetrecordid == 0:
                                                    strsqldelete = "DELETE FROM T_WC_T2S_SERIE_COLLECTION WHERE ID_T2S_COLLECTION = " + str(lngcollectionid) + " AND ID_SERIE NOT IN (" + ",".join(arrcurrentserieids) + ") "
                                                    print(strsqldelete)
                                                    cursor2.execute(strsqldelete)
                                                    strsqldelete = "DELETE sc1 FROM T_WC_T2S_SERIE_COLLECTION sc1 INNER JOIN T_WC_T2S_SERIE_COLLECTION sc2 ON sc1.ID_T2S_COLLECTION = sc2.ID_T2S_COLLECTION AND sc1.ID_SERIE = sc2.ID_SERIE AND sc1.ID_ROW > sc2.ID_ROW WHERE sc1.ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                    print(strsqldelete)
                                                    cursor2.execute(strsqldelete)
                                            arrcollectioncouples = {
                                                'MOVIE_COUNT': lngmoviecount,
                                                'SERIE_COUNT': lngseriescount
                                            }
                                            if lngt2stargetrecordid > 0:
                                                strsqlcount = "SELECT COUNT(*) AS MOVIE_COUNT FROM T_WC_T2S_MOVIE_COLLECTION WHERE ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                cursor3.execute(strsqlcount)
                                                lngtargetmoviecount = cursor3.fetchone()["MOVIE_COUNT"] if cursor3.rowcount > 0 else 0
                                                strsqlcount = "SELECT COUNT(*) AS SERIE_COUNT FROM T_WC_T2S_SERIE_COLLECTION WHERE ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                cursor3.execute(strsqlcount)
                                                lngtargetseriecount = cursor3.fetchone()["SERIE_COUNT"] if cursor3.rowcount > 0 else 0
                                                arrcollectioncouples = {
                                                    'MOVIE_COUNT': lngtargetmoviecount,
                                                    'SERIE_COUNT': lngtargetseriecount
                                                }
                                                cp.f_sqlupdatearray(strsqltablename, arrcollectioncouples, "ID_T2S_COLLECTION = " + str(lngcollectionid), 1)
                                            else:
                                                cp.f_sqlupdatearray(strsqltablename, arrcollectioncouples, strsqlupdatecondition, 1)
                                    else:
                                        # This collection has only one element or none
                                        # So we delete this collection if it already exists
                                        strsqltablename = "T_WC_T2S_COLLECTION"
                                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                        print(strsqldelete)
                                        cursor2.execute(strsqldelete)
                                        telcollection.deleted(cursor2.rowcount)
                                        #cursor2.commit()
                    if 1:
                        strsqltablename = "T_WC_T2S_COLLECTION"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE COLLECTION_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        telcollection.deleted(cursor2.rowcount)

                        strsqldelete = ""
                        strsqldelete += "DELETE FROM T_WC_T2S_COLLECTION "
                        strsqldelete += "WHERE COLLECTION_SOURCE = 'list' "
                        strsqldelete += "AND ID_RECORD NOT IN (SELECT ID_LIST FROM T_WC_TMDB_LIST WHERE USED_FOR_T2S_COLLECTION > 0) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        telcollection.deleted(cursor2.rowcount)

                        strsqldelete = ""
                        strsqldelete += "DELETE FROM T_WC_T2S_COLLECTION "
                        strsqldelete += "WHERE COLLECTION_SOURCE = 'collection' "
                        strsqldelete += "AND ID_RECORD NOT IN (SELECT ID_COLLECTION FROM T_WC_TMDB_COLLECTION) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        telcollection.deleted(cursor2.rowcount)

                        strsqldelete = ""
                        strsqldelete += "DELETE FROM T_WC_T2S_COLLECTION "
                        strsqldelete += "WHERE COLLECTION_SOURCE = 'custom' "
                        strsqldelete += "AND ID_RECORD NOT IN (SELECT ID_CUSTOM_LIST FROM T_WC_CUSTOM_LIST WHERE TARGET_TABLE = 2 AND DELETED = 0 AND (TMDB_TARGET_RECORD IS NULL OR TMDB_TARGET_RECORD = '')) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        telcollection.deleted(cursor2.rowcount)
                        
                        # Update T_WC_T2S_COLLECTION ratings and popularity from movies
                        strsql = """UPDATE T_WC_T2S_COLLECTION t
JOIN (
    SELECT
        mt.ID_T2S_COLLECTION,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_MOVIE_COLLECTION mt
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mt.ID_MOVIE
    INNER JOIN T_WC_T2S_COLLECTION t2
        ON t2.ID_T2S_COLLECTION = mt.ID_T2S_COLLECTION
       AND t2.COLLECTION_TYPE = 'collection'
    GROUP BY mt.ID_T2S_COLLECTION
) x
    ON x.ID_T2S_COLLECTION = t.ID_T2S_COLLECTION
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED,
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_COLLECTION ratings and popularity from series
                        strsql = """UPDATE T_WC_T2S_COLLECTION t
JOIN (
    SELECT
        st.ID_T2S_COLLECTION,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(s.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_SERIE_COLLECTION st
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = st.ID_SERIE
    INNER JOIN T_WC_T2S_COLLECTION t2
        ON t2.ID_T2S_COLLECTION = st.ID_T2S_COLLECTION
       AND t2.COLLECTION_TYPE = 'collection'
    GROUP BY st.ID_T2S_COLLECTION
) x
    ON x.ID_T2S_COLLECTION = t.ID_T2S_COLLECTION
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED),
    t.POPULARITY = COALESCE(t.POPULARITY, x.AVG_POPULARITY);
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        strsqltablename = "T_WC_T2S_MOVIE_COLLECTION"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_T2S_COLLECTION NOT IN (SELECT ID_T2S_COLLECTION FROM T_WC_T2S_COLLECTION) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        #cursor2.commit()
                        strsqltablename = "T_WC_T2S_SERIE_COLLECTION"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_T2S_COLLECTION NOT IN (SELECT ID_T2S_COLLECTION FROM T_WC_T2S_COLLECTION) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        #cursor2.commit()

                    telcollection.finish()

                elif intindex == 42:
                    #----------------------------------------------------
                    print("T2S_LIST processing")
                    tellist = EntityTelemetry("list", 42, "list")
                    tellist.begin()

                    arrlists = {1: 'en-list', 2: 'fr-list', 3: 'custom-list', 4: 'list-delete'}
                    for intlist, strlist in arrlists.items():
                        strsql = ""
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess",strlist,"Current sub process in the TMDb database movie preprocess",0)
                        if intlist == 1:
                            strcurrentprocess = f"{intlist}: Copying from TMDB_LIST to T2S_LIST"
                            strsql += "SELECT 'list' AS LIST_SOURCE, 'list' AS LIST_TYPE, T_WC_TMDB_LIST.ID_LIST AS ID_RECORD, T_WC_TMDB_LIST.NAME, T_WC_TMDB_LIST.DESCRIPTION AS OVERVIEW, 'en' AS LANG, T_WC_TMDB_LIST.POSTER_PATH, NULL AS ID_WIKIDATA "
                            strsql += "FROM T_WC_TMDB_LIST WHERE USED_FOR_T2S_LIST > 0 "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "LIST_NAME"
                        elif intlist == 2:
                            strcurrentprocess = f"{intlist}: Copying from T_WC_TMDB_LIST_LANG to T2S_LIST"
                            strsql += "SELECT 'list' AS LIST_SOURCE, 'list' AS LIST_TYPE, T_WC_TMDB_LIST.ID_LIST AS ID_RECORD, T_WC_TMDB_LIST_LANG.SHORT_NAME AS NAME, '' AS OVERVIEW, T_WC_TMDB_LIST_LANG.LANG, '' AS POSTER_PATH, NULL AS ID_WIKIDATA "
                            strsql += "FROM T_WC_TMDB_LIST "
                            strsql += "INNER JOIN T_WC_TMDB_LIST_LANG ON T_WC_TMDB_LIST.ID_LIST = T_WC_TMDB_LIST_LANG.ID_LIST "
                            strsql += "WHERE T_WC_TMDB_LIST.USED_FOR_T2S_LIST > 0 "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "LIST_NAME_FR"
                        elif intlist == 3:
                            strcurrentprocess = f"{intlist}: Copying from T_WC_CUSTOM_LIST to T2S_LIST"
                            strsql += "SELECT 'custom' AS LIST_SOURCE, 'list' AS LIST_TYPE, T_WC_CUSTOM_LIST.ID_CUSTOM_LIST AS ID_RECORD, T_WC_CUSTOM_LIST.LIST_NAME AS NAME, T_WC_CUSTOM_LIST.LIST_NAME_FR AS NAME_FR, T_WC_CUSTOM_LIST.OVERVIEW AS OVERVIEW, 'en' AS LANG, T_WC_CUSTOM_LIST.POSTER_PATH, NULL AS ID_WIKIDATA, T_WC_CUSTOM_LIST.ID_IMDB_LIST, T_WC_CUSTOM_LIST.WIKIDATA_PROPERTIES, T_WC_CUSTOM_LIST.TMDB_ELEMENTS, T_WC_CUSTOM_LIST.SORT_BY, T_WC_CUSTOM_LIST.TMDB_TARGET_RECORD "
                            strsql += "FROM T_WC_CUSTOM_LIST WHERE DELETED = 0 "
                            # Only processing custom lists targeting the T_WC_T2S_LIST table
                            strsql += "AND TARGET_TABLE = 1 "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "LIST_NAME"
                        elif intlist == 4:
                            strcurrentprocess = f"{intlist}: Deleting from T2S_LIST"
                            strsqldelete = ""
                            strsqldelete += "DELETE FROM T_WC_T2S_LIST WHERE LIST_SOURCE = 'list' AND ID_RECORD NOT IN (SELECT ID_LIST FROM T_WC_TMDB_LIST WHERE T_WC_TMDB_LIST.USED_FOR_T2S_LIST > 0) "
                            print(strsqldelete)
                            cursor.execute(strsqldelete)
                            tellist.deleted(cursor.rowcount)
                            #cursor.commit()
                            strsqldelete = ""
                            strsqldelete += "DELETE FROM T_WC_T2S_LIST WHERE LIST_SOURCE = 'custom' AND ID_RECORD NOT IN (SELECT ID_CUSTOM_LIST FROM T_WC_CUSTOM_LIST WHERE TARGET_TABLE = 1) "
                            print(strsqldelete)
                            cursor.execute(strsqldelete)
                            tellist.deleted(cursor.rowcount)
                            #cursor.commit()
                            continue
                        if strsql != "":
                            # Now we process the SELECT query
                            print(strsql)
                            cursor.execute(strsql)
                            lngrowcount = cursor.rowcount
                            print(f"{lngrowcount} lines")
                            lnglinesprocessed = 0
                            # Fetching all rows from the last executed statement
                            results = cursor.fetchall()
                            # Iterating through the results and printing
                            for row in results:
                                # print("------------------------------------------")
                                lnglinesprocessed += 1
                                lngrecordid = row['ID_RECORD']
                                strrecordname = row['NAME']
                                strrecordoverview = row['OVERVIEW']
                                strrecordlistsource = row['LIST_SOURCE']
                                strrecordlisttype = row['LIST_TYPE']
                                strrecordposterpath = row['POSTER_PATH']
                                strrecordidwikidata = row['ID_WIKIDATA'] if 'ID_WIKIDATA' in row else None
                                strrecordwikipediaimagepath = f_getwikidataimagepath(strrecordidwikidata)
                                print("Processing record: " + str(lngrecordid) + ": " + strrecordname + " (" + strrecordlistsource + ")")
                                tellist.position(recordid=lngrecordid, currentvalue=strrecordname, currentprocess=strcurrentprocess)
                                if target_field_name == "LIST_NAME":
                                    arrlistcouples = {
                                        'ID_RECORD': lngrecordid,
                                        'LIST_NAME': strrecordname,
                                        'OVERVIEW': strrecordoverview,
                                        'LIST_SOURCE': strrecordlistsource,
                                        'LIST_TYPE': strrecordlisttype,
                                        'POSTER_PATH': strrecordposterpath,
                                        'ID_WIKIDATA': strrecordidwikidata,
                                        'WIKIPEDIA_IMAGE_PATH': strrecordwikipediaimagepath
                                    }
                                    if intlist == 3:
                                        arrlistcouples['LIST_NAME_FR'] = row['NAME_FR'] or ''
                                elif target_field_name == "LIST_NAME_FR":
                                    arrlistcouples = {
                                        'ID_RECORD': lngrecordid,
                                        'LIST_NAME_FR': strrecordname,
                                        'LIST_SOURCE': strrecordlistsource,
                                        'LIST_TYPE': strrecordlisttype,
                                        'ID_WIKIDATA': strrecordidwikidata,
                                        'WIKIPEDIA_IMAGE_PATH': strrecordwikipediaimagepath
                                    }
                                strsqltablename = "T_WC_T2S_LIST"
                                strsqlupdatecondition = f"ID_RECORD = '{lngrecordid}' AND LIST_SOURCE = '{strrecordlistsource}'"
                                cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(lngrecordid),"Current record in the TMDb database movie preprocess",0)
                                strtmdbtargetrecord = row['TMDB_TARGET_RECORD'] if intlist == 3 and 'TMDB_TARGET_RECORD' in row and row['TMDB_TARGET_RECORD'] else ''
                                strtmdbtargetfieldname = ''
                                lngtmdbtargetrecordid = 0
                                lngt2stargetrecordid = 0
                                strt2stargettablename = ''
                                strt2smovietargettablename = ''
                                strt2sserietargettablename = ''
                                strt2stargetidfieldname = ''
                                if strtmdbtargetrecord:
                                    objtmdbtargetrecordmatch = re.match(r'^\s*(ID_COLLECTION|ID_LIST)\s*=\s*(\d+)\s*$', strtmdbtargetrecord)
                                    if objtmdbtargetrecordmatch:
                                        strtmdbtargetfieldname = objtmdbtargetrecordmatch.group(1)
                                        lngtmdbtargetrecordid = int(objtmdbtargetrecordmatch.group(2))
                                        if strtmdbtargetfieldname == 'ID_COLLECTION':
                                            strsqltargetrecord = "SELECT c.ID_T2S_COLLECTION FROM T_WC_T2S_COLLECTION c INNER JOIN T_WC_TMDB_COLLECTION tc ON tc.ID_COLLECTION = c.ID_RECORD WHERE c.COLLECTION_SOURCE = 'collection' AND c.ID_RECORD = " + str(lngtmdbtargetrecordid)
                                            cursor3.execute(strsqltargetrecord)
                                            if cursor3.rowcount > 0:
                                                lngt2stargetrecordid = cursor3.fetchone()["ID_T2S_COLLECTION"]
                                                strt2stargettablename = "T_WC_T2S_COLLECTION"
                                                strt2smovietargettablename = "T_WC_T2S_MOVIE_COLLECTION"
                                                strt2sserietargettablename = "T_WC_T2S_SERIE_COLLECTION"
                                                strt2stargetidfieldname = "ID_T2S_COLLECTION"
                                        elif strtmdbtargetfieldname == 'ID_LIST':
                                            strsqltargetrecord = "SELECT l.ID_T2S_LIST FROM T_WC_T2S_LIST l INNER JOIN T_WC_TMDB_LIST tl ON tl.ID_LIST = l.ID_RECORD WHERE l.LIST_SOURCE = 'list' AND l.ID_RECORD = " + str(lngtmdbtargetrecordid)
                                            cursor3.execute(strsqltargetrecord)
                                            if cursor3.rowcount > 0:
                                                lngt2stargetrecordid = cursor3.fetchone()["ID_T2S_LIST"]
                                                strt2stargettablename = "T_WC_T2S_LIST"
                                                strt2smovietargettablename = "T_WC_T2S_MOVIE_LIST"
                                                strt2sserietargettablename = "T_WC_T2S_SERIE_LIST"
                                                strt2stargetidfieldname = "ID_T2S_LIST"
                                    else:
                                        print("Invalid TMDB_TARGET_RECORD value for custom list " + str(lngrecordid) + ": " + strtmdbtargetrecord)
                                    if lngt2stargetrecordid == 0:
                                        print("TMDB_TARGET_RECORD target not found for custom list " + str(lngrecordid) + ": " + strtmdbtargetrecord)
                                
                                strsqlmovies = ""
                                strsqlseries = ""
                                if intlist == 1 or intlist == 2:
                                    # Retrieving movies for this list by excluding adult movies and movies without Wikidata ID
                                    strsqlmovies += "SELECT ml.ID_MOVIE, m.IMDB_RATING_WEIGHTED "
                                    strsqlmovies += "FROM T_WC_TMDB_MOVIE_LIST ml "
                                    strsqlmovies += "INNER JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = ml.ID_MOVIE "
                                    strsqlmovies += "WHERE ml.ID_LIST = " + str(lngrecordid) + " "
                                    strsqlmovies += "AND ml.DELETED = 0 "
                                    strsqlmovies += "AND m.ADULT = 0 "
                                    strsqlmovies += "AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                    strsqlmovies += "ORDER BY m.IMDB_RATING_WEIGHTED DESC, ml.ID_MOVIE ASC "
                                    # Retrieving series for this list by excluding adult series and series without Wikidata ID
                                    strsqlseries += "SELECT sl.ID_SERIE, s.IMDB_RATING_WEIGHTED "
                                    strsqlseries += "FROM T_WC_TMDB_SERIE_LIST sl "
                                    strsqlseries += "INNER JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = sl.ID_SERIE "
                                    strsqlseries += "WHERE sl.ID_LIST = " + str(lngrecordid) + " "
                                    strsqlseries += "AND sl.DELETED = 0 "
                                    strsqlseries += "AND s.ADULT = 0 "
                                    strsqlseries += "AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    strsqlseries += "ORDER BY s.IMDB_RATING_WEIGHTED DESC, sl.ID_SERIE ASC "
                                elif intlist == 3:
                                    # Retrieving elements for this custom list (movies/series)
                                    intsortby = f_getcustomsortby(row, 4)
                                    # Mechanism 1: parse IMDb IDs/URLs from ID_IMDB_LIST (newline-separated)
                                    strimdblist = row['ID_IMDB_LIST'] or ''
                                    arrimdbids = re.findall(r'(tt\d+)', strimdblist)
                                    strsqlmovies_imdb = ""
                                    strsqlseries_imdb = ""
                                    if arrimdbids:
                                        strimdbidlist = "'" + "','".join(arrimdbids) + "'"
                                        strfieldorder = "'" + "','".join(arrimdbids) + "'"
                                        strsqlmovies_imdb = "SELECT m.ID_MOVIE, FIELD(m.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE m.ID_IMDB IN (" + strimdbidlist + ") AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_imdb = "SELECT s.ID_SERIE, FIELD(s.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE s.ID_IMDB IN (" + strimdbidlist + ") AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Mechanism 2: Wikidata property/item filter from WIKIDATA_PROPERTIES
                                    strwikidataproperties = row['WIKIDATA_PROPERTIES'] or ''
                                    arrwdtokens = re.findall(r'[PQ]\d+', strwikidataproperties)
                                    strwdpropertyid = next((t for t in arrwdtokens if t.startswith('P')), '')
                                    strwditemid = next((t for t in arrwdtokens if t.startswith('Q')), '')
                                    if strwditemid:
                                        arrlistcouples['ID_WIKIDATA'] = strwditemid
                                        arrlistcouples['WIKIPEDIA_IMAGE_PATH'] = f_getwikidataimagepath(strwditemid)
                                    strsqlmovies_wikidata = ""
                                    strsqlseries_wikidata = ""
                                    if strwdpropertyid and strwditemid:
                                        strsqlmovies_wikidata = "SELECT DISTINCT m.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_WIKIDATA_ITEM_VALUE wv JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT STRAIGHT_JOIN T_WC_TMDB_MOVIE m ON m.ID_WIKIDATA = w.ID_WIKIDATA INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE w.ID_PROPERTY ='" + strwdpropertyid + "' AND wv.ID_ITEM = '" + strwditemid + "' AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated') AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_wikidata = "SELECT DISTINCT s.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_WIKIDATA_ITEM_VALUE wv JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT STRAIGHT_JOIN T_WC_TMDB_SERIE s ON s.ID_WIKIDATA = w.ID_WIKIDATA INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE w.ID_PROPERTY ='" + strwdpropertyid + "' AND wv.ID_ITEM = '" + strwditemid + "' AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated') AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Mechanism 3: TMDb keyword filter from TMDB_ELEMENTS
                                    strtmdbelements = row['TMDB_ELEMENTS'] or ''
                                    strsqlmovies_keyword = ""
                                    strsqlseries_keyword = ""
                                    strkeywordmatch = re.search(r"T_WC_TMDB_KEYWORD\.NAME\s*=\s*'([^']+)'", strtmdbelements.replace('&#039;', "'"))
                                    if strkeywordmatch:
                                        strkeywordname = strkeywordmatch.group(1).strip().replace("'", "''")
                                        strsqlmovies_keyword = "SELECT mk.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE_KEYWORD mk INNER JOIN T_WC_TMDB_KEYWORD k ON mk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = mk.ID_MOVIE INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE k.NAME = '" + strkeywordname + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_keyword = "SELECT sk.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE_KEYWORD sk INNER JOIN T_WC_TMDB_KEYWORD k ON sk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = sk.ID_SERIE INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE k.NAME = '" + strkeywordname + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Combine mechanisms cumulatively
                                    arrsqlmovies_sources = [s for s in [strsqlmovies_imdb, strsqlmovies_wikidata, strsqlmovies_keyword] if s]
                                    arrsqlseries_sources = [s for s in [strsqlseries_imdb, strsqlseries_wikidata, strsqlseries_keyword] if s]
                                    strsqlmovies = f_buildcustomaggregatequery(arrsqlmovies_sources, "ID_MOVIE", "IMDB_RATING_WEIGHTED", intsortby)
                                    strsqlseries = f_buildcustomaggregatequery(arrsqlseries_sources, "ID_SERIE", "IMDB_RATING_WEIGHTED", intsortby)

                                if strsqlmovies != "":
                                    # Retrieving elements for this list (list/list)
                                    cursor2.execute(strsqlmovies)
                                    lngmoviecount = cursor2.rowcount
                                    lngseriescount = 0
                                    #print(f"{lngmoviecount} lines")
                                    if strsqlseries != "":
                                        cursor4.execute(strsqlseries)
                                        lngseriescount = cursor4.rowcount
                                        #print(f"{lngseriescount} lines")
                                    if lngmoviecount + lngseriescount > 1:
                                        if strtmdbtargetrecord and lngt2stargetrecordid == 0:
                                            continue
                                        # This list has more than one element (movie or serie)
                                        # So we create/update this list
                                        if lngt2stargetrecordid > 0:
                                            lnglistid = lngt2stargetrecordid
                                        else:
                                            lnglistid = cp.f_sqlupdatearray(strsqltablename, arrlistcouples, strsqlupdatecondition, 1)
                                        if lnglistid is None:
                                            strsqllist = "SELECT ID_T2S_LIST FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                            cursor3.execute(strsqllist)
                                            lngrowcount = cursor3.rowcount
                                            if lngrowcount == 0:
                                                print("Error: Failed to create/update list - lnglistid is None")
                                                continue
                                            lnglistid = cursor3.fetchone()["ID_T2S_LIST"]
                                        tellist.created()
                                        tellist.set_entity_id(lnglistid)
                                        if intlist == 1 or intlist == 3:
                                            # Retrieve all movies for this list
                                            # Only processing when handling original English (records from T_WC_TMDB_LIST or T_WC_TMDB_LIST) to avoid duplicates with the translated versions
                                            results = cursor2.fetchall()
                                            lngdisplayorder = 0
                                            if lngt2stargetrecordid > 0:
                                                strsqlmaxdisplayorder = "SELECT COALESCE(MAX(DISPLAY_ORDER), 0) AS MAX_DISPLAY_ORDER FROM " + strt2smovietargettablename + " WHERE " + strt2stargetidfieldname + " = " + str(lngt2stargetrecordid)
                                                cursor3.execute(strsqlmaxdisplayorder)
                                                if cursor3.rowcount > 0:
                                                    lngdisplayorder = cursor3.fetchone()["MAX_DISPLAY_ORDER"]
                                            arrcurrentmovieids = []
                                            for row in results:
                                                lngmovieid = row["ID_MOVIE"]
                                                arrcurrentmovieids.append(str(lngmovieid))
                                                if lngt2stargetrecordid > 0:
                                                    strsqlcheckassociation = "SELECT 1 FROM " + strt2smovietargettablename + " WHERE ID_MOVIE = " + str(lngmovieid) + " AND " + strt2stargetidfieldname + " = " + str(lngt2stargetrecordid)
                                                    cursor3.execute(strsqlcheckassociation)
                                                    if cursor3.rowcount == 0:
                                                        lngdisplayorder += 1
                                                        arrmovielistcouples = {
                                                            'ID_MOVIE': lngmovieid,
                                                            strt2stargetidfieldname: lngt2stargetrecordid,
                                                            'DISPLAY_ORDER': lngdisplayorder
                                                        }
                                                        strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND " + strt2stargetidfieldname + " = " + str(lngt2stargetrecordid)
                                                        cp.f_sqlupdatearray(strt2smovietargettablename, arrmovielistcouples, strsqlupdatecondition2, 1)
                                                else:
                                                    lngdisplayorder += 1
                                                    arrmovielistcouples = {
                                                        'ID_MOVIE': lngmovieid,
                                                        'ID_T2S_LIST': lnglistid,
                                                        'DISPLAY_ORDER': lngdisplayorder
                                                    }
                                                    strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND ID_T2S_LIST = " + str(lnglistid)
                                                    #print(strsqlupdatecondition2)
                                                    cp.f_sqlupdatearray("T_WC_T2S_MOVIE_LIST", arrmovielistcouples, strsqlupdatecondition2, 1)
                                            if arrcurrentmovieids and lngt2stargetrecordid == 0:
                                                strsqldelete = "DELETE FROM T_WC_T2S_MOVIE_LIST WHERE ID_T2S_LIST = " + str(lnglistid) + " AND ID_MOVIE NOT IN (" + ",".join(arrcurrentmovieids) + ") "
                                                print(strsqldelete)
                                                cursor2.execute(strsqldelete)
                                                strsqldelete = "DELETE ml1 FROM T_WC_T2S_MOVIE_LIST ml1 INNER JOIN T_WC_T2S_MOVIE_LIST ml2 ON ml1.ID_T2S_LIST = ml2.ID_T2S_LIST AND ml1.ID_MOVIE = ml2.ID_MOVIE AND ml1.ID_ROW > ml2.ID_ROW WHERE ml1.ID_T2S_LIST = " + str(lnglistid)
                                                print(strsqldelete)
                                                cursor2.execute(strsqldelete)
                                            if strsqlseries != "":
                                                # Retrieve all series for this list
                                                results = cursor4.fetchall()
                                                lngdisplayorder = 0
                                                if lngt2stargetrecordid > 0:
                                                    strsqlmaxdisplayorder = "SELECT COALESCE(MAX(DISPLAY_ORDER), 0) AS MAX_DISPLAY_ORDER FROM " + strt2sserietargettablename + " WHERE " + strt2stargetidfieldname + " = " + str(lngt2stargetrecordid)
                                                    cursor3.execute(strsqlmaxdisplayorder)
                                                    if cursor3.rowcount > 0:
                                                        lngdisplayorder = cursor3.fetchone()["MAX_DISPLAY_ORDER"]
                                                arrcurrentserieids = []
                                                for row in results:
                                                    lngseriesid = row["ID_SERIE"]
                                                    arrcurrentserieids.append(str(lngseriesid))
                                                    if lngt2stargetrecordid > 0:
                                                        strsqlcheckassociation = "SELECT 1 FROM " + strt2sserietargettablename + " WHERE ID_SERIE = " + str(lngseriesid) + " AND " + strt2stargetidfieldname + " = " + str(lngt2stargetrecordid)
                                                        cursor3.execute(strsqlcheckassociation)
                                                        if cursor3.rowcount == 0:
                                                            lngdisplayorder += 1
                                                            arrserielistcouples = {
                                                                'ID_SERIE': lngseriesid,
                                                                strt2stargetidfieldname: lngt2stargetrecordid,
                                                                'DISPLAY_ORDER': lngdisplayorder
                                                            }
                                                            strsqlupdatecondition2 = "ID_SERIE = " + str(lngseriesid) + " AND " + strt2stargetidfieldname + " = " + str(lngt2stargetrecordid)
                                                            cp.f_sqlupdatearray(strt2sserietargettablename, arrserielistcouples, strsqlupdatecondition2, 1)
                                                    else:
                                                        lngdisplayorder += 1
                                                        arrserielistcouples = {
                                                            'ID_SERIE': lngseriesid,
                                                            'ID_T2S_LIST': lnglistid,
                                                            'DISPLAY_ORDER': lngdisplayorder
                                                        }
                                                        strsqlupdatecondition2 = "ID_SERIE = " + str(lngseriesid) + " AND ID_T2S_LIST = " + str(lnglistid)
                                                        #print(strsqlupdatecondition2)
                                                        cp.f_sqlupdatearray("T_WC_T2S_SERIE_LIST", arrserielistcouples, strsqlupdatecondition2, 1)
                                                if arrcurrentserieids and lngt2stargetrecordid == 0:
                                                    strsqldelete = "DELETE FROM T_WC_T2S_SERIE_LIST WHERE ID_T2S_LIST = " + str(lnglistid) + " AND ID_SERIE NOT IN (" + ",".join(arrcurrentserieids) + ") "
                                                    print(strsqldelete)
                                                    cursor2.execute(strsqldelete)
                                                    strsqldelete = "DELETE sl1 FROM T_WC_T2S_SERIE_LIST sl1 INNER JOIN T_WC_T2S_SERIE_LIST sl2 ON sl1.ID_T2S_LIST = sl2.ID_T2S_LIST AND sl1.ID_SERIE = sl2.ID_SERIE AND sl1.ID_ROW > sl2.ID_ROW WHERE sl1.ID_T2S_LIST = " + str(lnglistid)
                                                    print(strsqldelete)
                                                    cursor2.execute(strsqldelete)
                                            arrlistcouples = {
                                                'MOVIE_COUNT': lngmoviecount,
                                                'SERIE_COUNT': lngseriescount
                                            }
                                            if lngt2stargetrecordid > 0:
                                                strsqlcount = "SELECT COUNT(*) AS MOVIE_COUNT FROM " + strt2smovietargettablename + " WHERE " + strt2stargetidfieldname + " = " + str(lngt2stargetrecordid)
                                                cursor3.execute(strsqlcount)
                                                lngtargetmoviecount = cursor3.fetchone()["MOVIE_COUNT"] if cursor3.rowcount > 0 else 0
                                                strsqlcount = "SELECT COUNT(*) AS SERIE_COUNT FROM " + strt2sserietargettablename + " WHERE " + strt2stargetidfieldname + " = " + str(lngt2stargetrecordid)
                                                cursor3.execute(strsqlcount)
                                                lngtargetseriecount = cursor3.fetchone()["SERIE_COUNT"] if cursor3.rowcount > 0 else 0
                                                arrlistcouples = {
                                                    'MOVIE_COUNT': lngtargetmoviecount,
                                                    'SERIE_COUNT': lngtargetseriecount
                                                }
                                                cp.f_sqlupdatearray(strt2stargettablename, arrlistcouples, strt2stargetidfieldname + " = " + str(lngt2stargetrecordid), 1)
                                            else:
                                                cp.f_sqlupdatearray(strsqltablename, arrlistcouples, strsqlupdatecondition, 1)
                                    else:
                                        # This list has only one element or none
                                        # So we delete this list if it already exists
                                        if lngt2stargetrecordid == 0:
                                            strsqltablename = "T_WC_T2S_LIST"
                                            strsqldelete = "DELETE FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                            print(strsqldelete)
                                            cursor2.execute(strsqldelete)
                                            tellist.deleted(cursor2.rowcount)
                                        #cursor2.commit()
                    if 1:
                        strsqltablename = "T_WC_T2S_LIST"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE LIST_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        tellist.deleted(cursor2.rowcount)
                        
                        # Update T_WC_T2S_LIST ratings and popularity from movies
                        strsql = """UPDATE T_WC_T2S_LIST t
JOIN (
    SELECT
        mt.ID_T2S_LIST,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_MOVIE_LIST mt
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mt.ID_MOVIE
    INNER JOIN T_WC_T2S_LIST t2
        ON t2.ID_T2S_LIST = mt.ID_T2S_LIST
       AND t2.LIST_TYPE = 'list'
    GROUP BY mt.ID_T2S_LIST
) x
    ON x.ID_T2S_LIST = t.ID_T2S_LIST
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED,
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_LIST ratings and popularity from series
                        strsql = """UPDATE T_WC_T2S_LIST t
JOIN (
    SELECT
        st.ID_T2S_LIST,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(s.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_SERIE_LIST st
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = st.ID_SERIE
    INNER JOIN T_WC_T2S_LIST t2
        ON t2.ID_T2S_LIST = st.ID_T2S_LIST
       AND t2.LIST_TYPE = 'list'
    GROUP BY st.ID_T2S_LIST
) x
    ON x.ID_T2S_LIST = t.ID_T2S_LIST
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED),
    t.POPULARITY = COALESCE(t.POPULARITY, x.AVG_POPULARITY);
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        strsqltablename = "T_WC_T2S_MOVIE_LIST"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_T2S_LIST NOT IN (SELECT ID_T2S_LIST FROM T_WC_T2S_LIST) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        #cursor2.commit()
                        strsqltablename = "T_WC_T2S_SERIE_LIST"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_T2S_LIST NOT IN (SELECT ID_T2S_LIST FROM T_WC_T2S_LIST) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        #cursor2.commit()

                    tellist.finish()

                elif intindex == 43:
                    #----------------------------------------------------
                    print("T2S_GROUP processing")

                    # Group-derivation telemetry (process 43): publish start, running counts and the
                    # current position as server variables so the run is observable from srvvar.inc.php.
                    fltgroupprocessstart = time.time()
                    strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                    cp.f_setservervariable("strtmdbmoviepreprocessgroupstartdatetime",strnow,"Start datetime of the last T2S group derivation (process 43)",0)
                    cp.f_setservervariable("strtmdbmoviepreprocessgroupenddatetime","","End datetime of the last T2S group derivation (process 43)",0)
                    lnggroupprocessedcount = 0
                    lnggroupcreatedcount = 0
                    lnggroupdeletedcount = 0
                    cp.f_setservervariable("strtmdbmoviepreprocessgroupprocessedcount","0","Number of group records examined by the T2S group derivation (process 43)",0)
                    cp.f_setservervariable("strtmdbmoviepreprocessgroupcreatedcount","0","Number of groups created/updated by the T2S group derivation (process 43)",0)
                    cp.f_setservervariable("strtmdbmoviepreprocessgroupdeletedcount","0","Number of singleton groups deleted by the T2S group derivation (process 43)",0)

                    arrgroups = {1: 'en-group', 2: 'en-employer', 3: 'sport-team', 4: 'custom-group'}    
                    for intgroup, strgroup in arrgroups.items():
                        strsql = ""
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess",strgroup,"Current sub process in the TMDb database person preprocess",0)
                        if intgroup == 1:
                            strpropertyid = "P463"
                        elif intgroup == 2:
                            strpropertyid = "P108"
                        elif intgroup == 3:
                            strpropertyid = "P54"
                        elif intgroup == 4:
                            strpropertyid = ""
                        else:
                            strpropertyid = ""
                        if intgroup == 4:
                            strcurrentprocess = f"{intgroup}: Copying from CUSTOM_LIST to T2S_GROUP"
                            strsql += "SELECT T_WC_CUSTOM_LIST.ID_CUSTOM_LIST AS ID_RECORD, T_WC_CUSTOM_LIST.LIST_NAME AS NAME, T_WC_CUSTOM_LIST.LIST_NAME_FR AS NAME_FR, T_WC_CUSTOM_LIST.OVERVIEW AS OVERVIEW, T_WC_CUSTOM_LIST.POSTER_PATH, T_WC_CUSTOM_LIST.ID_IMDB_LIST, T_WC_CUSTOM_LIST.WIKIDATA_PROPERTIES, T_WC_CUSTOM_LIST.TMDB_ELEMENTS, T_WC_CUSTOM_LIST.SORT_BY "
                            strsql += "FROM T_WC_CUSTOM_LIST WHERE DELETED = 0 "
                            strsql += "AND TARGET_TABLE = 3 "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "GROUP_NAME"
                            strrecordgroupsource = "custom"
                        elif strpropertyid != "":
                            strcurrentprocess = f"{intgroup}: Copying from WIKIDATA {strpropertyid} to T2S_GROUP"
                            # Pre-filter the driving set to only items that resolve to >= 2 linked
                            # TMDb persons. This mirrors the per-item person query joins below
                            # (T_WC_TMDB_PERSON -> T_WC_WIKIDATA_PERSON -> les statements V2)
                            # and the "lngpersoncount > 1" group-creation gate, so we no longer iterate
                            # the hundreds of thousands of P463/P108/P54 items that would only ever be
                            # deleted as singletons. Degraded groups (>=2 persons previously, <2 now)
                            # are handled by the count-based stale delete at the end of this process.
                            strsql += f_persondrivingsql(strpropertyid)
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "GROUP_NAME"
                            strrecordgroupsource = strpropertyid
                        if strsql != "":
                            # Now we process the SELECT query
                            print(strsql)
                            cursor.execute(strsql)
                            lngrowcount = cursor.rowcount
                            print(f"{lngrowcount} lines")
                            lnglinesprocessed = 0
                            # Fetching all rows from the last executed statement
                            results = cursor.fetchall()
                            # Iterating through the results and printing
                            for row in results:
                                # print("------------------------------------------")
                                lnglinesprocessed += 1
                                if intgroup == 4:
                                    strrecordid = str(row['ID_RECORD'])
                                    strrecordname = row['NAME'] or ''
                                    strrecordnamefr = row['NAME_FR'] or ''
                                    strrecordoverview = row['OVERVIEW'] or ''
                                    strrecordposterpath = row['POSTER_PATH'] or ''
                                    strrecordwikipediaimagepath = ""
                                else:
                                    strrecordid = row['ID_ITEM']
                                    strrecordname = ""
                                    strrecordoverview = ""
                                    strrecordposterpath = ""
                                    strsqlitem = ""
                                    # Image: prefer T_WC_WIKIPEDIA_PAGE_LANG (WIKIPEDIA-CRAWLER-020), fall back on
                                    # the V1 column until the V1 tables are dropped. LABEL / DESCRIPTION still
                                    # come from V1: their migration is WIKIDATA-CRAWLER-017, not this one.
                                    # WIKIDATA-CRAWLER-017 : texte EN et image pris dans V2 / PAGE_LANG, V1 en repli.
                                    # La requete part d une table derivee d une ligne et non de V1, sinon une entite
                                    # absente de V1 ne rendrait rien du tout. COALESCE final sur '' pour garantir une
                                    # chaine et non NULL aux appelants, qui concatenent ces valeurs.
                                    strsqlitem += "SELECT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.en')), "
                                    strsqlitem += "  NULLIF(v2.LABEL_EN,''), v1.LABEL, '') AS LABEL, "
                                    strsqlitem += "COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.DESCRIPTIONS_JSON,'$.en')), "
                                    strsqlitem += "  NULLIF(v2.DESCRIPTION_EN,''), v1.DESCRIPTION, '') AS DESCRIPTION, "
                                    strsqlitem += "COALESCE(pl.MAIN_IMAGE_URL, v1.WIKIPEDIA_IMAGE_PATH, '') AS WIKIPEDIA_IMAGE_PATH "
                                    strsqlitem += "FROM (SELECT %s AS ID_WIKIDATA) k "
                                    strsqlitem += "LEFT JOIN T_WC_WIKIDATA_ITEM v2 ON v2.ID_WIKIDATA = k.ID_WIKIDATA "
                                    strsqlitem += "LEFT JOIN T_WC_WIKIDATA_ITEM_V1 v1 "
                                    strsqlitem += "  ON v1.ID_WIKIDATA = k.ID_WIKIDATA AND v1.LANG = 'en' "
                                    strsqlitem += "LEFT JOIN T_WC_WIKIPEDIA_PAGE_LANG pl "
                                    strsqlitem += "  ON pl.ID_WIKIDATA = k.ID_WIKIDATA AND pl.LANG = 'en' "
                                    strsqlitem += "  AND COALESCE(pl.MAIN_IMAGE_URL,'') <> ''"
                                    arrvalues = cp.f_fieldsfromquery(
                                        strsqlitem,
                                        "strrecordname|strrecordoverview|strrecordposterpath",
                                        "LABEL|DESCRIPTION|WIKIPEDIA_IMAGE_PATH",
                                        params=(strrecordid,),
                                        target_dict=None,
                                    )
                                    strrecordname = arrvalues.get("strrecordname", "")
                                    strrecordoverview = arrvalues.get("strrecordoverview", "")
                                    strrecordposterpath = arrvalues.get("strrecordposterpath", "")
                                    strrecordwikipediaimagepath = f_getwikidataimagepath(strrecordid)
                                    # WIKIDATA-CRAWLER-017 : le libelle FR vient desormais de V2 (LABELS_JSON),
                                    # V1 ne servant plus que de repli, le temps que le gap d entites se ferme.
                                    strrecordnamefr = f_getwikidatalabel(strrecordid, "fr")
                                strrecordgrouptype = "group"
                                print("Processing record: " + str(strrecordid) + ": " + strrecordname + " (" + strrecordgroupsource + ")")
                                if target_field_name == "GROUP_NAME":
                                    arrgroupcouples = {
                                        'ID_WIKIDATA': strrecordid,
                                        'GROUP_NAME': strrecordname,
                                        'GROUP_NAME_FR': strrecordnamefr,
                                        'OVERVIEW': strrecordoverview,
                                        'GROUP_SOURCE': strrecordgroupsource,
                                        'GROUP_TYPE': strrecordgrouptype,
                                        'WIKIPEDIA_IMAGE_PATH': strrecordwikipediaimagepath
                                    }
                                strsqltablename = "T_WC_T2S_GROUP"
                                strsqlupdatecondition = f"ID_WIKIDATA = '{strrecordid}' AND GROUP_SOURCE = '{strrecordgroupsource}'"
                                cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(strrecordid),"Current record in the TMDb database movie preprocess",0)
                                lnggroupprocessedcount += 1
                                cp.f_setservervariable("strtmdbmoviepreprocessgroupcurrentprocess",strcurrentprocess,"Current source/sub-process in the T2S group derivation (process 43)",0)
                                cp.f_setservervariable("strtmdbmoviepreprocessgroupwikidataid",str(strrecordid),"Current Wikidata/record id in the T2S group derivation (process 43)",0)
                                cp.f_setservervariable("strtmdbmoviepreprocessgroupcurrentvalue",strrecordname,"Current group name in the T2S group derivation (process 43)",0)
                                cp.f_setservervariable("strtmdbmoviepreprocessgroupprocessedcount",str(lnggroupprocessedcount),"Number of group records examined by the T2S group derivation (process 43)",0)
                                
                                strsqlpersons = ""
                                if intgroup == 4:
                                    # Retrieving elements for this custom group (persons)
                                    intsortby = f_getcustomsortby(row, 4)
                                    # Mechanism 1: parse IMDb IDs/URLs from ID_IMDB_LIST (newline-separated)
                                    strimdblist = row['ID_IMDB_LIST'] or ''
                                    arrimdbids = re.findall(r'(nm\d+)', strimdblist)
                                    strsqlpersons_imdb = ""
                                    if arrimdbids:
                                        strimdbidlist = "'" + "','".join(arrimdbids) + "'"
                                        strfieldorder = "'" + "','".join(arrimdbids) + "'"
                                        strsqlpersons_imdb = "SELECT ID_PERSON, FIELD(ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, POPULARITY, BIRTHDAY AS SORT_DATE FROM T_WC_TMDB_PERSON WHERE ID_IMDB IN (" + strimdbidlist + ") AND ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "

                                    # Mechanism 2: Wikidata property/item filter from WIKIDATA_PROPERTIES
                                    strwikidataproperties = row['WIKIDATA_PROPERTIES'] or ''
                                    arrwdtokens = re.findall(r'[PQ]\d+', strwikidataproperties)
                                    strwdpropertyid = next((t for t in arrwdtokens if t.startswith('P')), '')
                                    strwditemid = next((t for t in arrwdtokens if t.startswith('Q')), '')
                                    strsqlpersons_wikidata = ""
                                    if strwdpropertyid and strwditemid:
                                        strsqlpersons_wikidata = "SELECT DISTINCT T_WC_TMDB_PERSON.ID_PERSON, NULL AS ORIGINAL_ORDER, T_WC_TMDB_PERSON.POPULARITY, T_WC_TMDB_PERSON.BIRTHDAY AS SORT_DATE FROM T_WC_WIKIDATA_ITEM_VALUE wv JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT STRAIGHT_JOIN T_WC_TMDB_PERSON ON T_WC_TMDB_PERSON.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY ='" + strwdpropertyid + "' AND wv.ID_ITEM = '" + strwditemid + "' AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated') AND T_WC_TMDB_PERSON.ADULT = 0 AND T_WC_TMDB_PERSON.ID_WIKIDATA IS NOT NULL AND T_WC_TMDB_PERSON.ID_WIKIDATA <> '' "

                                    # Mechanism 3: TMDb person name filter from TMDB_ELEMENTS
                                    strtmdbelements = row['TMDB_ELEMENTS'] or ''
                                    strsqlpersons_name = ""
                                    strnamematch = re.search(r"T_WC_TMDB_PERSON\.NAME\s*=\s*'([^']+)'", strtmdbelements.replace('&#039;', "'"))
                                    if strnamematch:
                                        strpersonname = strnamematch.group(1).strip().replace("'", "''")
                                        strsqlpersons_name = "SELECT ID_PERSON, NULL AS ORIGINAL_ORDER, POPULARITY, BIRTHDAY AS SORT_DATE FROM T_WC_TMDB_PERSON WHERE NAME = '" + strpersonname + "' AND ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "

                                    # Combine mechanisms cumulatively
                                    arrsqlpersons_sources = [s for s in [strsqlpersons_imdb, strsqlpersons_wikidata, strsqlpersons_name] if s]
                                    strsqlpersons = f_buildcustomaggregatequery(arrsqlpersons_sources, "ID_PERSON", "POPULARITY", intsortby)
                                elif intgroup == 1 or intgroup == 2 or intgroup == 3:
                                    strsqlpersons += "SELECT DISTINCT T_WC_TMDB_PERSON.ID_PERSON, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.NAME, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.BIRTHDAY, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.DEATHDAY, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.ID_IMDB, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.BIOGRAPHY, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.PROFILE_PATH, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.ID_WIKIDATA "
                                    strsqlpersons += f_personlinkfromsql()
                                if strsqlpersons != "":
                                    # Retrieving elements for this group (group/group)
                                    if intgroup == 4:
                                        cursor2.execute(strsqlpersons)
                                    else:
                                        cursor2.execute(strsqlpersons, (strpropertyid, strrecordid))
                                    results = cursor2.fetchall()
                                    lngpersoncount = len(results)
                                    #print(f"{lngpersoncount} lines")
                                    if lngpersoncount > 1:
                                        # This group has more than one element (person)
                                        # So we create/update this group
                                        lnggroupid = cp.f_sqlupdatearray(strsqltablename, arrgroupcouples, strsqlupdatecondition, 1)
                                        if lnggroupid is None:
                                            strsqlgroup = "SELECT ID_GROUP FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                            cursor3.execute(strsqlgroup)
                                            lngrowcount = cursor3.rowcount
                                            if lngrowcount == 0:
                                                print("Error: Failed to create/update group - lnggroupid is None")
                                                continue
                                            lnggroupid = cursor3.fetchone()["ID_GROUP"]
                                        lnggroupcreatedcount += 1
                                        cp.f_setservervariable("strtmdbmoviepreprocessgroupid",str(lnggroupid),"Current group ID created/updated by the T2S group derivation (process 43)",0)
                                        if intgroup == 1 or intgroup == 2 or intgroup == 3 or intgroup == 4:
                                            # Retrieve all persons for this group
                                            # Only processing when handling original English (records from T_WC_TMDB_GROUP or T_WC_TMDB_GROUP) to avoid duplicates with the translated versions
                                            lngdisplayorder = 0
                                            arrcurrentpersonids = []
                                            for row in results:
                                                lngpersonid = row["ID_PERSON"]
                                                lngdisplayorder += 1
                                                arrcurrentpersonids.append(str(lngpersonid))
                                                arrpersongroupcouples = {
                                                    'ID_PERSON': lngpersonid,
                                                    'ID_GROUP': lnggroupid,
                                                    'DISPLAY_ORDER': lngdisplayorder
                                                }
                                                strsqlupdatecondition2 = "ID_PERSON = " + str(lngpersonid) + " AND ID_GROUP = " + str(lnggroupid)
                                                #print(strsqlupdatecondition2)
                                                cp.f_sqlupdatearray("T_WC_T2S_PERSON_GROUP", arrpersongroupcouples, strsqlupdatecondition2, 1)
                                            if arrcurrentpersonids:
                                                strsqldeleteperson = "DELETE FROM T_WC_T2S_PERSON_GROUP WHERE ID_GROUP = " + str(lnggroupid) + " AND ID_PERSON NOT IN (" + ",".join(arrcurrentpersonids) + ") "
                                                print(strsqldeleteperson)
                                                cursor2.execute(strsqldeleteperson)
                                                strsqldeleteduplicates = "DELETE pg1 FROM T_WC_T2S_PERSON_GROUP pg1 INNER JOIN T_WC_T2S_PERSON_GROUP pg2 ON pg1.ID_GROUP = pg2.ID_GROUP AND pg1.ID_PERSON = pg2.ID_PERSON AND pg1.ID_ROW > pg2.ID_ROW WHERE pg1.ID_GROUP = " + str(lnggroupid)
                                                print(strsqldeleteduplicates)
                                                cursor2.execute(strsqldeleteduplicates)
                                            arrgroupcouples = {
                                                'PERSON_COUNT': lngpersoncount
                                            }
                                            cp.f_sqlupdatearray(strsqltablename, arrgroupcouples, strsqlupdatecondition, 1)
                                    else:
                                        # This group has only one element or none
                                        # So we delete this group if it already exists
                                        strsqltablename = "T_WC_T2S_GROUP"
                                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                        print(strsqldelete)
                                        cursor2.execute(strsqldelete)
                                        lnggroupdeletedcount += 1
                                        #cursor2.commit()
                    if 1:
                        strsqltablename = "T_WC_T2S_GROUP"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE GROUP_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqltablename = "T_WC_T2S_GROUP"
                        # Count-based stale delete for Wikidata-sourced groups. Removes any non-custom
                        # group whose item no longer resolves to >= 2 linked TMDb persons. This covers
                        # both the "item gone entirely" case (count 0, the old NOT EXISTS behaviour) and
                        # the "degraded from >=2 to <2 persons" case that the per-item singleton delete
                        # used to handle before the driving query was pre-filtered. Mirrors the
                        # pre-filter joins so iteration and cleanup stay consistent. Runs over the small
                        # T_WC_T2S_GROUP table, so the correlated subquery executes only a few thousand times.
                        strsqldelete = f_persongrouppurgesql("T_WC_T2S_GROUP", "GROUP_SOURCE")
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqldelete = ""
                        strsqldelete += "DELETE FROM T_WC_T2S_GROUP "
                        strsqldelete += "WHERE GROUP_SOURCE = 'custom' "
                        strsqldelete += "AND ID_WIKIDATA NOT IN (SELECT ID_CUSTOM_LIST FROM T_WC_CUSTOM_LIST WHERE TARGET_TABLE = 3 AND DELETED = 0) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        
                        # Update T_WC_T2S_GROUP.POPULARITY 
                        strsql = """UPDATE T_WC_T2S_GROUP t
JOIN (
    SELECT
        mt.ID_GROUP,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_PERSON_GROUP mt
    INNER JOIN T_WC_T2S_PERSON m
        ON m.ID_PERSON = mt.ID_PERSON
    INNER JOIN T_WC_T2S_GROUP t2
        ON t2.ID_GROUP = mt.ID_GROUP
       AND t2.GROUP_TYPE = 'group'
    GROUP BY mt.ID_GROUP
) x
    ON x.ID_GROUP = t.ID_GROUP
SET
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        strsqltablename = "T_WC_T2S_PERSON_GROUP"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_GROUP NOT IN (SELECT ID_GROUP FROM T_WC_T2S_GROUP) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        # Group-derivation telemetry (process 43): final run summary
                        strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                        cp.f_setservervariable("strtmdbmoviepreprocessgroupenddatetime",strnow,"End datetime of the last T2S group derivation (process 43)",0)
                        cp.f_setservervariable("strtmdbmoviepreprocessgroupcreatedcount",str(lnggroupcreatedcount),"Number of groups created/updated by the T2S group derivation (process 43)",0)
                        cp.f_setservervariable("strtmdbmoviepreprocessgroupdeletedcount",str(lnggroupdeletedcount),"Number of singleton groups deleted by the T2S group derivation (process 43)",0)
                        cp.f_setservervariable("strtmdbmoviepreprocessgroupprocessedseconds",f"{time.time() - fltgroupprocessstart:.2f}","Elapsed seconds of the last T2S group derivation (process 43)",0)

                elif intindex == 44:
                    #----------------------------------------------------
                    print("T2S_AWARD processing")
                    # TMDB-MOVIE-PREPROCESS-036 : le cone P279 sous Q618779 dit ce qui est une
                    # recompense. Reconstruit ici depuis T_WC_WIKIDATA_SUBCLASS, que le crawler
                    # Wikidata reecrit a chaque run, d'ou le garde-fou : en dessous du plancher on
                    # saute le processus au lieu de lever une erreur, la boucle n'ayant pas de try
                    # et une erreur coutant les cinquante processus suivants. Voir f_awardconeguard.
                    blnconeok, lngconeclasses = f_awardconeguard(44)
                    if not blnconeok:
                        continue
                    telaward = EntityTelemetry("award", 44, "award")
                    telaward.begin()

                    strpropertyid = "P166"
                    strawardsource = strpropertyid
                    strawardtype = "award"
                    target_field_name = "AWARD_NAME"

                    # TMDB-MOVIE-PREPROCESS-039 : l'ensemble pilote est lu dans les statements
                    # V2. Ce n'est pas un debranchement, c'est une correction : ITEM_PROPERTY
                    # rangeait sous P166 la recompense, la ceremonie et l'oeuvre, indistinctement.
                    # Toute la requete, pre-filtre T2S et cone compris, vit dans
                    # f_awarddrivingsql() pour que la purge ne puisse pas en diverger.
                    strsql = f_awarddrivingsql()

                    print(strsql)
                    cursor.execute(strsql, (strpropertyid,))
                    lngrowcount = cursor.rowcount
                    print(f"{lngrowcount} lines")
                    results = cursor.fetchall()
                    for row in results:
                        strawardwikidataid = row['ID_ITEM']
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(strawardwikidataid),"Current record in the TMDb database movie preprocess",0)

                        strawardname = ""
                        strawardoverview = ""
                        strawardimagepath = ""
                        strsqlitem = ""
                        # Image: prefer T_WC_WIKIPEDIA_PAGE_LANG (WIKIPEDIA-CRAWLER-020), fall back on
                        # the V1 column until the V1 tables are dropped. LABEL / DESCRIPTION still
                        # come from V1: their migration is WIKIDATA-CRAWLER-017, not this one.
                        # WIKIDATA-CRAWLER-017 : texte EN et image pris dans V2 / PAGE_LANG, V1 en repli.
                        # La requete part d une table derivee d une ligne et non de V1, sinon une entite
                        # absente de V1 ne rendrait rien du tout. COALESCE final sur '' pour garantir une
                        # chaine et non NULL aux appelants, qui concatenent ces valeurs.
                        strsqlitem += "SELECT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.en')), "
                        strsqlitem += "  NULLIF(v2.LABEL_EN,''), v1.LABEL, '') AS LABEL, "
                        strsqlitem += "COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.DESCRIPTIONS_JSON,'$.en')), "
                        strsqlitem += "  NULLIF(v2.DESCRIPTION_EN,''), v1.DESCRIPTION, '') AS DESCRIPTION, "
                        strsqlitem += "COALESCE(pl.MAIN_IMAGE_URL, v1.WIKIPEDIA_IMAGE_PATH, '') AS WIKIPEDIA_IMAGE_PATH "
                        strsqlitem += "FROM (SELECT %s AS ID_WIKIDATA) k "
                        strsqlitem += "LEFT JOIN T_WC_WIKIDATA_ITEM v2 ON v2.ID_WIKIDATA = k.ID_WIKIDATA "
                        strsqlitem += "LEFT JOIN T_WC_WIKIDATA_ITEM_V1 v1 "
                        strsqlitem += "  ON v1.ID_WIKIDATA = k.ID_WIKIDATA AND v1.LANG = 'en' "
                        strsqlitem += "LEFT JOIN T_WC_WIKIPEDIA_PAGE_LANG pl "
                        strsqlitem += "  ON pl.ID_WIKIDATA = k.ID_WIKIDATA AND pl.LANG = 'en' "
                        strsqlitem += "  AND COALESCE(pl.MAIN_IMAGE_URL,'') <> ''"
                        arrvalues = cp.f_fieldsfromquery(
                            strsqlitem,
                            "strawardname|strawardoverview|strawardimagepath",
                            "LABEL|DESCRIPTION|WIKIPEDIA_IMAGE_PATH",
                            params=(strawardwikidataid,),
                            target_dict=None,
                        )
                        strawardname = arrvalues.get("strawardname", "")
                        strawardoverview = arrvalues.get("strawardoverview", "")
                        strawardimagepath = arrvalues.get("strawardimagepath", "")
                        strawardimagepath = f_getwikidataimagepath(strawardwikidataid)

                        # WIKIDATA-CRAWLER-017 : le libelle FR vient desormais de V2 (LABELS_JSON),
                        # V1 ne servant plus que de repli, le temps que le gap d entites se ferme.
                        strawardnamefr = f_getwikidatalabel(strawardwikidataid, "fr")

                        print("Processing record: " + str(strawardwikidataid) + ": " + strawardname + " (" + strawardsource + ")")
                        telaward.position(recordid=strawardwikidataid, currentvalue=strawardname, currentprocess=f"{strpropertyid}: Copying from WIKIDATA to T2S_AWARD")

                        if target_field_name == "AWARD_NAME":
                            arrawardcouples = {
                                'ID_WIKIDATA': strawardwikidataid,
                                'AWARD_NAME': strawardname,
                                'AWARD_NAME_FR': strawardnamefr,
                                'OVERVIEW': strawardoverview,
                                'AWARD_SOURCE': strawardsource,
                                'AWARD_TYPE': strawardtype,
                                'WIKIPEDIA_IMAGE_PATH': strawardimagepath,
                            }

                        strsqltablename = "T_WC_T2S_AWARD"
                        strsqlupdatecondition = f"ID_WIKIDATA = '{strawardwikidataid}' AND AWARD_SOURCE = '{strawardsource}'"
                        lngawardid = cp.f_sqlupdatearray(strsqltablename, arrawardcouples, strsqlupdatecondition, 1)
                        if lngawardid is None:
                            strsqlaward = "SELECT ID_AWARD FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                            cursor3.execute(strsqlaward)
                            lngrowcount = cursor3.rowcount
                            if lngrowcount == 0:
                                print("Error: Failed to create/update award - lngawardid is None")
                                continue
                            lngawardid = cursor3.fetchone()["ID_AWARD"]
                        telaward.created()
                        telaward.set_entity_id(lngawardid)

                        # Link to movies
                        strsqlmovies = f_awardlinksql("T_WC_T2S_MOVIE", "m", "ID_MOVIE", "IMDB_RATING_WEIGHTED")
                        cursor2.execute(strsqlmovies, (strpropertyid, strawardwikidataid))
                        results_movies = cursor2.fetchall()
                        lngmoviecount = len(results_movies)
                        lngdisplayorder = 0
                        arrcurrentmovieids = []
                        for rowm in results_movies:
                            lngdisplayorder += 1
                            lngmovieid = rowm["ID_MOVIE"]
                            arrcurrentmovieids.append(str(lngmovieid))
                            arrmovieawardcouples = {
                                'ID_MOVIE': lngmovieid,
                                'ID_AWARD': lngawardid,
                                'DISPLAY_ORDER': lngdisplayorder
                            }
                            strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND ID_AWARD = " + str(lngawardid)
                            cp.f_sqlupdatearray("T_WC_T2S_MOVIE_AWARD", arrmovieawardcouples, strsqlupdatecondition2, 1)
                        if arrcurrentmovieids:
                            strsqldelete = "DELETE FROM T_WC_T2S_MOVIE_AWARD WHERE ID_AWARD = " + str(lngawardid) + " AND ID_MOVIE NOT IN (" + ",".join(arrcurrentmovieids) + ") "
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)
                            strsqldelete = "DELETE ma1 FROM T_WC_T2S_MOVIE_AWARD ma1 INNER JOIN T_WC_T2S_MOVIE_AWARD ma2 ON ma1.ID_AWARD = ma2.ID_AWARD AND ma1.ID_MOVIE = ma2.ID_MOVIE AND ma1.ID_ROW > ma2.ID_ROW WHERE ma1.ID_AWARD = " + str(lngawardid)
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)

                        # Link to series
                        strsqlseries = f_awardlinksql("T_WC_T2S_SERIE", "s", "ID_SERIE", "IMDB_RATING_WEIGHTED")
                        cursor4.execute(strsqlseries, (strpropertyid, strawardwikidataid))
                        results_series = cursor4.fetchall()
                        lngseriecount = len(results_series)
                        lngdisplayorder = 0
                        arrcurrentserieids = []
                        for rows in results_series:
                            lngdisplayorder += 1
                            lngserieid = rows["ID_SERIE"]
                            arrcurrentserieids.append(str(lngserieid))
                            arrserieawardcouples = {
                                'ID_SERIE': lngserieid,
                                'ID_AWARD': lngawardid,
                                'DISPLAY_ORDER': lngdisplayorder
                            }
                            strsqlupdatecondition2 = "ID_SERIE = " + str(lngserieid) + " AND ID_AWARD = " + str(lngawardid)
                            cp.f_sqlupdatearray("T_WC_T2S_SERIE_AWARD", arrserieawardcouples, strsqlupdatecondition2, 1)
                        if arrcurrentserieids:
                            strsqldelete = "DELETE FROM T_WC_T2S_SERIE_AWARD WHERE ID_AWARD = " + str(lngawardid) + " AND ID_SERIE NOT IN (" + ",".join(arrcurrentserieids) + ") "
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)
                            strsqldelete = "DELETE sa1 FROM T_WC_T2S_SERIE_AWARD sa1 INNER JOIN T_WC_T2S_SERIE_AWARD sa2 ON sa1.ID_AWARD = sa2.ID_AWARD AND sa1.ID_SERIE = sa2.ID_SERIE AND sa1.ID_ROW > sa2.ID_ROW WHERE sa1.ID_AWARD = " + str(lngawardid)
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)

                        # Link to persons
                        strsqlpersons = f_awardlinksql("T_WC_T2S_PERSON", "p2", "ID_PERSON", "POPULARITY")
                        cursor5.execute(strsqlpersons, (strpropertyid, strawardwikidataid))
                        results_persons = cursor5.fetchall()
                        lngpersoncount = len(results_persons)
                        lngdisplayorder = 0
                        arrcurrentpersonids = []
                        for rowp in results_persons:
                            lngdisplayorder += 1
                            lngpersonid = rowp["ID_PERSON"]
                            arrcurrentpersonids.append(str(lngpersonid))
                            arrpersonawardcouples = {
                                'ID_PERSON': lngpersonid,
                                'ID_AWARD': lngawardid,
                                'DISPLAY_ORDER': lngdisplayorder
                            }
                            strsqlupdatecondition2 = "ID_PERSON = " + str(lngpersonid) + " AND ID_AWARD = " + str(lngawardid)
                            cp.f_sqlupdatearray("T_WC_T2S_PERSON_AWARD", arrpersonawardcouples, strsqlupdatecondition2, 1)
                        if arrcurrentpersonids:
                            strsqldelete = "DELETE FROM T_WC_T2S_PERSON_AWARD WHERE ID_AWARD = " + str(lngawardid) + " AND ID_PERSON NOT IN (" + ",".join(arrcurrentpersonids) + ") "
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)
                            strsqldelete = "DELETE pa1 FROM T_WC_T2S_PERSON_AWARD pa1 INNER JOIN T_WC_T2S_PERSON_AWARD pa2 ON pa1.ID_AWARD = pa2.ID_AWARD AND pa1.ID_PERSON = pa2.ID_PERSON AND pa1.ID_ROW > pa2.ID_ROW WHERE pa1.ID_AWARD = " + str(lngawardid)
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)

                        arrawardcounts = {
                            'MOVIE_COUNT': lngmoviecount,
                            'SERIE_COUNT': lngseriecount,
                            'PERSON_COUNT': lngpersoncount,
                        }
                        cp.f_sqlupdatearray(strsqltablename, arrawardcounts, strsqlupdatecondition, 1)

                    if 1:
                        strsqltablename = "T_WC_T2S_AWARD"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE AWARD_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        telaward.deleted(cursor2.rowcount)

                        strsqltablename = "T_WC_T2S_AWARD"
                        # Stale delete, inverse of the driving pre-filter. Removes any award whose item
                        # no longer has >= 1 linked T2S entity. Covers the "item gone" case (no property
                        # row at all, the old NOT EXISTS behaviour) and the "now empty / degraded to zero
                        # tracked recipients" case introduced by pre-filtering the driving query. Orphan
                        # junction rows are cleaned up by the ID_AWARD NOT IN (...) deletes below.
                        strsqldelete = f_awardpurgesql("T_WC_T2S_AWARD", "AWARD_SOURCE")
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        telaward.deleted(cursor2.rowcount)

                        # Update T_WC_T2S_AWARD.POPULARITY from persons
                        strsql = """UPDATE T_WC_T2S_AWARD t
JOIN (
    SELECT
        mt.ID_AWARD,
        AVG(p.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_PERSON_AWARD mt
    INNER JOIN T_WC_T2S_PERSON p
        ON p.ID_PERSON = mt.ID_PERSON
    GROUP BY mt.ID_AWARD
) x
    ON x.ID_AWARD = t.ID_AWARD
SET
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_AWARD ratings from movies
                        strsql = """UPDATE T_WC_T2S_AWARD t
JOIN (
    SELECT
        mt.ID_AWARD,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED
    FROM T_WC_T2S_MOVIE_AWARD mt
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mt.ID_MOVIE
    GROUP BY mt.ID_AWARD
) x
    ON x.ID_AWARD = t.ID_AWARD
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_AWARD ratings from series
                        strsql = """UPDATE T_WC_T2S_AWARD t
JOIN (
    SELECT
        mt.ID_AWARD,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED
    FROM T_WC_T2S_SERIE_AWARD mt
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = mt.ID_SERIE
    GROUP BY mt.ID_AWARD
) x
    ON x.ID_AWARD = t.ID_AWARD
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED);
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        strsqltablename = "T_WC_T2S_MOVIE_AWARD"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_AWARD NOT IN (SELECT ID_AWARD FROM T_WC_T2S_AWARD) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqltablename = "T_WC_T2S_SERIE_AWARD"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_AWARD NOT IN (SELECT ID_AWARD FROM T_WC_T2S_AWARD) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqltablename = "T_WC_T2S_PERSON_AWARD"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_AWARD NOT IN (SELECT ID_AWARD FROM T_WC_T2S_AWARD) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                    telaward.finish()

                elif intindex == 47:
                    #----------------------------------------------------
                    print("T2S_NOMINATION processing")
                    # TMDB-MOVIE-PREPROCESS-036 : le cone P279 sous Q618779 dit ce qui est une
                    # recompense. Reconstruit ici depuis T_WC_WIKIDATA_SUBCLASS, que le crawler
                    # Wikidata reecrit a chaque run, d'ou le garde-fou : en dessous du plancher on
                    # saute le processus au lieu de lever une erreur, la boucle n'ayant pas de try
                    # et une erreur coutant les cinquante processus suivants. Voir f_awardconeguard.
                    blnconeok, lngconeclasses = f_awardconeguard(47)
                    if not blnconeok:
                        continue
                    telnomination = EntityTelemetry("nomination", 47, "nomination")
                    telnomination.begin()

                    strpropertyid = "P1411"
                    strnominationsource = strpropertyid
                    strnominationtype = "nomination"
                    target_field_name = "NOMINATION_NAME"

                    # TMDB-MOVIE-PREPROCESS-039 : l'ensemble pilote est lu dans les statements
                    # V2. Ce n'est pas un debranchement, c'est une correction : ITEM_PROPERTY
                    # rangeait sous P166 la recompense, la ceremonie et l'oeuvre, indistinctement.
                    # Toute la requete, pre-filtre T2S et cone compris, vit dans
                    # f_awarddrivingsql() pour que la purge ne puisse pas en diverger.
                    strsql = f_awarddrivingsql()

                    print(strsql)
                    cursor.execute(strsql, (strpropertyid,))
                    lngrowcount = cursor.rowcount
                    print(f"{lngrowcount} lines")
                    results = cursor.fetchall()
                    for row in results:
                        strnominationwikidataid = row['ID_ITEM']
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(strnominationwikidataid),"Current record in the TMDb database movie preprocess",0)

                        strnominationname = ""
                        strnominationoverview = ""
                        strnominationimagepath = ""
                        strsqlitem = ""
                        # Image: prefer T_WC_WIKIPEDIA_PAGE_LANG (WIKIPEDIA-CRAWLER-020), fall back on
                        # the V1 column until the V1 tables are dropped. LABEL / DESCRIPTION still
                        # come from V1: their migration is WIKIDATA-CRAWLER-017, not this one.
                        # WIKIDATA-CRAWLER-017 : texte EN et image pris dans V2 / PAGE_LANG, V1 en repli.
                        # La requete part d une table derivee d une ligne et non de V1, sinon une entite
                        # absente de V1 ne rendrait rien du tout. COALESCE final sur '' pour garantir une
                        # chaine et non NULL aux appelants, qui concatenent ces valeurs.
                        strsqlitem += "SELECT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.en')), "
                        strsqlitem += "  NULLIF(v2.LABEL_EN,''), v1.LABEL, '') AS LABEL, "
                        strsqlitem += "COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.DESCRIPTIONS_JSON,'$.en')), "
                        strsqlitem += "  NULLIF(v2.DESCRIPTION_EN,''), v1.DESCRIPTION, '') AS DESCRIPTION, "
                        strsqlitem += "COALESCE(pl.MAIN_IMAGE_URL, v1.WIKIPEDIA_IMAGE_PATH, '') AS WIKIPEDIA_IMAGE_PATH "
                        strsqlitem += "FROM (SELECT %s AS ID_WIKIDATA) k "
                        strsqlitem += "LEFT JOIN T_WC_WIKIDATA_ITEM v2 ON v2.ID_WIKIDATA = k.ID_WIKIDATA "
                        strsqlitem += "LEFT JOIN T_WC_WIKIDATA_ITEM_V1 v1 "
                        strsqlitem += "  ON v1.ID_WIKIDATA = k.ID_WIKIDATA AND v1.LANG = 'en' "
                        strsqlitem += "LEFT JOIN T_WC_WIKIPEDIA_PAGE_LANG pl "
                        strsqlitem += "  ON pl.ID_WIKIDATA = k.ID_WIKIDATA AND pl.LANG = 'en' "
                        strsqlitem += "  AND COALESCE(pl.MAIN_IMAGE_URL,'') <> ''"
                        arrvalues = cp.f_fieldsfromquery(
                            strsqlitem,
                            "strnominationname|strnominationoverview|strnominationimagepath",
                            "LABEL|DESCRIPTION|WIKIPEDIA_IMAGE_PATH",
                            params=(strnominationwikidataid,),
                            target_dict=None,
                        )
                        strnominationname = arrvalues.get("strnominationname", "")
                        strnominationoverview = arrvalues.get("strnominationoverview", "")
                        strnominationimagepath = arrvalues.get("strnominationimagepath", "")
                        strnominationimagepath = f_getwikidataimagepath(strnominationwikidataid)

                        # WIKIDATA-CRAWLER-017 : le libelle FR vient desormais de V2 (LABELS_JSON),
                        # V1 ne servant plus que de repli, le temps que le gap d entites se ferme.
                        strnominationnamefr = f_getwikidatalabel(strnominationwikidataid, "fr")

                        print("Processing record: " + str(strnominationwikidataid) + ": " + strnominationname + " (" + strnominationsource + ")")
                        telnomination.position(recordid=strnominationwikidataid, currentvalue=strnominationname, currentprocess=f"{strpropertyid}: Copying from WIKIDATA to T2S_NOMINATION")

                        if target_field_name == "NOMINATION_NAME":
                            arrnominationcouples = {
                                'ID_WIKIDATA': strnominationwikidataid,
                                'NOMINATION_NAME': strnominationname,
                                'NOMINATION_NAME_FR': strnominationnamefr,
                                'OVERVIEW': strnominationoverview,
                                'NOMINATION_SOURCE': strnominationsource,
                                'NOMINATION_TYPE': strnominationtype,
                                'WIKIPEDIA_IMAGE_PATH': strnominationimagepath,
                            }

                        strsqltablename = "T_WC_T2S_NOMINATION"
                        strsqlupdatecondition = f"ID_WIKIDATA = '{strnominationwikidataid}' AND NOMINATION_SOURCE = '{strnominationsource}'"
                        lngnominationid = cp.f_sqlupdatearray(strsqltablename, arrnominationcouples, strsqlupdatecondition, 1)
                        if lngnominationid is None:
                            strsqlnomination = "SELECT ID_NOMINATION FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                            cursor3.execute(strsqlnomination)
                            lngrowcount2 = cursor3.rowcount
                            if lngrowcount2 == 0:
                                print("Error: Failed to create/update nomination - lngnominationid is None")
                                continue
                            lngnominationid = cursor3.fetchone()["ID_NOMINATION"]
                        telnomination.created()
                        telnomination.set_entity_id(lngnominationid)

                        # Link to movies
                        strsqlmovies = f_awardlinksql("T_WC_T2S_MOVIE", "m", "ID_MOVIE", "IMDB_RATING_WEIGHTED")
                        cursor2.execute(strsqlmovies, (strpropertyid, strnominationwikidataid))
                        results_movies = cursor2.fetchall()
                        lngmoviecount = len(results_movies)
                        lngdisplayorder = 0
                        arrcurrentmovieids = []
                        for rowm in results_movies:
                            lngdisplayorder += 1
                            lngmovieid = rowm["ID_MOVIE"]
                            arrcurrentmovieids.append(str(lngmovieid))
                            arrmovienominationcouples = {
                                'ID_MOVIE': lngmovieid,
                                'ID_NOMINATION': lngnominationid,
                                'DISPLAY_ORDER': lngdisplayorder,
                            }
                            strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND ID_NOMINATION = " + str(lngnominationid)
                            cp.f_sqlupdatearray("T_WC_T2S_MOVIE_NOMINATION", arrmovienominationcouples, strsqlupdatecondition2, 1)
                        if arrcurrentmovieids:
                            strsqldelete = "DELETE FROM T_WC_T2S_MOVIE_NOMINATION WHERE ID_NOMINATION = " + str(lngnominationid) + " AND ID_MOVIE NOT IN (" + ",".join(arrcurrentmovieids) + ") "
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)
                            strsqldelete = "DELETE mn1 FROM T_WC_T2S_MOVIE_NOMINATION mn1 INNER JOIN T_WC_T2S_MOVIE_NOMINATION mn2 ON mn1.ID_NOMINATION = mn2.ID_NOMINATION AND mn1.ID_MOVIE = mn2.ID_MOVIE AND mn1.ID_ROW > mn2.ID_ROW WHERE mn1.ID_NOMINATION = " + str(lngnominationid)
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)

                        # Link to series
                        strsqlseries = f_awardlinksql("T_WC_T2S_SERIE", "s", "ID_SERIE", "IMDB_RATING_WEIGHTED")
                        cursor4.execute(strsqlseries, (strpropertyid, strnominationwikidataid))
                        results_series = cursor4.fetchall()
                        lngseriecount = len(results_series)
                        lngdisplayorder = 0
                        arrcurrentserieids = []
                        for rows in results_series:
                            lngdisplayorder += 1
                            lngserieid = rows["ID_SERIE"]
                            arrcurrentserieids.append(str(lngserieid))
                            arrserienominationcouples = {
                                'ID_SERIE': lngserieid,
                                'ID_NOMINATION': lngnominationid,
                                'DISPLAY_ORDER': lngdisplayorder,
                            }
                            strsqlupdatecondition2 = "ID_SERIE = " + str(lngserieid) + " AND ID_NOMINATION = " + str(lngnominationid)
                            cp.f_sqlupdatearray("T_WC_T2S_SERIE_NOMINATION", arrserienominationcouples, strsqlupdatecondition2, 1)
                        if arrcurrentserieids:
                            strsqldelete = "DELETE FROM T_WC_T2S_SERIE_NOMINATION WHERE ID_NOMINATION = " + str(lngnominationid) + " AND ID_SERIE NOT IN (" + ",".join(arrcurrentserieids) + ") "
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)
                            strsqldelete = "DELETE sn1 FROM T_WC_T2S_SERIE_NOMINATION sn1 INNER JOIN T_WC_T2S_SERIE_NOMINATION sn2 ON sn1.ID_NOMINATION = sn2.ID_NOMINATION AND sn1.ID_SERIE = sn2.ID_SERIE AND sn1.ID_ROW > sn2.ID_ROW WHERE sn1.ID_NOMINATION = " + str(lngnominationid)
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)

                        # Link to persons
                        strsqlpersons = f_awardlinksql("T_WC_T2S_PERSON", "p2", "ID_PERSON", "POPULARITY")
                        cursor5.execute(strsqlpersons, (strpropertyid, strnominationwikidataid))
                        results_persons = cursor5.fetchall()
                        lngpersoncount = len(results_persons)
                        lngdisplayorder = 0
                        arrcurrentpersonids = []
                        for rowp in results_persons:
                            lngdisplayorder += 1
                            lngpersonid = rowp["ID_PERSON"]
                            arrcurrentpersonids.append(str(lngpersonid))
                            arrpersonnominationcouples = {
                                'ID_PERSON': lngpersonid,
                                'ID_NOMINATION': lngnominationid,
                                'DISPLAY_ORDER': lngdisplayorder,
                            }
                            strsqlupdatecondition2 = "ID_PERSON = " + str(lngpersonid) + " AND ID_NOMINATION = " + str(lngnominationid)
                            cp.f_sqlupdatearray("T_WC_T2S_PERSON_NOMINATION", arrpersonnominationcouples, strsqlupdatecondition2, 1)
                        if arrcurrentpersonids:
                            strsqldelete = "DELETE FROM T_WC_T2S_PERSON_NOMINATION WHERE ID_NOMINATION = " + str(lngnominationid) + " AND ID_PERSON NOT IN (" + ",".join(arrcurrentpersonids) + ") "
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)
                            strsqldelete = "DELETE pn1 FROM T_WC_T2S_PERSON_NOMINATION pn1 INNER JOIN T_WC_T2S_PERSON_NOMINATION pn2 ON pn1.ID_NOMINATION = pn2.ID_NOMINATION AND pn1.ID_PERSON = pn2.ID_PERSON AND pn1.ID_ROW > pn2.ID_ROW WHERE pn1.ID_NOMINATION = " + str(lngnominationid)
                            print(strsqldelete)
                            cursor2.execute(strsqldelete)

                        arrnominationcounts = {
                            'MOVIE_COUNT': lngmoviecount,
                            'SERIE_COUNT': lngseriecount,
                            'PERSON_COUNT': lngpersoncount,
                        }
                        cp.f_sqlupdatearray(strsqltablename, arrnominationcounts, strsqlupdatecondition, 1)

                    if 1:
                        strsqltablename = "T_WC_T2S_NOMINATION"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE NOMINATION_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        telnomination.deleted(cursor2.rowcount)

                        strsqltablename = "T_WC_T2S_NOMINATION"
                        # Stale delete, inverse of the driving pre-filter. Removes any nomination whose
                        # item no longer has >= 1 linked T2S entity. Covers the "item gone" case (no
                        # property row at all, the old NOT EXISTS behaviour) and the "now empty / degraded
                        # to zero tracked recipients" case introduced by pre-filtering the driving query.
                        # Orphan junction rows are cleaned up by the ID_NOMINATION NOT IN (...) deletes below.
                        strsqldelete = f_awardpurgesql("T_WC_T2S_NOMINATION", "NOMINATION_SOURCE")
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        telnomination.deleted(cursor2.rowcount)

                        # Update T_WC_T2S_NOMINATION.POPULARITY from persons
                        strsql = """UPDATE T_WC_T2S_NOMINATION t
JOIN (
    SELECT
        mt.ID_NOMINATION,
        AVG(p.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_PERSON_NOMINATION mt
    INNER JOIN T_WC_T2S_PERSON p
        ON p.ID_PERSON = mt.ID_PERSON
    GROUP BY mt.ID_NOMINATION
) x
    ON x.ID_NOMINATION = t.ID_NOMINATION
SET
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_NOMINATION ratings from movies
                        strsql = """UPDATE T_WC_T2S_NOMINATION t
JOIN (
    SELECT
        mt.ID_NOMINATION,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED
    FROM T_WC_T2S_MOVIE_NOMINATION mt
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mt.ID_MOVIE
    GROUP BY mt.ID_NOMINATION
) x
    ON x.ID_NOMINATION = t.ID_NOMINATION
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_NOMINATION ratings from series
                        strsql = """UPDATE T_WC_T2S_NOMINATION t
JOIN (
    SELECT
        mt.ID_NOMINATION,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED
    FROM T_WC_T2S_SERIE_NOMINATION mt
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = mt.ID_SERIE
    GROUP BY mt.ID_NOMINATION
) x
    ON x.ID_NOMINATION = t.ID_NOMINATION
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED);
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        strsqltablename = "T_WC_T2S_MOVIE_NOMINATION"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_NOMINATION NOT IN (SELECT ID_NOMINATION FROM T_WC_T2S_NOMINATION) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqltablename = "T_WC_T2S_SERIE_NOMINATION"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_NOMINATION NOT IN (SELECT ID_NOMINATION FROM T_WC_T2S_NOMINATION) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqltablename = "T_WC_T2S_PERSON_NOMINATION"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_NOMINATION NOT IN (SELECT ID_NOMINATION FROM T_WC_T2S_NOMINATION) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                    telnomination.finish()

                elif intindex == 45:
                    #----------------------------------------------------
                    print("T2S_MOVEMENT processing")
                    telmovement = EntityTelemetry("movement", 45, "movement")
                    telmovement.begin()

                    arrlists = {1: 'custom-movement', 2: 'movement-delete'}
                    for intlist, strlist in arrlists.items():
                        strsql = ""
                        strsqldelete = ""
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess",strlist,"Current sub process in the TMDb database movie preprocess",0)
                        if intlist == 1:
                            strcurrentprocess = f"{intlist}: Copying from T_WC_CUSTOM_LIST to T2S_MOVEMENT"
                            strsql += "SELECT 'custom' AS MOVEMENT_SOURCE, 'movement' AS MOVEMENT_TYPE, T_WC_CUSTOM_LIST.ID_CUSTOM_LIST AS ID_RECORD, T_WC_CUSTOM_LIST.LIST_NAME AS NAME, T_WC_CUSTOM_LIST.LIST_NAME_FR AS NAME_FR, T_WC_CUSTOM_LIST.OVERVIEW AS OVERVIEW, 'en' AS LANG, T_WC_CUSTOM_LIST.POSTER_PATH, NULL AS ID_WIKIDATA, T_WC_CUSTOM_LIST.ID_IMDB_LIST, T_WC_CUSTOM_LIST.WIKIDATA_PROPERTIES, T_WC_CUSTOM_LIST.TMDB_ELEMENTS, T_WC_CUSTOM_LIST.SORT_BY "
                            strsql += "FROM T_WC_CUSTOM_LIST WHERE DELETED = 0 AND TARGET_TABLE = 4 "
                            strsql += "ORDER BY ID_RECORD ASC "
                            target_field_name = "MOVEMENT_NAME"
                        elif intlist == 2:
                            strcurrentprocess = f"{intlist}: Deleting from T2S_MOVEMENT"
                            strsqldelete += "DELETE FROM T_WC_T2S_MOVEMENT WHERE ID_RECORD NOT IN (SELECT ID_CUSTOM_LIST FROM T_WC_CUSTOM_LIST WHERE TARGET_TABLE = 4 AND DELETED = 0) "
                            print(strsqldelete)
                            cursor.execute(strsqldelete)
                            continue
                        if strsql != "":
                            print(strsql)
                            cursor.execute(strsql)
                            lngrowcount = cursor.rowcount
                            print(f"{lngrowcount} lines")
                            lnglinesprocessed = 0
                            results = cursor.fetchall()
                            for row in results:
                                lnglinesprocessed += 1
                                lngrecordid = row['ID_RECORD']
                                strrecordname = row['NAME']
                                strrecordnamefr = row['NAME_FR'] or ''
                                strrecordoverview = row['OVERVIEW']
                                strrecordmovementsource = row['MOVEMENT_SOURCE']
                                strrecordmovementtype = row['MOVEMENT_TYPE']
                                strrecordposterpath = row['POSTER_PATH']
                                strrecordidwikidata = row['ID_WIKIDATA'] if 'ID_WIKIDATA' in row else None
                                strrecordwikipediaimagepath = f_getwikidataimagepath(strrecordidwikidata)
                                print("Processing record: " + str(lngrecordid) + ": " + strrecordname + " (" + strrecordmovementsource + ")")
                                telmovement.position(recordid=lngrecordid, currentvalue=strrecordname, currentprocess=strcurrentprocess)
                                arrlistcouples = {
                                    'ID_RECORD': lngrecordid,
                                    'MOVEMENT_NAME': strrecordname,
                                    'MOVEMENT_NAME_FR': strrecordnamefr,
                                    'OVERVIEW': strrecordoverview,
                                    'MOVEMENT_SOURCE': strrecordmovementsource,
                                    'MOVEMENT_TYPE': strrecordmovementtype,
                                    'POSTER_PATH': strrecordposterpath,
                                    'ID_WIKIDATA': strrecordidwikidata,
                                    'WIKIPEDIA_IMAGE_PATH': strrecordwikipediaimagepath
                                }
                                strsqltablename = "T_WC_T2S_MOVEMENT"
                                strsqlupdatecondition = f"ID_RECORD = '{lngrecordid}' AND MOVEMENT_SOURCE = '{strrecordmovementsource}'"
                                cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(lngrecordid),"Current record in the TMDb database movie preprocess",0)

                                strsqlmovies = ""
                                strsqlseries = ""
                                intsortby = f_getcustomsortby(row, 4)
                                # Mechanism 1: parse IMDb IDs/URLs from ID_IMDB_LIST (newline-separated)
                                strimdblist = row['ID_IMDB_LIST'] or ''
                                arrimdbids = re.findall(r'(tt\d+)', strimdblist)
                                strsqlmovies_imdb = ""
                                strsqlseries_imdb = ""
                                if arrimdbids:
                                    strimdbidlist = "'" + "','".join(arrimdbids) + "'"
                                    strfieldorder = "'" + "','".join(arrimdbids) + "'"
                                    strsqlmovies_imdb = "SELECT m.ID_MOVIE, FIELD(m.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE m.ID_IMDB IN (" + strimdbidlist + ") AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                    strsqlseries_imdb = "SELECT s.ID_SERIE, FIELD(s.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE s.ID_IMDB IN (" + strimdbidlist + ") AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                # Mechanism 2: Wikidata property/item filter from WIKIDATA_PROPERTIES
                                strwikidataproperties = row['WIKIDATA_PROPERTIES'] or ''
                                arrwdtokens = re.findall(r'[PQ]\d+', strwikidataproperties)
                                strwdpropertyid = next((t for t in arrwdtokens if t.startswith('P')), '')
                                strwditemid = next((t for t in arrwdtokens if t.startswith('Q')), '')
                                if strwditemid:
                                    arrlistcouples['ID_WIKIDATA'] = strwditemid
                                    arrlistcouples['WIKIPEDIA_IMAGE_PATH'] = f_getwikidataimagepath(strwditemid)
                                strsqlmovies_wikidata = ""
                                strsqlseries_wikidata = ""
                                if strwdpropertyid and strwditemid:
                                    strsqlmovies_wikidata = "SELECT DISTINCT m.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_WIKIDATA_ITEM_VALUE wv JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT STRAIGHT_JOIN T_WC_TMDB_MOVIE m ON m.ID_WIKIDATA = w.ID_WIKIDATA INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE w.ID_PROPERTY ='" + strwdpropertyid + "' AND wv.ID_ITEM = '" + strwditemid + "' AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated') AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                    strsqlseries_wikidata = "SELECT DISTINCT s.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_WIKIDATA_ITEM_VALUE wv JOIN T_WC_WIKIDATA_STATEMENT w ON w.ID_STATEMENT = wv.ID_STATEMENT STRAIGHT_JOIN T_WC_TMDB_SERIE s ON s.ID_WIKIDATA = w.ID_WIKIDATA INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE w.ID_PROPERTY ='" + strwdpropertyid + "' AND wv.ID_ITEM = '" + strwditemid + "' AND (w.`RANK` IS NULL OR w.`RANK` <> 'deprecated') AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                # Mechanism 3: TMDb keyword filter from TMDB_ELEMENTS
                                strtmdbelements = row['TMDB_ELEMENTS'] or ''
                                strsqlmovies_keyword = ""
                                strsqlseries_keyword = ""
                                strkeywordmatch = re.search(r"T_WC_TMDB_KEYWORD\.NAME\s*=\s*'([^']+)'", strtmdbelements.replace('&#039;', "'"))
                                if strkeywordmatch:
                                    strkeywordname = strkeywordmatch.group(1).strip().replace("'", "''")
                                    print(f"Found keyword in TMDB_ELEMENTS: {strkeywordname}")
                                    strsqlmovies_keyword = "SELECT mk.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE_KEYWORD mk INNER JOIN T_WC_TMDB_KEYWORD k ON mk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = mk.ID_MOVIE INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE k.NAME = '" + strkeywordname + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                    strsqlseries_keyword = "SELECT sk.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE_KEYWORD sk INNER JOIN T_WC_TMDB_KEYWORD k ON sk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = sk.ID_SERIE INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE k.NAME = '" + strkeywordname + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    print(f"Constructed SQL for keyword filter: {strsqlmovies_keyword} / {strsqlseries_keyword}")
                                # Combine mechanisms cumulatively
                                arrsqlmovies_sources = [s for s in [strsqlmovies_imdb, strsqlmovies_wikidata, strsqlmovies_keyword] if s]
                                arrsqlseries_sources = [s for s in [strsqlseries_imdb, strsqlseries_wikidata, strsqlseries_keyword] if s]
                                strsqlmovies = f_buildcustomaggregatequery(arrsqlmovies_sources, "ID_MOVIE", "IMDB_RATING_WEIGHTED", intsortby)
                                strsqlseries = f_buildcustomaggregatequery(arrsqlseries_sources, "ID_SERIE", "IMDB_RATING_WEIGHTED", intsortby)

                                if strsqlmovies != "":
                                    print(f"Executing SQL for movies: {strsqlmovies}")
                                    cursor2.execute(strsqlmovies)
                                    lngmoviecount = cursor2.rowcount
                                    lngseriescount = 0
                                    if strsqlseries != "":
                                        cursor4.execute(strsqlseries)
                                        lngseriescount = cursor4.rowcount
                                    if lngmoviecount + lngseriescount > 1:
                                        lngmovementid = cp.f_sqlupdatearray(strsqltablename, arrlistcouples, strsqlupdatecondition, 1)
                                        if lngmovementid is None:
                                            strsqlmovement = "SELECT ID_MOVEMENT FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                            cursor3.execute(strsqlmovement)
                                            lngrowcount2 = cursor3.rowcount
                                            if lngrowcount2 == 0:
                                                print("Error: Failed to create/update movement - lngmovementid is None")
                                                continue
                                            lngmovementid = cursor3.fetchone()["ID_MOVEMENT"]
                                        telmovement.created()
                                        telmovement.set_entity_id(lngmovementid)
                                        results_movies = cursor2.fetchall()
                                        lngdisplayorder = 0
                                        arrcurrentmovieids = []
                                        for rowm in results_movies:
                                            lngmovieid = rowm["ID_MOVIE"]
                                            lngdisplayorder += 1
                                            arrcurrentmovieids.append(str(lngmovieid))
                                            arrmovielistcouples = {
                                                'ID_MOVIE': lngmovieid,
                                                'ID_MOVEMENT': lngmovementid,
                                                'DISPLAY_ORDER': lngdisplayorder
                                            }
                                            strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND ID_MOVEMENT = " + str(lngmovementid)
                                            cp.f_sqlupdatearray("T_WC_T2S_MOVIE_MOVEMENT", arrmovielistcouples, strsqlupdatecondition2, 1)
                                        if arrcurrentmovieids:
                                            strsqldelete = "DELETE FROM T_WC_T2S_MOVIE_MOVEMENT WHERE ID_MOVEMENT = " + str(lngmovementid) + " AND ID_MOVIE NOT IN (" + ",".join(arrcurrentmovieids) + ") "
                                            print(strsqldelete)
                                            cursor2.execute(strsqldelete)
                                            strsqldelete = "DELETE mm1 FROM T_WC_T2S_MOVIE_MOVEMENT mm1 INNER JOIN T_WC_T2S_MOVIE_MOVEMENT mm2 ON mm1.ID_MOVEMENT = mm2.ID_MOVEMENT AND mm1.ID_MOVIE = mm2.ID_MOVIE AND mm1.ID_ROW > mm2.ID_ROW WHERE mm1.ID_MOVEMENT = " + str(lngmovementid)
                                            print(strsqldelete)
                                            cursor2.execute(strsqldelete)
                                        if strsqlseries != "":
                                            results_series = cursor4.fetchall()
                                            lngdisplayorder = 0
                                            arrcurrentserieids = []
                                            for rows in results_series:
                                                lngserieid = rows["ID_SERIE"]
                                                lngdisplayorder += 1
                                                arrcurrentserieids.append(str(lngserieid))
                                                arrserielistcouples = {
                                                    'ID_SERIE': lngserieid,
                                                    'ID_MOVEMENT': lngmovementid,
                                                    'DISPLAY_ORDER': lngdisplayorder
                                                }
                                                strsqlupdatecondition2 = "ID_SERIE = " + str(lngserieid) + " AND ID_MOVEMENT = " + str(lngmovementid)
                                                cp.f_sqlupdatearray("T_WC_T2S_SERIE_MOVEMENT", arrserielistcouples, strsqlupdatecondition2, 1)
                                            if arrcurrentserieids:
                                                strsqldelete = "DELETE FROM T_WC_T2S_SERIE_MOVEMENT WHERE ID_MOVEMENT = " + str(lngmovementid) + " AND ID_SERIE NOT IN (" + ",".join(arrcurrentserieids) + ") "
                                                print(strsqldelete)
                                                cursor2.execute(strsqldelete)
                                                strsqldelete = "DELETE sm1 FROM T_WC_T2S_SERIE_MOVEMENT sm1 INNER JOIN T_WC_T2S_SERIE_MOVEMENT sm2 ON sm1.ID_MOVEMENT = sm2.ID_MOVEMENT AND sm1.ID_SERIE = sm2.ID_SERIE AND sm1.ID_ROW > sm2.ID_ROW WHERE sm1.ID_MOVEMENT = " + str(lngmovementid)
                                                print(strsqldelete)
                                                cursor2.execute(strsqldelete)
                                        arrlistcouples = {
                                            'MOVIE_COUNT': lngmoviecount,
                                            'SERIE_COUNT': lngseriescount
                                        }
                                        cp.f_sqlupdatearray(strsqltablename, arrlistcouples, strsqlupdatecondition, 1)
                                    else:
                                        strsqldeletemvt = "DELETE FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                        print(strsqldeletemvt)
                                        cursor2.execute(strsqldeletemvt)
                                        telmovement.deleted(cursor2.rowcount)
                    if 1:
                        strsqltablename = "T_WC_T2S_MOVEMENT"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE MOVEMENT_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        telmovement.deleted(cursor2.rowcount)

                        # Update T_WC_T2S_MOVEMENT ratings and popularity from movies
                        strsql = """UPDATE T_WC_T2S_MOVEMENT t
JOIN (
    SELECT
        mt.ID_MOVEMENT,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_MOVIE_MOVEMENT mt
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mt.ID_MOVIE
    INNER JOIN T_WC_T2S_MOVEMENT t2
        ON t2.ID_MOVEMENT = mt.ID_MOVEMENT
       AND t2.MOVEMENT_TYPE = 'movement'
    GROUP BY mt.ID_MOVEMENT
) x
    ON x.ID_MOVEMENT = t.ID_MOVEMENT
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED,
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_MOVEMENT ratings and popularity from series
                        strsql = """UPDATE T_WC_T2S_MOVEMENT t
JOIN (
    SELECT
        st.ID_MOVEMENT,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(s.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_SERIE_MOVEMENT st
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = st.ID_SERIE
    INNER JOIN T_WC_T2S_MOVEMENT t2
        ON t2.ID_MOVEMENT = st.ID_MOVEMENT
       AND t2.MOVEMENT_TYPE = 'movement'
    GROUP BY st.ID_MOVEMENT
) x
    ON x.ID_MOVEMENT = t.ID_MOVEMENT
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED),
    t.POPULARITY = COALESCE(t.POPULARITY, x.AVG_POPULARITY);
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        strsqltablename = "T_WC_T2S_MOVIE_MOVEMENT"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_MOVEMENT NOT IN (SELECT ID_MOVEMENT FROM T_WC_T2S_MOVEMENT) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqltablename = "T_WC_T2S_SERIE_MOVEMENT"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_MOVEMENT NOT IN (SELECT ID_MOVEMENT FROM T_WC_T2S_MOVEMENT) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                    telmovement.finish()

                elif intindex == 46:
                    #----------------------------------------------------
                    print("T2S_DEATH processing")

                    # Death-derivation telemetry (process 46): publish start, running counts and the
                    # current position as server variables so the run is observable from srvvar.inc.php.
                    fltdeathprocessstart = time.time()
                    strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                    cp.f_setservervariable("strtmdbmoviepreprocessdeathstartdatetime",strnow,"Start datetime of the last T2S death derivation (process 46)",0)
                    cp.f_setservervariable("strtmdbmoviepreprocessdeathenddatetime","","End datetime of the last T2S death derivation (process 46)",0)
                    lngdeathprocessedcount = 0
                    lngdeathcreatedcount = 0
                    lngdeathdeletedcount = 0
                    cp.f_setservervariable("strtmdbmoviepreprocessdeathprocessedcount","0","Number of death records examined by the T2S death derivation (process 46)",0)
                    cp.f_setservervariable("strtmdbmoviepreprocessdeathcreatedcount","0","Number of deaths created/updated by the T2S death derivation (process 46)",0)
                    cp.f_setservervariable("strtmdbmoviepreprocessdeathdeletedcount","0","Number of singleton deaths deleted by the T2S death derivation (process 46)",0)

                    arrp1196excludeditems = ["Q110999040", "Q6682074"]
                    strp1196excludeditems = "'" + "','".join(arrp1196excludeditems) + "'"

                    arrgroups = {1: 'en-cause-of-death', 2: 'en-manner-of-death'}
                    for intgroup, strgroup in arrgroups.items():
                        strsql = ""
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess",strgroup,"Current sub process in the TMDb database person preprocess",0)
                        if intgroup == 1:
                            strpropertyid = "P509"
                        elif intgroup == 2:
                            strpropertyid = "P1196"
                        else:
                            strpropertyid = ""
                        if strpropertyid != "":
                            strcurrentprocess = f"{intgroup}: Copying from WIKIDATA {strpropertyid} to T2S_DEATH"
                            # Pre-filter the driving set to only items that resolve to >= 2 linked
                            # TMDb persons. Mirrors the per-item person query joins below
                            # (T_WC_TMDB_PERSON -> T_WC_WIKIDATA_PERSON -> les statements V2)
                            # and the "lngpersoncount > 1" creation gate, so we no longer iterate the
                            # P509/P1196 items that would only ever be deleted as singletons. Degraded
                            # deaths (>=2 persons previously, <2 now) are handled by the count-based
                            # stale delete at the end of this process.
                            strdrivingexclusion = ""
                            if strpropertyid == "P1196" and strp1196excludeditems != "''":
                                strdrivingexclusion = ("AND pv.ID_ITEM NOT IN ("
                                                       + strp1196excludeditems + ") ")
                            strsql += f_persondrivingsql(strpropertyid, strdrivingexclusion)
                            target_field_name = "DEATH_NAME"
                        strrecorddeathsource = strpropertyid
                        if strsql != "":
                            print(strsql)
                            cursor.execute(strsql)
                            lngrowcount = cursor.rowcount
                            print(f"{lngrowcount} lines")
                            results = cursor.fetchall()
                            for row in results:
                                strrecordid = row['ID_ITEM']
                                cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(strrecordid),"Current record in the TMDb database movie preprocess",0)

                                strrecordname = ""
                                strrecordoverview = ""
                                strrecordimagepath = ""
                                strsqlitem = ""
                                # Image: prefer T_WC_WIKIPEDIA_PAGE_LANG (WIKIPEDIA-CRAWLER-020), fall back on
                                # the V1 column until the V1 tables are dropped. LABEL / DESCRIPTION still
                                # come from V1: their migration is WIKIDATA-CRAWLER-017, not this one.
                                # WIKIDATA-CRAWLER-017 : texte EN et image pris dans V2 / PAGE_LANG, V1 en repli.
                                # La requete part d une table derivee d une ligne et non de V1, sinon une entite
                                # absente de V1 ne rendrait rien du tout. COALESCE final sur '' pour garantir une
                                # chaine et non NULL aux appelants, qui concatenent ces valeurs.
                                strsqlitem += "SELECT COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON,'$.en')), "
                                strsqlitem += "  NULLIF(v2.LABEL_EN,''), v1.LABEL, '') AS LABEL, "
                                strsqlitem += "COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.DESCRIPTIONS_JSON,'$.en')), "
                                strsqlitem += "  NULLIF(v2.DESCRIPTION_EN,''), v1.DESCRIPTION, '') AS DESCRIPTION, "
                                strsqlitem += "COALESCE(pl.MAIN_IMAGE_URL, v1.WIKIPEDIA_IMAGE_PATH, '') AS WIKIPEDIA_IMAGE_PATH "
                                strsqlitem += "FROM (SELECT %s AS ID_WIKIDATA) k "
                                strsqlitem += "LEFT JOIN T_WC_WIKIDATA_ITEM v2 ON v2.ID_WIKIDATA = k.ID_WIKIDATA "
                                strsqlitem += "LEFT JOIN T_WC_WIKIDATA_ITEM_V1 v1 "
                                strsqlitem += "  ON v1.ID_WIKIDATA = k.ID_WIKIDATA AND v1.LANG = 'en' "
                                strsqlitem += "LEFT JOIN T_WC_WIKIPEDIA_PAGE_LANG pl "
                                strsqlitem += "  ON pl.ID_WIKIDATA = k.ID_WIKIDATA AND pl.LANG = 'en' "
                                strsqlitem += "  AND COALESCE(pl.MAIN_IMAGE_URL,'') <> ''"
                                arrvalues = cp.f_fieldsfromquery(
                                    strsqlitem,
                                    "strrecordname|strrecordoverview|strrecordimagepath",
                                    "LABEL|DESCRIPTION|WIKIPEDIA_IMAGE_PATH",
                                    params=(strrecordid,),
                                    target_dict=None,
                                )
                                strrecordname = arrvalues.get("strrecordname", "")
                                strrecordoverview = arrvalues.get("strrecordoverview", "")
                                strrecordimagepath = arrvalues.get("strrecordimagepath", "")
                                strrecordimagepath = f_getwikidataimagepath(strrecordid)

                                # WIKIDATA-CRAWLER-017 : le libelle FR vient desormais de V2 (LABELS_JSON),
                                # V1 ne servant plus que de repli, le temps que le gap d entites se ferme.
                                strrecordnamefr = f_getwikidatalabel(strrecordid, "fr")

                                strrecorddeathtype = "death"
                                print("Processing record: " + str(strrecordid) + ": " + strrecordname + " (" + strrecorddeathsource + ")")
                                lngdeathprocessedcount += 1
                                cp.f_setservervariable("strtmdbmoviepreprocessdeathcurrentprocess",strcurrentprocess,"Current source/sub-process in the T2S death derivation (process 46)",0)
                                cp.f_setservervariable("strtmdbmoviepreprocessdeathwikidataid",str(strrecordid),"Current Wikidata/record id in the T2S death derivation (process 46)",0)
                                cp.f_setservervariable("strtmdbmoviepreprocessdeathcurrentvalue",strrecordname,"Current death name in the T2S death derivation (process 46)",0)
                                cp.f_setservervariable("strtmdbmoviepreprocessdeathprocessedcount",str(lngdeathprocessedcount),"Number of death records examined by the T2S death derivation (process 46)",0)

                                if target_field_name == "DEATH_NAME":
                                    arrdeathcouples = {
                                        'ID_WIKIDATA': strrecordid,
                                        'DEATH_NAME': strrecordname,
                                        'DEATH_NAME_FR': strrecordnamefr,
                                        'OVERVIEW': strrecordoverview,
                                        'DEATH_SOURCE': strrecorddeathsource,
                                        'DEATH_TYPE': strrecorddeathtype,
                                        'LANG': 'en',
                                        'WIKIPEDIA_IMAGE_PATH': strrecordimagepath,
                                    }
                                strsqltablename = "T_WC_T2S_DEATH"
                                strsqlupdatecondition = f"ID_WIKIDATA = '{strrecordid}' AND DEATH_SOURCE = '{strrecorddeathsource}'"

                                strsqlpersons = ""
                                if intgroup == 1 or intgroup == 2:
                                    strsqlpersons += "SELECT DISTINCT T_WC_TMDB_PERSON.ID_PERSON, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.NAME, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.BIRTHDAY, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.DEATHDAY, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.ID_IMDB, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.BIOGRAPHY, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.PROFILE_PATH, "
                                    strsqlpersons += "T_WC_TMDB_PERSON.ID_WIKIDATA "
                                    strsqlpersons += f_personlinkfromsql()
                                if strsqlpersons != "":
                                    cursor2.execute(strsqlpersons, (strpropertyid, strrecordid))
                                    person_results = cursor2.fetchall()
                                    lngpersoncount = len(person_results)
                                    if lngpersoncount > 1:
                                        lngdeathid = cp.f_sqlupdatearray(strsqltablename, arrdeathcouples, strsqlupdatecondition, 1)
                                        if lngdeathid is None:
                                            strsqldeath = "SELECT ID_DEATH FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                            cursor3.execute(strsqldeath)
                                            lngrowcount2 = cursor3.rowcount
                                            if lngrowcount2 == 0:
                                                print("Error: Failed to create/update death - lngdeathid is None")
                                                continue
                                            lngdeathid = cursor3.fetchone()["ID_DEATH"]
                                        lngdeathcreatedcount += 1
                                        cp.f_setservervariable("strtmdbmoviepreprocessdeathid",str(lngdeathid),"Current death ID created/updated by the T2S death derivation (process 46)",0)
                                        if intgroup == 1 or intgroup == 2:
                                            lngdisplayorder = 0
                                            arrcurrentpersonids = []
                                            for prow in person_results:
                                                lngpersonid = prow["ID_PERSON"]
                                                lngdisplayorder += 1
                                                arrcurrentpersonids.append(str(lngpersonid))
                                                arrpersondeathcouples = {
                                                    'ID_PERSON': lngpersonid,
                                                    'ID_DEATH': lngdeathid,
                                                    'DISPLAY_ORDER': lngdisplayorder,
                                                }
                                                strsqlupdatecondition2 = "ID_PERSON = " + str(lngpersonid) + " AND ID_DEATH = " + str(lngdeathid)
                                                cp.f_sqlupdatearray("T_WC_T2S_PERSON_DEATH", arrpersondeathcouples, strsqlupdatecondition2, 1)
                                            if arrcurrentpersonids:
                                                strsqldelete = "DELETE FROM T_WC_T2S_PERSON_DEATH WHERE ID_DEATH = " + str(lngdeathid) + " AND ID_PERSON NOT IN (" + ",".join(arrcurrentpersonids) + ") "
                                                print(strsqldelete)
                                                cursor2.execute(strsqldelete)
                                                strsqldelete = "DELETE pd1 FROM T_WC_T2S_PERSON_DEATH pd1 INNER JOIN T_WC_T2S_PERSON_DEATH pd2 ON pd1.ID_DEATH = pd2.ID_DEATH AND pd1.ID_PERSON = pd2.ID_PERSON AND pd1.ID_ROW > pd2.ID_ROW WHERE pd1.ID_DEATH = " + str(lngdeathid)
                                                print(strsqldelete)
                                                cursor2.execute(strsqldelete)
                                            arrdeathcouples = {
                                                'PERSON_COUNT': lngpersoncount,
                                            }
                                            cp.f_sqlupdatearray(strsqltablename, arrdeathcouples, strsqlupdatecondition, 1)
                                    else:
                                        strsqltablename = "T_WC_T2S_DEATH"
                                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                        print(strsqldelete)
                                        cursor2.execute(strsqldelete)
                                        lngdeathdeletedcount += 1

                    if 1:
                        strsqltablename = "T_WC_T2S_DEATH"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE DEATH_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqltablename = "T_WC_T2S_DEATH"
                        # Count-based stale delete. Removes any death whose item no longer resolves to
                        # >= 2 linked TMDb persons. Covers both the "item gone / excluded" case (count 0,
                        # the old NOT EXISTS behaviour incl. the P1196 exclusion) and the "degraded from
                        # >=2 to <2 persons" case that the per-item singleton delete used to handle before
                        # the driving query was pre-filtered. Mirrors the pre-filter joins so iteration
                        # and cleanup stay consistent.
                        strsqldelete = f_persongrouppurgesql(
                            "T_WC_T2S_DEATH", "DEATH_SOURCE", False,
                            "      AND NOT (\n"
                            "          w.ID_PROPERTY = 'P1196'\n"
                            "          AND wv.ID_ITEM IN (" + strp1196excludeditems + ")\n"
                            "      )\n")
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsql = """UPDATE T_WC_T2S_DEATH t
JOIN (
    SELECT
        mt.ID_DEATH,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_PERSON_DEATH mt
    INNER JOIN T_WC_T2S_PERSON m
        ON m.ID_PERSON = mt.ID_PERSON
    INNER JOIN T_WC_T2S_DEATH t2
        ON t2.ID_DEATH = mt.ID_DEATH
       AND t2.DEATH_TYPE = 'death'
    GROUP BY mt.ID_DEATH
) x
    ON x.ID_DEATH = t.ID_DEATH
SET
    t.POPULARITY = x.AVG_POPULARITY;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        strsqltablename = "T_WC_T2S_PERSON_DEATH"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE ID_DEATH NOT IN (SELECT ID_DEATH FROM T_WC_T2S_DEATH) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        # Death-derivation telemetry (process 46): final run summary
                        strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                        cp.f_setservervariable("strtmdbmoviepreprocessdeathenddatetime",strnow,"End datetime of the last T2S death derivation (process 46)",0)
                        cp.f_setservervariable("strtmdbmoviepreprocessdeathcreatedcount",str(lngdeathcreatedcount),"Number of deaths created/updated by the T2S death derivation (process 46)",0)
                        cp.f_setservervariable("strtmdbmoviepreprocessdeathdeletedcount",str(lngdeathdeletedcount),"Number of singleton deaths deleted by the T2S death derivation (process 46)",0)
                        cp.f_setservervariable("strtmdbmoviepreprocessdeathprocessedseconds",f"{time.time() - fltdeathprocessstart:.2f}","Elapsed seconds of the last T2S death derivation (process 46)",0)

                elif intindex == 4:
                    #----------------------------------------------------
                    print("T2S_MOVIE processing")
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from TMDB_MOVIE to T2S_MOVIE","Current sub process in the TMDb database movie preprocess",0)
                        # --- Incremental watermark (mirrors Process 1) ---------------------------
                        # Only re-copy movies whose source row changed since the last SUCCESSFUL run.
                        # T_WC_TMDB_MOVIE.TIM_UPDATED (datetime, indexed) is the change marker, and the
                        # qualification filter (ADULT / ID_IMDB) lives on the same row, so any change
                        # that makes a movie (dis)qualify also bumps TIM_UPDATED -> incremental is exact.
                        # The stale-delete and the enrichment passes still run over the FULL table every
                        # run, so source deletions and independently-refreshed ratings / FR titles /
                        # Wikidata are always picked up. A look-back buffer absorbs clock skew.
                        lngt2smovielookbackminutes = 60
                        strrunstart = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                        strlastrun = cp.f_getservervariable("strtmdbmoviepreprocesst2smovielastrun", 0)
                        strincrementalfilter = ""
                        if strlastrun:
                            strincrementalfilter = (
                                "AND TIM_UPDATED >= DATE_SUB('" + strlastrun + "', INTERVAL "
                                + str(lngt2smovielookbackminutes) + " MINUTE) "
                            )
                            print(f"Incremental run: movies changed since {strlastrun} (minus {lngt2smovielookbackminutes} min buffer)")
                        else:
                            print("First run (no watermark): full scan of all qualifying movies")

                        # Precompute the IMDb global weighted-rating average ONCE (previously recomputed
                        # via a full-table CROSS JOIN subquery on every chunk).
                        cursor.execute("SELECT AVG(averageRating) AS C FROM T_WC_IMDB_MOVIE_RATING_IMPORT WHERE averageRating IS NOT NULL AND numVotes > 0")
                        dblavgrating = cursor.fetchone()['C']
                        stravgrating = str(dblavgrating) if dblavgrating is not None else "NULL"

                        # Get the maximum ID_MOVIE value from the database
                        cursor.execute("SELECT MAX(ID_MOVIE) as max_id FROM T_WC_TMDB_MOVIE")
                        result = cursor.fetchone()
                        lngmovierangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_MOVIE in database: {lngmovierangemax}")

                        # Base copy in chunks (incremental runs touch few rows per chunk)
                        lngchunksize = 5000
                        for lngmovierangestart in range(1, lngmovierangemax + 1, lngchunksize):
                            lngmovierangeend = min(lngmovierangestart + lngchunksize - 1, lngmovierangemax)
                            print(f"Processing T2S_MOVIE rows from ID {lngmovierangestart} to {lngmovierangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovieid",str(lngmovierangestart),"Current movie ID in the TMDb database preprocess",0)
                            
                            strsqlmovies = f"""
INSERT INTO T_WC_T2S_MOVIE (
    ID_MOVIE, MOVIE_TITLE, ID_IMDB, ADULT, DAT_RELEASE, OVERVIEW,
    POSTER_PATH, BACKDROP_PATH, VOTE_AVERAGE, VOTE_COUNT,
    POPULARITY, ORIGINAL_LANGUAGE, ORIGINAL_TITLE,
    VIDEO, DAT_CREAT, TIM_UPDATED, RELEASE_YEAR, RELEASE_MONTH,
    RELEASE_DAY, ID_WIKIDATA, HOMEPAGE_URL, STATUS, BUDGET,
    REVENUE, RUNTIME, TAGLINE, IS_COLOR, IS_BLACK_AND_WHITE,
    IS_SILENT, IS_3D, COLOR_TECHNOLOGY, FILM_TECHNOLOGY,
    FILM_FORMAT, SOUND_SYSTEM, SOUND_TECHNOLOGY,
    IS_MOVIE, IS_DOCUMENTARY, IS_SHORT_FILM, DELETED
)
SELECT
    ID_MOVIE, TITLE, ID_IMDB, ADULT, DAT_RELEASE, OVERVIEW,
    POSTER_PATH, BACKDROP_PATH, VOTE_AVERAGE, VOTE_COUNT,
    POPULARITY, ORIGINAL_LANGUAGE, ORIGINAL_TITLE,
    VIDEO, DAT_CREAT, TIM_UPDATED, RELEASE_YEAR, RELEASE_MONTH,
    RELEASE_DAY, ID_WIKIDATA, HOMEPAGE_URL, STATUS, BUDGET,
    REVENUE, RUNTIME, TAGLINE, IS_COLOR, IS_BLACK_AND_WHITE,
    IS_SILENT, IS_3D, COLOR_TECHNOLOGY, FILM_TECHNOLOGY,
    FILM_FORMAT, SOUND_SYSTEM, SOUND_TECHNOLOGY,
    IS_MOVIE, IS_DOCUMENTARY, IS_SHORT_FILM, DELETED
FROM T_WC_TMDB_MOVIE
WHERE ADULT = 0 
AND ID_IMDB <> ''
AND ID_IMDB IS NOT NULL
AND ID_MOVIE >= {lngmovierangestart} AND ID_MOVIE <= {lngmovierangeend}
{strincrementalfilter}ON DUPLICATE KEY UPDATE
    MOVIE_TITLE = VALUES(MOVIE_TITLE),
    ID_IMDB = VALUES(ID_IMDB),
    ADULT = VALUES(ADULT),
    DAT_RELEASE = VALUES(DAT_RELEASE),
    OVERVIEW = VALUES(OVERVIEW),
    POSTER_PATH = VALUES(POSTER_PATH),
    BACKDROP_PATH = VALUES(BACKDROP_PATH),
    VOTE_AVERAGE = VALUES(VOTE_AVERAGE),
    VOTE_COUNT = VALUES(VOTE_COUNT),
    POPULARITY = VALUES(POPULARITY),
    ORIGINAL_LANGUAGE = VALUES(ORIGINAL_LANGUAGE),
    ORIGINAL_TITLE = VALUES(ORIGINAL_TITLE),
    VIDEO = VALUES(VIDEO),
    DAT_CREAT = VALUES(DAT_CREAT),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    RELEASE_YEAR = VALUES(RELEASE_YEAR),
    RELEASE_MONTH = VALUES(RELEASE_MONTH),
    RELEASE_DAY = VALUES(RELEASE_DAY),
    ID_WIKIDATA = VALUES(ID_WIKIDATA),
    HOMEPAGE_URL = VALUES(HOMEPAGE_URL),
    STATUS = VALUES(STATUS),
    BUDGET = VALUES(BUDGET),
    REVENUE = VALUES(REVENUE),
    RUNTIME = VALUES(RUNTIME),
    TAGLINE = VALUES(TAGLINE),
    IS_COLOR = VALUES(IS_COLOR),
    IS_BLACK_AND_WHITE = VALUES(IS_BLACK_AND_WHITE),
    IS_SILENT = VALUES(IS_SILENT),
    IS_3D = VALUES(IS_3D),
    COLOR_TECHNOLOGY = VALUES(COLOR_TECHNOLOGY),
    FILM_TECHNOLOGY = VALUES(FILM_TECHNOLOGY),
    FILM_FORMAT = VALUES(FILM_FORMAT),
    SOUND_SYSTEM = VALUES(SOUND_SYSTEM),
    SOUND_TECHNOLOGY = VALUES(SOUND_TECHNOLOGY),
    IS_MOVIE = VALUES(IS_MOVIE),
    IS_DOCUMENTARY = VALUES(IS_DOCUMENTARY),
    IS_SHORT_FILM = VALUES(IS_SHORT_FILM),
    DELETED = VALUES(DELETED) """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                        # ---- Stale delete: single full-table anti-join (full coverage) ----------
                        # Must run over the whole table regardless of the watermark: a movie deleted
                        # from source (or that became ADULT / lost its ID_IMDB) does not appear in the
                        # incremental change-set, but its now-orphaned T2S row must still be removed.
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Stale-delete T2S_MOVIE","Current sub process in the TMDb database movie preprocess",0)
                        strsqlmoviesdelete = """
DELETE t2s FROM T_WC_T2S_MOVIE t2s
LEFT JOIN T_WC_TMDB_MOVIE src
    ON src.ID_MOVIE = t2s.ID_MOVIE
   AND src.ADULT = 0
   AND src.ID_IMDB <> ''
   AND src.ID_IMDB IS NOT NULL
WHERE src.ID_MOVIE IS NULL """
                        cursor2.execute(strsqlmoviesdelete)
                        cp.connectioncp.commit()

                        # ---- Enrichment: full-table set-based passes, CHUNKED by ID range ---------
                        # Run over the whole table every run because their source data (IMDb ratings,
                        # FR title, localized display, Wikidata) changes independently of a movie's
                        # TIM_UPDATED, so scoping them to the incremental set would let them go stale.
                        # TMDB-MOVIE-PREPROCESS-032: these used to be single full-table UPDATEs, too heavy
                        # on the DB (long locks on the hot T_WC_T2S_MOVIE table, one huge transaction each).
                        # Now chunked by ID_MOVIE range like the base copy above, so each transaction stays
                        # small; full coverage is preserved (the loop spans every id). Reuses lngchunksize
                        # and lngmovierangemax computed for the base copy.
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Enrich T2S_MOVIE (ratings / FR title / display _LANG / Wikidata), chunked","Current sub process in the TMDb database movie preprocess",0)

                        # TMDB-MOVIE-PREPROCESS-030: create the localized TEXT table ONCE, before the loop.
                        # Text-only (OVERVIEW / TAGLINE). POSTER_PATH/BACKDROP_PATH were dropped: measured
                        # 88% of localized posters are identical to the EN poster (494792/561941), and the
                        # 12% genuinely-distinct ones are already served by the image mechanism
                        # (apply_localized_main_image on T_WC_TMDB_MOVIE_IMAGE, FR poster pinned at
                        # DISPLAY_ORDER=1). Keeping them here only bloated the table (~450k poster-only rows).
                        strsqlmovies = """
CREATE TABLE IF NOT EXISTS T_WC_T2S_MOVIE_LANG (
  ID_MOVIE int(11) NOT NULL,
  LANG varchar(10) NOT NULL,
  OVERVIEW mediumtext DEFAULT NULL,
  TAGLINE mediumtext DEFAULT NULL,
  TIM_UPDATED timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (ID_MOVIE, LANG),
  KEY LANG (LANG)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci """
                        cursor2.execute(strsqlmovies)
                        cp.connectioncp.commit()

                        for lngmovierangestart in range(1, lngmovierangemax + 1, lngchunksize):
                            lngmovierangeend = min(lngmovierangestart + lngchunksize - 1, lngmovierangemax)
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovieid",str(lngmovierangestart),"Current movie ID in the TMDb database movie preprocess (enrichment)",0)

                            # IMDb rating and vote count. IMDB_VOTES rides on this very pass
                            # (FASTAPI-TEXT2SQL-194): the join already carries numVotes for the
                            # weighted rating below, so the count costs one more SET and no extra
                            # scan. It became necessary when the TMDb VOTE_COUNT left the API and
                            # movies and series were left with no vote count at all.
                            strsqlmovies = f"""
UPDATE T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb
    ON t2s.ID_IMDB = imdb.tconst
SET t2s.IMDB_RATING = imdb.averageRating,
    t2s.IMDB_VOTES = imdb.numVotes
WHERE t2s.ID_MOVIE BETWEEN {lngmovierangestart} AND {lngmovierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                            # IMDb weighted rating
                            strsqlmovies = f"""
UPDATE T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb
    ON t2s.ID_IMDB = imdb.tconst
SET t2s.IMDB_RATING_WEIGHTED =
    ((imdb.numVotes / (imdb.numVotes + {lngimdbweightedratingm})) * imdb.averageRating) +
    (({lngimdbweightedratingm} / (imdb.numVotes + {lngimdbweightedratingm})) * {stravgrating})
WHERE t2s.ID_MOVIE BETWEEN {lngmovierangestart} AND {lngmovierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL
    AND imdb.numVotes > 0 """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                            # FR title (search column, stays on T_WC_T2S_MOVIE)
                            strsqlmovies = f"""
UPDATE T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_TMDB_MOVIE_LANG t
    ON t2s.ID_MOVIE = t.ID_MOVIE
SET t2s.MOVIE_TITLE_FR = t.TITLE
WHERE t2s.ID_MOVIE BETWEEN {lngmovierangestart} AND {lngmovierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND t.LANG = 'fr' """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                            # TMDB-MOVIE-PREPROCESS-030: localized TEXT fields into T_WC_T2S_MOVIE_LANG,
                            # row-per-(movie,language). Language-AGNOSTIC (no LANG='fr' literal so a future
                            # crawled language flows through), curated to the T2S subset, lean (skip rows
                            # with no text), upsert so it is re-runnable. Text-only: posters come from the
                            # image mechanism, not here (see the CREATE TABLE note above). Table created above.
                            strsqlmovies = f"""
INSERT INTO T_WC_T2S_MOVIE_LANG (ID_MOVIE, LANG, OVERVIEW, TAGLINE)
SELECT l.ID_MOVIE, l.LANG, l.OVERVIEW, l.TAGLINE
FROM T_WC_TMDB_MOVIE_LANG l
INNER JOIN T_WC_T2S_MOVIE t2s
    ON t2s.ID_MOVIE = l.ID_MOVIE
WHERE l.ID_MOVIE BETWEEN {lngmovierangestart} AND {lngmovierangeend}
    AND (l.DELETED IS NULL OR l.DELETED = 0)
    AND l.LANG IS NOT NULL AND l.LANG <> ''
    AND ( (l.OVERVIEW IS NOT NULL AND l.OVERVIEW <> '')
       OR (l.TAGLINE IS NOT NULL AND l.TAGLINE <> '') )
ON DUPLICATE KEY UPDATE
    OVERVIEW = VALUES(OVERVIEW),
    TAGLINE = VALUES(TAGLINE) """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                            # ---- TMDB-MOVIE-PREPROCESS-043 : enrichissement Wikidata, V1 vers V2 ----
                            # Quatre colonnes passent aux statements V2 : Plex, les deux Criterion et
                            # INSTANCE_OF. La regle de choix d'une valeur unique, et pourquoi il faut
                            # en avoir une, sont expliquees sur f_wikidatabestvaluesql.
                            #
                            # ALIASES disparait de l'UPDATE : donnee morte, V1 ne la collecte plus
                            # (arbitrage du 2026-08-17). La colonne T2S garde donc sa derniere valeur.
                            # La supprimer est une decision de schema, pas de ce ticket.
                            #
                            # La jointure sur V1 RESTE, et ce n'est pas un oubli. WIKIDATA_TITLE est un
                            # libelle, bloque par WIKIDATA-CRAWLER-017 (voir -042). Tant qu'elle tient,
                            # la population enrichie reste celle de V1. Changer les colonnes ET la
                            # population dans le meme pas rendrait la comparaison avant/apres
                            # illisible : on ne saurait plus si un ecart vient de la nouvelle source
                            # ou de lignes nouvellement eligibles. La jointure tombera avec -042.
                            strsqlplex = f_wikidataexternalidsql(STR_WD_PROPERTY_PLEX, "t2s.ID_WIKIDATA", False, "spx", 50)
                            strsqlcriterion = f_wikidataexternalidsql(STR_WD_PROPERTY_CRITERION, "t2s.ID_WIKIDATA", True, "scr")
                            strsqlspine = f_wikidataexternalidsql(STR_WD_PROPERTY_CRITERION_SPINE, "t2s.ID_WIKIDATA", True, "scs")
                            strsqlinstanceof = f_wikidatainstanceofsql("t2s.ID_WIKIDATA", "sio")
                            strsqlmovies = f"""
UPDATE T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w
    ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
SET t2s.WIKIDATA_TITLE = w.TITLE,
    t2s.PLEX_MEDIA_KEY = {strsqlplex},
    t2s.ID_CRITERION = {strsqlcriterion},
    t2s.ID_CRITERION_SPINE = {strsqlspine},
    t2s.INSTANCE_OF = {strsqlinstanceof}
WHERE t2s.ID_MOVIE BETWEEN {lngmovierangestart} AND {lngmovierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> '' """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                        # Persist the watermark only after a successful run (an exception earlier aborts
                        # before this line, leaving the previous watermark so the failed window retries).
                        cp.f_setservervariable("strtmdbmoviepreprocesst2smovielastrun", strrunstart, "Start datetime of the last successful T2S_MOVIE run; incremental watermark on T_WC_TMDB_MOVIE.TIM_UPDATED", 0)

                    print(f"T2S_MOVIE processing completed. ")

                elif intindex == 5:
                    #----------------------------------------------------
                    print("T2S_SERIE processing")
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from TMDB_SERIE to T2S_SERIE","Current sub process in the TMDb database series preprocess",0)

                        # --- Incremental watermark (mirrors Process 1/4) -------------------------
                        # Self-gated like T2S_MOVIE: the qualification filter (ADULT / ID_IMDB) lives
                        # on the source row, so any (dis)qualifying change bumps TIM_UPDATED ->
                        # incremental is exact. Stale-delete and enrichment run full-table every run.
                        lngt2sserielookbackminutes = 60
                        strrunstart = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                        strlastrun = cp.f_getservervariable("strtmdbmoviepreprocesst2sserielastrun", 0)
                        strincrementalfilter = ""
                        if strlastrun:
                            strincrementalfilter = (
                                "AND TIM_UPDATED >= DATE_SUB('" + strlastrun + "', INTERVAL "
                                + str(lngt2sserielookbackminutes) + " MINUTE) "
                            )
                            print(f"Incremental run: series changed since {strlastrun} (minus {lngt2sserielookbackminutes} min buffer)")
                        else:
                            print("First run (no watermark): full scan of all qualifying series")

                        # Precompute the IMDb global weighted-rating average ONCE.
                        cursor.execute("SELECT AVG(averageRating) AS C FROM T_WC_IMDB_MOVIE_RATING_IMPORT WHERE averageRating IS NOT NULL AND numVotes > 0")
                        dblavgrating = cursor.fetchone()['C']
                        stravgrating = str(dblavgrating) if dblavgrating is not None else "NULL"

                        # Get the maximum ID_SERIE value from the database
                        cursor.execute("SELECT MAX(ID_SERIE) as max_id FROM T_WC_TMDB_SERIE")
                        result = cursor.fetchone()
                        lngserierangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_SERIE in database: {lngserierangemax}")

                        # Base copy in chunks (incremental runs touch few rows per chunk)
                        lngchunksize = 5000
                        for lngserierangestart in range(1, lngserierangemax + 1, lngchunksize):
                            lngserierangeend = min(lngserierangestart + lngchunksize - 1, lngserierangemax)
                            print(f"Processing T2S_SERIE rows from ID {lngserierangestart} to {lngserierangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentserieid",str(lngserierangestart),"Current serie ID in the TMDb database preprocess",0)
                            
                            strsqlseries = f"""
INSERT INTO T_WC_T2S_SERIE (
    ID_SERIE, SERIE_TITLE, ID_IMDB, ADULT, DAT_FIRST_AIR, DAT_LAST_AIR,OVERVIEW,
    POSTER_PATH, BACKDROP_PATH, VOTE_AVERAGE, VOTE_COUNT,
    POPULARITY, ORIGINAL_LANGUAGE, ORIGINAL_TITLE,
    SERIE_TYPE, DAT_CREAT, TIM_UPDATED, FIRST_AIR_YEAR, FIRST_AIR_MONTH,
    FIRST_AIR_DAY, ID_WIKIDATA, HOMEPAGE_URL, STATUS, NUMBER_OF_EPISODES,
    NUMBER_OF_SEASONS, TAGLINE, LAST_AIR_YEAR, LAST_AIR_MONTH, LAST_AIR_DAY, DELETED
)
SELECT 
    ID_SERIE, TITLE, ID_IMDB, ADULT, DAT_FIRST_AIR, DAT_LAST_AIR, OVERVIEW,
    POSTER_PATH, BACKDROP_PATH, VOTE_AVERAGE, VOTE_COUNT,
    POPULARITY, ORIGINAL_LANGUAGE, ORIGINAL_TITLE,
    SERIE_TYPE, DAT_CREAT, TIM_UPDATED, FIRST_AIR_YEAR, FIRST_AIR_MONTH,
    FIRST_AIR_DAY, ID_WIKIDATA, HOMEPAGE_URL, STATUS, NUMBER_OF_EPISODES,
    NUMBER_OF_SEASONS, TAGLINE, LAST_AIR_YEAR, LAST_AIR_MONTH, LAST_AIR_DAY, DELETED
FROM T_WC_TMDB_SERIE
WHERE ADULT = 0 
AND ID_IMDB <> ''
AND ID_IMDB IS NOT NULL
AND ID_SERIE >= {lngserierangestart} AND ID_SERIE <= {lngserierangeend}
{strincrementalfilter}ON DUPLICATE KEY UPDATE
    SERIE_TITLE = VALUES(SERIE_TITLE),
    ID_IMDB = VALUES(ID_IMDB),
    ADULT = VALUES(ADULT),
    DAT_FIRST_AIR = VALUES(DAT_FIRST_AIR),
    DAT_LAST_AIR = VALUES(DAT_LAST_AIR),
    OVERVIEW = VALUES(OVERVIEW),
    POSTER_PATH = VALUES(POSTER_PATH),
    BACKDROP_PATH = VALUES(BACKDROP_PATH),
    VOTE_AVERAGE = VALUES(VOTE_AVERAGE),
    VOTE_COUNT = VALUES(VOTE_COUNT),
    POPULARITY = VALUES(POPULARITY),
    ORIGINAL_LANGUAGE = VALUES(ORIGINAL_LANGUAGE),
    ORIGINAL_TITLE = VALUES(ORIGINAL_TITLE),
    SERIE_TYPE = VALUES(SERIE_TYPE),
    DAT_CREAT = VALUES(DAT_CREAT),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    FIRST_AIR_YEAR = VALUES(FIRST_AIR_YEAR),
    FIRST_AIR_MONTH = VALUES(FIRST_AIR_MONTH),
    FIRST_AIR_DAY = VALUES(FIRST_AIR_DAY),
    ID_WIKIDATA = VALUES(ID_WIKIDATA),
    HOMEPAGE_URL = VALUES(HOMEPAGE_URL),
    STATUS = VALUES(STATUS),
    NUMBER_OF_EPISODES = VALUES(NUMBER_OF_EPISODES),
    NUMBER_OF_SEASONS = VALUES(NUMBER_OF_SEASONS),
    TAGLINE = VALUES(TAGLINE),
    LAST_AIR_YEAR = VALUES(LAST_AIR_YEAR),
    LAST_AIR_MONTH = VALUES(LAST_AIR_MONTH),
    LAST_AIR_DAY = VALUES(LAST_AIR_DAY),
    DELETED = VALUES(DELETED) """
                            cursor2.execute(strsqlseries)
                            cp.connectioncp.commit()
                            
                        # ---- Stale delete: single full-table anti-join (full coverage) ----------
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Stale-delete T2S_SERIE","Current sub process in the TMDb database series preprocess",0)
                        strsqlseriesdelete = """
DELETE t2s FROM T_WC_T2S_SERIE t2s
LEFT JOIN T_WC_TMDB_SERIE src
    ON src.ID_SERIE = t2s.ID_SERIE
   AND src.ADULT = 0
   AND src.ID_IMDB <> ''
   AND src.ID_IMDB IS NOT NULL
WHERE src.ID_SERIE IS NULL """
                        cursor2.execute(strsqlseriesdelete)
                        cp.connectioncp.commit()

                        # ---- Enrichment: full-table set-based passes, ONCE (were per-chunk) ------
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Enrich T2S_SERIE (ratings / FR title / Wikidata)","Current sub process in the TMDb database series preprocess",0)
                        # IMDb rating and vote count, same pass as the movies above
                        # (FASTAPI-TEXT2SQL-194).
                        strsqlseries = """
UPDATE T_WC_T2S_SERIE t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb
    ON t2s.ID_IMDB = imdb.tconst
SET t2s.IMDB_RATING = imdb.averageRating,
    t2s.IMDB_VOTES = imdb.numVotes
WHERE t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL """
                        cursor2.execute(strsqlseries)
                        cp.connectioncp.commit()

                        strsqlseries = f"""
UPDATE T_WC_T2S_SERIE t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb
    ON t2s.ID_IMDB = imdb.tconst
SET t2s.IMDB_RATING_WEIGHTED =
    ((imdb.numVotes / (imdb.numVotes + {lngimdbweightedratingm})) * imdb.averageRating) +
    (({lngimdbweightedratingm} / (imdb.numVotes + {lngimdbweightedratingm})) * {stravgrating})
WHERE t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL
    AND imdb.numVotes > 0 """
                        cursor2.execute(strsqlseries)
                        cp.connectioncp.commit()

                        strsqlseries = """
UPDATE T_WC_T2S_SERIE t2s
INNER JOIN T_WC_TMDB_SERIE_LANG t
    ON t2s.ID_SERIE = t.ID_SERIE
SET t2s.SERIE_TITLE_FR = t.TITLE
WHERE t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND t.LANG = 'fr' """
                        cursor2.execute(strsqlseries)
                        cp.connectioncp.commit()

                        # TMDB-MOVIE-PREPROCESS-043 : jumeau du bloc film, memes regles et memes
                        # raisons (voir le commentaire du processus 4). La serie n'a pas de
                        # Criterion : deux colonnes seulement passent en V2.
                        strsqlplex = f_wikidataexternalidsql(STR_WD_PROPERTY_PLEX, "t2s.ID_WIKIDATA", False, "spx", 50)
                        strsqlinstanceof = f_wikidatainstanceofsql("t2s.ID_WIKIDATA", "sio")
                        strsqlseries = f"""
UPDATE T_WC_T2S_SERIE t2s
INNER JOIN T_WC_WIKIDATA_SERIE_V1 w
    ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
SET t2s.WIKIDATA_TITLE = w.TITLE,
    t2s.PLEX_MEDIA_KEY = {strsqlplex},
    t2s.INSTANCE_OF = {strsqlinstanceof}
WHERE t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> '' """
                        cursor2.execute(strsqlseries)
                        cp.connectioncp.commit()

                        # ---- TMDB-MOVIE-PREPROCESS-031: localized TEXT into T_WC_T2S_SERIE_LANG ---------
                        # Twin of the movie T_WC_T2S_MOVIE_LANG copy. Text-only (OVERVIEW / TAGLINE); posters
                        # come from the image mechanism, not here. Language-AGNOSTIC (no LANG='fr' literal),
                        # curated to the T2S subset, lean (skip rows with no text), upsert (re-runnable).
                        # Chunked by ID_SERIE range so each transaction stays small (large source table).
                        # Reuses lngchunksize and lngserierangemax computed for the base copy above.
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copy localized text into T_WC_T2S_SERIE_LANG, chunked","Current sub process in the TMDb database series preprocess",0)
                        strsqlseries = """
CREATE TABLE IF NOT EXISTS T_WC_T2S_SERIE_LANG (
  ID_SERIE int(11) NOT NULL,
  LANG varchar(10) NOT NULL,
  OVERVIEW mediumtext DEFAULT NULL,
  TAGLINE mediumtext DEFAULT NULL,
  TIM_UPDATED timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (ID_SERIE, LANG),
  KEY LANG (LANG)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci """
                        cursor2.execute(strsqlseries)
                        cp.connectioncp.commit()

                        for lngserierangestart in range(1, lngserierangemax + 1, lngchunksize):
                            lngserierangeend = min(lngserierangestart + lngchunksize - 1, lngserierangemax)
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentserieid",str(lngserierangestart),"Current serie ID in the TMDb database preprocess (SERIE_LANG)",0)
                            strsqlseries = f"""
INSERT INTO T_WC_T2S_SERIE_LANG (ID_SERIE, LANG, OVERVIEW, TAGLINE)
SELECT l.ID_SERIE, l.LANG, l.OVERVIEW, l.TAGLINE
FROM T_WC_TMDB_SERIE_LANG l
INNER JOIN T_WC_T2S_SERIE t2s
    ON t2s.ID_SERIE = l.ID_SERIE
WHERE l.ID_SERIE BETWEEN {lngserierangestart} AND {lngserierangeend}
    AND (l.DELETED IS NULL OR l.DELETED = 0)
    AND l.LANG IS NOT NULL AND l.LANG <> ''
    AND ( (l.OVERVIEW IS NOT NULL AND l.OVERVIEW <> '')
       OR (l.TAGLINE IS NOT NULL AND l.TAGLINE <> '') )
ON DUPLICATE KEY UPDATE
    OVERVIEW = VALUES(OVERVIEW),
    TAGLINE = VALUES(TAGLINE) """
                            cursor2.execute(strsqlseries)
                            cp.connectioncp.commit()

                        # Persist the watermark only after a successful run.
                        cp.f_setservervariable("strtmdbmoviepreprocesst2sserielastrun", strrunstart, "Start datetime of the last successful T2S_SERIE run; incremental watermark on T_WC_TMDB_SERIE.TIM_UPDATED", 0)

                    # Now copy Wikipedia content to the serie records




                elif intindex == 6:
                    #----------------------------------------------------
                    print("T2S_PERSON processing")
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from TMDB_PERSON to T2S_PERSON","Current sub process in the TMDb database person preprocess",0)

                        # --- Incremental watermark (mirrors Process 1/4) -------------------------
                        # Self-gated like T2S_MOVIE: the qualification filter (ADULT / ID_IMDB /
                        # ID_WIKIDATA) lives on the source row, so any (dis)qualifying change bumps
                        # TIM_UPDATED -> incremental is exact. Stale-delete and the Wikidata
                        # enrichment run full-table every run.
                        lngt2spersonlookbackminutes = 60
                        strrunstart = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                        strlastrun = cp.f_getservervariable("strtmdbmoviepreprocesst2spersonlastrun", 0)
                        strincrementalfilter = ""
                        if strlastrun:
                            strincrementalfilter = (
                                "AND TIM_UPDATED >= DATE_SUB('" + strlastrun + "', INTERVAL "
                                + str(lngt2spersonlookbackminutes) + " MINUTE) "
                            )
                            print(f"Incremental run: persons changed since {strlastrun} (minus {lngt2spersonlookbackminutes} min buffer)")
                        else:
                            print("First run (no watermark): full scan of all qualifying persons")

                        # Get the maximum ID_PERSON value from the database
                        cursor.execute("SELECT MAX(ID_PERSON) as max_id FROM T_WC_TMDB_PERSON")
                        result = cursor.fetchone()
                        lngpersonrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_PERSON in database: {lngpersonrangemax}")

                        # Base copy in chunks (incremental runs touch few rows per chunk)
                        lngchunksize = 5000
                        for lngpersonrangestart in range(1, lngpersonrangemax + 1, lngchunksize):
                            lngpersonrangeend = min(lngpersonrangestart + lngchunksize - 1, lngpersonrangemax)
                            print(f"Processing T2S_PERSON rows from ID {lngpersonrangestart} to {lngpersonrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentpersonid",str(lngpersonrangestart),"Current person ID in the TMDb database preprocess",0)
                            
                            strsqlpersons = f"""
    INSERT INTO T_WC_T2S_PERSON (
        ID_PERSON, PERSON_NAME, ID_IMDB, ADULT, BIRTHDAY, DEATHDAY, BIOGRAPHY,
        PROFILE_PATH, KNOWN_FOR_DEPARTMENT, TIM_CREDITS_DOWNLOADED, 
        POPULARITY, ALSO_KNOWN_AS, DELETED, 
        COUNTRY_OF_BIRTH, DAT_CREAT, TIM_UPDATED, ID_WIKIDATA, HOMEPAGE_URL, GENDER, BIRTH_YEAR, BIRTH_MONTH,
        BIRTH_DAY, DEATH_YEAR, DEATH_MONTH, DEATH_DAY
    )
    SELECT 
        ID_PERSON, NAME, ID_IMDB, ADULT, BIRTHDAY, DEATHDAY, BIOGRAPHY,
        PROFILE_PATH, KNOWN_FOR_DEPARTMENT, TIM_CREDITS_DOWNLOADED,
        POPULARITY, ALSO_KNOWN_AS, DELETED,
        COUNTRY_OF_BIRTH, DAT_CREAT, TIM_UPDATED, ID_WIKIDATA, HOMEPAGE_URL, GENDER, BIRTH_YEAR, BIRTH_MONTH,
        BIRTH_DAY, DEATH_YEAR, DEATH_MONTH, DEATH_DAY
    FROM T_WC_TMDB_PERSON
    WHERE ADULT = 0 
    AND ID_IMDB <> ''
    AND ID_IMDB IS NOT NULL
    AND ID_WIKIDATA <> ''
    AND ID_WIKIDATA IS NOT NULL
    AND ID_PERSON >= {lngpersonrangestart} AND ID_PERSON <= {lngpersonrangeend}
    {strincrementalfilter}ON DUPLICATE KEY UPDATE
        PERSON_NAME = VALUES(PERSON_NAME),
        ID_IMDB = VALUES(ID_IMDB),
        ADULT = VALUES(ADULT),
        ALSO_KNOWN_AS = VALUES(ALSO_KNOWN_AS),
        DELETED = VALUES(DELETED),
        BIRTHDAY = VALUES(BIRTHDAY),
        DEATHDAY = VALUES(DEATHDAY),
        BIOGRAPHY = VALUES(BIOGRAPHY),
        PROFILE_PATH = VALUES(PROFILE_PATH),
        KNOWN_FOR_DEPARTMENT = VALUES(KNOWN_FOR_DEPARTMENT),
        TIM_CREDITS_DOWNLOADED = VALUES(TIM_CREDITS_DOWNLOADED),
        POPULARITY = VALUES(POPULARITY),
        COUNTRY_OF_BIRTH = VALUES(COUNTRY_OF_BIRTH),
        DAT_CREAT = VALUES(DAT_CREAT),
        TIM_UPDATED = VALUES(TIM_UPDATED),
        ID_WIKIDATA = VALUES(ID_WIKIDATA),
        HOMEPAGE_URL = VALUES(HOMEPAGE_URL),
        GENDER = VALUES(GENDER),
        BIRTH_YEAR = VALUES(BIRTH_YEAR),
        BIRTH_MONTH = VALUES(BIRTH_MONTH),
        BIRTH_DAY = VALUES(BIRTH_DAY),
        DEATH_YEAR = VALUES(DEATH_YEAR),
        DEATH_MONTH = VALUES(DEATH_MONTH),
        DEATH_DAY = VALUES(DEATH_DAY) """
                            cursor2.execute(strsqlpersons)
                            cp.connectioncp.commit()
                            
                        # ---- Stale delete: single full-table anti-join (full coverage) ----------
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Stale-delete T2S_PERSON","Current sub process in the TMDb database person preprocess",0)
                        strsqlpersonsdelete = """
DELETE t2s FROM T_WC_T2S_PERSON t2s
LEFT JOIN T_WC_TMDB_PERSON src
    ON src.ID_PERSON = t2s.ID_PERSON
   AND src.ADULT = 0
   AND src.ID_IMDB <> ''
   AND src.ID_IMDB IS NOT NULL
   AND src.ID_WIKIDATA <> ''
   AND src.ID_WIKIDATA IS NOT NULL
WHERE src.ID_PERSON IS NULL """
                        cursor2.execute(strsqlpersonsdelete)
                        cp.connectioncp.commit()

                        # ---- Enrichment: full-table set-based pass, ONCE (was per-chunk) ---------
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Enrich T2S_PERSON (Wikidata)","Current sub process in the TMDb database person preprocess",0)
                        # TMDB-MOVIE-PREPROCESS-043 : troisieme jumeau (voir le processus 4).
                        # Une seule colonne passe en V2, INSTANCE_OF ; WIKIDATA_NAME est un
                        # libelle et reste sur V1 jusqu'a WIKIDATA-CRAWLER-017.
                        strsqlinstanceof = f_wikidatainstanceofsql("t2s.ID_WIKIDATA", "sio")
                        strsqlpersons = f"""
UPDATE T_WC_T2S_PERSON t2s
INNER JOIN T_WC_WIKIDATA_PERSON_V1 w
    ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
SET t2s.WIKIDATA_NAME = w.NAME,
    t2s.INSTANCE_OF = {strsqlinstanceof}
WHERE t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND t2s.ID_WIKIDATA IS NOT NULL
    AND t2s.ID_WIKIDATA <> '' """
                        cursor2.execute(strsqlpersons)
                        cp.connectioncp.commit()

                        # Persist the watermark only after a successful run.
                        cp.f_setservervariable("strtmdbmoviepreprocesst2spersonlastrun", strrunstart, "Start datetime of the last successful T2S_PERSON run; incremental watermark on T_WC_TMDB_PERSON.TIM_UPDATED", 0)

                elif intindex == 7:
                    #----------------------------------------------------
                    print("T2S_COMPANY processing")
                    if 1:
                        # Compute MOVIE_COUNT — set-based, keyed on ID_COMPANY (was a per-row
                        # f_sqlupdatearray loop grouped by NAME). Reset-then-update so companies that
                        # lost all their movies fall back to 0 and drop out of the rebuild below.
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Computing MOVIE_COUNT for T2S_COMPANY","Current sub process in the TMDb database company preprocess",0)
                        cursor2.execute("UPDATE T_WC_TMDB_COMPANY SET MOVIE_COUNT = 0")
                        cp.connectioncp.commit()
                        strsqlcompanies = """
UPDATE T_WC_TMDB_COMPANY c
INNER JOIN (
    SELECT mc.ID_COMPANY, COUNT(DISTINCT m.ID_MOVIE) AS COMPTE
    FROM T_WC_T2S_MOVIE m
    INNER JOIN T_WC_TMDB_MOVIE_COMPANY mc ON mc.ID_MOVIE = m.ID_MOVIE
    GROUP BY mc.ID_COMPANY
) x ON x.ID_COMPANY = c.ID_COMPANY
SET c.MOVIE_COUNT = x.COMPTE """
                        print(strsqlcompanies)
                        cursor2.execute(strsqlcompanies)
                        cp.connectioncp.commit()
                    if 1:
                        # Compute SERIE_COUNT — set-based, keyed on ID_COMPANY (see MOVIE_COUNT note).
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Computing SERIE_COUNT for T2S_COMPANY","Current sub process in the TMDb database company preprocess",0)
                        cursor2.execute("UPDATE T_WC_TMDB_COMPANY SET SERIE_COUNT = 0")
                        cp.connectioncp.commit()
                        strsqlcompanies = """
UPDATE T_WC_TMDB_COMPANY c
INNER JOIN (
    SELECT sc.ID_COMPANY, COUNT(DISTINCT s.ID_SERIE) AS COMPTE
    FROM T_WC_T2S_SERIE s
    INNER JOIN T_WC_TMDB_SERIE_COMPANY sc ON sc.ID_SERIE = s.ID_SERIE
    GROUP BY sc.ID_COMPANY
) x ON x.ID_COMPANY = c.ID_COMPANY
SET c.SERIE_COUNT = x.COMPTE """
                        print(strsqlcompanies)
                        cursor2.execute(strsqlcompanies)
                        cp.connectioncp.commit()

                    if 1:
                        # Get the maximum ID_COMPANY value from the database
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from TMDB_COMPANY to T2S_COMPANY","Current sub process in the TMDb database company preprocess",0)
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_COMPANY WHERE ((MOVIE_COUNT IS NOT NULL AND MOVIE_COUNT > 0) OR (SERIE_COUNT IS NOT NULL AND SERIE_COUNT > 0))")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_COMPANY from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentcompanyid","BUILD","Current company ID in the TMDb database preprocess",0)
                        strsqlcompanies = "DROP TABLE IF EXISTS T_WC_T2S_COMPANY_BUILD"
                        cursor2.execute(strsqlcompanies)
                        cp.connectioncp.commit()
                        strsqlcompanies = "CREATE TABLE T_WC_T2S_COMPANY_BUILD LIKE T_WC_T2S_COMPANY"
                        cursor2.execute(strsqlcompanies)
                        cp.connectioncp.commit()
                        strsqlcompanies = """
INSERT INTO T_WC_T2S_COMPANY_BUILD (
    ID_COMPANY, COMPANY_NAME, DESCRIPTION,
    LOGO_PATH, TIM_CREDITS_DOWNLOADED,
    ORIGIN_COUNTRY, HEADQUARTERS,
    DAT_CREAT, TIM_UPDATED, DELETED,
    ID_PARENT, HOMEPAGE_URL, MOVIE_COUNT, SERIE_COUNT
)
SELECT
    ID_COMPANY, NAME, DESCRIPTION,
    LOGO_PATH, TIM_CREDITS_DOWNLOADED,
    ORIGIN_COUNTRY, HEADQUARTERS,
    DAT_CREAT, TIM_UPDATED, DELETED,
    ID_PARENT, HOMEPAGE_URL, MOVIE_COUNT, SERIE_COUNT
FROM T_WC_TMDB_COMPANY
WHERE ((MOVIE_COUNT IS NOT NULL AND MOVIE_COUNT > 0) OR (SERIE_COUNT IS NOT NULL AND SERIE_COUNT > 0))
"""
                        cursor2.execute(strsqlcompanies)
                        cp.connectioncp.commit()

                        strsqlcompanies = """UPDATE T_WC_T2S_COMPANY_BUILD t
JOIN (
    SELECT
        mc.ID_COMPANY,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(m.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_MOVIE_COMPANY mc
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mc.ID_MOVIE
    GROUP BY mc.ID_COMPANY
) x
    ON x.ID_COMPANY = t.ID_COMPANY
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_WEIGHTED = x.AVG_IMDB_RATING_WEIGHTED,
    t.POPULARITY = x.AVG_POPULARITY
"""
                        print(strsqlcompanies)
                        cursor2.execute(strsqlcompanies)
                        cp.connectioncp.commit()

                        strsqlcompanies = """UPDATE T_WC_T2S_COMPANY_BUILD t
JOIN (
    SELECT
        sc.ID_COMPANY,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED,
        AVG(s.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_SERIE_COMPANY sc
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = sc.ID_SERIE
    GROUP BY sc.ID_COMPANY
) x
    ON x.ID_COMPANY = t.ID_COMPANY
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_WEIGHTED = COALESCE(t.IMDB_RATING_WEIGHTED, x.AVG_IMDB_RATING_WEIGHTED),
    t.POPULARITY = COALESCE(t.POPULARITY, x.AVG_POPULARITY)
"""
                        print(strsqlcompanies)
                        cursor2.execute(strsqlcompanies)
                        cp.connectioncp.commit()

                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_T2S_COMPANY_BUILD")
                        result = cursor.fetchone()
                        lngbuildrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Validated T_WC_T2S_COMPANY_BUILD with {lngbuildrowcount} rows")
                        strsqlcompanies = "DROP TABLE IF EXISTS T_WC_T2S_COMPANY_OLD"
                        cursor2.execute(strsqlcompanies)
                        cp.connectioncp.commit()
                        strsqlcompanies = """
RENAME TABLE
    T_WC_T2S_COMPANY TO T_WC_T2S_COMPANY_OLD,
    T_WC_T2S_COMPANY_BUILD TO T_WC_T2S_COMPANY
"""
                        cursor2.execute(strsqlcompanies)
                        cp.connectioncp.commit()
                        strsqlcompanies = "DROP TABLE IF EXISTS T_WC_T2S_COMPANY_OLD"
                        cursor2.execute(strsqlcompanies)
                        cp.connectioncp.commit()

                elif intindex == 8:
                    #----------------------------------------------------
                    print("T2S_NETWORK processing")
                    if 1:
                        # Compute SERIE_COUNT — set-based, keyed on ID_NETWORK (was a per-row
                        # f_sqlupdatearray loop grouped by NAME). Reset-then-update so networks that
                        # lost all their series fall back to 0 and drop out of the rebuild below.
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Computing SERIE_COUNT for T2S_NETWORK","Current sub process in the TMDb database network preprocess",0)
                        cursor2.execute("UPDATE T_WC_TMDB_NETWORK SET SERIE_COUNT = 0")
                        cp.connectioncp.commit()
                        strsqlnetworks = """
UPDATE T_WC_TMDB_NETWORK n
INNER JOIN (
    SELECT sn.ID_NETWORK, COUNT(DISTINCT s.ID_SERIE) AS COMPTE
    FROM T_WC_T2S_SERIE s
    INNER JOIN T_WC_TMDB_SERIE_NETWORK sn ON sn.ID_SERIE = s.ID_SERIE
    GROUP BY sn.ID_NETWORK
) x ON x.ID_NETWORK = n.ID_NETWORK
SET n.SERIE_COUNT = x.COMPTE """
                        print(strsqlnetworks)
                        cursor2.execute(strsqlnetworks)
                        cp.connectioncp.commit()
                    if 1:
                        # Get the maximum ID_NETWORK value from the database
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from TMDB_NETWORK to T2S_NETWORK","Current sub process in the TMDb database network preprocess",0)
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_NETWORK WHERE (SERIE_COUNT IS NOT NULL AND SERIE_COUNT > 0)")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_NETWORK from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentnetworkid","BUILD","Current network ID in the TMDb database preprocess",0)
                        strsqlnetworks = "DROP TABLE IF EXISTS T_WC_T2S_NETWORK_BUILD"
                        cursor2.execute(strsqlnetworks)
                        cp.connectioncp.commit()
                        strsqlnetworks = "CREATE TABLE T_WC_T2S_NETWORK_BUILD LIKE T_WC_T2S_NETWORK"
                        cursor2.execute(strsqlnetworks)
                        cp.connectioncp.commit()
                        strsqlnetworks = """
INSERT INTO T_WC_T2S_NETWORK_BUILD (
    ID_NETWORK, NETWORK_NAME,
    LOGO_PATH, TIM_CREDITS_DOWNLOADED,
    ORIGIN_COUNTRY, HEADQUARTERS,
    DAT_CREAT, TIM_UPDATED,
    HOMEPAGE_URL, SERIE_COUNT, DELETED
)
SELECT
    ID_NETWORK, NAME,
    LOGO_PATH, TIM_CREDITS_DOWNLOADED,
    ORIGIN_COUNTRY, HEADQUARTERS,
    DAT_CREAT, TIM_UPDATED,
    HOMEPAGE_URL, SERIE_COUNT, DELETED
FROM T_WC_TMDB_NETWORK
WHERE (SERIE_COUNT IS NOT NULL AND SERIE_COUNT > 0)
"""
                        cursor2.execute(strsqlnetworks)
                        cp.connectioncp.commit()
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_T2S_NETWORK_BUILD")
                        result = cursor.fetchone()
                        lngbuildrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Validated T_WC_T2S_NETWORK_BUILD with {lngbuildrowcount} rows")
                        strsqlnetworks = "DROP TABLE IF EXISTS T_WC_T2S_NETWORK_OLD"
                        cursor2.execute(strsqlnetworks)
                        cp.connectioncp.commit()
                        strsqlnetworks = """
RENAME TABLE
    T_WC_T2S_NETWORK TO T_WC_T2S_NETWORK_OLD,
    T_WC_T2S_NETWORK_BUILD TO T_WC_T2S_NETWORK
"""
                        cursor2.execute(strsqlnetworks)
                        cp.connectioncp.commit()
                        strsqlnetworks = "DROP TABLE IF EXISTS T_WC_T2S_NETWORK_OLD"
                        cursor2.execute(strsqlnetworks)
                        cp.connectioncp.commit()

                elif intindex == 9:
                    #----------------------------------------------------
                    print("T2S_PERSON_MOVIE processing")
                    if 1:
                        # Get the maximum ID_PERSON_MOVIE value from the database
                        cursor.execute("SELECT MAX(ID_TMDB_PERSON_MOVIE) as max_id FROM T_WC_TMDB_PERSON_MOVIE")
                        result = cursor.fetchone()
                        lngpersonmovierangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_TMDB_PERSON_MOVIE in database: {lngpersonmovierangemax}")
                        
                        # Process database in chunks of 1000 records
                        lngchunksize = 1000
                        lngtotalprocessed = 0
                        
                        for lngpersonmovierangestart in range(1, lngpersonmovierangemax + 1, lngchunksize):
                            lngpersonmovierangeend = min(lngpersonmovierangestart + lngchunksize - 1, lngpersonmovierangemax)
                            print(f"Processing T2S_PERSON_MOVIE rows from ID {lngpersonmovierangestart} to {lngpersonmovierangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentpersonmovieid",str(lngpersonmovierangestart),"Current person-movie relation ID in the TMDb database preprocess",0)
                            
                            strsqlpersonmovies = f"""
    INSERT INTO T_WC_T2S_PERSON_MOVIE (
        ID_T2S_PERSON_MOVIE, ID_PERSON, ID_MOVIE, ID_CREDIT, 
        CREDIT_TYPE, CAST_CHARACTER, CREW_DEPARTMENT, CREW_JOB, 
        DISPLAY_ORDER 
    )
    SELECT 
        ID_TMDB_PERSON_MOVIE, ID_PERSON, ID_MOVIE, ID_CREDIT,
        CREDIT_TYPE, CAST_CHARACTER, CREW_DEPARTMENT, CREW_JOB, 
        DISPLAY_ORDER 
    FROM T_WC_TMDB_PERSON_MOVIE
    WHERE ID_TMDB_PERSON_MOVIE >= {lngpersonmovierangestart} AND ID_TMDB_PERSON_MOVIE <= {lngpersonmovierangeend}
    AND ID_PERSON IN (
        SELECT ID_PERSON FROM T_WC_T2S_PERSON
    )
    AND ID_MOVIE IN (
        SELECT ID_MOVIE FROM T_WC_T2S_MOVIE
    )
    ON DUPLICATE KEY UPDATE
        ID_PERSON = VALUES(ID_PERSON),
        ID_MOVIE = VALUES(ID_MOVIE),
        ID_CREDIT = VALUES(ID_CREDIT),
        CREDIT_TYPE = VALUES(CREDIT_TYPE),
        CAST_CHARACTER = VALUES(CAST_CHARACTER),
        CREW_DEPARTMENT = VALUES(CREW_DEPARTMENT),
        CREW_JOB = VALUES(CREW_JOB),
        DISPLAY_ORDER = VALUES(DISPLAY_ORDER) """
                            cursor2.execute(strsqlpersonmovies)
                            cp.connectioncp.commit()
                            
                            strsqlpersonmoviesdelete = f"""
    DELETE FROM T_WC_T2S_PERSON_MOVIE 
    WHERE ID_T2S_PERSON_MOVIE >= {lngpersonmovierangestart} AND ID_T2S_PERSON_MOVIE <= {lngpersonmovierangeend}
    AND ID_T2S_PERSON_MOVIE NOT IN (
        SELECT ID_TMDB_PERSON_MOVIE FROM T_WC_TMDB_PERSON_MOVIE 
        WHERE ID_TMDB_PERSON_MOVIE >= {lngpersonmovierangestart} AND ID_TMDB_PERSON_MOVIE <= {lngpersonmovierangeend}
        AND ID_PERSON IN (
            SELECT ID_PERSON FROM T_WC_T2S_PERSON
        )
        AND ID_MOVIE IN (
            SELECT ID_MOVIE FROM T_WC_T2S_MOVIE
        )
    ) """
                            cursor2.execute(strsqlpersonmoviesdelete)
                            cp.connectioncp.commit()

                elif intindex == 10:
                    #----------------------------------------------------
                    print("T2S_PERSON_SERIE processing")
                    if 1:
                        # Get the maximum ID_PERSON_MOVIE value from the database
                        cursor.execute("SELECT MAX(ID_TMDB_PERSON_SERIE) as max_id FROM T_WC_TMDB_PERSON_SERIE")
                        result = cursor.fetchone()
                        lngpersonserierangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_TMDB_PERSON_SERIE in database: {lngpersonserierangemax}")
                        
                        # Process database in chunks of 1000 records
                        lngchunksize = 1000
                        lngtotalprocessed = 0
                        
                        for lngpersonserierangestart in range(1, lngpersonserierangemax + 1, lngchunksize):
                            lngpersonserierangeend = min(lngpersonserierangestart + lngchunksize - 1, lngpersonserierangemax)
                            print(f"Processing T2S_PERSON_SERIE rows from ID {lngpersonserierangestart} to {lngpersonserierangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentpersonserieid",str(lngpersonserierangestart),"Current person-serie relation ID in the TMDb database preprocess",0)
                            
                            strsqlpersonmovies = f"""
    INSERT INTO T_WC_T2S_PERSON_SERIE (
        ID_T2S_PERSON_SERIE, ID_PERSON, ID_SERIE, ID_CREDIT, 
        CREDIT_TYPE, CAST_CHARACTER, CREW_DEPARTMENT, CREW_JOB, 
        DISPLAY_ORDER 
    )
    SELECT 
        ID_TMDB_PERSON_SERIE, ID_PERSON, ID_SERIE, ID_CREDIT,
        CREDIT_TYPE, CAST_CHARACTER, CREW_DEPARTMENT, CREW_JOB, 
        DISPLAY_ORDER 
    FROM T_WC_TMDB_PERSON_SERIE
    WHERE ID_TMDB_PERSON_SERIE >= {lngpersonserierangestart} AND ID_TMDB_PERSON_SERIE <= {lngpersonserierangeend}
    AND ID_PERSON IN (
        SELECT ID_PERSON FROM T_WC_T2S_PERSON
    )
    AND ID_SERIE IN (
        SELECT ID_SERIE FROM T_WC_T2S_SERIE
    )
    ON DUPLICATE KEY UPDATE
        ID_PERSON = VALUES(ID_PERSON),
        ID_SERIE = VALUES(ID_SERIE),
        ID_CREDIT = VALUES(ID_CREDIT),
        CREDIT_TYPE = VALUES(CREDIT_TYPE),
        CAST_CHARACTER = VALUES(CAST_CHARACTER),
        CREW_DEPARTMENT = VALUES(CREW_DEPARTMENT),
        CREW_JOB = VALUES(CREW_JOB),
        DISPLAY_ORDER = VALUES(DISPLAY_ORDER) """
                            cursor2.execute(strsqlpersonmovies)
                            cp.connectioncp.commit()
                            
                            strsqlpersonmoviesdelete = f"""
    DELETE FROM T_WC_T2S_PERSON_SERIE 
    WHERE ID_T2S_PERSON_SERIE >= {lngpersonserierangestart} AND ID_T2S_PERSON_SERIE <= {lngpersonserierangeend}
    AND ID_T2S_PERSON_SERIE NOT IN (
        SELECT ID_TMDB_PERSON_SERIE FROM T_WC_TMDB_PERSON_SERIE 
        WHERE ID_TMDB_PERSON_SERIE >= {lngpersonserierangestart} AND ID_TMDB_PERSON_SERIE <= {lngpersonserierangeend}
        AND ID_PERSON IN (
            SELECT ID_PERSON FROM T_WC_T2S_PERSON
        )
        AND ID_SERIE IN (
            SELECT ID_SERIE FROM T_WC_T2S_SERIE
        )
    ) """
                            cursor2.execute(strsqlpersonmoviesdelete)
                            cp.connectioncp.commit()

                elif intindex == 11:
                    #----------------------------------------------------
                    print("T2S_MOVIE_GENRE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_MOVIE_GENRE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_MOVIE_GENRE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentmoviegenreid","BUILD","Current movie-genre ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_GENRE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_MOVIE_GENRE_BUILD LIKE T_WC_T2S_MOVIE_GENRE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_MOVIE_GENRE_BUILD (
    ID_ROW, ID_MOVIE, ID_GENRE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_MOVIE, ID_GENRE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM (
    SELECT
        T_WC_TMDB_MOVIE_GENRE.ID_ROW,
        T_WC_TMDB_MOVIE_GENRE.ID_MOVIE,
        T_WC_TMDB_MOVIE_GENRE.ID_GENRE,
        T_WC_TMDB_MOVIE_GENRE.DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY T_WC_TMDB_MOVIE_GENRE.ID_GENRE
            ORDER BY CASE WHEN T_WC_T2S_MOVIE.IMDB_RATING_WEIGHTED IS NULL THEN 1 ELSE 0 END,
                     T_WC_T2S_MOVIE.IMDB_RATING_WEIGHTED DESC,
                     T_WC_TMDB_MOVIE_GENRE.ID_MOVIE ASC
        ) AS DISPLAY_ORDER,
        T_WC_TMDB_MOVIE_GENRE.ID_CREATOR,
        T_WC_TMDB_MOVIE_GENRE.DAT_CREAT,
        T_WC_TMDB_MOVIE_GENRE.ID_OWNER,
        T_WC_TMDB_MOVIE_GENRE.TIM_UPDATED,
        T_WC_TMDB_MOVIE_GENRE.ID_USER_UPDATED
    FROM T_WC_TMDB_MOVIE_GENRE
    INNER JOIN T_WC_T2S_MOVIE ON T_WC_TMDB_MOVIE_GENRE.ID_MOVIE = T_WC_T2S_MOVIE.ID_MOVIE
) ranked_movie_genres
"""
                        execute_sql_with_retry(
                            cp.connectioncp,
                            cursor2,
                            strsql,
                            "T2S_MOVIE_GENRE build table population",
                        )
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_GENRE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_MOVIE_GENRE TO T_WC_T2S_MOVIE_GENRE_OLD,
    T_WC_T2S_MOVIE_GENRE_BUILD TO T_WC_T2S_MOVIE_GENRE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_GENRE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 12:
                    #----------------------------------------------------
                    print("T2S_SERIE_GENRE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SERIE_GENRE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SERIE_GENRE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentseriegenreid","BUILD","Current serie-genre ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_GENRE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SERIE_GENRE_BUILD LIKE T_WC_T2S_SERIE_GENRE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SERIE_GENRE_BUILD (
    ID_ROW, ID_SERIE, ID_GENRE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_SERIE, ID_GENRE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM (
    SELECT
        T_WC_TMDB_SERIE_GENRE.ID_ROW,
        T_WC_TMDB_SERIE_GENRE.ID_SERIE,
        T_WC_TMDB_SERIE_GENRE.ID_GENRE,
        T_WC_TMDB_SERIE_GENRE.DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY T_WC_TMDB_SERIE_GENRE.ID_GENRE
            ORDER BY CASE WHEN T_WC_T2S_SERIE.IMDB_RATING_WEIGHTED IS NULL THEN 1 ELSE 0 END,
                     T_WC_T2S_SERIE.IMDB_RATING_WEIGHTED DESC,
                     T_WC_TMDB_SERIE_GENRE.ID_SERIE ASC
        ) AS DISPLAY_ORDER,
        T_WC_TMDB_SERIE_GENRE.ID_CREATOR,
        T_WC_TMDB_SERIE_GENRE.DAT_CREAT,
        T_WC_TMDB_SERIE_GENRE.ID_OWNER,
        T_WC_TMDB_SERIE_GENRE.TIM_UPDATED,
        T_WC_TMDB_SERIE_GENRE.ID_USER_UPDATED
    FROM T_WC_TMDB_SERIE_GENRE
    INNER JOIN T_WC_T2S_SERIE ON T_WC_TMDB_SERIE_GENRE.ID_SERIE = T_WC_T2S_SERIE.ID_SERIE
) ranked_serie_genres
"""
                        execute_sql_with_retry(
                            cp.connectioncp,
                            cursor2,
                            strsql,
                            "T2S_SERIE_GENRE build table population",
                        )
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_GENRE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SERIE_GENRE TO T_WC_T2S_SERIE_GENRE_OLD,
    T_WC_T2S_SERIE_GENRE_BUILD TO T_WC_T2S_SERIE_GENRE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_GENRE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 36:
                    #----------------------------------------------------
                    print("T2S_MOVIE_SIMILAR processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_MOVIE_SIMILAR")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_MOVIE_SIMILAR from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentmoviesimilarid","BUILD","Current movie-similar ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_SIMILAR_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_MOVIE_SIMILAR_BUILD LIKE T_WC_T2S_MOVIE_SIMILAR"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_MOVIE_SIMILAR_BUILD (
    ID_MOVIE, ID_MOVIE_SIMILAR,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_MOVIE, ID_MOVIE_SIMILAR,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM (
    SELECT
        T_WC_TMDB_MOVIE_SIMILAR.ID_MOVIE,
        T_WC_TMDB_MOVIE_SIMILAR.ID_MOVIE_SIMILAR,
        T_WC_TMDB_MOVIE_SIMILAR.DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY T_WC_TMDB_MOVIE_SIMILAR.ID_MOVIE
            ORDER BY T_WC_TMDB_MOVIE_SIMILAR.DISPLAY_ORDER ASC,
                     T_WC_TMDB_MOVIE_SIMILAR.ID_MOVIE_SIMILAR ASC
        ) AS DISPLAY_ORDER,
        T_WC_TMDB_MOVIE_SIMILAR.ID_CREATOR,
        T_WC_TMDB_MOVIE_SIMILAR.DAT_CREAT,
        T_WC_TMDB_MOVIE_SIMILAR.ID_OWNER,
        T_WC_TMDB_MOVIE_SIMILAR.TIM_UPDATED,
        T_WC_TMDB_MOVIE_SIMILAR.ID_USER_UPDATED
    FROM T_WC_TMDB_MOVIE_SIMILAR
    INNER JOIN T_WC_T2S_MOVIE AS T2S_SRC ON T_WC_TMDB_MOVIE_SIMILAR.ID_MOVIE = T2S_SRC.ID_MOVIE
    INNER JOIN T_WC_T2S_MOVIE AS T2S_NB ON T_WC_TMDB_MOVIE_SIMILAR.ID_MOVIE_SIMILAR = T2S_NB.ID_MOVIE
    WHERE (T_WC_TMDB_MOVIE_SIMILAR.DELETED IS NULL OR T_WC_TMDB_MOVIE_SIMILAR.DELETED = 0)
) ranked_movie_similar
"""
                        execute_sql_with_retry(
                            cp.connectioncp,
                            cursor2,
                            strsql,
                            "T2S_MOVIE_SIMILAR build table population",
                        )
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_SIMILAR_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_MOVIE_SIMILAR TO T_WC_T2S_MOVIE_SIMILAR_OLD,
    T_WC_T2S_MOVIE_SIMILAR_BUILD TO T_WC_T2S_MOVIE_SIMILAR
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_SIMILAR_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 37:
                    #----------------------------------------------------
                    print("T2S_MOVIE_RECOMMENDATION processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_MOVIE_RECOMMENDATION")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_MOVIE_RECOMMENDATION from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovierecommendationid","BUILD","Current movie-recommendation ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_RECOMMENDATION_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_MOVIE_RECOMMENDATION_BUILD LIKE T_WC_T2S_MOVIE_RECOMMENDATION"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_MOVIE_RECOMMENDATION_BUILD (
    ID_MOVIE, ID_MOVIE_RECOMMENDED,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_MOVIE, ID_MOVIE_RECOMMENDED,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM (
    SELECT
        T_WC_TMDB_MOVIE_RECOMMENDATION.ID_MOVIE,
        T_WC_TMDB_MOVIE_RECOMMENDATION.ID_MOVIE_RECOMMENDED,
        T_WC_TMDB_MOVIE_RECOMMENDATION.DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY T_WC_TMDB_MOVIE_RECOMMENDATION.ID_MOVIE
            ORDER BY T_WC_TMDB_MOVIE_RECOMMENDATION.DISPLAY_ORDER ASC,
                     T_WC_TMDB_MOVIE_RECOMMENDATION.ID_MOVIE_RECOMMENDED ASC
        ) AS DISPLAY_ORDER,
        T_WC_TMDB_MOVIE_RECOMMENDATION.ID_CREATOR,
        T_WC_TMDB_MOVIE_RECOMMENDATION.DAT_CREAT,
        T_WC_TMDB_MOVIE_RECOMMENDATION.ID_OWNER,
        T_WC_TMDB_MOVIE_RECOMMENDATION.TIM_UPDATED,
        T_WC_TMDB_MOVIE_RECOMMENDATION.ID_USER_UPDATED
    FROM T_WC_TMDB_MOVIE_RECOMMENDATION
    INNER JOIN T_WC_T2S_MOVIE AS T2S_SRC ON T_WC_TMDB_MOVIE_RECOMMENDATION.ID_MOVIE = T2S_SRC.ID_MOVIE
    INNER JOIN T_WC_T2S_MOVIE AS T2S_NB ON T_WC_TMDB_MOVIE_RECOMMENDATION.ID_MOVIE_RECOMMENDED = T2S_NB.ID_MOVIE
    WHERE (T_WC_TMDB_MOVIE_RECOMMENDATION.DELETED IS NULL OR T_WC_TMDB_MOVIE_RECOMMENDATION.DELETED = 0)
) ranked_movie_recommendation
"""
                        execute_sql_with_retry(
                            cp.connectioncp,
                            cursor2,
                            strsql,
                            "T2S_MOVIE_RECOMMENDATION build table population",
                        )
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_RECOMMENDATION_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_MOVIE_RECOMMENDATION TO T_WC_T2S_MOVIE_RECOMMENDATION_OLD,
    T_WC_T2S_MOVIE_RECOMMENDATION_BUILD TO T_WC_T2S_MOVIE_RECOMMENDATION
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_RECOMMENDATION_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 38:
                    #----------------------------------------------------
                    print("T2S_SERIE_SIMILAR processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SERIE_SIMILAR")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SERIE_SIMILAR from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentseriesimilarid","BUILD","Current serie-similar ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_SIMILAR_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SERIE_SIMILAR_BUILD LIKE T_WC_T2S_SERIE_SIMILAR"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SERIE_SIMILAR_BUILD (
    ID_SERIE, ID_SERIE_SIMILAR,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_SERIE, ID_SERIE_SIMILAR,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM (
    SELECT
        T_WC_TMDB_SERIE_SIMILAR.ID_SERIE,
        T_WC_TMDB_SERIE_SIMILAR.ID_SERIE_SIMILAR,
        T_WC_TMDB_SERIE_SIMILAR.DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY T_WC_TMDB_SERIE_SIMILAR.ID_SERIE
            ORDER BY T_WC_TMDB_SERIE_SIMILAR.DISPLAY_ORDER ASC,
                     T_WC_TMDB_SERIE_SIMILAR.ID_SERIE_SIMILAR ASC
        ) AS DISPLAY_ORDER,
        T_WC_TMDB_SERIE_SIMILAR.ID_CREATOR,
        T_WC_TMDB_SERIE_SIMILAR.DAT_CREAT,
        T_WC_TMDB_SERIE_SIMILAR.ID_OWNER,
        T_WC_TMDB_SERIE_SIMILAR.TIM_UPDATED,
        T_WC_TMDB_SERIE_SIMILAR.ID_USER_UPDATED
    FROM T_WC_TMDB_SERIE_SIMILAR
    INNER JOIN T_WC_T2S_SERIE AS T2S_SRC ON T_WC_TMDB_SERIE_SIMILAR.ID_SERIE = T2S_SRC.ID_SERIE
    INNER JOIN T_WC_T2S_SERIE AS T2S_NB ON T_WC_TMDB_SERIE_SIMILAR.ID_SERIE_SIMILAR = T2S_NB.ID_SERIE
    WHERE (T_WC_TMDB_SERIE_SIMILAR.DELETED IS NULL OR T_WC_TMDB_SERIE_SIMILAR.DELETED = 0)
) ranked_serie_similar
"""
                        execute_sql_with_retry(
                            cp.connectioncp,
                            cursor2,
                            strsql,
                            "T2S_SERIE_SIMILAR build table population",
                        )
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_SIMILAR_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SERIE_SIMILAR TO T_WC_T2S_SERIE_SIMILAR_OLD,
    T_WC_T2S_SERIE_SIMILAR_BUILD TO T_WC_T2S_SERIE_SIMILAR
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_SIMILAR_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 39:
                    #----------------------------------------------------
                    print("T2S_SERIE_RECOMMENDATION processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SERIE_RECOMMENDATION")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SERIE_RECOMMENDATION from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentserierecommendationid","BUILD","Current serie-recommendation ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_RECOMMENDATION_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SERIE_RECOMMENDATION_BUILD LIKE T_WC_T2S_SERIE_RECOMMENDATION"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SERIE_RECOMMENDATION_BUILD (
    ID_SERIE, ID_SERIE_RECOMMENDED,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_SERIE, ID_SERIE_RECOMMENDED,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM (
    SELECT
        T_WC_TMDB_SERIE_RECOMMENDATION.ID_SERIE,
        T_WC_TMDB_SERIE_RECOMMENDATION.ID_SERIE_RECOMMENDED,
        T_WC_TMDB_SERIE_RECOMMENDATION.DELETED,
        ROW_NUMBER() OVER (
            PARTITION BY T_WC_TMDB_SERIE_RECOMMENDATION.ID_SERIE
            ORDER BY T_WC_TMDB_SERIE_RECOMMENDATION.DISPLAY_ORDER ASC,
                     T_WC_TMDB_SERIE_RECOMMENDATION.ID_SERIE_RECOMMENDED ASC
        ) AS DISPLAY_ORDER,
        T_WC_TMDB_SERIE_RECOMMENDATION.ID_CREATOR,
        T_WC_TMDB_SERIE_RECOMMENDATION.DAT_CREAT,
        T_WC_TMDB_SERIE_RECOMMENDATION.ID_OWNER,
        T_WC_TMDB_SERIE_RECOMMENDATION.TIM_UPDATED,
        T_WC_TMDB_SERIE_RECOMMENDATION.ID_USER_UPDATED
    FROM T_WC_TMDB_SERIE_RECOMMENDATION
    INNER JOIN T_WC_T2S_SERIE AS T2S_SRC ON T_WC_TMDB_SERIE_RECOMMENDATION.ID_SERIE = T2S_SRC.ID_SERIE
    INNER JOIN T_WC_T2S_SERIE AS T2S_NB ON T_WC_TMDB_SERIE_RECOMMENDATION.ID_SERIE_RECOMMENDED = T2S_NB.ID_SERIE
    WHERE (T_WC_TMDB_SERIE_RECOMMENDATION.DELETED IS NULL OR T_WC_TMDB_SERIE_RECOMMENDATION.DELETED = 0)
) ranked_serie_recommendation
"""
                        execute_sql_with_retry(
                            cp.connectioncp,
                            cursor2,
                            strsql,
                            "T2S_SERIE_RECOMMENDATION build table population",
                        )
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_RECOMMENDATION_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SERIE_RECOMMENDATION TO T_WC_T2S_SERIE_RECOMMENDATION_OLD,
    T_WC_T2S_SERIE_RECOMMENDATION_BUILD TO T_WC_T2S_SERIE_RECOMMENDATION
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_RECOMMENDATION_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 13:
                    #----------------------------------------------------
                    print("T2S_MOVIE_COMPANY processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_MOVIE_COMPANY")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_MOVIE_COMPANY from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentmoviecompanyid","BUILD","Current movie-company ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_COMPANY_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_MOVIE_COMPANY_BUILD LIKE T_WC_T2S_MOVIE_COMPANY"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_MOVIE_COMPANY_BUILD (
    ID_ROW, ID_MOVIE, ID_COMPANY,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_MOVIE, ID_COMPANY,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_MOVIE_COMPANY
WHERE ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
  AND ID_COMPANY IN (SELECT ID_COMPANY FROM T_WC_T2S_COMPANY)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_COMPANY_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_MOVIE_COMPANY TO T_WC_T2S_MOVIE_COMPANY_OLD,
    T_WC_T2S_MOVIE_COMPANY_BUILD TO T_WC_T2S_MOVIE_COMPANY
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_COMPANY_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 14:
                    #----------------------------------------------------
                    print("T2S_SERIE_COMPANY processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SERIE_COMPANY")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SERIE_COMPANY from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentseriecompanyid","BUILD","Current serie-company ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_COMPANY_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SERIE_COMPANY_BUILD LIKE T_WC_T2S_SERIE_COMPANY"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SERIE_COMPANY_BUILD (
    ID_ROW, ID_SERIE, ID_COMPANY,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_SERIE, ID_COMPANY,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_SERIE_COMPANY
WHERE ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
  AND ID_COMPANY IN (SELECT ID_COMPANY FROM T_WC_T2S_COMPANY)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_COMPANY_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SERIE_COMPANY TO T_WC_T2S_SERIE_COMPANY_OLD,
    T_WC_T2S_SERIE_COMPANY_BUILD TO T_WC_T2S_SERIE_COMPANY
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_COMPANY_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 15:
                    #----------------------------------------------------
                    print("T2S_SERIE_NETWORK processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SERIE_NETWORK")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SERIE_NETWORK from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentserienetworkid","BUILD","Current serie-network ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_NETWORK_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SERIE_NETWORK_BUILD LIKE T_WC_T2S_SERIE_NETWORK"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SERIE_NETWORK_BUILD (
    ID_ROW, ID_SERIE, ID_NETWORK,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_SERIE, ID_NETWORK,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_SERIE_NETWORK
WHERE ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
  AND ID_NETWORK IN (SELECT ID_NETWORK FROM T_WC_T2S_NETWORK)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_NETWORK_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SERIE_NETWORK TO T_WC_T2S_SERIE_NETWORK_OLD,
    T_WC_T2S_SERIE_NETWORK_BUILD TO T_WC_T2S_SERIE_NETWORK
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_NETWORK_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 16:
                    #----------------------------------------------------
                    print("T2S_MOVIE_PRODUCTION_COUNTRY processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_MOVIE_PRODUCTION_COUNTRY from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentmoviecountryid","BUILD","Current movie production country ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_PRODUCTION_COUNTRY_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_MOVIE_PRODUCTION_COUNTRY_BUILD LIKE T_WC_T2S_MOVIE_PRODUCTION_COUNTRY"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_MOVIE_PRODUCTION_COUNTRY_BUILD (
    ID_ROW, ID_MOVIE, COUNTRY_CODE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_MOVIE, COUNTRY_CODE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY
WHERE ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_PRODUCTION_COUNTRY_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_MOVIE_PRODUCTION_COUNTRY TO T_WC_T2S_MOVIE_PRODUCTION_COUNTRY_OLD,
    T_WC_T2S_MOVIE_PRODUCTION_COUNTRY_BUILD TO T_WC_T2S_MOVIE_PRODUCTION_COUNTRY
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_PRODUCTION_COUNTRY_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 17:
                    #----------------------------------------------------
                    print("T2S_SERIE_PRODUCTION_COUNTRY processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SERIE_PRODUCTION_COUNTRY")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SERIE_PRODUCTION_COUNTRY from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentseriedcountryid","BUILD","Current serie production country ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_PRODUCTION_COUNTRY_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SERIE_PRODUCTION_COUNTRY_BUILD LIKE T_WC_T2S_SERIE_PRODUCTION_COUNTRY"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SERIE_PRODUCTION_COUNTRY_BUILD (
    ID_ROW, ID_SERIE, COUNTRY_CODE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_SERIE, COUNTRY_CODE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_SERIE_PRODUCTION_COUNTRY
WHERE ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_PRODUCTION_COUNTRY_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SERIE_PRODUCTION_COUNTRY TO T_WC_T2S_SERIE_PRODUCTION_COUNTRY_OLD,
    T_WC_T2S_SERIE_PRODUCTION_COUNTRY_BUILD TO T_WC_T2S_SERIE_PRODUCTION_COUNTRY
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_PRODUCTION_COUNTRY_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 18:
                    #----------------------------------------------------
                    print("T2S_MOVIE_SPOKEN_LANGUAGE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_MOVIE_SPOKEN_LANGUAGE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_MOVIE_SPOKEN_LANGUAGE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentmoviespokenlangid","BUILD","Current movie spoken language ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_SPOKEN_LANGUAGE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_MOVIE_SPOKEN_LANGUAGE_BUILD LIKE T_WC_T2S_MOVIE_SPOKEN_LANGUAGE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_MOVIE_SPOKEN_LANGUAGE_BUILD (
    ID_ROW, ID_MOVIE, SPOKEN_LANGUAGE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_MOVIE, SPOKEN_LANGUAGE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_MOVIE_SPOKEN_LANGUAGE
WHERE ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_SPOKEN_LANGUAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_MOVIE_SPOKEN_LANGUAGE TO T_WC_T2S_MOVIE_SPOKEN_LANGUAGE_OLD,
    T_WC_T2S_MOVIE_SPOKEN_LANGUAGE_BUILD TO T_WC_T2S_MOVIE_SPOKEN_LANGUAGE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_SPOKEN_LANGUAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 19:
                    #----------------------------------------------------
                    print("T2S_SERIE_SPOKEN_LANGUAGE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SERIE_SPOKEN_LANGUAGE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SERIE_SPOKEN_LANGUAGE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentseriespokenlangid","BUILD","Current serie spoken language ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_SPOKEN_LANGUAGE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SERIE_SPOKEN_LANGUAGE_BUILD LIKE T_WC_T2S_SERIE_SPOKEN_LANGUAGE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SERIE_SPOKEN_LANGUAGE_BUILD (
    ID_ROW, ID_SERIE, SPOKEN_LANGUAGE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_SERIE, SPOKEN_LANGUAGE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_SERIE_SPOKEN_LANGUAGE
WHERE ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_SPOKEN_LANGUAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SERIE_SPOKEN_LANGUAGE TO T_WC_T2S_SERIE_SPOKEN_LANGUAGE_OLD,
    T_WC_T2S_SERIE_SPOKEN_LANGUAGE_BUILD TO T_WC_T2S_SERIE_SPOKEN_LANGUAGE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_SPOKEN_LANGUAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 20:
                    #----------------------------------------------------
                    print("T2S_COMPANY_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_COMPANY_IMAGE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_COMPANY_IMAGE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentcompanyimageid","BUILD","Current company image ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_COMPANY_IMAGE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_COMPANY_IMAGE_BUILD LIKE T_WC_T2S_COMPANY_IMAGE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_COMPANY_IMAGE_BUILD (
    ID_ROW, ID_COMPANY,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
)
SELECT
    ID_ROW, ID_COMPANY,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
FROM T_WC_TMDB_COMPANY_IMAGE
WHERE ID_COMPANY IN (SELECT ID_COMPANY FROM T_WC_T2S_COMPANY)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_COMPANY_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_COMPANY_IMAGE TO T_WC_T2S_COMPANY_IMAGE_OLD,
    T_WC_T2S_COMPANY_IMAGE_BUILD TO T_WC_T2S_COMPANY_IMAGE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_COMPANY_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 21:
                    #----------------------------------------------------
                    print("T2S_MOVIE_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_MOVIE_IMAGE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_MOVIE_IMAGE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovieimageid","BUILD","Current movie image ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_IMAGE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_MOVIE_IMAGE_BUILD LIKE T_WC_T2S_MOVIE_IMAGE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        # Get the maximum ID_MOVIE value to drive chunked loading
                        cursor.execute("SELECT MAX(ID_MOVIE) as max_id FROM T_WC_TMDB_MOVIE_IMAGE")
                        result = cursor.fetchone()
                        lngmovierangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_MOVIE in T_WC_TMDB_MOVIE_IMAGE: {lngmovierangemax}")

                        # Process in chunks by ID_MOVIE so each transaction stays small;
                        # a KILL then only rolls back the current chunk, not the whole load.
                        lngchunksize = 500
                        lngtotalinserted = 0

                        for lngmovierangestart in range(1, lngmovierangemax + 1, lngchunksize):
                            lngmovierangeend = min(lngmovierangestart + lngchunksize - 1, lngmovierangemax)
                            print(f"Processing T_WC_T2S_MOVIE_IMAGE_BUILD for ID_MOVIE {lngmovierangestart} to {lngmovierangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovieimageid",str(lngmovierangestart),"Current movie image ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_MOVIE_IMAGE_BUILD (
    ID_ROW, ID_MOVIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
)
SELECT
    mi.ID_ROW, mi.ID_MOVIE,
    mi.DELETED, mi.DISPLAY_ORDER,
    mi.ID_CREATOR, mi.DAT_CREAT, mi.ID_OWNER, mi.TIM_UPDATED, mi.ID_USER_UPDATED,
    mi.TYPE_IMAGE, mi.LANG, mi.IMAGE_PATH, mi.ASPECT_RATIO, mi.WIDTH, mi.HEIGHT, mi.VOTE_AVERAGE, mi.VOTE_COUNT
FROM T_WC_TMDB_MOVIE_IMAGE mi
INNER JOIN T_WC_T2S_MOVIE m ON m.ID_MOVIE = mi.ID_MOVIE
WHERE mi.ID_MOVIE >= {lngmovierangestart} AND mi.ID_MOVIE <= {lngmovierangeend}
"""
                            cursor2.execute(strsql)
                            lngrowsinserted = cursor2.rowcount if cursor2.rowcount and cursor2.rowcount > 0 else 0
                            cp.connectioncp.commit()
                            lngtotalinserted += lngrowsinserted

                        print(f"Inserted {lngtotalinserted} rows into T_WC_T2S_MOVIE_IMAGE_BUILD")
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_MOVIE_IMAGE TO T_WC_T2S_MOVIE_IMAGE_OLD,
    T_WC_T2S_MOVIE_IMAGE_BUILD TO T_WC_T2S_MOVIE_IMAGE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 22:
                    #----------------------------------------------------
                    print("T2S_NETWORK_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_NETWORK_IMAGE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_NETWORK_IMAGE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentnetworkimageid","BUILD","Current network image ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_NETWORK_IMAGE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_NETWORK_IMAGE_BUILD LIKE T_WC_T2S_NETWORK_IMAGE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_NETWORK_IMAGE_BUILD (
    ID_ROW, ID_NETWORK,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
)
SELECT
    ID_ROW, ID_NETWORK,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
FROM T_WC_TMDB_NETWORK_IMAGE
WHERE ID_NETWORK IN (SELECT ID_NETWORK FROM T_WC_T2S_NETWORK)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_NETWORK_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_NETWORK_IMAGE TO T_WC_T2S_NETWORK_IMAGE_OLD,
    T_WC_T2S_NETWORK_IMAGE_BUILD TO T_WC_T2S_NETWORK_IMAGE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_NETWORK_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 23:
                    #----------------------------------------------------
                    print("T2S_PERSON_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_PERSON_IMAGE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_PERSON_IMAGE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentpersonimageid","BUILD","Current person image ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_PERSON_IMAGE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_PERSON_IMAGE_BUILD LIKE T_WC_T2S_PERSON_IMAGE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_PERSON_IMAGE_BUILD (
    ID_ROW, ID_PERSON,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
)
SELECT
    ID_ROW, ID_PERSON,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
FROM T_WC_TMDB_PERSON_IMAGE
WHERE ID_PERSON IN (SELECT ID_PERSON FROM T_WC_T2S_PERSON)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_PERSON_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_PERSON_IMAGE TO T_WC_T2S_PERSON_IMAGE_OLD,
    T_WC_T2S_PERSON_IMAGE_BUILD TO T_WC_T2S_PERSON_IMAGE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_PERSON_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 24:
                    #----------------------------------------------------
                    print("T2S_SERIE_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SERIE_IMAGE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SERIE_IMAGE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentserieimageid","BUILD","Current serie image ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_IMAGE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SERIE_IMAGE_BUILD LIKE T_WC_T2S_SERIE_IMAGE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SERIE_IMAGE_BUILD (
    ID_ROW, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
)
SELECT
    ID_ROW, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
FROM T_WC_TMDB_SERIE_IMAGE
WHERE ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SERIE_IMAGE TO T_WC_T2S_SERIE_IMAGE_OLD,
    T_WC_T2S_SERIE_IMAGE_BUILD TO T_WC_T2S_SERIE_IMAGE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 25:
                    #----------------------------------------------------
                    print("T2S_MOVIE_VIDEO processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_MOVIE_VIDEO")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_MOVIE_VIDEO from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovievideoid","BUILD","Current movie video ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_VIDEO_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_MOVIE_VIDEO_BUILD LIKE T_WC_T2S_MOVIE_VIDEO"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_MOVIE_VIDEO_BUILD (
    ID_ROW, ID_MOVIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    LANG, COUNTRY_CODE, VIDEO_KEY, VIDEO_NAME, VIDEO_SITE, VIDEO_TYPE,
    QUALITY, QUALITY_TEXT, DAT_PUBLISHED, ID_CREDIT, OFFICIAL
)
SELECT
    ID_ROW, ID_MOVIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    LANG, COUNTRY_CODE, VIDEO_KEY, VIDEO_NAME, VIDEO_SITE, VIDEO_TYPE,
    QUALITY, QUALITY_TEXT, DAT_PUBLISHED, ID_CREDIT, OFFICIAL
FROM T_WC_TMDB_MOVIE_VIDEO
WHERE ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_VIDEO_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_MOVIE_VIDEO TO T_WC_T2S_MOVIE_VIDEO_OLD,
    T_WC_T2S_MOVIE_VIDEO_BUILD TO T_WC_T2S_MOVIE_VIDEO
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_MOVIE_VIDEO_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 26:
                    #----------------------------------------------------
                    print("T2S_SERIE_VIDEO processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SERIE_VIDEO")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SERIE_VIDEO from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentserievideoid","BUILD","Current serie video ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_VIDEO_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SERIE_VIDEO_BUILD LIKE T_WC_T2S_SERIE_VIDEO"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SERIE_VIDEO_BUILD (
    ID_ROW, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    LANG, COUNTRY_CODE, VIDEO_KEY, VIDEO_NAME, VIDEO_SITE, VIDEO_TYPE,
    QUALITY, QUALITY_TEXT, DAT_PUBLISHED, ID_CREDIT, OFFICIAL
)
SELECT
    ID_ROW, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    LANG, COUNTRY_CODE, VIDEO_KEY, VIDEO_NAME, VIDEO_SITE, VIDEO_TYPE,
    QUALITY, QUALITY_TEXT, DAT_PUBLISHED, ID_CREDIT, OFFICIAL
FROM T_WC_TMDB_SERIE_VIDEO
WHERE ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_VIDEO_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SERIE_VIDEO TO T_WC_T2S_SERIE_VIDEO_OLD,
    T_WC_T2S_SERIE_VIDEO_BUILD TO T_WC_T2S_SERIE_VIDEO
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SERIE_VIDEO_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 27:
                    #----------------------------------------------------
                    print("T2S_SEASON processing")
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from TMDB_SEASON to T2S_SEASON","Current sub process in the TMDb database season preprocess",0)

                        # --- Incremental watermark -----------------------------------------------
                        # Re-copy seasons whose own source row changed since the last successful run,
                        # PLUS seasons whose parent serie changed: a parent serie newly qualifying for
                        # T2S bumps its source TIM_UPDATED but not the season's, so both must be checked
                        # to stay correct. The stale-delete and enrichment passes run over the FULL
                        # table every run regardless. A look-back buffer absorbs clock skew.
                        lngt2sseasonlookbackminutes = 60
                        strrunstart = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                        strlastrun = cp.f_getservervariable("strtmdbmoviepreprocesst2sseasonlastrun", 0)
                        strincrementalfilter = ""
                        if strlastrun:
                            strwatermark = "DATE_SUB('" + strlastrun + "', INTERVAL " + str(lngt2sseasonlookbackminutes) + " MINUTE)"
                            strincrementalfilter = (
                                "AND ( s.TIM_UPDATED >= " + strwatermark + " "
                                "OR s.ID_SERIE IN (SELECT ID_SERIE FROM T_WC_TMDB_SERIE WHERE TIM_UPDATED >= " + strwatermark + ") ) "
                            )
                            print(f"Incremental run: seasons changed since {strlastrun} (minus {lngt2sseasonlookbackminutes} min buffer)")
                        else:
                            print("First run (no watermark): full scan of all qualifying seasons")

                        # Precompute the IMDb global weighted-rating average ONCE (was recomputed via a
                        # full-table CROSS JOIN subquery on every chunk).
                        cursor.execute("SELECT AVG(averageRating) AS C FROM T_WC_IMDB_MOVIE_RATING_IMPORT WHERE averageRating IS NOT NULL AND numVotes > 0")
                        dblavgrating = cursor.fetchone()['C']
                        stravgrating = str(dblavgrating) if dblavgrating is not None else "NULL"

                        # Get the maximum ID_SEASON value from the database
                        cursor.execute("SELECT MAX(ID_SEASON) as max_id FROM T_WC_TMDB_SEASON")
                        result = cursor.fetchone()
                        lngseasonrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_SEASON in database: {lngseasonrangemax}")

                        # Base copy in chunks (incremental runs touch few rows per chunk)
                        lngchunksize = 5000
                        for lngseasonrangestart in range(1, lngseasonrangemax + 1, lngchunksize):
                            lngseasonrangeend = min(lngseasonrangestart + lngchunksize - 1, lngseasonrangemax)
                            print(f"Processing T2S_SEASON rows from ID {lngseasonrangestart} to {lngseasonrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentseasonid",str(lngseasonrangestart),"Current season ID in the TMDb database preprocess",0)

                            strsqlseasons = f"""
INSERT INTO T_WC_T2S_SEASON (
    ID_SEASON, ID_SERIE, SEASON_NUMBER, SEASON_TITLE, OVERVIEW,
    AIR_YEAR, AIR_MONTH, AIR_DAY, DAT_AIR,
    POSTER_PATH, EPISODE_COUNT, VOTE_AVERAGE,
    ID_IMDB, ID_WIKIDATA, ID_TVDB,
    DELETED, DISPLAY_ORDER, ID_CREATOR, DAT_CREAT, ID_OWNER,
    TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    s.ID_SEASON, s.ID_SERIE, s.SEASON_NUMBER, s.TITLE, s.OVERVIEW,
    s.AIR_YEAR, s.AIR_MONTH, s.AIR_DAY, s.DAT_AIR,
    s.POSTER_PATH, s.EPISODE_COUNT, s.VOTE_AVERAGE,
    s.ID_IMDB, s.ID_WIKIDATA, s.ID_TVDB,
    s.DELETED, s.DISPLAY_ORDER, s.ID_CREATOR, s.DAT_CREAT, s.ID_OWNER,
    s.TIM_UPDATED, s.ID_USER_UPDATED
FROM T_WC_TMDB_SEASON s
INNER JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = s.ID_SERIE
WHERE s.ID_SEASON >= {lngseasonrangestart} AND s.ID_SEASON <= {lngseasonrangeend}
{strincrementalfilter}ON DUPLICATE KEY UPDATE
    ID_SERIE = VALUES(ID_SERIE),
    SEASON_NUMBER = VALUES(SEASON_NUMBER),
    SEASON_TITLE = VALUES(SEASON_TITLE),
    OVERVIEW = VALUES(OVERVIEW),
    AIR_YEAR = VALUES(AIR_YEAR),
    AIR_MONTH = VALUES(AIR_MONTH),
    AIR_DAY = VALUES(AIR_DAY),
    DAT_AIR = VALUES(DAT_AIR),
    POSTER_PATH = VALUES(POSTER_PATH),
    EPISODE_COUNT = VALUES(EPISODE_COUNT),
    VOTE_AVERAGE = VALUES(VOTE_AVERAGE),
    ID_IMDB = VALUES(ID_IMDB),
    ID_WIKIDATA = VALUES(ID_WIKIDATA),
    ID_TVDB = VALUES(ID_TVDB),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsqlseasons)
                            cp.connectioncp.commit()

                        # ---- Stale delete: single full-table anti-join (full coverage) ----------
                        # Removes seasons gone from source, or whose parent serie is no longer in T2S.
                        # Uses the source season's parent id (authoritative) like the original per-chunk
                        # delete, but in one pass independent of the incremental change-set.
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Stale-delete T2S_SEASON","Current sub process in the TMDb database season preprocess",0)
                        strsqlseasonsdelete = """
DELETE t2s FROM T_WC_T2S_SEASON t2s
LEFT JOIN T_WC_TMDB_SEASON src ON src.ID_SEASON = t2s.ID_SEASON
LEFT JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = src.ID_SERIE
WHERE src.ID_SEASON IS NULL OR se.ID_SERIE IS NULL """
                        cursor2.execute(strsqlseasonsdelete)
                        cp.connectioncp.commit()

                        # ---- Enrichment: full-table set-based passes, ONCE (were per-chunk) ------
                        # Run over the whole table every run because the IMDb rating source changes
                        # independently of a season's TIM_UPDATED.
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Enrich T2S_SEASON (IMDb ratings)","Current sub process in the TMDb database season preprocess",0)
                        strsqlseasons = """
UPDATE T_WC_T2S_SEASON t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb
    ON t2s.ID_IMDB = imdb.tconst
SET t2s.IMDB_RATING = imdb.averageRating
WHERE t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL """
                        cursor2.execute(strsqlseasons)
                        cp.connectioncp.commit()

                        strsqlseasons = f"""
UPDATE T_WC_T2S_SEASON t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb
    ON t2s.ID_IMDB = imdb.tconst
SET t2s.IMDB_RATING_WEIGHTED =
    ((imdb.numVotes / (imdb.numVotes + {lngimdbweightedratingm})) * imdb.averageRating) +
    (({lngimdbweightedratingm} / (imdb.numVotes + {lngimdbweightedratingm})) * {stravgrating})
WHERE t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL
    AND imdb.numVotes > 0 """
                        cursor2.execute(strsqlseasons)
                        cp.connectioncp.commit()

                        # Persist the watermark only after a successful run.
                        cp.f_setservervariable("strtmdbmoviepreprocesst2sseasonlastrun", strrunstart, "Start datetime of the last successful T2S_SEASON run; incremental watermark on T_WC_TMDB_SEASON.TIM_UPDATED", 0)

                    print("T2S_SEASON processing completed. ")

                elif intindex == 28:
                    #----------------------------------------------------
                    print("T2S_EPISODE processing")
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from TMDB_EPISODE to T2S_EPISODE","Current sub process in the TMDb database episode preprocess",0)

                        # --- Incremental watermark -----------------------------------------------
                        # Re-copy episodes whose own source row changed since the last successful run,
                        # PLUS episodes whose parent serie or season changed: a parent newly qualifying
                        # for T2S bumps its source TIM_UPDATED but not the episode's, so all three must
                        # be checked to stay correct. The stale-delete and enrichment passes run over
                        # the FULL table every run regardless. A look-back buffer absorbs clock skew.
                        lngt2sepisodelookbackminutes = 60
                        strrunstart = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                        strlastrun = cp.f_getservervariable("strtmdbmoviepreprocesst2sepisodelastrun", 0)
                        strincrementalfilter = ""
                        if strlastrun:
                            strwatermark = "DATE_SUB('" + strlastrun + "', INTERVAL " + str(lngt2sepisodelookbackminutes) + " MINUTE)"
                            strincrementalfilter = (
                                "AND ( e.TIM_UPDATED >= " + strwatermark + " "
                                "OR e.ID_SERIE IN (SELECT ID_SERIE FROM T_WC_TMDB_SERIE WHERE TIM_UPDATED >= " + strwatermark + ") "
                                "OR e.ID_SEASON IN (SELECT ID_SEASON FROM T_WC_TMDB_SEASON WHERE TIM_UPDATED >= " + strwatermark + ") ) "
                            )
                            print(f"Incremental run: episodes changed since {strlastrun} (minus {lngt2sepisodelookbackminutes} min buffer)")
                        else:
                            print("First run (no watermark): full scan of all qualifying episodes")

                        # Precompute the IMDb global weighted-rating average ONCE (previously recomputed
                        # via a full-table CROSS JOIN subquery on every chunk).
                        cursor.execute("SELECT AVG(averageRating) AS C FROM T_WC_IMDB_MOVIE_RATING_IMPORT WHERE averageRating IS NOT NULL AND numVotes > 0")
                        dblavgrating = cursor.fetchone()['C']
                        stravgrating = str(dblavgrating) if dblavgrating is not None else "NULL"

                        # TMDB-MOVIE-PREPROCESS-033: IMDB_VOTES carries the IMDb vote count next to
                        # the rating. IMDB_RATING alone is not interpretable on an episode: a 9.3 on
                        # 43000 votes and a 9.3 on 12 votes are not the same claim, and consumers
                        # (API / front) need the count to decide what to display and to prefer the
                        # IMDb figure over the much thinner TMDb VOTE_COUNT. Idempotent, so the
                        # process stays safe to re-run and self-installs on a fresh database.
                        cursor2.execute("ALTER TABLE T_WC_T2S_EPISODE ADD COLUMN IF NOT EXISTS IMDB_VOTES INT DEFAULT NULL AFTER IMDB_RATING_WEIGHTED")
                        cp.connectioncp.commit()

                        # Get the maximum ID_EPISODE value from the database
                        cursor.execute("SELECT MAX(ID_EPISODE) as max_id FROM T_WC_TMDB_EPISODE")
                        result = cursor.fetchone()
                        lngepisoderangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_EPISODE in database: {lngepisoderangemax}")

                        # Base copy in chunks (incremental runs touch few rows per chunk)
                        lngchunksize = 5000
                        for lngepisoderangestart in range(1, lngepisoderangemax + 1, lngchunksize):
                            lngepisoderangeend = min(lngepisoderangestart + lngchunksize - 1, lngepisoderangemax)
                            print(f"Processing T2S_EPISODE rows from ID {lngepisoderangestart} to {lngepisoderangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentepisodeid",str(lngepisoderangestart),"Current episode ID in the TMDb database preprocess",0)

                            strsqlepisodes = f"""
INSERT INTO T_WC_T2S_EPISODE (
    ID_EPISODE, ID_SERIE, ID_SEASON, SEASON_NUMBER, EPISODE_NUMBER,
    EPISODE_TITLE, OVERVIEW,
    AIR_YEAR, AIR_MONTH, AIR_DAY, DAT_AIR,
    RUNTIME, PRODUCTION_CODE, EPISODE_TYPE, STILL_PATH,
    VOTE_AVERAGE, VOTE_COUNT,
    ID_IMDB, ID_WIKIDATA, ID_TVDB,
    DELETED, DISPLAY_ORDER, ID_CREATOR, DAT_CREAT, ID_OWNER,
    TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    e.ID_EPISODE, e.ID_SERIE, e.ID_SEASON, e.SEASON_NUMBER, e.EPISODE_NUMBER,
    e.TITLE, e.OVERVIEW,
    e.AIR_YEAR, e.AIR_MONTH, e.AIR_DAY, e.DAT_AIR,
    e.RUNTIME, e.PRODUCTION_CODE, e.EPISODE_TYPE, e.STILL_PATH,
    e.VOTE_AVERAGE, e.VOTE_COUNT,
    e.ID_IMDB, e.ID_WIKIDATA, e.ID_TVDB,
    e.DELETED, e.DISPLAY_ORDER, e.ID_CREATOR, e.DAT_CREAT, e.ID_OWNER,
    e.TIM_UPDATED, e.ID_USER_UPDATED
FROM T_WC_TMDB_EPISODE e
INNER JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = e.ID_SERIE
INNER JOIN T_WC_T2S_SEASON sea ON sea.ID_SEASON = e.ID_SEASON
WHERE e.ID_EPISODE >= {lngepisoderangestart} AND e.ID_EPISODE <= {lngepisoderangeend}
{strincrementalfilter}ON DUPLICATE KEY UPDATE
    ID_SERIE = VALUES(ID_SERIE),
    ID_SEASON = VALUES(ID_SEASON),
    SEASON_NUMBER = VALUES(SEASON_NUMBER),
    EPISODE_NUMBER = VALUES(EPISODE_NUMBER),
    EPISODE_TITLE = VALUES(EPISODE_TITLE),
    OVERVIEW = VALUES(OVERVIEW),
    AIR_YEAR = VALUES(AIR_YEAR),
    AIR_MONTH = VALUES(AIR_MONTH),
    AIR_DAY = VALUES(AIR_DAY),
    DAT_AIR = VALUES(DAT_AIR),
    RUNTIME = VALUES(RUNTIME),
    PRODUCTION_CODE = VALUES(PRODUCTION_CODE),
    EPISODE_TYPE = VALUES(EPISODE_TYPE),
    STILL_PATH = VALUES(STILL_PATH),
    VOTE_AVERAGE = VALUES(VOTE_AVERAGE),
    VOTE_COUNT = VALUES(VOTE_COUNT),
    ID_IMDB = VALUES(ID_IMDB),
    ID_WIKIDATA = VALUES(ID_WIKIDATA),
    ID_TVDB = VALUES(ID_TVDB),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsqlepisodes)
                            cp.connectioncp.commit()

                        # ---- Stale delete: single full-table anti-join (full coverage) ----------
                        # Removes episodes gone from source, or whose parent serie/season is no longer
                        # in T2S. Uses the source episode's parent ids (authoritative) like the original
                        # per-chunk delete, but in one pass independent of the incremental change-set.
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Stale-delete T2S_EPISODE","Current sub process in the TMDb database episode preprocess",0)
                        strsqlepisodesdelete = """
DELETE t2s FROM T_WC_T2S_EPISODE t2s
LEFT JOIN T_WC_TMDB_EPISODE src ON src.ID_EPISODE = t2s.ID_EPISODE
LEFT JOIN T_WC_T2S_SERIE se ON se.ID_SERIE = src.ID_SERIE
LEFT JOIN T_WC_T2S_SEASON sea ON sea.ID_SEASON = src.ID_SEASON
WHERE src.ID_EPISODE IS NULL OR se.ID_SERIE IS NULL OR sea.ID_SEASON IS NULL """
                        cursor2.execute(strsqlepisodesdelete)
                        cp.connectioncp.commit()

                        # ---- Enrichment: set-based passes over the WHOLE table, once per run -----
                        # Run over the whole table every run because the IMDb rating source changes
                        # independently of an episode's TIM_UPDATED.
                        # TMDB-MOVIE-PREPROCESS-033: the two passes are now chunked by ID_EPISODE
                        # range, same reasoning as -032 on Process 4. Coverage is unchanged (the loop
                        # sweeps every id, "full-table" != "one transaction"): only the transaction
                        # granularity changes, so a table in the millions of rows no longer holds a
                        # single long write lock on T_WC_T2S_EPISODE.
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Enrich T2S_EPISODE (IMDb ratings)","Current sub process in the TMDb database episode preprocess",0)
                        for lngepisoderangestart in range(1, lngepisoderangemax + 1, lngchunksize):
                            lngepisoderangeend = min(lngepisoderangestart + lngchunksize - 1, lngepisoderangemax)
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentepisodeid",str(lngepisoderangestart),"Current episode ID in the TMDb database preprocess",0)

                            # IMDb rating AND vote count in a single pass: same join, same predicate,
                            # so splitting them would scan the range twice for nothing.
                            strsqlepisodes = f"""
UPDATE T_WC_T2S_EPISODE t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb
    ON t2s.ID_IMDB = imdb.tconst
SET t2s.IMDB_RATING = imdb.averageRating,
    t2s.IMDB_VOTES = imdb.numVotes
WHERE t2s.ID_EPISODE BETWEEN {lngepisoderangestart} AND {lngepisoderangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL """
                            cursor2.execute(strsqlepisodes)
                            cp.connectioncp.commit()

                            strsqlepisodes = f"""
UPDATE T_WC_T2S_EPISODE t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb
    ON t2s.ID_IMDB = imdb.tconst
SET t2s.IMDB_RATING_WEIGHTED =
    ((imdb.numVotes / (imdb.numVotes + {lngimdbweightedratingm})) * imdb.averageRating) +
    (({lngimdbweightedratingm} / (imdb.numVotes + {lngimdbweightedratingm})) * {stravgrating})
WHERE t2s.ID_EPISODE BETWEEN {lngepisoderangestart} AND {lngepisoderangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL
    AND imdb.numVotes > 0 """
                            cursor2.execute(strsqlepisodes)
                            cp.connectioncp.commit()

                        # ---- Season rollup: derive the season's IMDb rating from its episodes ----
                        # TMDB-MOVIE-PREPROCESS-034. IMDb rates titles and episodes, never seasons,
                        # so a season rating has to be derived. Two columns, same meaning as
                        # everywhere else in the schema:
                        #   IMDB_RATING          = plain mean of the season's rated episodes. Answers
                        #                          "were these episodes good", which is what a season
                        #                          score is taken to mean. NOT weighted by votes: the
                        #                          opening episodes are always the most watched, so
                        #                          vote-weighting would systematically over-represent
                        #                          the start of a season.
                        #   IMDB_RATING_WEIGHTED = the SAME bayesian shrinkage used for movies, series
                        #                          and episodes, applied to that mean with the season's
                        #                          summed episode votes as the vote mass. Keeping the
                        #                          formula identical is the whole point: the column must
                        #                          mean one thing across every entity type, otherwise
                        #                          any cross-type sort compares unlike numbers.
                        # MUST run here, at the end of Process 28, not in Process 27: the scope dict
                        # executes 27 (T2S_SEASON) BEFORE 28, so rolling up there would average the
                        # PREVIOUS run's episode ratings and stay one run behind forever.
                        # Episodes with no rating are excluded, never counted as zero, so a season in
                        # flight is scored on the episodes actually aired and rated. Both aggregates
                        # use the same episode population, so the two columns always describe the same
                        # set of rows.
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Roll up IMDb season averages from episodes","Current sub process in the TMDb database episode preprocess",0)
                        cursor.execute("SELECT MAX(ID_SEASON) as max_id FROM T_WC_T2S_SEASON")
                        result = cursor.fetchone()
                        lngseasonrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Rolling up IMDb season averages up to ID_SEASON {lngseasonrangemax}")
                        for lngseasonrangestart in range(1, lngseasonrangemax + 1, lngchunksize):
                            lngseasonrangeend = min(lngseasonrangestart + lngchunksize - 1, lngseasonrangemax)
                            strsqlseasons = f"""
UPDATE T_WC_T2S_SEASON s
INNER JOIN (
    SELECT ID_SEASON,
           AVG(IMDB_RATING) AS EPISODES_AVG,
           SUM(IMDB_VOTES)  AS EPISODES_VOTES
    FROM T_WC_T2S_EPISODE
    WHERE ID_SEASON BETWEEN {lngseasonrangestart} AND {lngseasonrangeend}
        AND IMDB_RATING IS NOT NULL
        AND IMDB_VOTES IS NOT NULL
        AND IMDB_VOTES > 0
    GROUP BY ID_SEASON
) e ON e.ID_SEASON = s.ID_SEASON
SET s.IMDB_RATING = e.EPISODES_AVG,
    s.IMDB_RATING_WEIGHTED =
        ((e.EPISODES_VOTES / (e.EPISODES_VOTES + {lngimdbweightedratingm})) * e.EPISODES_AVG) +
        (({lngimdbweightedratingm} / (e.EPISODES_VOTES + {lngimdbweightedratingm})) * {stravgrating})
WHERE s.ID_SEASON BETWEEN {lngseasonrangestart} AND {lngseasonrangeend} """
                            cursor2.execute(strsqlseasons)
                            cp.connectioncp.commit()

                        # Persist the watermark only after a successful run.
                        cp.f_setservervariable("strtmdbmoviepreprocesst2sepisodelastrun", strrunstart, "Start datetime of the last successful T2S_EPISODE run; incremental watermark on T_WC_TMDB_EPISODE.TIM_UPDATED", 0)

                    print("T2S_EPISODE processing completed. ")

                elif intindex == 29:
                    #----------------------------------------------------
                    print("T2S_PERSON_SEASON processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_TMDB_PERSON_SEASON) as max_id FROM T_WC_TMDB_PERSON_SEASON")
                        result = cursor.fetchone()
                        lngpersonseasonrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_TMDB_PERSON_SEASON in database: {lngpersonseasonrangemax}")

                        lngchunksize = 1000
                        lngtotalprocessed = 0

                        for lngpersonseasonrangestart in range(1, lngpersonseasonrangemax + 1, lngchunksize):
                            lngpersonseasonrangeend = min(lngpersonseasonrangestart + lngchunksize - 1, lngpersonseasonrangemax)
                            print(f"Processing T2S_PERSON_SEASON rows from ID {lngpersonseasonrangestart} to {lngpersonseasonrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentpersonseasonid",str(lngpersonseasonrangestart),"Current person-season relation ID in the TMDb database preprocess",0)

                            strsqlpersonseasons = f"""
    INSERT INTO T_WC_T2S_PERSON_SEASON (
        ID_T2S_PERSON_SEASON, ID_PERSON, ID_SERIE, ID_SEASON, SEASON_NUMBER,
        ID_CREDIT, CREDIT_TYPE, CAST_CHARACTER, CREW_DEPARTMENT, CREW_JOB,
        TOTAL_EPISODE_COUNT, DISPLAY_ORDER
    )
    SELECT
        ID_TMDB_PERSON_SEASON, ID_PERSON, ID_SERIE, ID_SEASON, SEASON_NUMBER,
        ID_CREDIT, CREDIT_TYPE, CAST_CHARACTER, CREW_DEPARTMENT, CREW_JOB,
        TOTAL_EPISODE_COUNT, DISPLAY_ORDER
    FROM T_WC_TMDB_PERSON_SEASON
    WHERE ID_TMDB_PERSON_SEASON >= {lngpersonseasonrangestart} AND ID_TMDB_PERSON_SEASON <= {lngpersonseasonrangeend}
    AND ID_PERSON IN (SELECT ID_PERSON FROM T_WC_T2S_PERSON)
    AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
    AND ID_SEASON IN (SELECT ID_SEASON FROM T_WC_T2S_SEASON)
    ON DUPLICATE KEY UPDATE
        ID_PERSON = VALUES(ID_PERSON),
        ID_SERIE = VALUES(ID_SERIE),
        ID_SEASON = VALUES(ID_SEASON),
        SEASON_NUMBER = VALUES(SEASON_NUMBER),
        ID_CREDIT = VALUES(ID_CREDIT),
        CREDIT_TYPE = VALUES(CREDIT_TYPE),
        CAST_CHARACTER = VALUES(CAST_CHARACTER),
        CREW_DEPARTMENT = VALUES(CREW_DEPARTMENT),
        CREW_JOB = VALUES(CREW_JOB),
        TOTAL_EPISODE_COUNT = VALUES(TOTAL_EPISODE_COUNT),
        DISPLAY_ORDER = VALUES(DISPLAY_ORDER) """
                            cursor2.execute(strsqlpersonseasons)
                            cp.connectioncp.commit()

                            strsqlpersonseasonsdelete = f"""
    DELETE FROM T_WC_T2S_PERSON_SEASON
    WHERE ID_T2S_PERSON_SEASON >= {lngpersonseasonrangestart} AND ID_T2S_PERSON_SEASON <= {lngpersonseasonrangeend}
    AND ID_T2S_PERSON_SEASON NOT IN (
        SELECT ID_TMDB_PERSON_SEASON FROM T_WC_TMDB_PERSON_SEASON
        WHERE ID_TMDB_PERSON_SEASON >= {lngpersonseasonrangestart} AND ID_TMDB_PERSON_SEASON <= {lngpersonseasonrangeend}
        AND ID_PERSON IN (SELECT ID_PERSON FROM T_WC_T2S_PERSON)
        AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
        AND ID_SEASON IN (SELECT ID_SEASON FROM T_WC_T2S_SEASON)
    ) """
                            cursor2.execute(strsqlpersonseasonsdelete)
                            cp.connectioncp.commit()

                elif intindex == 31:
                    #----------------------------------------------------
                    print("T2S_PERSON_EPISODE processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_TMDB_PERSON_EPISODE) as max_id FROM T_WC_TMDB_PERSON_EPISODE")
                        result = cursor.fetchone()
                        lngpersonepisoderangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_TMDB_PERSON_EPISODE in database: {lngpersonepisoderangemax}")

                        lngchunksize = 1000
                        lngtotalprocessed = 0

                        for lngpersonepisoderangestart in range(1, lngpersonepisoderangemax + 1, lngchunksize):
                            lngpersonepisoderangeend = min(lngpersonepisoderangestart + lngchunksize - 1, lngpersonepisoderangemax)
                            print(f"Processing T2S_PERSON_EPISODE rows from ID {lngpersonepisoderangestart} to {lngpersonepisoderangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentpersonepisodeid",str(lngpersonepisoderangestart),"Current person-episode relation ID in the TMDb database preprocess",0)

                            strsqlpersonepisodes = f"""
    INSERT INTO T_WC_T2S_PERSON_EPISODE (
        ID_T2S_PERSON_EPISODE, ID_PERSON, ID_SERIE, ID_SEASON, ID_EPISODE,
        SEASON_NUMBER, EPISODE_NUMBER,
        ID_CREDIT, CREDIT_TYPE, CAST_CHARACTER, CREW_DEPARTMENT, CREW_JOB,
        DISPLAY_ORDER
    )
    SELECT
        ID_TMDB_PERSON_EPISODE, ID_PERSON, ID_SERIE, ID_SEASON, ID_EPISODE,
        SEASON_NUMBER, EPISODE_NUMBER,
        ID_CREDIT, CREDIT_TYPE, CAST_CHARACTER, CREW_DEPARTMENT, CREW_JOB,
        DISPLAY_ORDER
    FROM T_WC_TMDB_PERSON_EPISODE
    WHERE ID_TMDB_PERSON_EPISODE >= {lngpersonepisoderangestart} AND ID_TMDB_PERSON_EPISODE <= {lngpersonepisoderangeend}
    AND ID_PERSON IN (SELECT ID_PERSON FROM T_WC_T2S_PERSON)
    AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
    AND ID_SEASON IN (SELECT ID_SEASON FROM T_WC_T2S_SEASON)
    AND ID_EPISODE IN (SELECT ID_EPISODE FROM T_WC_T2S_EPISODE)
    ON DUPLICATE KEY UPDATE
        ID_PERSON = VALUES(ID_PERSON),
        ID_SERIE = VALUES(ID_SERIE),
        ID_SEASON = VALUES(ID_SEASON),
        ID_EPISODE = VALUES(ID_EPISODE),
        SEASON_NUMBER = VALUES(SEASON_NUMBER),
        EPISODE_NUMBER = VALUES(EPISODE_NUMBER),
        ID_CREDIT = VALUES(ID_CREDIT),
        CREDIT_TYPE = VALUES(CREDIT_TYPE),
        CAST_CHARACTER = VALUES(CAST_CHARACTER),
        CREW_DEPARTMENT = VALUES(CREW_DEPARTMENT),
        CREW_JOB = VALUES(CREW_JOB),
        DISPLAY_ORDER = VALUES(DISPLAY_ORDER) """
                            cursor2.execute(strsqlpersonepisodes)
                            cp.connectioncp.commit()

                            strsqlpersonepisodesdelete = f"""
    DELETE FROM T_WC_T2S_PERSON_EPISODE
    WHERE ID_T2S_PERSON_EPISODE >= {lngpersonepisoderangestart} AND ID_T2S_PERSON_EPISODE <= {lngpersonepisoderangeend}
    AND ID_T2S_PERSON_EPISODE NOT IN (
        SELECT ID_TMDB_PERSON_EPISODE FROM T_WC_TMDB_PERSON_EPISODE
        WHERE ID_TMDB_PERSON_EPISODE >= {lngpersonepisoderangestart} AND ID_TMDB_PERSON_EPISODE <= {lngpersonepisoderangeend}
        AND ID_PERSON IN (SELECT ID_PERSON FROM T_WC_T2S_PERSON)
        AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
        AND ID_SEASON IN (SELECT ID_SEASON FROM T_WC_T2S_SEASON)
        AND ID_EPISODE IN (SELECT ID_EPISODE FROM T_WC_T2S_EPISODE)
    ) """
                            cursor2.execute(strsqlpersonepisodesdelete)
                            cp.connectioncp.commit()

                elif intindex == 32:
                    #----------------------------------------------------
                    print("T2S_SEASON_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SEASON_IMAGE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SEASON_IMAGE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentseasonimageid","BUILD","Current season image ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SEASON_IMAGE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SEASON_IMAGE_BUILD LIKE T_WC_T2S_SEASON_IMAGE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SEASON_IMAGE_BUILD (
    ID_ROW, ID_SEASON, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
)
SELECT
    ID_ROW, ID_SEASON, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
FROM T_WC_TMDB_SEASON_IMAGE
WHERE ID_SEASON IN (SELECT ID_SEASON FROM T_WC_T2S_SEASON)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SEASON_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SEASON_IMAGE TO T_WC_T2S_SEASON_IMAGE_OLD,
    T_WC_T2S_SEASON_IMAGE_BUILD TO T_WC_T2S_SEASON_IMAGE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SEASON_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 33:
                    #----------------------------------------------------
                    print("T2S_EPISODE_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_EPISODE_IMAGE")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_EPISODE_IMAGE from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentepisodeimageid","BUILD","Current episode image ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_EPISODE_IMAGE_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_EPISODE_IMAGE_BUILD LIKE T_WC_T2S_EPISODE_IMAGE"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_EPISODE_IMAGE_BUILD (
    ID_ROW, ID_EPISODE, ID_SEASON, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
)
SELECT
    ID_ROW, ID_EPISODE, ID_SEASON, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
FROM T_WC_TMDB_EPISODE_IMAGE
WHERE ID_EPISODE IN (SELECT ID_EPISODE FROM T_WC_T2S_EPISODE)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_EPISODE_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_EPISODE_IMAGE TO T_WC_T2S_EPISODE_IMAGE_OLD,
    T_WC_T2S_EPISODE_IMAGE_BUILD TO T_WC_T2S_EPISODE_IMAGE
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_EPISODE_IMAGE_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 34:
                    #----------------------------------------------------
                    print("T2S_SEASON_VIDEO processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_SEASON_VIDEO")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_SEASON_VIDEO from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentseasonvideoid","BUILD","Current season video ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SEASON_VIDEO_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_SEASON_VIDEO_BUILD LIKE T_WC_T2S_SEASON_VIDEO"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_SEASON_VIDEO_BUILD (
    ID_ROW, ID_SEASON, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    LANG, COUNTRY_CODE, VIDEO_KEY, VIDEO_NAME, VIDEO_SITE, VIDEO_TYPE,
    QUALITY, QUALITY_TEXT, DAT_PUBLISHED, ID_CREDIT, OFFICIAL
)
SELECT
    ID_ROW, ID_SEASON, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    LANG, COUNTRY_CODE, VIDEO_KEY, VIDEO_NAME, VIDEO_SITE, VIDEO_TYPE,
    QUALITY, QUALITY_TEXT, DAT_PUBLISHED, ID_CREDIT, OFFICIAL
FROM T_WC_TMDB_SEASON_VIDEO
WHERE ID_SEASON IN (SELECT ID_SEASON FROM T_WC_T2S_SEASON)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SEASON_VIDEO_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_SEASON_VIDEO TO T_WC_T2S_SEASON_VIDEO_OLD,
    T_WC_T2S_SEASON_VIDEO_BUILD TO T_WC_T2S_SEASON_VIDEO
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_SEASON_VIDEO_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 35:
                    #----------------------------------------------------
                    print("T2S_EPISODE_VIDEO processing")
                    if 1:
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_TMDB_EPISODE_VIDEO")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_EPISODE_VIDEO from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentepisodevideoid","BUILD","Current episode video ID in the TMDb database preprocess",0)
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_EPISODE_VIDEO_BUILD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "CREATE TABLE T_WC_T2S_EPISODE_VIDEO_BUILD LIKE T_WC_T2S_EPISODE_VIDEO"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
INSERT INTO T_WC_T2S_EPISODE_VIDEO_BUILD (
    ID_ROW, ID_EPISODE, ID_SEASON, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    LANG, COUNTRY_CODE, VIDEO_KEY, VIDEO_NAME, VIDEO_SITE, VIDEO_TYPE,
    QUALITY, QUALITY_TEXT, DAT_PUBLISHED, ID_CREDIT, OFFICIAL
)
SELECT
    ID_ROW, ID_EPISODE, ID_SEASON, ID_SERIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    LANG, COUNTRY_CODE, VIDEO_KEY, VIDEO_NAME, VIDEO_SITE, VIDEO_TYPE,
    QUALITY, QUALITY_TEXT, DAT_PUBLISHED, ID_CREDIT, OFFICIAL
FROM T_WC_TMDB_EPISODE_VIDEO
WHERE ID_EPISODE IN (SELECT ID_EPISODE FROM T_WC_T2S_EPISODE)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_EPISODE_VIDEO_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = """
RENAME TABLE
    T_WC_T2S_EPISODE_VIDEO TO T_WC_T2S_EPISODE_VIDEO_OLD,
    T_WC_T2S_EPISODE_VIDEO_BUILD TO T_WC_T2S_EPISODE_VIDEO
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                        strsql = "DROP TABLE IF EXISTS T_WC_T2S_EPISODE_VIDEO_OLD"
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()


                elif intindex == 40:
                    #----------------------------------------------------
                    print("T2S_ITEM processing")
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from WIKIDATA_ITEM to T2S_ITEM","Current sub process in the TMDb database movie preprocess",0)
                        cursor.execute("SELECT COUNT(*) as row_count FROM T_WC_WIKIDATA_ITEM_V1 WHERE LANG = 'en'")
                        result = cursor.fetchone()
                        lngrowcount = result['row_count'] if result['row_count'] is not None else 0
                        print(f"Rebuilding T_WC_T2S_ITEM from {lngrowcount} source rows")
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentitemid","BUILD","Current row ID in the TMDb database preprocess",0)
                        strsqlitems = "DROP TABLE IF EXISTS T_WC_T2S_ITEM_BUILD"
                        cursor2.execute(strsqlitems)
                        cp.connectioncp.commit()
                        strsqlitems = "CREATE TABLE T_WC_T2S_ITEM_BUILD LIKE T_WC_T2S_ITEM"
                        cursor2.execute(strsqlitems)
                        cp.connectioncp.commit()
                        strsqlitems = """
INSERT INTO T_WC_T2S_ITEM_BUILD (
    ID_ROW, ID_WIKIDATA, ITEM_LABEL, ALIASES, DESCRIPTION,
    WIKIPEDIA_IMAGE_PATH, INSTANCE_OF,
    DAT_CREAT, TIM_UPDATED,
    DELETED
)
SELECT
    v1.ID_ROW, v1.ID_WIKIDATA,
    -- WIKIDATA-CRAWLER-017 : texte EN pris dans V2 (LABELS_JSON / DESCRIPTIONS_JSON),
    -- V1 en dernier recours. LABEL_EN est intercale car il existe sur une partie des
    -- lignes ou le document JSON n'a pas de cle 'en'.
    COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON, '$.en')),
             NULLIF(v2.LABEL_EN, ''),
             v1.LABEL)                                            AS LABEL,
    v1.ALIASES,
    COALESCE(JSON_UNQUOTE(JSON_EXTRACT(v2.DESCRIPTIONS_JSON, '$.en')),
             NULLIF(v2.DESCRIPTION_EN, ''),
             v1.DESCRIPTION)                                      AS DESCRIPTION,
    v1.WIKIPEDIA_IMAGE_PATH, v1.INSTANCE_OF,
    v1.DAT_CREAT, v1.TIM_UPDATED,
    v1.DELETED
-- La POPULATION reste celle de V1, deliberement : V2 ne porte pas toutes les entites
-- que V1 connait, et partir de V2 supprimerait des lignes de T2S_ITEM. On ne change
-- donc que la SOURCE DES VALEURS, pas le perimetre. Inverser le sens de cette jointure
-- est le geste a faire le jour ou le gap d entites sera ferme, pas avant.
FROM T_WC_WIKIDATA_ITEM_V1 v1
LEFT JOIN T_WC_WIKIDATA_ITEM v2
    ON v2.ID_WIKIDATA = v1.ID_WIKIDATA
WHERE v1.LANG = 'en'
"""
                        cursor2.execute(strsqlitems)
                        cp.connectioncp.commit()

                        strsqlitems = """
-- WIKIDATA-CRAWLER-017 : le libelle FR vient de V2 (LABELS_JSON, un document par
-- entite portant toutes les langues), V1 (une ligne par langue) ne servant plus que
-- de repli, a retirer avec les tables V1.
--
-- LEFT JOIN et non INNER : l'ancienne requete ne touchait que les lignes ayant un
-- libellé FR en V1. Avec deux sources il faut atteindre les lignes servies par l'une
-- OU l'autre, d'ou les deux jointures gauches et le WHERE final, qui empeche
-- d'ecrire NULL sur une ligne qu'aucune des deux sources ne couvre. Meme regle de
-- non-effacement que pour les images : une source vide n'ecrase jamais une valeur.
UPDATE T_WC_T2S_ITEM_BUILD t2s
LEFT JOIN T_WC_WIKIDATA_ITEM v2
    ON v2.ID_WIKIDATA = t2s.ID_WIKIDATA
LEFT JOIN T_WC_WIKIDATA_ITEM_V1 t
    ON t.ID_WIKIDATA = t2s.ID_WIKIDATA
   AND t.LANG = 'fr'
SET t2s.ITEM_LABEL_FR = COALESCE(
        JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON, '$.fr')),
        t.LABEL)
WHERE COALESCE(
        JSON_UNQUOTE(JSON_EXTRACT(v2.LABELS_JSON, '$.fr')),
        t.LABEL) IS NOT NULL
"""
                        cursor2.execute(strsqlitems)
                        cp.connectioncp.commit()
                        strsqlitems = "DROP TABLE IF EXISTS T_WC_T2S_ITEM_OLD"
                        cursor2.execute(strsqlitems)
                        cp.connectioncp.commit()
                        strsqlitems = """
RENAME TABLE
    T_WC_T2S_ITEM TO T_WC_T2S_ITEM_OLD,
    T_WC_T2S_ITEM_BUILD TO T_WC_T2S_ITEM
"""
                        cursor2.execute(strsqlitems)
                        cp.connectioncp.commit()
                        strsqlitems = "DROP TABLE IF EXISTS T_WC_T2S_ITEM_OLD"
                        cursor2.execute(strsqlitems)
                        cp.connectioncp.commit()

                    print(f"T2S_ITEM processing completed. ")

                elif intindex == 48:
                    #----------------------------------------------------
                    print("TMDB_CHARACTER processing")
                    telcharacter = EntityTelemetry("character", 48, "character")
                    telcharacter.begin()
                    if 1:
                        def f_printsqlprocess48(strsql):
                            print(datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S"))
                            print(strsql)

                        cp.f_setservervariable(
                            "strtmdbmoviepreprocesscurrentsubprocess",
                            "Copying from T2S_PERSON_MOVIE/SERIE to TMDB_CHARACTER",
                            "Current sub process in the TMDb database movie preprocess",
                            0,
                        )
                        telcharacter.position(currentprocess="Building character source tables and junctions", increment=False)

                        # CAST_CHARACTER_KEY is pinned to utf8mb4_bin in T_WC_TMDB_CHARACTER /
                        # T_WC_T2S_CHARACTER (byte-exact unique key; see tmdb-front
                        # doc/db-collation/collation-migration-runbook.md). The temp source keys
                        # below MUST carry COLLATE utf8mb4_bin so the GROUP BY / unique index dedup
                        # byte-exactly (consistent with the pinned column) and the joins onto
                        # T_WC_TMDB_CHARACTER.CAST_CHARACTER_KEY don't raise #1267 (illegal mix of
                        # collations) now that the rest of the schema is utf8mb4_unicode_ci.
                        strsql = "DROP TEMPORARY TABLE IF EXISTS TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """CREATE TEMPORARY TABLE TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE AS
SELECT
    ID_MOVIE,
    ID_PERSON,
    CAST_CHARACTER,
    replace(trim(regexp_replace(lcase(regexp_replace(CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','') COLLATE utf8mb4_bin AS CAST_CHARACTER_KEY
FROM T_WC_T2S_PERSON_MOVIE
WHERE CREDIT_TYPE = 'cast'
  AND (DELETED = 0 OR DELETED IS NULL)
  AND CAST_CHARACTER IS NOT NULL
  AND CAST_CHARACTER <> ''
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE INDEX IDX_TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE_KEY ON TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE (CAST_CHARACTER_KEY)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE INDEX IDX_TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE_MOVIE_CHARACTER ON TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE (ID_MOVIE, CAST_CHARACTER_KEY(255))"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE INDEX IDX_TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE_PERSON_CHARACTER ON TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE (ID_PERSON, CAST_CHARACTER_KEY(255))"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "DROP TEMPORARY TABLE IF EXISTS TMP_WC_TMDB_CHARACTER_SOURCE_SERIE"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """CREATE TEMPORARY TABLE TMP_WC_TMDB_CHARACTER_SOURCE_SERIE AS
SELECT
    ID_SERIE,
    ID_PERSON,
    CAST_CHARACTER,
    replace(trim(regexp_replace(lcase(regexp_replace(CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','') COLLATE utf8mb4_bin AS CAST_CHARACTER_KEY
FROM T_WC_T2S_PERSON_SERIE
WHERE CREDIT_TYPE = 'cast'
  AND (DELETED = 0 OR DELETED IS NULL)
  AND CAST_CHARACTER IS NOT NULL
  AND CAST_CHARACTER <> ''
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE INDEX IDX_TMP_WC_TMDB_CHARACTER_SOURCE_SERIE_KEY ON TMP_WC_TMDB_CHARACTER_SOURCE_SERIE (CAST_CHARACTER_KEY)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE INDEX IDX_TMP_WC_TMDB_CHARACTER_SOURCE_SERIE_SERIE_CHARACTER ON TMP_WC_TMDB_CHARACTER_SOURCE_SERIE (ID_SERIE, CAST_CHARACTER_KEY(255))"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE INDEX IDX_TMP_WC_TMDB_CHARACTER_SOURCE_SERIE_PERSON_CHARACTER ON TMP_WC_TMDB_CHARACTER_SOURCE_SERIE (ID_PERSON, CAST_CHARACTER_KEY(255))"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "DROP TEMPORARY TABLE IF EXISTS TMP_WC_TMDB_CHARACTER_SOURCE_ALL"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """CREATE TEMPORARY TABLE TMP_WC_TMDB_CHARACTER_SOURCE_ALL AS
SELECT CAST_CHARACTER, CAST_CHARACTER_KEY
FROM TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE
UNION ALL
SELECT CAST_CHARACTER, CAST_CHARACTER_KEY
FROM TMP_WC_TMDB_CHARACTER_SOURCE_SERIE
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE INDEX IDX_TMP_WC_TMDB_CHARACTER_SOURCE_ALL_KEY ON TMP_WC_TMDB_CHARACTER_SOURCE_ALL (CAST_CHARACTER_KEY)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "DROP TEMPORARY TABLE IF EXISTS TMP_WC_TMDB_CHARACTER_KEYS"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """CREATE TEMPORARY TABLE TMP_WC_TMDB_CHARACTER_KEYS AS
SELECT
    MIN(CAST_CHARACTER) AS CAST_CHARACTER,
    CAST_CHARACTER_KEY
FROM TMP_WC_TMDB_CHARACTER_SOURCE_ALL
GROUP BY CAST_CHARACTER_KEY
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE UNIQUE INDEX UQ_TMP_WC_TMDB_CHARACTER_KEYS_KEY ON TMP_WC_TMDB_CHARACTER_KEYS (CAST_CHARACTER_KEY)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """INSERT INTO T_WC_TMDB_CHARACTER (CAST_CHARACTER, CAST_CHARACTER_KEY)
SELECT src.CAST_CHARACTER,
       src.CAST_CHARACTER_KEY
FROM TMP_WC_TMDB_CHARACTER_KEYS src
LEFT JOIN T_WC_TMDB_CHARACTER c
  ON c.CAST_CHARACTER_KEY = src.CAST_CHARACTER_KEY
WHERE c.ID_CHARACTER IS NULL
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        telcharacter.created(cursor2.rowcount)
                        cp.connectioncp.commit()

                        strsql = "DROP TEMPORARY TABLE IF EXISTS TMP_WC_TMDB_MOVIE_CHARACTER_SOURCE"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """CREATE TEMPORARY TABLE TMP_WC_TMDB_MOVIE_CHARACTER_SOURCE AS
SELECT
    sm.ID_MOVIE,
    c.ID_CHARACTER
FROM TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE sm
INNER JOIN T_WC_TMDB_CHARACTER c
    ON c.CAST_CHARACTER_KEY = sm.CAST_CHARACTER_KEY
GROUP BY sm.ID_MOVIE, c.ID_CHARACTER
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE UNIQUE INDEX UQ_TMP_WC_TMDB_MOVIE_CHARACTER_SOURCE ON TMP_WC_TMDB_MOVIE_CHARACTER_SOURCE (ID_MOVIE, ID_CHARACTER)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "DROP TEMPORARY TABLE IF EXISTS TMP_WC_TMDB_SERIE_CHARACTER_SOURCE"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """CREATE TEMPORARY TABLE TMP_WC_TMDB_SERIE_CHARACTER_SOURCE AS
SELECT
    ss.ID_SERIE,
    c.ID_CHARACTER
FROM TMP_WC_TMDB_CHARACTER_SOURCE_SERIE ss
INNER JOIN T_WC_TMDB_CHARACTER c
    ON c.CAST_CHARACTER_KEY = ss.CAST_CHARACTER_KEY
GROUP BY ss.ID_SERIE, c.ID_CHARACTER
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE UNIQUE INDEX UQ_TMP_WC_TMDB_SERIE_CHARACTER_SOURCE ON TMP_WC_TMDB_SERIE_CHARACTER_SOURCE (ID_SERIE, ID_CHARACTER)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "DROP TEMPORARY TABLE IF EXISTS TMP_WC_TMDB_PERSON_CHARACTER_SOURCE"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """CREATE TEMPORARY TABLE TMP_WC_TMDB_PERSON_CHARACTER_SOURCE AS
SELECT
    src.ID_PERSON,
    c.ID_CHARACTER
FROM (
    SELECT ID_PERSON, CAST_CHARACTER_KEY
    FROM TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE
    UNION ALL
    SELECT ID_PERSON, CAST_CHARACTER_KEY
    FROM TMP_WC_TMDB_CHARACTER_SOURCE_SERIE
) src
INNER JOIN T_WC_TMDB_CHARACTER c
    ON c.CAST_CHARACTER_KEY = src.CAST_CHARACTER_KEY
GROUP BY src.ID_PERSON, c.ID_CHARACTER
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE UNIQUE INDEX UQ_TMP_WC_TMDB_PERSON_CHARACTER_SOURCE ON TMP_WC_TMDB_PERSON_CHARACTER_SOURCE (ID_PERSON, ID_CHARACTER)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """UPDATE T_WC_TMDB_MOVIE_CHARACTER mc
INNER JOIN TMP_WC_TMDB_MOVIE_CHARACTER_SOURCE src
    ON src.ID_MOVIE = mc.ID_MOVIE
   AND src.ID_CHARACTER = mc.ID_CHARACTER
SET mc.ID_MOVIE = mc.ID_MOVIE
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """INSERT INTO T_WC_TMDB_MOVIE_CHARACTER (ID_MOVIE, ID_CHARACTER)
SELECT
    src.ID_MOVIE,
    src.ID_CHARACTER
FROM TMP_WC_TMDB_MOVIE_CHARACTER_SOURCE src
LEFT JOIN T_WC_TMDB_MOVIE_CHARACTER mc
    ON mc.ID_MOVIE = src.ID_MOVIE
   AND mc.ID_CHARACTER = src.ID_CHARACTER
WHERE mc.ID_ROW IS NULL
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """DELETE mc
FROM T_WC_TMDB_MOVIE_CHARACTER mc
LEFT JOIN TMP_WC_TMDB_MOVIE_CHARACTER_SOURCE src
  ON src.ID_MOVIE = mc.ID_MOVIE
 AND src.ID_CHARACTER = mc.ID_CHARACTER
WHERE src.ID_MOVIE IS NULL
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """UPDATE T_WC_TMDB_SERIE_CHARACTER sc
INNER JOIN TMP_WC_TMDB_SERIE_CHARACTER_SOURCE src
    ON src.ID_SERIE = sc.ID_SERIE
   AND src.ID_CHARACTER = sc.ID_CHARACTER
SET sc.ID_SERIE = sc.ID_SERIE
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """INSERT INTO T_WC_TMDB_SERIE_CHARACTER (ID_SERIE, ID_CHARACTER)
SELECT
    src.ID_SERIE,
    src.ID_CHARACTER
FROM TMP_WC_TMDB_SERIE_CHARACTER_SOURCE src
LEFT JOIN T_WC_TMDB_SERIE_CHARACTER sc
    ON sc.ID_SERIE = src.ID_SERIE
   AND sc.ID_CHARACTER = src.ID_CHARACTER
WHERE sc.ID_ROW IS NULL
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """DELETE sc
FROM T_WC_TMDB_SERIE_CHARACTER sc
LEFT JOIN TMP_WC_TMDB_SERIE_CHARACTER_SOURCE src
  ON src.ID_SERIE = sc.ID_SERIE
 AND src.ID_CHARACTER = sc.ID_CHARACTER
WHERE src.ID_SERIE IS NULL
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """UPDATE T_WC_TMDB_PERSON_CHARACTER pc
INNER JOIN TMP_WC_TMDB_PERSON_CHARACTER_SOURCE src
    ON src.ID_PERSON = pc.ID_PERSON
   AND src.ID_CHARACTER = pc.ID_CHARACTER
SET pc.ID_PERSON = pc.ID_PERSON
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """INSERT INTO T_WC_TMDB_PERSON_CHARACTER (ID_PERSON, ID_CHARACTER)
SELECT
    src.ID_PERSON,
    src.ID_CHARACTER
FROM TMP_WC_TMDB_PERSON_CHARACTER_SOURCE src
LEFT JOIN T_WC_TMDB_PERSON_CHARACTER pc
    ON pc.ID_PERSON = src.ID_PERSON
   AND pc.ID_CHARACTER = src.ID_CHARACTER
WHERE pc.ID_ROW IS NULL
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """DELETE pc
FROM T_WC_TMDB_PERSON_CHARACTER pc
LEFT JOIN TMP_WC_TMDB_PERSON_CHARACTER_SOURCE src
  ON src.ID_PERSON = pc.ID_PERSON
 AND src.ID_CHARACTER = pc.ID_CHARACTER
WHERE src.ID_PERSON IS NULL
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                    if 1:
                        telcharacter.position(currentprocess="Updating character KPI (counts, ratings, popularity)", increment=False)
                        strsql = """UPDATE T_WC_TMDB_CHARACTER ch
LEFT JOIN (
    SELECT ID_CHARACTER, COUNT(DISTINCT ID_MOVIE) AS MOVIE_COUNT
    FROM T_WC_TMDB_MOVIE_CHARACTER
    GROUP BY ID_CHARACTER
) m ON m.ID_CHARACTER = ch.ID_CHARACTER
LEFT JOIN (
    SELECT ID_CHARACTER, COUNT(DISTINCT ID_SERIE) AS SERIE_COUNT
    FROM T_WC_TMDB_SERIE_CHARACTER
    GROUP BY ID_CHARACTER
) s ON s.ID_CHARACTER = ch.ID_CHARACTER
LEFT JOIN (
    SELECT ID_CHARACTER, COUNT(DISTINCT ID_PERSON) AS PERSON_COUNT
    FROM T_WC_TMDB_PERSON_CHARACTER
    GROUP BY ID_CHARACTER
) p ON p.ID_CHARACTER = ch.ID_CHARACTER
SET
    ch.MOVIE_COUNT = COALESCE(m.MOVIE_COUNT, 0),
    ch.SERIE_COUNT = COALESCE(s.SERIE_COUNT, 0),
    ch.PERSON_COUNT = COALESCE(p.PERSON_COUNT, 0)
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """UPDATE T_WC_TMDB_CHARACTER ch
JOIN (
    SELECT
        x.ID_CHARACTER,
        AVG(x.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(x.IMDB_RATING_WEIGHTED) AS AVG_IMDB_RATING_WEIGHTED
    FROM (
        SELECT mc.ID_CHARACTER, m.IMDB_RATING, m.IMDB_RATING_WEIGHTED
        FROM T_WC_TMDB_MOVIE_CHARACTER mc
        INNER JOIN T_WC_T2S_MOVIE m
            ON m.ID_MOVIE = mc.ID_MOVIE
        UNION ALL
        SELECT sc.ID_CHARACTER, s.IMDB_RATING, s.IMDB_RATING_WEIGHTED
        FROM T_WC_TMDB_SERIE_CHARACTER sc
        INNER JOIN T_WC_T2S_SERIE s
            ON s.ID_SERIE = sc.ID_SERIE
    ) x
    GROUP BY x.ID_CHARACTER
) r ON r.ID_CHARACTER = ch.ID_CHARACTER
SET
    ch.IMDB_RATING = r.AVG_IMDB_RATING,
    ch.IMDB_RATING_WEIGHTED = r.AVG_IMDB_RATING_WEIGHTED
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """UPDATE T_WC_TMDB_CHARACTER ch
JOIN (
    SELECT
        pc.ID_CHARACTER,
        AVG(p.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_TMDB_PERSON_CHARACTER pc
    INNER JOIN T_WC_T2S_PERSON p
        ON p.ID_PERSON = pc.ID_PERSON
    GROUP BY pc.ID_CHARACTER
) x ON x.ID_CHARACTER = ch.ID_CHARACTER
SET
    ch.POPULARITY = x.AVG_POPULARITY
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                    if 1:
                        telcharacter.position(currentprocess="Removing characters no longer referenced", increment=False)
                        strsql = "DROP TEMPORARY TABLE IF EXISTS TMP_WC_TMDB_CHARACTER_ACTIVE"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """CREATE TEMPORARY TABLE TMP_WC_TMDB_CHARACTER_ACTIVE AS
SELECT ID_CHARACTER
FROM T_WC_TMDB_MOVIE_CHARACTER
UNION
SELECT ID_CHARACTER
FROM T_WC_TMDB_SERIE_CHARACTER
UNION
SELECT ID_CHARACTER
FROM T_WC_TMDB_PERSON_CHARACTER
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "CREATE UNIQUE INDEX UQ_TMP_WC_TMDB_CHARACTER_ACTIVE ON TMP_WC_TMDB_CHARACTER_ACTIVE (ID_CHARACTER)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """DELETE ch
FROM T_WC_TMDB_CHARACTER ch
LEFT JOIN TMP_WC_TMDB_CHARACTER_ACTIVE a
  ON a.ID_CHARACTER = ch.ID_CHARACTER
WHERE a.ID_CHARACTER IS NULL
"""
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        telcharacter.deleted(cursor2.rowcount)
                        cp.connectioncp.commit()

                        strsql = "DELETE FROM T_WC_TMDB_MOVIE_CHARACTER WHERE ID_CHARACTER NOT IN (SELECT ID_CHARACTER FROM T_WC_TMDB_CHARACTER)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)

                        strsql = "DELETE FROM T_WC_TMDB_SERIE_CHARACTER WHERE ID_CHARACTER NOT IN (SELECT ID_CHARACTER FROM T_WC_TMDB_CHARACTER)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)

                        strsql = "DELETE FROM T_WC_TMDB_PERSON_CHARACTER WHERE ID_CHARACTER NOT IN (SELECT ID_CHARACTER FROM T_WC_TMDB_CHARACTER)"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        cursor2.execute("SELECT COUNT(*) AS CHARACTER_COUNT FROM T_WC_TMDB_CHARACTER")
                        telcharacter.set_processed(cursor2.fetchone()["CHARACTER_COUNT"])
                    telcharacter.finish()

                elif intindex == 49:
                    #----------------------------------------------------
                    print("TMDB_CHARACTER_ALT processing")
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Initialize KPI fields for CHARACTER","Current sub process in the TMDb database movie preprocess",0)
                        print("Initialize KPI fields for CHARACTER")
                        strsqlcharacters = """
UPDATE T_WC_TMDB_CHARACTER
SET MOVIE_COUNT = 0,
    SERIE_COUNT = 0,
    PERSON_COUNT = 0,
    IS_EMPTY = 1,
    WORD_COUNT = 0
"""
                        print(strsqlcharacters)
                        cursor2.execute(strsqlcharacters)
                        cp.connectioncp.commit()
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Compute MOVIE_COUNT for CHARACTER","Current sub process in the TMDb database movie preprocess",0)
                        print("Compute MOVIE_COUNT for CHARACTER")
                        strsqlcharacters = """
SELECT COUNT(DISTINCT T_WC_TMDB_MOVIE_CHARACTER.ID_MOVIE) AS COMPTE,
       T_WC_TMDB_CHARACTER.CAST_CHARACTER,
       T_WC_TMDB_CHARACTER.ID_CHARACTER
FROM T_WC_TMDB_CHARACTER
JOIN T_WC_TMDB_MOVIE_CHARACTER ON T_WC_TMDB_CHARACTER.ID_CHARACTER = T_WC_TMDB_MOVIE_CHARACTER.ID_CHARACTER
GROUP BY T_WC_TMDB_CHARACTER.ID_CHARACTER, T_WC_TMDB_CHARACTER.CAST_CHARACTER
ORDER BY COMPTE DESC
"""
                        print(strsqlcharacters)
                        cursor2.execute(strsqlcharacters)
                        print("Number of rows: " + str(cursor2.rowcount))
                        results = cursor2.fetchall()
                        for row in results:
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentcharacterid",str(row['ID_CHARACTER']),"Current character ID in the TMDb database movie preprocess",0)
                            arrcharactercouples = {}
                            arrcharactercouples["MOVIE_COUNT"] = row['COMPTE']
                            cp.f_sqlupdatearray("T_WC_TMDB_CHARACTER",arrcharactercouples,"ID_CHARACTER = " + str(row['ID_CHARACTER']),0)
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Compute SERIE_COUNT for CHARACTER","Current sub process in the TMDb database movie preprocess",0)
                        print("Compute SERIE_COUNT for CHARACTER")
                        strsqlcharacters = """
SELECT COUNT(DISTINCT T_WC_TMDB_SERIE_CHARACTER.ID_SERIE) AS COMPTE,
       T_WC_TMDB_CHARACTER.CAST_CHARACTER,
       T_WC_TMDB_CHARACTER.ID_CHARACTER
FROM T_WC_TMDB_CHARACTER
JOIN T_WC_TMDB_SERIE_CHARACTER ON T_WC_TMDB_CHARACTER.ID_CHARACTER = T_WC_TMDB_SERIE_CHARACTER.ID_CHARACTER
GROUP BY T_WC_TMDB_CHARACTER.ID_CHARACTER, T_WC_TMDB_CHARACTER.CAST_CHARACTER
ORDER BY COMPTE DESC
"""
                        print(strsqlcharacters)
                        cursor2.execute(strsqlcharacters)
                        print("Number of rows: " + str(cursor2.rowcount))
                        results = cursor2.fetchall()
                        for row in results:
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentcharacterid",str(row['ID_CHARACTER']),"Current character ID in the TMDb database movie preprocess",0)
                            arrcharactercouples = {}
                            arrcharactercouples["SERIE_COUNT"] = row['COMPTE']
                            cp.f_sqlupdatearray("T_WC_TMDB_CHARACTER",arrcharactercouples,"ID_CHARACTER = " + str(row['ID_CHARACTER']),0)
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Compute PERSON_COUNT for CHARACTER","Current sub process in the TMDb database movie preprocess",0)
                        print("Compute PERSON_COUNT for CHARACTER")
                        strsqlcharacters = """
SELECT COUNT(DISTINCT T_WC_TMDB_PERSON_CHARACTER.ID_PERSON) AS COMPTE,
       T_WC_TMDB_CHARACTER.CAST_CHARACTER,
       T_WC_TMDB_CHARACTER.ID_CHARACTER
FROM T_WC_TMDB_CHARACTER
JOIN T_WC_TMDB_PERSON_CHARACTER ON T_WC_TMDB_CHARACTER.ID_CHARACTER = T_WC_TMDB_PERSON_CHARACTER.ID_CHARACTER
GROUP BY T_WC_TMDB_CHARACTER.ID_CHARACTER, T_WC_TMDB_CHARACTER.CAST_CHARACTER
ORDER BY COMPTE DESC
"""
                        print(strsqlcharacters)
                        cursor2.execute(strsqlcharacters)
                        print("Number of rows: " + str(cursor2.rowcount))
                        results = cursor2.fetchall()
                        for row in results:
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentcharacterid",str(row['ID_CHARACTER']),"Current character ID in the TMDb database movie preprocess",0)
                            arrcharactercouples = {}
                            arrcharactercouples["PERSON_COUNT"] = row['COMPTE']
                            cp.f_sqlupdatearray("T_WC_TMDB_CHARACTER",arrcharactercouples,"ID_CHARACTER = " + str(row['ID_CHARACTER']),0)
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Compute KPI for CHARACTER","Current sub process in the TMDb database movie preprocess",0)
                        print("Compute KPI for CHARACTER")
                        strsqlcharacters = ""
                        strsqlcharacters += "SELECT * FROM T_WC_TMDB_CHARACTER "
                        strsqlcharacters += "ORDER BY ID_CHARACTER ASC "
                        cursor2.execute(strsqlcharacters)
                        print("Number of rows: " + str(cursor2.rowcount))
                        results = cursor2.fetchall()
                        for row in results:
                            lngcharacterid = row['ID_CHARACTER']
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentcharacterid",str(lngcharacterid),"Current character ID in the TMDb database movie preprocess",0)
                            strcharactername = row['CAST_CHARACTER']
                            lngwordcount = 0
                            try:
                                if strcharactername is not None:
                                    lngwordcount = len(re.findall(r'\b\w+\b', strcharactername))
                            except:
                                pass
                            print(f"Character: '{strcharactername}' - Word count: {lngwordcount}")

                            lngmoviecount = 0
                            if row['MOVIE_COUNT'] is not None:
                                lngmoviecount = row['MOVIE_COUNT']
                            lngseriecount = 0
                            if row['SERIE_COUNT'] is not None:
                                lngseriecount = row['SERIE_COUNT']
                            lngpersoncount = 0
                            if row['PERSON_COUNT'] is not None:
                                lngpersoncount = row['PERSON_COUNT']
                            lngtotalcount = lngmoviecount + lngseriecount + lngpersoncount
                            if lngtotalcount >= 3:
                                intisempty = 0
                            else:
                                intisempty = 1

                            arrcharactercouples = {}
                            arrcharactercouples["IS_EMPTY"] = intisempty
                            arrcharactercouples["WORD_COUNT"] = lngwordcount
                            cp.f_sqlupdatearray("T_WC_TMDB_CHARACTER",arrcharactercouples,"ID_CHARACTER = " + str(lngcharacterid),0)

                elif intindex == 70:
                    #----------------------------------------------------
                    # Refresh "living" evaluation assertions (AES-05 /
                    # TMDB-MOVIE-PREPROCESS-026). For every T_WC_T2S_EVALUATION row
                    # carrying an ASSERTION_REFRESH_SQL, re-run that canonical SELECT
                    # and rewrite ASSERTIONS_QUERY_RESULT = "<ID_COL> IN (...)" so
                    # time-varying samples (e.g. "trending series") stay current.
                    # Runs LAST in the pipeline, after POPULARITY (Process 5) is fresh.
                    # Guardrails: single read-only SELECT, exactly one ID_* column,
                    # per-statement timeout, and a cap on the number of ids written (a
                    # refresh SQL missing its LIMIT would otherwise write 100k+ ids -> a
                    # ~1 MB assertion that bloats /samples); skip + log on anything else.
                    print("T2S_EVALUATION_ASSERTION_REFRESH processing")
                    start_time = time.time()
                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Refresh living-eval assertions","Current sub process in the TMDb database preprocess",0)
                    intassertionmaxstatementtime = 15
                    intassertionmaxids = 50  # a well-formed living assertion is small (LIMIT N); many more = a missing LIMIT
                    cursor2.execute(
                        "SELECT ID_T2S_EVALUATION, ASSERTION_REFRESH_SQL "
                        "FROM T_WC_T2S_EVALUATION "
                        "WHERE ASSERTION_REFRESH_SQL IS NOT NULL AND TRIM(ASSERTION_REFRESH_SQL) <> '' "
                        "AND (DELETED IS NULL OR DELETED = 0) "
                        "ORDER BY ASSERTION_REFRESH_LAST ASC, ID_T2S_EVALUATION ASC "
                    )
                    arrrefreshevals = cursor2.fetchall()
                    print(f"Found {len(arrrefreshevals)} eval(s) with a refresh SQL")
                    lngrefreshed = 0
                    lngskipped = 0
                    for rowrefresh in arrrefreshevals:
                        lngevalid = rowrefresh["ID_T2S_EVALUATION"]
                        # html.unescape FIRST: some rows are stored HTML-entity-encoded by the
                        # admin write path (htmlspecialchars: > -> &gt;, ' -> &#039;). Each entity
                        # ends in ';', so the interior-semicolon guard below would wrongly skip them
                        # -- and the SQL would not execute as-is (WHERE ... &gt; 0). Decoding restores
                        # valid SQL; a genuine interior ';' is still caught after this.
                        strrefreshsql = html.unescape(rowrefresh["ASSERTION_REFRESH_SQL"] or "").strip().rstrip(";").strip()
                        strrefreshlower = strrefreshsql.lower()
                        if (not strrefreshlower.startswith("select")) or (";" in strrefreshsql) or ("into outfile" in strrefreshlower) or ("into dumpfile" in strrefreshlower):
                            print(f"  eval {lngevalid}: SKIP (not a single read-only SELECT)")
                            lngskipped += 1
                            continue
                        try:
                            cursor2.execute(f"SET STATEMENT max_statement_time={intassertionmaxstatementtime} FOR {strrefreshsql}")
                            arridrows = cursor2.fetchall()
                            arridcols = [strd[0] for strd in cursor2.description]
                        except Exception as exrefresh:
                            print(f"  eval {lngevalid}: SKIP (query error: {exrefresh})")
                            lngskipped += 1
                            continue
                        if len(arridcols) != 1 or not str(arridcols[0]).upper().startswith("ID_"):
                            print(f"  eval {lngevalid}: SKIP (query must return exactly one ID_* column, got {arridcols})")
                            lngskipped += 1
                            continue
                        strcol = arridcols[0]
                        arrids = []
                        intbadvalue = 0
                        for rowid in arridrows:
                            valid = rowid[strcol]
                            if valid is None:
                                continue
                            try:
                                arrids.append(int(valid))
                            except (TypeError, ValueError):
                                intbadvalue = 1
                                break
                        if intbadvalue == 1 or len(arrids) == 0:
                            print(f"  eval {lngevalid}: SKIP (no usable integer ids returned)")
                            lngskipped += 1
                            continue
                        if len(arrids) > intassertionmaxids:
                            print(f"  eval {lngevalid}: SKIP ({len(arrids)} ids > {intassertionmaxids} cap -- add a LIMIT to its ASSERTION_REFRESH_SQL)")
                            lngskipped += 1
                            continue
                        strassertion = strcol + " IN (" + ", ".join(str(intid) for intid in arrids) + ")"
                        cursor2.execute(
                            "UPDATE T_WC_T2S_EVALUATION "
                            "SET ASSERTIONS_QUERY_RESULT = %s, ASSERTION_REFRESH_LAST = NOW() "
                            "WHERE ID_T2S_EVALUATION = %s ",
                            [strassertion, lngevalid],
                        )
                        lngrefreshed += 1
                        print(f"  eval {lngevalid}: {strassertion}")
                    cp.connectioncp.commit()
                    cp.f_setservervariable("strtmdbmoviepreprocessassertionrefreshcount", str(lngrefreshed), "Living-eval assertions refreshed in the last run", 0)
                    cp.f_setservervariable("strtmdbmoviepreprocessassertionrefreshskipped", str(lngskipped), "Living-eval assertions skipped by a guardrail in the last run", 0)
                    print(f"T2S_EVALUATION_ASSERTION_REFRESH complete: {lngrefreshed} refreshed, {lngskipped} skipped")
                    print(f"Elapsed time: {time.time() - start_time:.2f} seconds")
                elif intindex == 71:
                    #----------------------------------------------------
                    # Wikipedia main image into the T2S serving layer
                    # (WIKIPEDIA-CRAWLER-020, unblocks WIKIDATA-CRAWLER-015).
                    #
                    # The lead image used to live ONLY in the entity's V1 row
                    # (WIKIPEDIA_POSTER_PATH / _PROFILE_PATH / _IMAGE_PATH), which is
                    # precisely why the V1 tables could not be dropped. wikipedia-crawler
                    # now also writes it to its own home, keyed per language, in
                    # T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL. This process copies it into
                    # T2S, where the 172 consumer read sites already look, so each of them
                    # moves from one local column to another instead of learning a join.
                    #
                    # TWO COLUMNS, ONE PER LANGUAGE, one UPDATE each. V1 had a single
                    # image column while the crawler runs once per language, so the second
                    # language silently overwrote the first: that is how collection 4845
                    # lost its English lead image to a French portal banner. Never pivot
                    # the two languages into a single join.
                    #
                    # NEVER BLANK: the WHERE keeps an empty source from erasing what an
                    # earlier pass had found. A crawl that fails today must not cost the
                    # image found last week, which is the rule the crawler applies to its
                    # own writes.
                    #
                    # Placed LAST: it reads the T2S rows every other process builds.
                    # Per-statement try/except so a table missing the columns (migration
                    # not run yet) logs and is skipped instead of aborting the pipeline.
                    print("T2S_WIKIPEDIA_MAIN_IMAGE processing")
                    start_time = time.time()
                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copy Wikipedia main image into T2S","Current sub process in the TMDb database preprocess",0)
                    arrwikipediaimageentities = ["MOVIE","SERIE","PERSON","SEASON","EPISODE","CHARACTER","ITEM","AWARD","NOMINATION","DEATH","GROUP","MOVEMENT","COLLECTION","LIST","TOPIC","TECHNICAL"]
                    arrwikipediaimagelangs = [("en","WIKIPEDIA_MAIN_IMAGE_URL"),("fr","WIKIPEDIA_MAIN_IMAGE_URL_FR")]
                    lngwikipediaimagerowstotal = 0
                    lngwikipediaimageskipped = 0
                    for strentity in arrwikipediaimageentities:
                        for strlang, strcolumn in arrwikipediaimagelangs:
                            strsqlmainimage = f"""
UPDATE T_WC_T2S_{strentity} t2s
INNER JOIN T_WC_WIKIPEDIA_PAGE_LANG pl
    ON  pl.ID_WIKIDATA = t2s.ID_WIKIDATA
    AND pl.LANG = '{strlang}'
SET t2s.{strcolumn} = pl.MAIN_IMAGE_URL
WHERE COALESCE(pl.MAIN_IMAGE_URL,'') <> '' """
                            try:
                                cursor2.execute(strsqlmainimage)
                                lngwikipediaimagerows = cursor2.rowcount
                                cp.connectioncp.commit()
                                lngwikipediaimagerowstotal += lngwikipediaimagerows
                                print(f"  T2S_{strentity} [{strlang}]: {lngwikipediaimagerows} row(s)")
                            except pymysql.MySQLError as e:
                                lngwikipediaimageskipped += 1
                                print(f"  T2S_{strentity} [{strlang}]: SKIPPED ({e})")
                    cp.f_setservervariable("strtmdbmoviepreprocesswikipediamainimagerows", str(lngwikipediaimagerowstotal), "Rows updated with a Wikipedia main image in the last run", 0)
                    print(f"T2S_WIKIPEDIA_MAIN_IMAGE complete: {lngwikipediaimagerowstotal} row(s), {lngwikipediaimageskipped} statement(s) skipped")
                    print(f"Elapsed time: {time.time() - start_time:.2f} seconds")
                if telcopy is not None:
                    telcopy.finish()
                dblprocesselapsed = time.time() - dblprocessstarttime
                arrprocessdurations[intindex] = (strdesc, dblprocesselapsed)
                # Live per-process elapsed seconds (uniform name) for external monitoring.
                cp.f_setservervariable(
                    f"strtmdbmoviepreprocessprocesselapsedseconds{intindex}",
                    f"{dblprocesselapsed:.2f}",
                    f"Elapsed seconds of process {intindex} ({strdesc}) in the last run",
                    0,
                )

            # ---- Per-process duration ranking (optimization candidates) ----
            # Single consolidated, comparable view so the slowest processes are
            # obvious without collecting ~45 differently-named per-entity vars.
            if arrprocessdurations:
                arrsortedprocessdurations = sorted(
                    arrprocessdurations.items(), key=lambda kv: kv[1][1], reverse=True
                )
                print("=== Process duration ranking (longest first) ===")
                arrrankingparts = []
                for intidx, (strlabel, dblseconds) in arrsortedprocessdurations:
                    strreadable = cp.convert_seconds_to_duration(int(dblseconds))
                    print(f"  {dblseconds:10.2f}s  process {intidx:>3}  {strlabel}  ({strreadable})")
                    arrrankingparts.append(f"{intidx}:{strlabel}={dblseconds:.2f}s")
                strprocessdurationranking = " | ".join(arrrankingparts)
                # VAR_VALUE is varchar(255); the full ranking (~45 processes) far
                # exceeds that. Truncate to fit — since parts are sorted longest
                # first, the slowest (optimization candidates) are always kept.
                if len(strprocessdurationranking) > 255:
                    strprocessdurationranking = strprocessdurationranking[:252].rstrip(" |") + "..."
                cp.f_setservervariable(
                    "strtmdbmoviepreprocessprocessdurationranking",
                    strprocessdurationranking,
                    "Per-process elapsed seconds, longest first, for the last run (optimization candidates)",
                    0,
                )

            print("------------------------------------------")
            strcurrentprocess = ""
            cp.f_setservervariable("strtmdbmoviepreprocesscurrentprocess",strcurrentprocess,"Current process in the TMDb database preprocess",0)
            strsql = ""
            cp.f_setservervariable("strtmdbmoviepreprocesscurrentsql",strsql,"Current SQL query in the TMDb database preprocess",0)
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strtmdbmoviepreprocessenddatetime",strnow,"Date and time of the TMDb database preprocess ending",0)
            # Calculate total runtime and convert to readable format
            dblrunendtime = time.time()
            strtotalruntime = int(dblrunendtime - dblrunstarttime)  # Total runtime in seconds
            readable_duration = cp.convert_seconds_to_duration(strtotalruntime)
            cp.f_setservervariable("strtmdbmoviepreprocesstotalruntime",readable_duration,strtotalruntimedesc,0)
            print(f"Total runtime: {strtotalruntime} seconds ({readable_duration})")
    print("Process completed")
except pymysql.MySQLError as e:
    print(f"❌ MySQL Error: {e}")
    conn = getattr(cp, "connectioncp", None)
    if conn is not None and getattr(conn, "open", False):
        conn.rollback()

