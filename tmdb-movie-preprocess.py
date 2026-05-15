import time
import os
import requests
import pymysql.cursors
#from pymysql import Error
import json
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
    f_getcustomsortby,
    f_getwikidataimagepath,
    f_getlemma,
    f_linktmdbkeywordtowikidata,
    f_tmdbpersonsetusedfortags,
    f_wikidataitemproperties,
    normalize_extracted_components,
    process_value,
    validate_format_line,
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
            start_time = time.time()
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

            #arrprocessscope = {1: 'WIKIPEDIA_FORMAT_LINE', 2: 'T2S_MOVIE_TECHNICAL', 3: 'T2S_TOPIC', 4: 'T2S_MOVIE', 5: 'T2S_SERIE', 6: 'T2S_PERSON', 7: 'T2S_COMPANY', 8: 'T2S_NETWORK', 9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE', 20: 'TMDB_KEYWORD', 30: 'TMDB_MOVIE_LANG_META'}
            #arrprocessscope = {2: 'T2S_MOVIE_TECHNICAL'}
            #arrprocessscope = {20: 'TMDB_KEYWORD'}
            #arrprocessscope = {6: 'T2S_PERSON'}
            #arrprocessscope = {4: 'T2S_MOVIE'}
            #arrprocessscope = {5: 'T2S_SERIE'}
            arrprocessscope = {1: 'WIKIPEDIA_FORMAT_LINE', 2: 'T2S_MOVIE_TECHNICAL', 60: 'Link Wikidata items to topics', 3: 'T2S_TOPIC', 41: 'T2S_COLLECTION', 61: 'Link Wikidata items to collections', 42: 'T2S_LIST', 43: 'T2S_GROUP', 44: 'T2S_AWARD', 47: 'T2S_NOMINATION', 45: 'T2S_MOVEMENT', 46: 'T2S_DEATH', 4: 'T2S_MOVIE', 5: 'T2S_SERIE', 6: 'T2S_PERSON', 7: 'T2S_COMPANY', 8: 'T2S_NETWORK', 9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE', 11: 'T2S_MOVIE_GENRE', 12: 'T2S_SERIE_GENRE', 13: 'T2S_MOVIE_COMPANY', 14: 'T2S_SERIE_COMPANY', 15: 'T2S_SERIE_NETWORK', 16: 'T2S_MOVIE_PRODUCTION_COUNTRY', 17: 'T2S_SERIE_PRODUCTION_COUNTRY', 18: 'T2S_MOVIE_SPOKEN_LANGUAGE', 19: 'T2S_SERIE_SPOKEN_LANGUAGE', 20: 'T2S_COMPANY_IMAGE', 21: 'T2S_MOVIE_IMAGE', 22: 'T2S_NETWORK_IMAGE', 23: 'T2S_PERSON_IMAGE', 24: 'T2S_SERIE_IMAGE', 25: 'T2S_MOVIE_VIDEO', 26: 'T2S_SERIE_VIDEO', 40: 'T2S_ITEM', 48: 'TMDB_CHARACTER', 49: 'TMDB_CHARACTER_ALT'}
            #arrprocessscope = {9: 'T2S_PERSON_MOVIE'}
            #arrprocessscope = {10: 'T2S_PERSON_SERIE'}
            #arrprocessscope = {9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE'}
            #arrprocessscope = {4: 'T2S_MOVIE', 5: 'T2S_SERIE'}
            #arrprocessscope = {7: 'T2S_COMPANY'}
            #arrprocessscope = {8: 'T2S_NETWORK'}
            #arrprocessscope = {3: 'T2S_TOPIC', 4: 'T2S_MOVIE', 5: 'T2S_SERIE', 6: 'T2S_PERSON', 7: 'T2S_COMPANY', 8: 'T2S_NETWORK', 9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE'}
            #arrprocessscope = {1: 'WIKIPEDIA_FORMAT_LINE'}
            #arrprocessscope = {30: 'TMDB_MOVIE_LANG_META'}
            #arrprocessscope = {40: 'T2S_ITEM'}
            #arrprocessscope = {41: 'T2S_COLLECTION'}
            #arrprocessscope = {41: 'T2S_COLLECTION', 42: 'T2S_LIST'}
            #arrprocessscope = {3: 'T2S_TOPIC'}
            #arrprocessscope = {43: 'T2S_GROUP'}
            if strnow.startswith("2026-05-14"):
                arrprocessscope = {4: 'T2S_MOVIE', 5: 'T2S_SERIE', 6: 'T2S_PERSON', 7: 'T2S_COMPANY', 8: 'T2S_NETWORK', 9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE', 11: 'T2S_MOVIE_GENRE', 12: 'T2S_SERIE_GENRE', 13: 'T2S_MOVIE_COMPANY', 14: 'T2S_SERIE_COMPANY', 15: 'T2S_SERIE_NETWORK', 16: 'T2S_MOVIE_PRODUCTION_COUNTRY', 17: 'T2S_SERIE_PRODUCTION_COUNTRY', 18: 'T2S_MOVIE_SPOKEN_LANGUAGE', 19: 'T2S_SERIE_SPOKEN_LANGUAGE', 20: 'T2S_COMPANY_IMAGE', 21: 'T2S_MOVIE_IMAGE', 22: 'T2S_NETWORK_IMAGE', 23: 'T2S_PERSON_IMAGE', 24: 'T2S_SERIE_IMAGE', 25: 'T2S_MOVIE_VIDEO', 26: 'T2S_SERIE_VIDEO', 40: 'T2S_ITEM', 48: 'TMDB_CHARACTER', 49: 'TMDB_CHARACTER_ALT'}
            for intindex, strdesc in arrprocessscope.items():
                strprocessesexecuted += str(intindex) + ", "
                cp.f_setservervariable("strtmdbmoviepreprocessprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
                cp.f_setservervariable("strtmdbmoviepreprocesscurrentprocess",strdesc,"Current process in the TMDb database preprocess",0)
                cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","","Current sub process in the TMDb database preprocess",0)
                if intindex == 1:
                    #----------------------------------------------------
                    print("WIKIPEDIA_FORMAT_LINE processing")
                    start_time = time.time()
                    
                    # Check memory
                    dblavailableram = check_memory()
                    
                    # Read data from database using fetchall()
                    query = """
SELECT ID_MOVIE, WIKIPEDIA_FORMAT_LINE 
FROM T_WC_TMDB_MOVIE 
WHERE WIKIPEDIA_FORMAT_LINE IS NOT NULL 
AND WIKIPEDIA_FORMAT_LINE <> '' 
ORDER BY ID_MOVIE ASC 
                    """
                    print(query)
                    cursor2.execute(query)
                    result = cursor2.fetchall()
                    # Convert the result to a pandas DataFrame
                    data = pd.DataFrame(result)
                    print(f"Loaded {len(data)} rows of data")
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
                            strsqldelete = "DELETE FROM " + cp.strsqlns + "T2S_MOVIE_TECHNICAL WHERE ID_MOVIE = " + str(lngmovieid) + " AND ID_TECHNICAL NOT IN (" + strtechidlist + ") "
                            cursor2.execute(strsqldelete)
                            cp.connectioncp.commit()

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
                        strsqlitem += "SELECT ID_ITEM "
                        strsqlitem += "FROM T_WC_WIKIDATA_ITEM_PROPERTY "
                        strsqlitem += "WHERE ID_WIKIDATA IN (" + strwikidataidlist + ") "
                        strsqlitem += "AND ID_PROPERTY = 'P179' "
                        strsqlitem += "GROUP BY ID_ITEM "
                        strsqlitem += "HAVING COUNT(DISTINCT ID_WIKIDATA) = " + str(intmoviecount) + " "
                        strsqlitem += "AND (SELECT COUNT(DISTINCT ip2.ID_WIKIDATA) "
                        strsqlitem += "FROM T_WC_WIKIDATA_ITEM_PROPERTY ip2 "
                        strsqlitem += "WHERE ip2.ID_ITEM = T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM "
                        strsqlitem += "AND ip2.ID_PROPERTY = 'P179') = " + str(intmoviecount) + " "
                        strsqlitem += "ORDER BY ID_ITEM ASC "
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
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Compute MOVIE_COUNT for KEYWORD","Current sub process in the TMDb database movie preprocess",0)
                        # Compute MOVIE_COUNT for KEYWORD
                        print("Compute MOVIE_COUNT for KEYWORD")
                        strsqlcompanies = """
SELECT COUNT(DISTINCT T_WC_T2S_MOVIE.ID_MOVIE) AS COMPTE, T_WC_TMDB_KEYWORD.NAME, T_WC_TMDB_KEYWORD.ID_KEYWORD 
FROM T_WC_T2S_MOVIE 
JOIN T_WC_TMDB_MOVIE_KEYWORD ON T_WC_T2S_MOVIE.ID_MOVIE = T_WC_TMDB_MOVIE_KEYWORD.ID_MOVIE 
JOIN T_WC_TMDB_KEYWORD ON T_WC_TMDB_MOVIE_KEYWORD.ID_KEYWORD = T_WC_TMDB_KEYWORD.ID_KEYWORD 
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
                        strsqlcompanies = """
SELECT COUNT(DISTINCT T_WC_T2S_SERIE.ID_SERIE) AS COMPTE, T_WC_TMDB_KEYWORD.NAME, T_WC_TMDB_KEYWORD.ID_KEYWORD 
FROM T_WC_T2S_SERIE 
JOIN T_WC_TMDB_SERIE_KEYWORD ON T_WC_T2S_SERIE.ID_SERIE = T_WC_TMDB_SERIE_KEYWORD.ID_SERIE 
JOIN T_WC_TMDB_KEYWORD ON T_WC_TMDB_SERIE_KEYWORD.ID_KEYWORD = T_WC_TMDB_KEYWORD.ID_KEYWORD 
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
                                strsql += "WHERE T_WC_TMDB_KEYWORD.USED_FOR_T2S_TOPIC > 0 "
                                strsql += "OR T_WC_TMDB_KEYWORD.USE_FOR_TAGGING > 0 "
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
                                    strrecordname = row['NAME']
                                    strrecordoverview = row['OVERVIEW']
                                    strrecordlang = row['LANG']
                                    strrecordtopicsource = row['TOPIC_SOURCE']
                                    strrecordtopictype = row['TOPIC_TYPE']
                                    strrecordposterpath = row['POSTER_PATH']
                                    strrecordidwikidata = row['ID_WIKIDATA'] if 'ID_WIKIDATA' in row else None
                                    strrecordwikipediaimagepath = f_getwikidataimagepath(strrecordidwikidata)
                                    print("Processing record: " + str(lngrecordid) + ": " + strrecordname + " (" + strrecordtopicsource + ")")
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
                                            #cursor2.commit()
                    if 1:
                        strsqltablename = "T_WC_T2S_TOPIC"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE TOPIC_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

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

                elif intindex == 41:
                    #----------------------------------------------------
                    print("T2S_COLLECTION processing")

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
                            strsql += "SELECT 'custom' AS COLLECTION_SOURCE, 'collection' AS COLLECTION_TYPE, T_WC_CUSTOM_LIST.ID_CUSTOM_LIST AS ID_RECORD, T_WC_CUSTOM_LIST.LIST_NAME AS NAME, T_WC_CUSTOM_LIST.LIST_NAME_FR AS NAME_FR, T_WC_CUSTOM_LIST.OVERVIEW AS OVERVIEW, 'en' AS LANG, T_WC_CUSTOM_LIST.POSTER_PATH, NULL AS ID_WIKIDATA, T_WC_CUSTOM_LIST.ID_IMDB_LIST, T_WC_CUSTOM_LIST.WIKIDATA_PROPERTIES, T_WC_CUSTOM_LIST.TMDB_ELEMENTS, T_WC_CUSTOM_LIST.SORT_BY "
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
                                    arrwdtokens = re.findall(r'[PQ]\d+', strwikidataproperties)
                                    strwdpropertyid = next((t for t in arrwdtokens if t.startswith('P')), '')
                                    strwditemid = next((t for t in arrwdtokens if t.startswith('Q')), '')
                                    if strwditemid:
                                        arrcollectioncouples['ID_WIKIDATA'] = strwditemid
                                        arrcollectioncouples['WIKIPEDIA_IMAGE_PATH'] = f_getwikidataimagepath(strwditemid)
                                    strsqlmovies_wikidata = ""
                                    strsqlseries_wikidata = ""
                                    if strwdpropertyid and strwditemid:
                                        strsqlmovies_wikidata = "SELECT DISTINCT m.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON m.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_wikidata = "SELECT DISTINCT s.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON s.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
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
                                        # This collection has more than one element (movie or serie)
                                        # So we create/update this collection
                                        lngcollectionid = cp.f_sqlupdatearray(strsqltablename, arrcollectioncouples, strsqlupdatecondition, 1)
                                        if lngcollectionid is None:
                                            strsqlcollection = "SELECT ID_T2S_COLLECTION FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                            cursor3.execute(strsqlcollection)
                                            lngrowcount = cursor3.rowcount
                                            if lngrowcount == 0:
                                                print("Error: Failed to create/update collection - lngcollectionid is None")
                                                continue
                                            lngcollectionid = cursor3.fetchone()["ID_T2S_COLLECTION"]
                                        if intcollection == 1 or intcollection == 3 or intcollection == 5:
                                            # Retrieve all movies for this collection
                                            # Only processing when handling original English (records from T_WC_TMDB_LIST or T_WC_TMDB_COLLECTION) to avoid duplicates with the translated versions
                                            results = cursor2.fetchall()
                                            lngdisplayorder = 0
                                            arrcurrentmovieids = []
                                            for row in results:
                                                lngmovieid = row["ID_MOVIE"]
                                                lngdisplayorder += 1
                                                arrcurrentmovieids.append(str(lngmovieid))
                                                arrmoviecollectioncouples = {
                                                    'ID_MOVIE': lngmovieid,
                                                    'ID_T2S_COLLECTION': lngcollectionid,
                                                    'DISPLAY_ORDER': lngdisplayorder
                                                }
                                                strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                #print(strsqlupdatecondition2)
                                                cp.f_sqlupdatearray("T_WC_T2S_MOVIE_COLLECTION", arrmoviecollectioncouples, strsqlupdatecondition2, 1)
                                            if arrcurrentmovieids:
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
                                                arrcurrentserieids = []
                                                for row in results:
                                                    lngseriesid = row["ID_SERIE"]
                                                    lngdisplayorder += 1
                                                    arrcurrentserieids.append(str(lngseriesid))
                                                    arrseriecollectioncouples = {
                                                        'ID_SERIE': lngseriesid,
                                                        'ID_T2S_COLLECTION': lngcollectionid,
                                                        'DISPLAY_ORDER': lngdisplayorder
                                                    }
                                                    strsqlupdatecondition2 = "ID_SERIE = " + str(lngseriesid) + " AND ID_T2S_COLLECTION = " + str(lngcollectionid)
                                                    #print(strsqlupdatecondition2)
                                                    cp.f_sqlupdatearray("T_WC_T2S_SERIE_COLLECTION", arrseriecollectioncouples, strsqlupdatecondition2, 1)
                                                if arrcurrentserieids:
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
                                            cp.f_sqlupdatearray(strsqltablename, arrcollectioncouples, strsqlupdatecondition, 1)
                                    else:
                                        # This collection has only one element or none
                                        # So we delete this collection if it already exists
                                        strsqltablename = "T_WC_T2S_COLLECTION"
                                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE " + strsqlupdatecondition
                                        print(strsqldelete)
                                        cursor2.execute(strsqldelete)
                                        #cursor2.commit()
                    if 1:
                        strsqltablename = "T_WC_T2S_COLLECTION"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE COLLECTION_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqldelete = ""
                        strsqldelete += "DELETE FROM T_WC_T2S_COLLECTION "
                        strsqldelete += "WHERE COLLECTION_SOURCE = 'list' "
                        strsqldelete += "AND ID_RECORD NOT IN (SELECT ID_LIST FROM T_WC_TMDB_LIST WHERE USED_FOR_T2S_COLLECTION > 0) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqldelete = ""
                        strsqldelete += "DELETE FROM T_WC_T2S_COLLECTION "
                        strsqldelete += "WHERE COLLECTION_SOURCE = 'collection' "
                        strsqldelete += "AND ID_RECORD NOT IN (SELECT ID_COLLECTION FROM T_WC_TMDB_COLLECTION) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqldelete = ""
                        strsqldelete += "DELETE FROM T_WC_T2S_COLLECTION "
                        strsqldelete += "WHERE COLLECTION_SOURCE = 'custom' "
                        strsqldelete += "AND ID_RECORD NOT IN (SELECT ID_CUSTOM_LIST FROM T_WC_CUSTOM_LIST WHERE TARGET_TABLE = 2 AND DELETED = 0) "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        
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

                elif intindex == 42:
                    #----------------------------------------------------
                    print("T2S_LIST processing")

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
                            #cursor.commit()
                            strsqldelete = ""
                            strsqldelete += "DELETE FROM T_WC_T2S_LIST WHERE LIST_SOURCE = 'custom' AND ID_RECORD NOT IN (SELECT ID_CUSTOM_LIST FROM T_WC_CUSTOM_LIST WHERE TARGET_TABLE = 1) "
                            print(strsqldelete)
                            cursor.execute(strsqldelete)
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
                                        strsqlmovies_wikidata = "SELECT DISTINCT m.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON m.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_wikidata = "SELECT DISTINCT s.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON s.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
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
                                        #cursor2.commit()
                    if 1:
                        strsqltablename = "T_WC_T2S_LIST"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE LIST_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)
                        
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

                elif intindex == 43:
                    #----------------------------------------------------
                    print("T2S_GROUP processing")

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
                            strsql += "SELECT DISTINCT T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM "
                            strsql += "FROM T_WC_WIKIDATA_ITEM_PROPERTY "
                            strsql += "WHERE T_WC_WIKIDATA_ITEM_PROPERTY.ID_PROPERTY = '" + strpropertyid + "' "
                            strsql += "ORDER BY T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM ASC "
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
                                    strsqlitem += "SELECT LABEL, DESCRIPTION, WIKIPEDIA_IMAGE_PATH "
                                    strsqlitem += "FROM T_WC_WIKIDATA_ITEM_V1 "
                                    strsqlitem += "WHERE ID_WIKIDATA = %s AND LANG = 'en'"
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
                                    strsqlitem = ""
                                    strsqlitem += "SELECT LABEL "
                                    strsqlitem += "FROM T_WC_WIKIDATA_ITEM_V1 "
                                    strsqlitem += "WHERE ID_WIKIDATA = %s AND LANG = 'fr'"
                                    arrvalues = cp.f_fieldsfromquery(
                                        strsqlitem,
                                        "strrecordname",
                                        "LABEL",
                                        params=(strrecordid,),
                                        target_dict=None,
                                    )
                                    strrecordnamefr = arrvalues.get("strrecordname", "")
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
                                        strsqlpersons_wikidata = "SELECT DISTINCT T_WC_TMDB_PERSON.ID_PERSON, NULL AS ORIGINAL_ORDER, T_WC_TMDB_PERSON.POPULARITY, T_WC_TMDB_PERSON.BIRTHDAY AS SORT_DATE FROM T_WC_TMDB_PERSON INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY ON T_WC_TMDB_PERSON.ID_WIKIDATA = T_WC_WIKIDATA_ITEM_PROPERTY.ID_WIKIDATA WHERE T_WC_WIKIDATA_ITEM_PROPERTY.ID_PROPERTY = '" + strwdpropertyid + "' AND T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM = '" + strwditemid + "' AND T_WC_TMDB_PERSON.ADULT = 0 AND T_WC_TMDB_PERSON.ID_WIKIDATA IS NOT NULL AND T_WC_TMDB_PERSON.ID_WIKIDATA <> '' "

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
                                    strsqlpersons += "FROM T_WC_TMDB_PERSON "
                                    strsqlpersons += "INNER JOIN T_WC_WIKIDATA_PERSON_V1 ON T_WC_TMDB_PERSON.ID_WIKIDATA = T_WC_WIKIDATA_PERSON_V1.ID_WIKIDATA "
                                    strsqlpersons += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY ON T_WC_TMDB_PERSON.ID_WIKIDATA = T_WC_WIKIDATA_ITEM_PROPERTY.ID_WIKIDATA "
                                    strsqlpersons += "WHERE T_WC_WIKIDATA_ITEM_PROPERTY.ID_PROPERTY = %s "
                                    strsqlpersons += "AND T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM = %s "
                                    strsqlpersons += "ORDER BY T_WC_TMDB_PERSON.POPULARITY DESC "
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
                                        #cursor2.commit()
                    if 1:
                        strsqltablename = "T_WC_T2S_GROUP"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE GROUP_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqltablename = "T_WC_T2S_GROUP"
                        strsqldelete = """DELETE FROM T_WC_T2S_GROUP
WHERE GROUP_SOURCE <> 'custom'
  AND NOT EXISTS (
    SELECT 1
    FROM T_WC_WIKIDATA_ITEM_PROPERTY w
    WHERE w.ID_PROPERTY = T_WC_T2S_GROUP.GROUP_SOURCE
      AND w.ID_ITEM = T_WC_T2S_GROUP.ID_WIKIDATA
);
                        """
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

                elif intindex == 44:
                    #----------------------------------------------------
                    print("T2S_AWARD processing")

                    strpropertyid = "P166"
                    strawardsource = strpropertyid
                    strawardtype = "award"
                    target_field_name = "AWARD_NAME"

                    strsql = ""
                    strsql += "SELECT DISTINCT T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_PROPERTY "
                    strsql += "WHERE T_WC_WIKIDATA_ITEM_PROPERTY.ID_PROPERTY = %s "
                    strsql += "ORDER BY T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM ASC "

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
                        strsqlitem += "SELECT LABEL, DESCRIPTION, WIKIPEDIA_IMAGE_PATH "
                        strsqlitem += "FROM T_WC_WIKIDATA_ITEM_V1 "
                        strsqlitem += "WHERE ID_WIKIDATA = %s AND LANG = 'en'"
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

                        strsqlitem = ""
                        strsqlitem += "SELECT LABEL "
                        strsqlitem += "FROM T_WC_WIKIDATA_ITEM_V1 "
                        strsqlitem += "WHERE ID_WIKIDATA = %s AND LANG = 'fr'"
                        arrvalues = cp.f_fieldsfromquery(
                            strsqlitem,
                            "strawardnamefr",
                            "LABEL",
                            params=(strawardwikidataid,),
                            target_dict=None,
                        )
                        strawardnamefr = arrvalues.get("strawardnamefr", "")

                        print("Processing record: " + str(strawardwikidataid) + ": " + strawardname + " (" + strawardsource + ")")

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

                        # Link to movies
                        strsqlmovies = ""
                        strsqlmovies += "SELECT DISTINCT m.ID_MOVIE, m.IMDB_RATING_WEIGHTED "
                        strsqlmovies += "FROM T_WC_T2S_MOVIE m "
                        strsqlmovies += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY p ON p.ID_WIKIDATA = m.ID_WIKIDATA "
                        strsqlmovies += "WHERE p.ID_PROPERTY = %s AND p.ID_ITEM = %s "
                        strsqlmovies += "AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                        strsqlmovies += "ORDER BY m.IMDB_RATING_WEIGHTED DESC, m.ID_MOVIE ASC "
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
                        strsqlseries = ""
                        strsqlseries += "SELECT DISTINCT s.ID_SERIE, s.IMDB_RATING_WEIGHTED "
                        strsqlseries += "FROM T_WC_T2S_SERIE s "
                        strsqlseries += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY p ON p.ID_WIKIDATA = s.ID_WIKIDATA "
                        strsqlseries += "WHERE p.ID_PROPERTY = %s AND p.ID_ITEM = %s "
                        strsqlseries += "AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                        strsqlseries += "ORDER BY s.IMDB_RATING_WEIGHTED DESC, s.ID_SERIE ASC "
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
                        strsqlpersons = ""
                        strsqlpersons += "SELECT DISTINCT p2.ID_PERSON, p2.POPULARITY "
                        strsqlpersons += "FROM T_WC_T2S_PERSON p2 "
                        strsqlpersons += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY p ON p.ID_WIKIDATA = p2.ID_WIKIDATA "
                        strsqlpersons += "WHERE p.ID_PROPERTY = %s AND p.ID_ITEM = %s "
                        strsqlpersons += "AND p2.ID_WIKIDATA IS NOT NULL AND p2.ID_WIKIDATA <> '' "
                        strsqlpersons += "ORDER BY p2.POPULARITY DESC, p2.ID_PERSON ASC "
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

                        strsqltablename = "T_WC_T2S_AWARD"
                        strsqldelete = """DELETE FROM T_WC_T2S_AWARD
WHERE NOT EXISTS (
    SELECT 1
    FROM T_WC_WIKIDATA_ITEM_PROPERTY w
    WHERE w.ID_PROPERTY = T_WC_T2S_AWARD.AWARD_SOURCE
      AND w.ID_ITEM = T_WC_T2S_AWARD.ID_WIKIDATA
);
                        """
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

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

                elif intindex == 47:
                    #----------------------------------------------------
                    print("T2S_NOMINATION processing")

                    strpropertyid = "P1411"
                    strnominationsource = strpropertyid
                    strnominationtype = "nomination"
                    target_field_name = "NOMINATION_NAME"

                    strsql = ""
                    strsql += "SELECT DISTINCT T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_PROPERTY "
                    strsql += "WHERE T_WC_WIKIDATA_ITEM_PROPERTY.ID_PROPERTY = %s "
                    strsql += "ORDER BY T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM ASC "

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
                        strsqlitem += "SELECT LABEL, DESCRIPTION, WIKIPEDIA_IMAGE_PATH "
                        strsqlitem += "FROM T_WC_WIKIDATA_ITEM_V1 "
                        strsqlitem += "WHERE ID_WIKIDATA = %s AND LANG = 'en'"
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

                        strsqlitem = ""
                        strsqlitem += "SELECT LABEL "
                        strsqlitem += "FROM T_WC_WIKIDATA_ITEM_V1 "
                        strsqlitem += "WHERE ID_WIKIDATA = %s AND LANG = 'fr'"
                        arrvalues = cp.f_fieldsfromquery(
                            strsqlitem,
                            "strnominationnamefr",
                            "LABEL",
                            params=(strnominationwikidataid,),
                            target_dict=None,
                        )
                        strnominationnamefr = arrvalues.get("strnominationnamefr", "")

                        print("Processing record: " + str(strnominationwikidataid) + ": " + strnominationname + " (" + strnominationsource + ")")

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

                        # Link to movies
                        strsqlmovies = ""
                        strsqlmovies += "SELECT DISTINCT m.ID_MOVIE, m.IMDB_RATING_WEIGHTED "
                        strsqlmovies += "FROM T_WC_T2S_MOVIE m "
                        strsqlmovies += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY p ON p.ID_WIKIDATA = m.ID_WIKIDATA "
                        strsqlmovies += "WHERE p.ID_PROPERTY = %s AND p.ID_ITEM = %s "
                        strsqlmovies += "AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                        strsqlmovies += "ORDER BY m.IMDB_RATING_WEIGHTED DESC, m.ID_MOVIE ASC "
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
                        strsqlseries = ""
                        strsqlseries += "SELECT DISTINCT s.ID_SERIE, s.IMDB_RATING_WEIGHTED "
                        strsqlseries += "FROM T_WC_T2S_SERIE s "
                        strsqlseries += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY p ON p.ID_WIKIDATA = s.ID_WIKIDATA "
                        strsqlseries += "WHERE p.ID_PROPERTY = %s AND p.ID_ITEM = %s "
                        strsqlseries += "AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                        strsqlseries += "ORDER BY s.IMDB_RATING_WEIGHTED DESC, s.ID_SERIE ASC "
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
                        strsqlpersons = ""
                        strsqlpersons += "SELECT DISTINCT p2.ID_PERSON, p2.POPULARITY "
                        strsqlpersons += "FROM T_WC_T2S_PERSON p2 "
                        strsqlpersons += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY p ON p.ID_WIKIDATA = p2.ID_WIKIDATA "
                        strsqlpersons += "WHERE p.ID_PROPERTY = %s AND p.ID_ITEM = %s "
                        strsqlpersons += "AND p2.ID_WIKIDATA IS NOT NULL AND p2.ID_WIKIDATA <> '' "
                        strsqlpersons += "ORDER BY p2.POPULARITY DESC, p2.ID_PERSON ASC "
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

                        strsqltablename = "T_WC_T2S_NOMINATION"
                        strsqldelete = """DELETE FROM T_WC_T2S_NOMINATION
WHERE NOT EXISTS (
    SELECT 1
    FROM T_WC_WIKIDATA_ITEM_PROPERTY w
    WHERE w.ID_PROPERTY = T_WC_T2S_NOMINATION.NOMINATION_SOURCE
      AND w.ID_ITEM = T_WC_T2S_NOMINATION.ID_WIKIDATA
);
                        """
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

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

                elif intindex == 45:
                    #----------------------------------------------------
                    print("T2S_MOVEMENT processing")

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
                                    strsqlmovies_wikidata = "SELECT DISTINCT m.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON m.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                    strsqlseries_wikidata = "SELECT DISTINCT s.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_WEIGHTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON s.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
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
                    if 1:
                        strsqltablename = "T_WC_T2S_MOVEMENT"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE MOVEMENT_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

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

                elif intindex == 46:
                    #----------------------------------------------------
                    print("T2S_DEATH processing")

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
                            strsql += "SELECT DISTINCT T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM "
                            strsql += "FROM T_WC_WIKIDATA_ITEM_PROPERTY "
                            strsql += "WHERE T_WC_WIKIDATA_ITEM_PROPERTY.ID_PROPERTY = '" + strpropertyid + "' "
                            if intgroup == 2:
                                strsql += "AND T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM NOT IN (" + strp1196excludeditems + ") "
                            strsql += "ORDER BY T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM ASC "
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
                                strsqlitem += "SELECT LABEL, DESCRIPTION, WIKIPEDIA_IMAGE_PATH "
                                strsqlitem += "FROM T_WC_WIKIDATA_ITEM_V1 "
                                strsqlitem += "WHERE ID_WIKIDATA = %s AND LANG = 'en'"
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

                                strsqlitem = ""
                                strsqlitem += "SELECT LABEL "
                                strsqlitem += "FROM T_WC_WIKIDATA_ITEM_V1 "
                                strsqlitem += "WHERE ID_WIKIDATA = %s AND LANG = 'fr'"
                                arrvalues = cp.f_fieldsfromquery(
                                    strsqlitem,
                                    "strrecordnamefr",
                                    "LABEL",
                                    params=(strrecordid,),
                                    target_dict=None,
                                )
                                strrecordnamefr = arrvalues.get("strrecordnamefr", "")

                                strrecorddeathtype = "death"
                                print("Processing record: " + str(strrecordid) + ": " + strrecordname + " (" + strrecorddeathsource + ")")

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
                                    strsqlpersons += "FROM T_WC_TMDB_PERSON "
                                    strsqlpersons += "INNER JOIN T_WC_WIKIDATA_PERSON_V1 ON T_WC_TMDB_PERSON.ID_WIKIDATA = T_WC_WIKIDATA_PERSON_V1.ID_WIKIDATA "
                                    strsqlpersons += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY ON T_WC_TMDB_PERSON.ID_WIKIDATA = T_WC_WIKIDATA_ITEM_PROPERTY.ID_WIKIDATA "
                                    strsqlpersons += "WHERE T_WC_WIKIDATA_ITEM_PROPERTY.ID_PROPERTY = %s "
                                    strsqlpersons += "AND T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM = %s "
                                    strsqlpersons += "ORDER BY T_WC_TMDB_PERSON.POPULARITY DESC "
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

                    if 1:
                        strsqltablename = "T_WC_T2S_DEATH"
                        strsqldelete = "DELETE FROM " + strsqltablename + " WHERE DEATH_TYPE IS NULL "
                        print(strsqldelete)
                        cursor2.execute(strsqldelete)

                        strsqltablename = "T_WC_T2S_DEATH"
                        strsqldelete = """DELETE FROM T_WC_T2S_DEATH
WHERE NOT EXISTS (
    SELECT 1
    FROM T_WC_WIKIDATA_ITEM_PROPERTY w
    WHERE w.ID_PROPERTY = T_WC_T2S_DEATH.DEATH_SOURCE
      AND w.ID_ITEM = T_WC_T2S_DEATH.ID_WIKIDATA
      AND NOT (
          w.ID_PROPERTY = 'P1196'
          AND w.ID_ITEM IN (""" + strp1196excludeditems + """ )
      )
);
                        """
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

                elif intindex == 4:
                    #----------------------------------------------------
                    print("T2S_MOVIE processing")
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from TMDB_MOVIE to T2S_MOVIE","Current sub process in the TMDb database movie preprocess",0)
                        # Get the maximum ID_MOVIE value from the database
                        cursor.execute("SELECT MAX(ID_MOVIE) as max_id FROM T_WC_TMDB_MOVIE")
                        result = cursor.fetchone()
                        lngmovierangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_MOVIE in database: {lngmovierangemax}")
                        
                        # Process database in chunks of 1000 records
                        lngchunksize = 250
                        lngtotalprocessed = 0
                        
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
    ASPECT_RATIO, FILM_FORMAT, SOUND_SYSTEM, SOUND_TECHNOLOGY, 
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
    ASPECT_RATIO, FILM_FORMAT, SOUND_SYSTEM, SOUND_TECHNOLOGY, 
    IS_MOVIE, IS_DOCUMENTARY, IS_SHORT_FILM, DELETED
FROM T_WC_TMDB_MOVIE
WHERE ADULT = 0 
AND ID_IMDB <> ''
AND ID_IMDB IS NOT NULL
AND ID_MOVIE >= {lngmovierangestart} AND ID_MOVIE <= {lngmovierangeend}
ON DUPLICATE KEY UPDATE
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
    ASPECT_RATIO = VALUES(ASPECT_RATIO),
    FILM_FORMAT = VALUES(FILM_FORMAT),
    SOUND_SYSTEM = VALUES(SOUND_SYSTEM),
    SOUND_TECHNOLOGY = VALUES(SOUND_TECHNOLOGY),
    IS_MOVIE = VALUES(IS_MOVIE),
    IS_DOCUMENTARY = VALUES(IS_DOCUMENTARY),
    IS_SHORT_FILM = VALUES(IS_SHORT_FILM),
    DELETED = VALUES(DELETED) """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()
                            
                            strsqlmoviesdelete = f"""
DELETE FROM T_WC_T2S_MOVIE 
WHERE ID_MOVIE >= {lngmovierangestart} AND ID_MOVIE <= {lngmovierangeend}
AND ID_MOVIE NOT IN (
    SELECT ID_MOVIE FROM T_WC_TMDB_MOVIE 
    WHERE ADULT = 0 AND ID_IMDB <> '' AND ID_IMDB IS NOT NULL
        AND ID_MOVIE >= {lngmovierangestart} AND ID_MOVIE <= {lngmovierangeend}
) """
                            cursor2.execute(strsqlmoviesdelete)
                            cp.connectioncp.commit()

                            strsqlmovies = f"""
UPDATE T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb 
    ON t2s.ID_IMDB = imdb.tconst
SET t2s.IMDB_RATING = imdb.averageRating
WHERE t2s.ID_MOVIE >= {lngmovierangestart} 
    AND t2s.ID_MOVIE <= {lngmovierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                            strsqlmovies = f"""
UPDATE T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb 
    ON t2s.ID_IMDB = imdb.tconst
CROSS JOIN (
    SELECT AVG(averageRating) AS C
    FROM T_WC_IMDB_MOVIE_RATING_IMPORT
    WHERE averageRating IS NOT NULL
      AND numVotes > 0
) stats
SET t2s.IMDB_RATING_WEIGHTED =
    ((imdb.numVotes / (imdb.numVotes + {lngimdbweightedratingm})) * imdb.averageRating) +
    (({lngimdbweightedratingm} / (imdb.numVotes + {lngimdbweightedratingm})) * stats.C)
WHERE t2s.ID_MOVIE >= {lngmovierangestart} 
    AND t2s.ID_MOVIE <= {lngmovierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL
    AND imdb.numVotes > 0 """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                            strsqlmovies = f"""
UPDATE T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_TMDB_MOVIE_LANG t 
    ON t2s.ID_MOVIE = t.ID_MOVIE
SET t2s.MOVIE_TITLE_FR = t.TITLE
WHERE t2s.ID_MOVIE >= {lngmovierangestart} 
    AND t2s.ID_MOVIE <= {lngmovierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> '' 
    AND t.LANG = 'fr' """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                            strsqlmovies = f"""
UPDATE T_WC_T2S_MOVIE t2s
INNER JOIN T_WC_WIKIDATA_MOVIE_V1 w 
    ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
SET t2s.WIKIDATA_TITLE = w.TITLE, 
    t2s.ALIASES = w.ALIASES, 
    t2s.PLEX_MEDIA_KEY = w.PLEX_MEDIA_KEY, 
    t2s.ID_CRITERION = w.ID_CRITERION, 
    t2s.ID_CRITERION_SPINE = w.ID_CRITERION_SPINE, 
    t2s.INSTANCE_OF = w.INSTANCE_OF 
WHERE t2s.ID_MOVIE >= {lngmovierangestart} 
    AND t2s.ID_MOVIE <= {lngmovierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> '' """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                    # Now copy Wikipedia content to the movie records
                    strsqlmovies = f"""
SELECT T_WC_TMDB_MOVIE.ID_MOVIE, T_WC_WIKIPEDIA_PAGE_LANG_SECTION.ID_WIKIDATA, 
T_WC_WIKIPEDIA_PAGE_LANG_SECTION.TITLE, T_WC_WIKIPEDIA_PAGE_LANG_SECTION.CONTENT 
FROM T_WC_WIKIPEDIA_PAGE_LANG_SECTION 
INNER JOIN T_WC_TMDB_MOVIE ON T_WC_WIKIPEDIA_PAGE_LANG_SECTION.ID_WIKIDATA = T_WC_TMDB_MOVIE.ID_WIKIDATA 
INNER JOIN T_WC_T2S_MOVIE ON T_WC_TMDB_MOVIE.ID_MOVIE = T_WC_T2S_MOVIE.ID_MOVIE 
WHERE T_WC_WIKIPEDIA_PAGE_LANG_SECTION.LANG = 'en' 
AND T_WC_WIKIPEDIA_PAGE_LANG_SECTION.ITEM_TYPE = 'movie' 
ORDER BY T_WC_TMDB_MOVIE.ID_MOVIE ASC, T_WC_WIKIPEDIA_PAGE_LANG_SECTION.DISPLAY_ORDER ASC """
                    print(strsqlmovies)
                    cursor.execute(strsqlmovies)
                    result = cursor.fetchall()
                    lngmovieidold = 0
                    strothersections = "|"
                    strsqltablename = "T_WC_T2S_MOVIE"
                    for row in result:
                        lngmovieid = row['ID_MOVIE']
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovieid",str(lngmovieid),"Current movie ID in the TMDb database preprocess",0)
                        strwikidataid = row['ID_WIKIDATA']
                        strtitle = row['TITLE'].strip()
                        strcontent = row['CONTENT'].strip()
                        if lngmovieidold != lngmovieid:
                            # This is a new movie 
                            if lngmovieidold != 0 and arrwikidatacouples:
                                # Insert data into the table only if there's data to update
                                strsqlupdatecondition = f"ID_MOVIE = {lngmovieidold}"
                                cp.f_sqlupdatearray(strsqltablename,arrwikidatacouples,strsqlupdatecondition,0)
                            arrwikidatacouples = {}
                            strothersections = "|"
                        if strtitle == "Intro":
                            #arrwikidatacouples["INTRO"] = strcontent
                            pass
                        elif strtitle == "External links":
                            #arrwikidatacouples["EXTERNAL_LINKS"] = strcontent
                            pass
                        elif strtitle == "References":
                            #arrwikidatacouples["REFERENCES"] = strcontent
                            pass
                        elif strtitle == "See also":
                            pass
                        elif strtitle == "Notes":
                            pass
                        elif strtitle == "Cast":
                            arrwikidatacouples["CAST"] = strcontent
                        elif strtitle == "Plot":
                            arrwikidatacouples["PLOT"] = strcontent
                        elif strtitle == "Production":
                            arrwikidatacouples["PRODUCTION"] = strcontent
                        elif strtitle == "Reception":
                            arrwikidatacouples["RECEPTION"] = strcontent
                        elif strtitle == "Soundtrack":
                            arrwikidatacouples["SOUNDTRACK"] = strcontent
                        elif strtitle == "Plot summary" and "PLOT" not in arrwikidatacouples:
                            arrwikidatacouples["PLOT"] = strcontent
                        elif strtitle == "Synopsis" and "PLOT" not in arrwikidatacouples:
                            arrwikidatacouples["PLOT"] = strcontent
                        elif strtitle == "Premise" and "PLOT" not in arrwikidatacouples:
                            arrwikidatacouples["PLOT"] = strcontent
                        elif strtitle == "Voice cast" and "CAST" not in arrwikidatacouples:
                            arrwikidatacouples["CAST"] = strcontent
                        elif strtitle == "Main characters" and "CAST" not in arrwikidatacouples:
                            arrwikidatacouples["CAST"] = strcontent
                        elif strtitle == "Reception and legacy" and "RECEPTION" not in arrwikidatacouples:
                            arrwikidatacouples["RECEPTION"] = strcontent
                        elif strtitle == "Release and reception" and "RECEPTION" not in arrwikidatacouples:
                            arrwikidatacouples["RECEPTION"] = strcontent
                        elif strtitle == "Release" and "RECEPTION" not in arrwikidatacouples:
                            arrwikidatacouples["RECEPTION"] = strcontent
                        elif strtitle == "Critical response" and "RECEPTION" not in arrwikidatacouples:
                            arrwikidatacouples["RECEPTION"] = strcontent
                        elif strtitle == "Release history" and "RECEPTION" not in arrwikidatacouples:
                            arrwikidatacouples["RECEPTION"] = strcontent
                        elif strtitle == "Reception and box office" and "RECEPTION" not in arrwikidatacouples:
                            arrwikidatacouples["RECEPTION"] = strcontent
                        elif strtitle == "Production notes" and "PRODUCTION" not in arrwikidatacouples:
                            arrwikidatacouples["PRODUCTION"] = strcontent
                        elif strtitle == "Production and release" and "PRODUCTION" not in arrwikidatacouples:
                            arrwikidatacouples["PRODUCTION"] = strcontent
                        elif strtitle == "Development and production" and "PRODUCTION" not in arrwikidatacouples:
                            arrwikidatacouples["PRODUCTION"] = strcontent
                        elif strtitle == "Development" and "PRODUCTION" not in arrwikidatacouples:
                            arrwikidatacouples["PRODUCTION"] = strcontent
                        elif strtitle == "Music" and "SOUNDTRACK" not in arrwikidatacouples:
                            arrwikidatacouples["SOUNDTRACK"] = strcontent
                        elif strtitle == "Soundtrack and score" and "SOUNDTRACK" not in arrwikidatacouples:
                            arrwikidatacouples["SOUNDTRACK"] = strcontent
                        else:
                            strothersections += strtitle + "|"
                            arrwikidatacouples["OTHER_SECTIONS"] = strothersections
                        lngmovieidold = lngmovieid
                    # This is the last movie 
                    if lngmovieidold != 0 and arrwikidatacouples:
                        # Insert data into the table only if there's data to update
                        strsqlupdatecondition = f"ID_MOVIE = {lngmovieidold}"
                        cp.f_sqlupdatearray(strsqltablename,arrwikidatacouples,strsqlupdatecondition,0)
                    cp.connectioncp.commit()

                    print(f"T2S_MOVIE processing completed. ")

                elif intindex == 5:
                    #----------------------------------------------------
                    print("T2S_SERIE processing")
                    if 1:
                        # Get the maximum ID_SERIE value from the database
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from TMDB_SERIE to T2S_SERIE","Current sub process in the TMDb database series preprocess",0)
                        cursor.execute("SELECT MAX(ID_SERIE) as max_id FROM T_WC_TMDB_SERIE")
                        result = cursor.fetchone()
                        lngserierangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_SERIE in database: {lngserierangemax}")
                        
                        # Process database in chunks of 1000 records
                        lngchunksize = 250
                        lngtotalprocessed = 0
                        
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
ON DUPLICATE KEY UPDATE
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
                            
                            strsqlseriesdelete = f"""
DELETE FROM T_WC_T2S_SERIE 
WHERE ID_SERIE >= {lngserierangestart} AND ID_SERIE <= {lngserierangeend}
AND ID_SERIE NOT IN (
    SELECT ID_SERIE FROM T_WC_TMDB_SERIE 
    WHERE ADULT = 0 AND ID_IMDB <> '' AND ID_IMDB IS NOT NULL
        AND ID_SERIE >= {lngserierangestart} AND ID_SERIE <= {lngserierangeend}
) """
                            cursor2.execute(strsqlseriesdelete)
                            cp.connectioncp.commit()

                            strsqlseries = f"""
UPDATE T_WC_T2S_SERIE t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb 
    ON t2s.ID_IMDB = imdb.tconst
SET t2s.IMDB_RATING = imdb.averageRating
WHERE t2s.ID_SERIE >= {lngserierangestart} 
    AND t2s.ID_SERIE <= {lngserierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL """
                            cursor2.execute(strsqlseries)
                            cp.connectioncp.commit()

                            strsqlseries = f"""
UPDATE T_WC_T2S_SERIE t2s
INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT imdb 
    ON t2s.ID_IMDB = imdb.tconst
CROSS JOIN (
    SELECT AVG(averageRating) AS C
    FROM T_WC_IMDB_MOVIE_RATING_IMPORT
    WHERE averageRating IS NOT NULL
      AND numVotes > 0
) stats
SET t2s.IMDB_RATING_WEIGHTED =
    ((imdb.numVotes / (imdb.numVotes + {lngimdbweightedratingm})) * imdb.averageRating) +
    (({lngimdbweightedratingm} / (imdb.numVotes + {lngimdbweightedratingm})) * stats.C)
WHERE t2s.ID_SERIE >= {lngserierangestart} 
    AND t2s.ID_SERIE <= {lngserierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> ''
    AND imdb.averageRating IS NOT NULL
    AND imdb.numVotes > 0 """
                            cursor2.execute(strsqlseries)
                            cp.connectioncp.commit()

                            strsqlseries = f"""
UPDATE T_WC_T2S_SERIE t2s
INNER JOIN T_WC_TMDB_SERIE_LANG t 
    ON t2s.ID_SERIE = t.ID_SERIE
SET t2s.SERIE_TITLE_FR = t.TITLE
WHERE t2s.ID_SERIE >= {lngserierangestart} 
    AND t2s.ID_SERIE <= {lngserierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> '' 
    AND t.LANG = 'fr' """
                            cursor2.execute(strsqlseries)
                            cp.connectioncp.commit()

                            strsqlseries = f"""
UPDATE T_WC_T2S_SERIE t2s
INNER JOIN T_WC_WIKIDATA_SERIE_V1 w 
    ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
SET t2s.WIKIDATA_TITLE = w.TITLE, 
    t2s.ALIASES = w.ALIASES, 
    t2s.PLEX_MEDIA_KEY = w.PLEX_MEDIA_KEY, 
    t2s.INSTANCE_OF = w.INSTANCE_OF 
WHERE t2s.ID_SERIE >= {lngserierangestart} 
    AND t2s.ID_SERIE <= {lngserierangeend}
    AND t2s.ID_IMDB IS NOT NULL
    AND t2s.ID_IMDB <> '' """
                            cursor2.execute(strsqlseries)
                            cp.connectioncp.commit()

                    # Now copy Wikipedia content to the serie records




                elif intindex == 6:
                    #----------------------------------------------------
                    print("T2S_PERSON processing")
                    if 1:
                        # Get the maximum ID_PERSON value from the database
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from TMDB_PERSON to T2S_PERSON","Current sub process in the TMDb database person preprocess",0)
                        cursor.execute("SELECT MAX(ID_PERSON) as max_id FROM T_WC_TMDB_PERSON")
                        result = cursor.fetchone()
                        lngpersonrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_PERSON in database: {lngpersonrangemax}")
                        
                        # Process database in chunks of 1000 records
                        lngchunksize = 1000
                        lngtotalprocessed = 0
                        
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
    ON DUPLICATE KEY UPDATE
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
                            
                            strsqlpersonsdelete = f"""
    DELETE FROM T_WC_T2S_PERSON 
    WHERE ID_PERSON >= {lngpersonrangestart} AND ID_PERSON <= {lngpersonrangeend}
    AND ID_PERSON NOT IN (
        SELECT ID_PERSON FROM T_WC_TMDB_PERSON 
        WHERE ADULT = 0 AND ID_IMDB <> '' AND ID_IMDB IS NOT NULL AND ID_WIKIDATA <> '' AND ID_WIKIDATA IS NOT NULL
            AND ID_PERSON >= {lngpersonrangestart} AND ID_PERSON <= {lngpersonrangeend}
    ) """
                            cursor2.execute(strsqlpersonsdelete)
                            cp.connectioncp.commit()

                            strsqlpersons = f"""
    UPDATE T_WC_T2S_PERSON t2s
    INNER JOIN T_WC_WIKIDATA_PERSON_V1 w 
        ON t2s.ID_WIKIDATA = w.ID_WIKIDATA
    SET t2s.WIKIDATA_NAME = w.NAME, 
        t2s.ALIASES = w.ALIASES, 
        t2s.INSTANCE_OF = w.INSTANCE_OF 
    WHERE t2s.ID_PERSON >= {lngpersonrangestart} 
        AND t2s.ID_PERSON <= {lngpersonrangeend}
        AND t2s.ID_IMDB IS NOT NULL
        AND t2s.ID_IMDB <> ''
        AND t2s.ID_WIKIDATA IS NOT NULL
        AND t2s.ID_WIKIDATA <> '' """
                            cursor2.execute(strsqlpersons)
                            cp.connectioncp.commit()

                elif intindex == 7:
                    #----------------------------------------------------
                    print("T2S_COMPANY processing")
                    if 1:
                        # Compute MOVIE_COUNT 
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Computing MOVIE_COUNT for T2S_COMPANY","Current sub process in the TMDb database company preprocess",0)
                        strsqlcompanies = """
SELECT COUNT(DISTINCT T_WC_T2S_MOVIE.ID_MOVIE) AS COMPTE, T_WC_TMDB_COMPANY.NAME, T_WC_TMDB_COMPANY.ID_COMPANY 
FROM T_WC_T2S_MOVIE 
JOIN T_WC_TMDB_MOVIE_COMPANY ON T_WC_T2S_MOVIE.ID_MOVIE = T_WC_TMDB_MOVIE_COMPANY.ID_MOVIE 
JOIN T_WC_TMDB_COMPANY ON T_WC_TMDB_MOVIE_COMPANY.ID_COMPANY = T_WC_TMDB_COMPANY.ID_COMPANY 
GROUP BY T_WC_TMDB_COMPANY.NAME 
ORDER BY COMPTE DESC """
                        print(strsqlcompanies)
                        cursor2.execute(strsqlcompanies)
                        print("Number of rows: " + str(cursor2.rowcount))
                        results = cursor2.fetchall()
                        for row in results:
                            print(row)
                            arrcompanycouples = {}
                            arrcompanycouples["MOVIE_COUNT"] = row['COMPTE']
                            cp.f_sqlupdatearray("T_WC_TMDB_COMPANY",arrcompanycouples,"ID_COMPANY = " + str(row['ID_COMPANY']),0)
                    if 1:
                        # Compute SERIE_COUNT 
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Computing SERIE_COUNT for T2S_COMPANY","Current sub process in the TMDb database company preprocess",0)
                        strsqlcompanies = """
SELECT COUNT(DISTINCT T_WC_T2S_SERIE.ID_SERIE) AS COMPTE, T_WC_TMDB_COMPANY.NAME, T_WC_TMDB_COMPANY.ID_COMPANY 
FROM T_WC_T2S_SERIE 
JOIN T_WC_TMDB_SERIE_COMPANY ON T_WC_T2S_SERIE.ID_SERIE = T_WC_TMDB_SERIE_COMPANY.ID_SERIE 
JOIN T_WC_TMDB_COMPANY ON T_WC_TMDB_SERIE_COMPANY.ID_COMPANY = T_WC_TMDB_COMPANY.ID_COMPANY 
GROUP BY T_WC_TMDB_COMPANY.NAME 
ORDER BY COMPTE DESC """
                        print(strsqlcompanies)
                        cursor2.execute(strsqlcompanies)
                        print("Number of rows: " + str(cursor2.rowcount))
                        results = cursor2.fetchall()
                        for row in results:
                            print(row)
                            arrcompanycouples = {}
                            arrcompanycouples["SERIE_COUNT"] = row['COMPTE']
                            cp.f_sqlupdatearray("T_WC_TMDB_COMPANY",arrcompanycouples,"ID_COMPANY = " + str(row['ID_COMPANY']),0)

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
                        # Compute SERIE_COUNT 
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Computing SERIE_COUNT for T2S_NETWORK","Current sub process in the TMDb database network preprocess",0)
                        strsqlnetworks = """
SELECT COUNT(DISTINCT T_WC_T2S_SERIE.ID_SERIE) AS COMPTE, T_WC_TMDB_NETWORK.NAME, T_WC_TMDB_NETWORK.ID_NETWORK 
FROM T_WC_T2S_SERIE 
JOIN T_WC_TMDB_SERIE_NETWORK ON T_WC_T2S_SERIE.ID_SERIE = T_WC_TMDB_SERIE_NETWORK.ID_SERIE 
JOIN T_WC_TMDB_NETWORK ON T_WC_TMDB_SERIE_NETWORK.ID_NETWORK = T_WC_TMDB_NETWORK.ID_NETWORK 
GROUP BY T_WC_TMDB_NETWORK.NAME 
ORDER BY COMPTE DESC """
                        print(strsqlnetworks)
                        cursor2.execute(strsqlnetworks)
                        print("Number of rows: " + str(cursor2.rowcount))
                        results = cursor2.fetchall()
                        for row in results:
                            print(row)
                            arrnetworkcouples = {}
                            arrnetworkcouples["SERIE_COUNT"] = row['COMPTE']
                            cp.f_sqlupdatearray("T_WC_TMDB_NETWORK",arrnetworkcouples,"ID_NETWORK = " + str(row['ID_NETWORK']),0)
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
                        strsql = """
INSERT INTO T_WC_T2S_MOVIE_IMAGE_BUILD (
    ID_ROW, ID_MOVIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
)
SELECT
    ID_ROW, ID_MOVIE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED,
    TYPE_IMAGE, LANG, IMAGE_PATH, ASPECT_RATIO, WIDTH, HEIGHT, VOTE_AVERAGE, VOTE_COUNT
FROM T_WC_TMDB_MOVIE_IMAGE
WHERE ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
"""
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
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
    ID_ROW, ID_WIKIDATA, LABEL, ALIASES, DESCRIPTION,
    WIKIPEDIA_IMAGE_PATH, INSTANCE_OF,
    DAT_CREAT, TIM_UPDATED,
    DELETED
FROM T_WC_WIKIDATA_ITEM_V1
WHERE LANG = 'en'
"""
                        cursor2.execute(strsqlitems)
                        cp.connectioncp.commit()

                        strsqlitems = """
UPDATE T_WC_T2S_ITEM_BUILD t2s
INNER JOIN T_WC_WIKIDATA_ITEM_V1 t
    ON t2s.ID_WIKIDATA = t.ID_WIKIDATA
SET t2s.ITEM_LABEL_FR = t.LABEL
WHERE t.LANG = 'fr'
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

                        strsql = "DROP TEMPORARY TABLE IF EXISTS TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE"
                        f_printsqlprocess48(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """CREATE TEMPORARY TABLE TMP_WC_TMDB_CHARACTER_SOURCE_MOVIE AS
SELECT
    ID_MOVIE,
    ID_PERSON,
    CAST_CHARACTER,
    replace(trim(regexp_replace(lcase(regexp_replace(CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','') AS CAST_CHARACTER_KEY
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
    replace(trim(regexp_replace(lcase(regexp_replace(CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','') AS CAST_CHARACTER_KEY
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

            print("------------------------------------------")
            strcurrentprocess = ""
            cp.f_setservervariable("strtmdbmoviepreprocesscurrentprocess",strcurrentprocess,"Current process in the TMDb database preprocess",0)
            strsql = ""
            cp.f_setservervariable("strtmdbmoviepreprocesscurrentsql",strsql,"Current SQL query in the TMDb database preprocess",0)
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strtmdbmoviepreprocessenddatetime",strnow,"Date and time of the TMDb database preprocess ending",0)
            # Calculate total runtime and convert to readable format
            end_time = time.time()
            strtotalruntime = int(end_time - start_time)  # Total runtime in seconds
            readable_duration = cp.convert_seconds_to_duration(strtotalruntime)
            cp.f_setservervariable("strtmdbmoviepreprocesstotalruntime",readable_duration,strtotalruntimedesc,0)
            print(f"Total runtime: {strtotalruntime} seconds ({readable_duration})")
    print("Process completed")
except pymysql.MySQLError as e:
    print(f"❌ MySQL Error: {e}")
    conn = getattr(cp, "connectioncp", None)
    if conn is not None and getattr(conn, "open", False):
        conn.rollback()

