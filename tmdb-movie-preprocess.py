import time
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
import spacy
import psutil
import re
import sys

if 0:
    #python -m spacy download fr_core_news_lg
    nlp = spacy.load("fr_core_news_lg")

# Global settings for pre processing
# Test settings 2024-11-29
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

def f_getlemma(sentence):
    # Tokenize the sentence
    doc = nlp(sentence)
    # Return tokens and their POS tags only for NOUN, PROPN, VERB, or ADJ
    lemmas = [(token.lemma_) for token in doc if token.pos_ in ["NOUN", "PROPN", "VERB", "ADJ", "X", "NUM"]]
    return " ".join(lemmas)

def f_tmdbpersonsetusedfortags(lngpersonid):
    if lngpersonid > 0:
        cursor2 = cp.connectioncp.cursor()
        strsqlupdate = "UPDATE T_WC_TMDB_PERSON SET USED_FOR_SIMILARITY = 1 WHERE ID_PERSON = " + str(lngpersonid)
        # print(strsqlupdate)
        cursor2.execute(strsqlupdate)
        # Commit the changes to the database
        cp.connectioncp.commit()

def f_wikidataitemproperties(strlang,stritemidwikidata,strpropertyid,strsep):
    strsql = ""
    strsql += "SELECT T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM, T_WC_WIKIDATA_ITEM_V1.LABEL, T_WC_WIKIDATA_ITEM_V1.ALIASES, T_WC_WIKIDATA_ITEM_V1.DESCRIPTION, T_WC_WIKIDATA_ITEM_V1.LANG "
    strsql += "FROM T_WC_WIKIDATA_ITEM_PROPERTY "
    strsql += "LEFT JOIN T_WC_WIKIDATA_ITEM_V1 ON T_WC_WIKIDATA_ITEM_PROPERTY.ID_ITEM = T_WC_WIKIDATA_ITEM_V1.ID_WIKIDATA "
    strsql += "WHERE T_WC_WIKIDATA_ITEM_PROPERTY.ID_WIKIDATA = '" + stritemidwikidata + "' "
    #strsql += "AND T_WC_WIKIDATA_ITEM_V1.LANG = '" + strlang + "' "
    strsql += "AND T_WC_WIKIDATA_ITEM_PROPERTY.ID_PROPERTY = '" + strpropertyid + "' "
    strsql += "ORDER BY T_WC_WIKIDATA_ITEM_PROPERTY.DISPLAY_ORDER "
    #print(strsql)
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
        'cinecolor': ['cinecolor','cinécolor'],
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
    #return '|'.join(sorted(found_technologies)) if found_technologies else ""
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
    #return ', '.join(sorted(found_tech)) if found_tech else None
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
            'westrex'  # Successor to Western Electric
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

def extract_format_components(text):
    """Extract format components from a format line."""
    components = {
        'SOUND_SYSTEM': None,
        'ASPECT_RATIO': None,
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
    
    # Extract aspect ratio
    aspect_ratio_patterns = [
        r'(\d+,\d+):1',  # e.g., 2,39:1
        r'(\d+\.\d+):1',  # e.g., 2.39:1
        r'(\d+:\d+)',     # e.g., 16:9
        r'(\d+/\d+)'      # e.g., 16/9 or 4/3
    ]
    
    for pattern in aspect_ratio_patterns:
        match = re.search(pattern, text)
        if match:
            ratio = match.group(1)
            # Convert dots to commas in decimal ratios
            if '.' in ratio and ':1' in text:
                ratio = ratio.replace('.', ',')
            components['ASPECT_RATIO'] = ratio
            break
    
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
            arrprocessscope = {1: 'WIKIPEDIA_FORMAT_LINE', 2: 'T2S_MOVIE_TECHNICAL', 3: 'T2S_TOPIC', 41: 'T2S_COLLECTION', 42: 'T2S_LIST', 43: 'T2S_GROUP', 44: 'T2S_AWARD', 47: 'T2S_NOMINATION', 45: 'T2S_MOVEMENT', 46: 'T2S_DEATH', 4: 'T2S_MOVIE', 5: 'T2S_SERIE', 6: 'T2S_PERSON', 7: 'T2S_COMPANY', 8: 'T2S_NETWORK', 9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE', 11: 'T2S_MOVIE_GENRE', 12: 'T2S_SERIE_GENRE', 13: 'T2S_MOVIE_COMPANY', 14: 'T2S_SERIE_COMPANY', 15: 'T2S_SERIE_NETWORK', 16: 'T2S_MOVIE_PRODUCTION_COUNTRY', 17: 'T2S_SERIE_PRODUCTION_COUNTRY', 18: 'T2S_MOVIE_SPOKEN_LANGUAGE', 19: 'T2S_SERIE_SPOKEN_LANGUAGE', 20: 'T2S_COMPANY_IMAGE', 21: 'T2S_MOVIE_IMAGE', 22: 'T2S_NETWORK_IMAGE', 23: 'T2S_PERSON_IMAGE', 24: 'T2S_SERIE_IMAGE', 25: 'T2S_MOVIE_VIDEO', 26: 'T2S_SERIE_VIDEO', 40: 'T2S_ITEM'}
            #arrprocessscope = {9: 'T2S_PERSON_MOVIE'}
            #arrprocessscope = {10: 'T2S_PERSON_SERIE'}
            #arrprocessscope = {9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE'}
            #arrprocessscope = {4: 'T2S_MOVIE', 5: 'T2S_SERIE'}
            #arrprocessscope = {7: 'T2S_COMPANY'}
            #arrprocessscope = {8: 'T2S_NETWORK'}
            #arrprocessscope = {3: 'T2S_TOPIC', 4: 'T2S_MOVIE', 5: 'T2S_SERIE', 6: 'T2S_PERSON', 7: 'T2S_COMPANY', 8: 'T2S_NETWORK', 9: 'T2S_PERSON_MOVIE', 10: 'T2S_PERSON_SERIE'}
            #arrprocessscope = {1: 'WIKIPEDIA_FORMAT_LINE'}
            #if strnow.startswith("2026-03-03"):
            #    arrprocessscope = {3: 'T2S_TOPIC'}
            #arrprocessscope = {30: 'TMDB_MOVIE_LANG_META'}
            #arrprocessscope = {40: 'T2S_ITEM'}
            #arrprocessscope = {41: 'T2S_COLLECTION'}
            #arrprocessscope = {41: 'T2S_COLLECTION', 42: 'T2S_LIST'}
            #arrprocessscope = {3: 'T2S_TOPIC'}
            #arrprocessscope = {43: 'T2S_GROUP'}
            #if strnow.startswith("2026-04-15"):
            #    arrprocessscope = {41: 'T2S_COLLECTION', 45: 'T2S_MOVEMENT', 46: 'T2S_DEATH'}
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

                    #arrtopics = {1: 'en-list', 2: 'fr-list', 3: 'en-collection', 4: 'fr-collection', 5: 'en-keyword'}
                    # Lists and collections are copied to the Topic table anymore!
                    arrtopics = {5: 'en-keyword'}
                    for inttopic, strtopic in arrtopics.items():
                        strsql = ""
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess",strtopic,"Current sub process in the TMDb database movie preprocess",0)
                        if inttopic == 1:
                            strcurrentprocess = f"{inttopic}: Copying from TMDB_LIST to T2S_TOPIC"
                            strsql += "SELECT 'list' AS TOPIC_SOURCE, 'collection' AS TOPIC_TYPE, T_WC_TMDB_LIST.ID_LIST AS ID_RECORD, T_WC_TMDB_LIST.NAME, T_WC_TMDB_LIST.DESCRIPTION AS OVERVIEW, 'en' AS LANG, T_WC_TMDB_LIST.POSTER_PATH, NULL AS ID_WIKIDATA "
                            strsql += "FROM T_WC_TMDB_LIST WHERE USE_FOR_TAGGING > 0 "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "TOPIC_NAME"
                        elif inttopic == 2:
                            strcurrentprocess = f"{inttopic}: Copying from T_WC_TMDB_LIST_LANG to T2S_TOPIC"
                            strsql += "SELECT 'list' AS TOPIC_SOURCE, 'collection' AS TOPIC_TYPE, T_WC_TMDB_LIST.ID_LIST AS ID_RECORD, T_WC_TMDB_LIST_LANG.SHORT_NAME AS NAME, '' AS OVERVIEW, T_WC_TMDB_LIST_LANG.LANG, '' AS POSTER_PATH, NULL AS ID_WIKIDATA "
                            strsql += "FROM T_WC_TMDB_LIST "
                            strsql += "INNER JOIN T_WC_TMDB_LIST_LANG ON T_WC_TMDB_LIST.ID_LIST = T_WC_TMDB_LIST_LANG.ID_LIST "
                            strsql += "WHERE T_WC_TMDB_LIST.USE_FOR_TAGGING > 0 "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "TOPIC_NAME_FR"
                        elif inttopic == 3:
                            strcurrentprocess = f"{inttopic}: Copying from TMDB_COLLECTION to T2S_TOPIC"
                            strsql += "SELECT 'collection' AS TOPIC_SOURCE, 'collection' AS TOPIC_TYPE, T_WC_TMDB_COLLECTION.ID_COLLECTION AS ID_RECORD, T_WC_TMDB_COLLECTION.NAME, T_WC_TMDB_COLLECTION.OVERVIEW, 'en' AS LANG, T_WC_TMDB_COLLECTION.POSTER_PATH, NULL AS ID_WIKIDATA "
                            strsql += "FROM T_WC_TMDB_COLLECTION "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "TOPIC_NAME"
                        elif inttopic == 4:
                            strcurrentprocess = f"{inttopic}: Copying from T_WC_TMDB_COLLECTION_LANG to T2S_TOPIC"
                            strsql += "SELECT 'collection' AS TOPIC_SOURCE, 'collection' AS TOPIC_TYPE, T_WC_TMDB_COLLECTION.ID_COLLECTION AS ID_RECORD, T_WC_TMDB_COLLECTION_LANG.NAME, T_WC_TMDB_COLLECTION_LANG.OVERVIEW, T_WC_TMDB_COLLECTION_LANG.LANG, T_WC_TMDB_COLLECTION_LANG.POSTER_PATH, NULL AS ID_WIKIDATA "
                            strsql += "FROM T_WC_TMDB_COLLECTION "
                            strsql += "INNER JOIN T_WC_TMDB_COLLECTION_LANG ON T_WC_TMDB_COLLECTION.ID_COLLECTION = T_WC_TMDB_COLLECTION_LANG.ID_COLLECTION "
                            strsql += "ORDER BY ID_RECORD ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            target_field_name = "TOPIC_NAME_FR"
                        elif inttopic == 5:
                            strcurrentprocess = f"{inttopic}: Copying from TMDB_KEYWORD to T2S_TOPIC"
                            strsql += "SELECT 'keyword' AS TOPIC_SOURCE, 'keyword' AS TOPIC_TYPE, T_WC_TMDB_KEYWORD.ID_KEYWORD AS ID_RECORD, T_WC_TMDB_KEYWORD.NAME, '' AS OVERVIEW, 'en' AS LANG, '' AS POSTER_PATH, NULL AS ID_WIKIDATA "
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
                                        'ID_WIKIDATA': strrecordidwikidata
                                    }
                                elif target_field_name == "TOPIC_NAME_FR":
                                    arrtopiccouples = {
                                        'ID_RECORD': lngrecordid,
                                        'TOPIC_NAME_FR': strrecordname,
                                        'TOPIC_SOURCE': strrecordtopicsource,
                                        'TOPIC_TYPE': strrecordtopictype,
                                        'ID_WIKIDATA': strrecordidwikidata
                                    }
                                strsqltablename = "T_WC_T2S_TOPIC"
                                strsqlupdatecondition = f"ID_RECORD = '{lngrecordid}' AND TOPIC_SOURCE = '{strrecordtopicsource}'"
                                cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(lngrecordid),"Current record in the TMDb database movie preprocess",0)
                                
                                strsqlmovies = ""
                                strsqlseries = ""
                                if inttopic == 1 or inttopic == 2:
                                    # Retrieving movies for this list by excluding adult movies and movies without Wikidata ID
                                    strsqlmovies += "SELECT ID_MOVIE, DISPLAY_ORDER "
                                    strsqlmovies += "FROM T_WC_TMDB_MOVIE_LIST "
                                    strsqlmovies += "WHERE ID_LIST = " + str(lngrecordid) + " "
                                    strsqlmovies += "AND DELETED = 0 "
                                    strsqlmovies += "AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_TMDB_MOVIE WHERE ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '') "
                                    strsqlmovies += "ORDER BY DISPLAY_ORDER "
                                    # Retrieving series for this list by excluding adult series and series without Wikidata ID
                                    strsqlseries += "SELECT ID_SERIE, DISPLAY_ORDER "
                                    strsqlseries += "FROM T_WC_TMDB_SERIE_LIST "
                                    strsqlseries += "WHERE ID_LIST = " + str(lngrecordid) + " "
                                    strsqlseries += "AND DELETED = 0 "
                                    strsqlseries += "AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_TMDB_SERIE WHERE ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '') "
                                    strsqlseries += "ORDER BY DISPLAY_ORDER "
                                elif inttopic == 3 or inttopic == 4:
                                    # Retrieving movies for this collection by excluding adult movies and movies without Wikidata ID
                                    strsqlmovies += "SELECT ID_MOVIE, 0 AS DISPLAY_ORDER "
                                    strsqlmovies += "FROM T_WC_TMDB_MOVIE "
                                    strsqlmovies += "WHERE ID_COLLECTION = " + str(lngrecordid) + " "
                                    strsqlmovies += "AND DELETED = 0 "
                                    strsqlmovies += "AND ADULT = 0 "
                                    strsqlmovies += "AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
                                    strsqlmovies += "ORDER BY ID_MOVIE "
                                elif inttopic == 5:
                                    # Retrieving movies for this keyword by excluding adult movies and movies without Wikidata ID
                                    strsqlmovies += "SELECT ID_MOVIE, DISPLAY_ORDER "
                                    strsqlmovies += "FROM T_WC_TMDB_MOVIE_KEYWORD "
                                    strsqlmovies += "WHERE ID_KEYWORD = " + str(lngrecordid) + " "
                                    strsqlmovies += "AND DELETED = 0 "
                                    strsqlmovies += "AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_TMDB_MOVIE WHERE ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '') "
                                    strsqlmovies += "ORDER BY ID_MOVIE "
                                    # Retrieving series for this keyword by excluding adult series and series without Wikidata ID
                                    strsqlseries += "SELECT ID_SERIE, DISPLAY_ORDER "
                                    strsqlseries += "FROM T_WC_TMDB_SERIE_KEYWORD "
                                    strsqlseries += "WHERE ID_KEYWORD = " + str(lngrecordid) + " "
                                    strsqlseries += "AND DELETED = 0 "
                                    strsqlseries += "AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_TMDB_SERIE WHERE ADULT = 0 AND ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '') "
                                    strsqlseries += "ORDER BY DISPLAY_ORDER "
                                if strsqlmovies != "":
                                    # Retrieving elements for this topic (list/collection/keyword)
                                    cursor2.execute(strsqlmovies)
                                    lngmoviecount = cursor2.rowcount
                                    lngseriescount = 0
                                    #print(f"{lngmoviecount} lines")
                                    if strsqlseries != "":
                                        cursor4.execute(strsqlseries)
                                        lngseriescount = cursor4.rowcount
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
                                        if inttopic == 1 or inttopic == 3 or inttopic == 5:
                                            # Retrieve all movies for this topic
                                            # Only processing when handling original English (records from T_WC_TMDB_LIST or T_WC_TMDB_COLLECTION or T_WC_TMDB_KEYWORD) to avoid duplicates with the translated versions
                                            results = cursor2.fetchall()
                                            lngdisplayorderprev = 0
                                            for row in results:
                                                lngmovieid = row["ID_MOVIE"]
                                                lngdisplayorder = row["DISPLAY_ORDER"]
                                                if lngdisplayorder is None:
                                                    lngdisplayorder = lngdisplayorderprev
                                                else:
                                                    lngdisplayorderprev = lngdisplayorder
                                                arrmovietopiccouples = {
                                                    'ID_MOVIE': lngmovieid,
                                                    'ID_TOPIC': lngtopicid,
                                                    'DISPLAY_ORDER': lngdisplayorder
                                                }
                                                strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND ID_TOPIC = " + str(lngtopicid) + " AND DISPLAY_ORDER = " + str(lngdisplayorder)
                                                #print(strsqlupdatecondition2)
                                                cp.f_sqlupdatearray("T_WC_T2S_MOVIE_TOPIC", arrmovietopiccouples, strsqlupdatecondition2, 1)
                                            if strsqlseries != "":
                                                # Retrieve all series for this topic
                                                results = cursor4.fetchall()
                                                lngdisplayorderprev = 0
                                                for row in results:
                                                    lngseriesid = row["ID_SERIE"]
                                                    lngdisplayorder = row["DISPLAY_ORDER"]
                                                    if lngdisplayorder is None:
                                                        lngdisplayorder = lngdisplayorderprev
                                                    else:
                                                        lngdisplayorderprev = lngdisplayorder
                                                    arrserietopiccouples = {
                                                        'ID_SERIE': lngseriesid,
                                                        'ID_TOPIC': lngtopicid,
                                                        'DISPLAY_ORDER': lngdisplayorder
                                                    }
                                                    strsqlupdatecondition2 = "ID_SERIE = " + str(lngseriesid) + " AND ID_TOPIC = " + str(lngtopicid) + " AND DISPLAY_ORDER = " + str(lngdisplayorder)
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
                        
                        # Update T_WC_T2S_TOPIC.IMDB_RATING and T_WC_T2S_TOPIC.IMDB_RATING_ADJUSTED 
                        strsql = """UPDATE T_WC_T2S_TOPIC t
JOIN (
    SELECT
        mt.ID_TOPIC,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_ADJUSTED) AS AVG_IMDB_RATING_ADJUSTED
    FROM T_WC_T2S_MOVIE_TOPIC mt
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mt.ID_MOVIE
    INNER JOIN T_WC_T2S_TOPIC t2
        ON t2.ID_TOPIC = mt.ID_TOPIC
       AND t2.TOPIC_TYPE = 'collection'
    GROUP BY mt.ID_TOPIC
) x
    ON x.ID_TOPIC = t.ID_TOPIC
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_ADJUSTED = x.AVG_IMDB_RATING_ADJUSTED;
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
                                        'ID_WIKIDATA': strrecordidwikidata
                                    }
                                    if intcollection == 5:
                                        arrcollectioncouples['COLLECTION_NAME_FR'] = row['NAME_FR'] or ''
                                elif target_field_name == "COLLECTION_NAME_FR":
                                    arrcollectioncouples = {
                                        'ID_RECORD': lngrecordid,
                                        'COLLECTION_NAME_FR': strrecordname,
                                        'COLLECTION_SOURCE': strrecordcollectionsource,
                                        'COLLECTION_TYPE': strrecordcollectiontype,
                                        'ID_WIKIDATA': strrecordidwikidata
                                    }
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
                                        strsqlmovies_imdb = "SELECT m.ID_MOVIE, FIELD(m.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE m.ID_IMDB IN (" + strimdbidlist + ") AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_imdb = "SELECT s.ID_SERIE, FIELD(s.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE s.ID_IMDB IN (" + strimdbidlist + ") AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Mechanism 2: Wikidata property/item filter from WIKIDATA_PROPERTIES
                                    strwikidataproperties = row['WIKIDATA_PROPERTIES'] or ''
                                    arrwdtokens = re.findall(r'[PQ]\d+', strwikidataproperties)
                                    strwdpropertyid = next((t for t in arrwdtokens if t.startswith('P')), '')
                                    strwditemid = next((t for t in arrwdtokens if t.startswith('Q')), '')
                                    if strwditemid:
                                        arrcollectioncouples['ID_WIKIDATA'] = strwditemid
                                    strsqlmovies_wikidata = ""
                                    strsqlseries_wikidata = ""
                                    if strwdpropertyid and strwditemid:
                                        strsqlmovies_wikidata = "SELECT DISTINCT m.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON m.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_wikidata = "SELECT DISTINCT s.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON s.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Mechanism 3: TMDb keyword filter from TMDB_ELEMENTS
                                    strtmdbelements = row['TMDB_ELEMENTS'] or ''
                                    strsqlmovies_keyword = ""
                                    strsqlseries_keyword = ""
                                    strkeywordmatch = re.search(r"T_WC_TMDB_KEYWORD\.NAME\s*=\s*'([^']+)'", strtmdbelements.replace('&#039;', "'"))
                                    if strkeywordmatch:
                                        strkeywordname = strkeywordmatch.group(1).strip().replace("'", "''")
                                        strsqlmovies_keyword = "SELECT mk.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE_KEYWORD mk INNER JOIN T_WC_TMDB_KEYWORD k ON mk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = mk.ID_MOVIE INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE k.NAME = '" + strkeywordname + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_keyword = "SELECT sk.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE_KEYWORD sk INNER JOIN T_WC_TMDB_KEYWORD k ON sk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = sk.ID_SERIE INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE k.NAME = '" + strkeywordname + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Combine mechanisms cumulatively
                                    arrsqlmovies_sources = [s for s in [strsqlmovies_imdb, strsqlmovies_wikidata, strsqlmovies_keyword] if s]
                                    arrsqlseries_sources = [s for s in [strsqlseries_imdb, strsqlseries_wikidata, strsqlseries_keyword] if s]
                                    strsqlmovies = f_buildcustomaggregatequery(arrsqlmovies_sources, "ID_MOVIE", "IMDB_RATING_ADJUSTED", intsortby)
                                    strsqlseries = f_buildcustomaggregatequery(arrsqlseries_sources, "ID_SERIE", "IMDB_RATING_ADJUSTED", intsortby)
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
                        
                        # Update T_WC_T2S_COLLECTION.IMDB_RATING and T_WC_T2S_COLLECTION.IMDB_RATING_ADJUSTED 
                        strsql = """UPDATE T_WC_T2S_COLLECTION t
JOIN (
    SELECT
        mt.ID_T2S_COLLECTION,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_ADJUSTED) AS AVG_IMDB_RATING_ADJUSTED
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
    t.IMDB_RATING_ADJUSTED = x.AVG_IMDB_RATING_ADJUSTED;
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
                            strsql += "SELECT 'custom' AS LIST_SOURCE, 'list' AS LIST_TYPE, T_WC_CUSTOM_LIST.ID_CUSTOM_LIST AS ID_RECORD, T_WC_CUSTOM_LIST.LIST_NAME AS NAME, T_WC_CUSTOM_LIST.LIST_NAME_FR AS NAME_FR, T_WC_CUSTOM_LIST.OVERVIEW AS OVERVIEW, 'en' AS LANG, T_WC_CUSTOM_LIST.POSTER_PATH, NULL AS ID_WIKIDATA, T_WC_CUSTOM_LIST.ID_IMDB_LIST, T_WC_CUSTOM_LIST.WIKIDATA_PROPERTIES, T_WC_CUSTOM_LIST.TMDB_ELEMENTS, T_WC_CUSTOM_LIST.SORT_BY "
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
                                print("Processing record: " + str(lngrecordid) + ": " + strrecordname + " (" + strrecordlistsource + ")")
                                if target_field_name == "LIST_NAME":
                                    arrlistcouples = {
                                        'ID_RECORD': lngrecordid,
                                        'LIST_NAME': strrecordname,
                                        'OVERVIEW': strrecordoverview,
                                        'LIST_SOURCE': strrecordlistsource,
                                        'LIST_TYPE': strrecordlisttype,
                                        'POSTER_PATH': strrecordposterpath,
                                        'ID_WIKIDATA': strrecordidwikidata
                                    }
                                    if intlist == 3:
                                        arrlistcouples['LIST_NAME_FR'] = row['NAME_FR'] or ''
                                elif target_field_name == "LIST_NAME_FR":
                                    arrlistcouples = {
                                        'ID_RECORD': lngrecordid,
                                        'LIST_NAME_FR': strrecordname,
                                        'LIST_SOURCE': strrecordlistsource,
                                        'LIST_TYPE': strrecordlisttype,
                                        'ID_WIKIDATA': strrecordidwikidata
                                    }
                                strsqltablename = "T_WC_T2S_LIST"
                                strsqlupdatecondition = f"ID_RECORD = '{lngrecordid}' AND LIST_SOURCE = '{strrecordlistsource}'"
                                cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(lngrecordid),"Current record in the TMDb database movie preprocess",0)
                                
                                strsqlmovies = ""
                                strsqlseries = ""
                                if intlist == 1 or intlist == 2:
                                    # Retrieving movies for this list by excluding adult movies and movies without Wikidata ID
                                    strsqlmovies += "SELECT ml.ID_MOVIE, m.IMDB_RATING_ADJUSTED "
                                    strsqlmovies += "FROM T_WC_TMDB_MOVIE_LIST ml "
                                    strsqlmovies += "INNER JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = ml.ID_MOVIE "
                                    strsqlmovies += "WHERE ml.ID_LIST = " + str(lngrecordid) + " "
                                    strsqlmovies += "AND ml.DELETED = 0 "
                                    strsqlmovies += "AND m.ADULT = 0 "
                                    strsqlmovies += "AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                    strsqlmovies += "ORDER BY m.IMDB_RATING_ADJUSTED DESC, ml.ID_MOVIE ASC "
                                    # Retrieving series for this list by excluding adult series and series without Wikidata ID
                                    strsqlseries += "SELECT sl.ID_SERIE, s.IMDB_RATING_ADJUSTED "
                                    strsqlseries += "FROM T_WC_TMDB_SERIE_LIST sl "
                                    strsqlseries += "INNER JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = sl.ID_SERIE "
                                    strsqlseries += "WHERE sl.ID_LIST = " + str(lngrecordid) + " "
                                    strsqlseries += "AND sl.DELETED = 0 "
                                    strsqlseries += "AND s.ADULT = 0 "
                                    strsqlseries += "AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    strsqlseries += "ORDER BY s.IMDB_RATING_ADJUSTED DESC, sl.ID_SERIE ASC "
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
                                        strsqlmovies_imdb = "SELECT m.ID_MOVIE, FIELD(m.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE m.ID_IMDB IN (" + strimdbidlist + ") AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_imdb = "SELECT s.ID_SERIE, FIELD(s.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE s.ID_IMDB IN (" + strimdbidlist + ") AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Mechanism 2: Wikidata property/item filter from WIKIDATA_PROPERTIES
                                    strwikidataproperties = row['WIKIDATA_PROPERTIES'] or ''
                                    arrwdtokens = re.findall(r'[PQ]\d+', strwikidataproperties)
                                    strwdpropertyid = next((t for t in arrwdtokens if t.startswith('P')), '')
                                    strwditemid = next((t for t in arrwdtokens if t.startswith('Q')), '')
                                    if strwditemid:
                                        arrlistcouples['ID_WIKIDATA'] = strwditemid
                                    strsqlmovies_wikidata = ""
                                    strsqlseries_wikidata = ""
                                    if strwdpropertyid and strwditemid:
                                        strsqlmovies_wikidata = "SELECT DISTINCT m.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON m.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_wikidata = "SELECT DISTINCT s.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON s.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Mechanism 3: TMDb keyword filter from TMDB_ELEMENTS
                                    strtmdbelements = row['TMDB_ELEMENTS'] or ''
                                    strsqlmovies_keyword = ""
                                    strsqlseries_keyword = ""
                                    strkeywordmatch = re.search(r"T_WC_TMDB_KEYWORD\.NAME\s*=\s*'([^']+)'", strtmdbelements.replace('&#039;', "'"))
                                    if strkeywordmatch:
                                        strkeywordname = strkeywordmatch.group(1).strip().replace("'", "''")
                                        strsqlmovies_keyword = "SELECT mk.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE_KEYWORD mk INNER JOIN T_WC_TMDB_KEYWORD k ON mk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = mk.ID_MOVIE INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE k.NAME = '" + strkeywordname + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                        strsqlseries_keyword = "SELECT sk.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE_KEYWORD sk INNER JOIN T_WC_TMDB_KEYWORD k ON sk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = sk.ID_SERIE INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE k.NAME = '" + strkeywordname + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    # Combine mechanisms cumulatively
                                    arrsqlmovies_sources = [s for s in [strsqlmovies_imdb, strsqlmovies_wikidata, strsqlmovies_keyword] if s]
                                    arrsqlseries_sources = [s for s in [strsqlseries_imdb, strsqlseries_wikidata, strsqlseries_keyword] if s]
                                    strsqlmovies = f_buildcustomaggregatequery(arrsqlmovies_sources, "ID_MOVIE", "IMDB_RATING_ADJUSTED", intsortby)
                                    strsqlseries = f_buildcustomaggregatequery(arrsqlseries_sources, "ID_SERIE", "IMDB_RATING_ADJUSTED", intsortby)

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
                                        # This list has more than one element (movie or serie)
                                        # So we create/update this list
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
                                            arrcurrentmovieids = []
                                            for row in results:
                                                lngmovieid = row["ID_MOVIE"]
                                                lngdisplayorder += 1
                                                arrcurrentmovieids.append(str(lngmovieid))
                                                arrmovielistcouples = {
                                                    'ID_MOVIE': lngmovieid,
                                                    'ID_T2S_LIST': lnglistid,
                                                    'DISPLAY_ORDER': lngdisplayorder
                                                }
                                                strsqlupdatecondition2 = "ID_MOVIE = " + str(lngmovieid) + " AND ID_T2S_LIST = " + str(lnglistid)
                                                #print(strsqlupdatecondition2)
                                                cp.f_sqlupdatearray("T_WC_T2S_MOVIE_LIST", arrmovielistcouples, strsqlupdatecondition2, 1)
                                            if arrcurrentmovieids:
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
                                                arrcurrentserieids = []
                                                for row in results:
                                                    lngseriesid = row["ID_SERIE"]
                                                    lngdisplayorder += 1
                                                    arrcurrentserieids.append(str(lngseriesid))
                                                    arrserielistcouples = {
                                                        'ID_SERIE': lngseriesid,
                                                        'ID_T2S_LIST': lnglistid,
                                                        'DISPLAY_ORDER': lngdisplayorder
                                                    }
                                                    strsqlupdatecondition2 = "ID_SERIE = " + str(lngseriesid) + " AND ID_T2S_LIST = " + str(lnglistid)
                                                    #print(strsqlupdatecondition2)
                                                    cp.f_sqlupdatearray("T_WC_T2S_SERIE_LIST", arrserielistcouples, strsqlupdatecondition2, 1)
                                                if arrcurrentserieids:
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
                                            cp.f_sqlupdatearray(strsqltablename, arrlistcouples, strsqlupdatecondition, 1)
                                    else:
                                        # This list has only one element or none
                                        # So we delete this list if it already exists
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
                        
                        # Update T_WC_T2S_LIST.IMDB_RATING and T_WC_T2S_LIST.IMDB_RATING_ADJUSTED 
                        strsql = """UPDATE T_WC_T2S_LIST t
JOIN (
    SELECT
        mt.ID_T2S_LIST,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_ADJUSTED) AS AVG_IMDB_RATING_ADJUSTED
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
    t.IMDB_RATING_ADJUSTED = x.AVG_IMDB_RATING_ADJUSTED;
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

                    arrgroups = {1: 'en-group', 2: 'en-employer', 3: 'custom-group'}    
                    for intgroup, strgroup in arrgroups.items():
                        strsql = ""
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess",strgroup,"Current sub process in the TMDb database person preprocess",0)
                        if intgroup == 1:
                            strpropertyid = "P463"
                        elif intgroup == 2:
                            strpropertyid = "P108"
                        elif intgroup == 3:
                            strpropertyid = ""
                        else:
                            strpropertyid = ""
                        if intgroup == 3:
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
                                if intgroup == 3:
                                    strrecordid = str(row['ID_RECORD'])
                                    strrecordname = row['NAME'] or ''
                                    strrecordnamefr = row['NAME_FR'] or ''
                                    strrecordoverview = row['OVERVIEW'] or ''
                                    strrecordposterpath = row['POSTER_PATH'] or ''
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
                                        'WIKIPEDIA_IMAGE_PATH': strrecordposterpath
                                    }
                                strsqltablename = "T_WC_T2S_GROUP"
                                strsqlupdatecondition = f"ID_WIKIDATA = '{strrecordid}' AND GROUP_SOURCE = '{strrecordgroupsource}'"
                                cp.f_setservervariable("strtmdbmoviepreprocesscurrentrecord",str(strrecordid),"Current record in the TMDb database movie preprocess",0)
                                
                                strsqlpersons = ""
                                if intgroup == 3:
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
                                elif intgroup == 1 or intgroup == 2:
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
                                    if intgroup == 3:
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
                                        if intgroup == 1 or intgroup == 2 or intgroup == 3:
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
                        strsqlmovies += "SELECT DISTINCT m.ID_MOVIE, m.IMDB_RATING_ADJUSTED "
                        strsqlmovies += "FROM T_WC_T2S_MOVIE m "
                        strsqlmovies += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY p ON p.ID_WIKIDATA = m.ID_WIKIDATA "
                        strsqlmovies += "WHERE p.ID_PROPERTY = %s AND p.ID_ITEM = %s "
                        strsqlmovies += "AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                        strsqlmovies += "ORDER BY m.IMDB_RATING_ADJUSTED DESC, m.ID_MOVIE ASC "
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
                        strsqlseries += "SELECT DISTINCT s.ID_SERIE, s.IMDB_RATING_ADJUSTED "
                        strsqlseries += "FROM T_WC_T2S_SERIE s "
                        strsqlseries += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY p ON p.ID_WIKIDATA = s.ID_WIKIDATA "
                        strsqlseries += "WHERE p.ID_PROPERTY = %s AND p.ID_ITEM = %s "
                        strsqlseries += "AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                        strsqlseries += "ORDER BY s.IMDB_RATING_ADJUSTED DESC, s.ID_SERIE ASC "
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
        AVG(m.IMDB_RATING_ADJUSTED) AS AVG_IMDB_RATING_ADJUSTED
    FROM T_WC_T2S_MOVIE_AWARD mt
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mt.ID_MOVIE
    GROUP BY mt.ID_AWARD
) x
    ON x.ID_AWARD = t.ID_AWARD
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_ADJUSTED = x.AVG_IMDB_RATING_ADJUSTED;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_AWARD ratings from series
                        strsql = """UPDATE T_WC_T2S_AWARD t
JOIN (
    SELECT
        mt.ID_AWARD,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_ADJUSTED) AS AVG_IMDB_RATING_ADJUSTED
    FROM T_WC_T2S_SERIE_AWARD mt
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = mt.ID_SERIE
    GROUP BY mt.ID_AWARD
) x
    ON x.ID_AWARD = t.ID_AWARD
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_ADJUSTED = COALESCE(t.IMDB_RATING_ADJUSTED, x.AVG_IMDB_RATING_ADJUSTED);
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
                        strsqlmovies += "SELECT DISTINCT m.ID_MOVIE, m.IMDB_RATING_ADJUSTED "
                        strsqlmovies += "FROM T_WC_T2S_MOVIE m "
                        strsqlmovies += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY p ON p.ID_WIKIDATA = m.ID_WIKIDATA "
                        strsqlmovies += "WHERE p.ID_PROPERTY = %s AND p.ID_ITEM = %s "
                        strsqlmovies += "AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                        strsqlmovies += "ORDER BY m.IMDB_RATING_ADJUSTED DESC, m.ID_MOVIE ASC "
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
                        strsqlseries += "SELECT DISTINCT s.ID_SERIE, s.IMDB_RATING_ADJUSTED "
                        strsqlseries += "FROM T_WC_T2S_SERIE s "
                        strsqlseries += "INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY p ON p.ID_WIKIDATA = s.ID_WIKIDATA "
                        strsqlseries += "WHERE p.ID_PROPERTY = %s AND p.ID_ITEM = %s "
                        strsqlseries += "AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                        strsqlseries += "ORDER BY s.IMDB_RATING_ADJUSTED DESC, s.ID_SERIE ASC "
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
        AVG(m.IMDB_RATING_ADJUSTED) AS AVG_IMDB_RATING_ADJUSTED
    FROM T_WC_T2S_MOVIE_NOMINATION mt
    INNER JOIN T_WC_T2S_MOVIE m
        ON m.ID_MOVIE = mt.ID_MOVIE
    GROUP BY mt.ID_NOMINATION
) x
    ON x.ID_NOMINATION = t.ID_NOMINATION
SET
    t.IMDB_RATING = x.AVG_IMDB_RATING,
    t.IMDB_RATING_ADJUSTED = x.AVG_IMDB_RATING_ADJUSTED;
                        """
                        print(strsql)
                        cursor2.execute(strsql)

                        # Update T_WC_T2S_NOMINATION ratings from series
                        strsql = """UPDATE T_WC_T2S_NOMINATION t
JOIN (
    SELECT
        mt.ID_NOMINATION,
        AVG(s.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(s.IMDB_RATING_ADJUSTED) AS AVG_IMDB_RATING_ADJUSTED
    FROM T_WC_T2S_SERIE_NOMINATION mt
    INNER JOIN T_WC_T2S_SERIE s
        ON s.ID_SERIE = mt.ID_SERIE
    GROUP BY mt.ID_NOMINATION
) x
    ON x.ID_NOMINATION = t.ID_NOMINATION
SET
    t.IMDB_RATING = COALESCE(t.IMDB_RATING, x.AVG_IMDB_RATING),
    t.IMDB_RATING_ADJUSTED = COALESCE(t.IMDB_RATING_ADJUSTED, x.AVG_IMDB_RATING_ADJUSTED);
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
                                print("Processing record: " + str(lngrecordid) + ": " + strrecordname + " (" + strrecordmovementsource + ")")
                                arrlistcouples = {
                                    'ID_RECORD': lngrecordid,
                                    'MOVEMENT_NAME': strrecordname,
                                    'MOVEMENT_NAME_FR': strrecordnamefr,
                                    'OVERVIEW': strrecordoverview,
                                    'MOVEMENT_SOURCE': strrecordmovementsource,
                                    'MOVEMENT_TYPE': strrecordmovementtype,
                                    'POSTER_PATH': strrecordposterpath,
                                    'ID_WIKIDATA': strrecordidwikidata
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
                                    strsqlmovies_imdb = "SELECT m.ID_MOVIE, FIELD(m.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE m.ID_IMDB IN (" + strimdbidlist + ") AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                    strsqlseries_imdb = "SELECT s.ID_SERIE, FIELD(s.ID_IMDB, " + strfieldorder + ") AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE s.ID_IMDB IN (" + strimdbidlist + ") AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                # Mechanism 2: Wikidata property/item filter from WIKIDATA_PROPERTIES
                                strwikidataproperties = row['WIKIDATA_PROPERTIES'] or ''
                                arrwdtokens = re.findall(r'[PQ]\d+', strwikidataproperties)
                                strwdpropertyid = next((t for t in arrwdtokens if t.startswith('P')), '')
                                strwditemid = next((t for t in arrwdtokens if t.startswith('Q')), '')
                                if strwditemid:
                                    arrlistcouples['ID_WIKIDATA'] = strwditemid
                                strsqlmovies_wikidata = ""
                                strsqlseries_wikidata = ""
                                if strwdpropertyid and strwditemid:
                                    strsqlmovies_wikidata = "SELECT DISTINCT m.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE m INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON m.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                    strsqlseries_wikidata = "SELECT DISTINCT s.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE s INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE INNER JOIN T_WC_WIKIDATA_ITEM_PROPERTY w ON s.ID_WIKIDATA = w.ID_WIKIDATA WHERE w.ID_PROPERTY = '" + strwdpropertyid + "' AND w.ID_ITEM = '" + strwditemid + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                # Mechanism 3: TMDb keyword filter from TMDB_ELEMENTS
                                strtmdbelements = row['TMDB_ELEMENTS'] or ''
                                strsqlmovies_keyword = ""
                                strsqlseries_keyword = ""
                                strkeywordmatch = re.search(r"T_WC_TMDB_KEYWORD\.NAME\s*=\s*'([^']+)'", strtmdbelements.replace('&#039;', "'"))
                                if strkeywordmatch:
                                    strkeywordname = strkeywordmatch.group(1).strip().replace("'", "''")
                                    print(f"Found keyword in TMDB_ELEMENTS: {strkeywordname}")
                                    strsqlmovies_keyword = "SELECT mk.ID_MOVIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, m.DAT_RELEASE AS SORT_DATE FROM T_WC_TMDB_MOVIE_KEYWORD mk INNER JOIN T_WC_TMDB_KEYWORD k ON mk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_MOVIE m ON m.ID_MOVIE = mk.ID_MOVIE INNER JOIN T_WC_T2S_MOVIE t ON t.ID_MOVIE = m.ID_MOVIE WHERE k.NAME = '" + strkeywordname + "' AND m.ADULT = 0 AND m.ID_WIKIDATA IS NOT NULL AND m.ID_WIKIDATA <> '' "
                                    strsqlseries_keyword = "SELECT sk.ID_SERIE, NULL AS ORIGINAL_ORDER, t.IMDB_RATING_ADJUSTED, s.DAT_FIRST_AIR AS SORT_DATE FROM T_WC_TMDB_SERIE_KEYWORD sk INNER JOIN T_WC_TMDB_KEYWORD k ON sk.ID_KEYWORD = k.ID_KEYWORD INNER JOIN T_WC_TMDB_SERIE s ON s.ID_SERIE = sk.ID_SERIE INNER JOIN T_WC_T2S_SERIE t ON t.ID_SERIE = s.ID_SERIE WHERE k.NAME = '" + strkeywordname + "' AND s.ADULT = 0 AND s.ID_WIKIDATA IS NOT NULL AND s.ID_WIKIDATA <> '' "
                                    print(f"Constructed SQL for keyword filter: {strsqlmovies_keyword} / {strsqlseries_keyword}")
                                # Combine mechanisms cumulatively
                                arrsqlmovies_sources = [s for s in [strsqlmovies_imdb, strsqlmovies_wikidata, strsqlmovies_keyword] if s]
                                arrsqlseries_sources = [s for s in [strsqlseries_imdb, strsqlseries_wikidata, strsqlseries_keyword] if s]
                                strsqlmovies = f_buildcustomaggregatequery(arrsqlmovies_sources, "ID_MOVIE", "IMDB_RATING_ADJUSTED", intsortby)
                                strsqlseries = f_buildcustomaggregatequery(arrsqlseries_sources, "ID_SERIE", "IMDB_RATING_ADJUSTED", intsortby)

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

                        # Update T_WC_T2S_MOVEMENT.IMDB_RATING and T_WC_T2S_MOVEMENT.IMDB_RATING_ADJUSTED
                        strsql = """UPDATE T_WC_T2S_MOVEMENT t
JOIN (
    SELECT
        mt.ID_MOVEMENT,
        AVG(m.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(m.IMDB_RATING_ADJUSTED) AS AVG_IMDB_RATING_ADJUSTED
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
    t.IMDB_RATING_ADJUSTED = x.AVG_IMDB_RATING_ADJUSTED;
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
                        lngchunksize = 1000
                        lngtotalprocessed = 0
                        
                        for lngmovierangestart in range(1, lngmovierangemax + 1, lngchunksize):
                            lngmovierangeend = min(lngmovierangestart + lngchunksize - 1, lngmovierangemax)
                            print(f"Processing movies from ID {lngmovierangestart} to {lngmovierangeend}")
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
UPDATE T_WC_T2S_MOVIE 
SET IMDB_RATING_ADJUSTED = IMDB_RATING 
WHERE ID_MOVIE >= {lngmovierangestart} 
    AND ID_MOVIE <= {lngmovierangeend}
    AND IS_MOVIE = 1 """
                            cursor2.execute(strsqlmovies)
                            cp.connectioncp.commit()

                            strsqlmovies = f"""
UPDATE T_WC_T2S_MOVIE 
SET IMDB_RATING_ADJUSTED = IMDB_RATING - 1.5 
WHERE ID_MOVIE >= {lngmovierangestart} 
    AND ID_MOVIE <= {lngmovierangeend}
    AND IS_DOCUMENTARY = 1 """
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
                        lngchunksize = 1000
                        lngtotalprocessed = 0
                        
                        for lngserierangestart in range(1, lngserierangemax + 1, lngchunksize):
                            lngserierangeend = min(lngserierangestart + lngchunksize - 1, lngserierangemax)
                            print(f"Processing series from ID {lngserierangestart} to {lngserierangeend}")
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
UPDATE T_WC_T2S_SERIE 
SET IMDB_RATING_ADJUSTED = IMDB_RATING 
WHERE ID_SERIE >= {lngserierangestart} 
    AND ID_SERIE <= {lngserierangeend}
    AND SERIE_TYPE <> 'Documentary' """
                            cursor2.execute(strsqlseries)
                            cp.connectioncp.commit()

                            strsqlseries = f"""
UPDATE T_WC_T2S_SERIE 
SET IMDB_RATING_ADJUSTED = IMDB_RATING - 1.5 
WHERE ID_SERIE >= {lngserierangestart} 
    AND ID_SERIE <= {lngserierangeend}
    AND SERIE_TYPE = 'Documentary' """
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
                            print(f"Processing persons from ID {lngpersonrangestart} to {lngpersonrangeend}")
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
                        cursor.execute("SELECT MAX(ID_COMPANY) as max_id FROM T_WC_TMDB_COMPANY")
                        result = cursor.fetchone()
                        lngcompanyrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_COMPANY in database: {lngcompanyrangemax}")
                        
                        # Process database in chunks of 1000 records
                        lngchunksize = 1000
                        lngtotalprocessed = 0
                        
                        for lngcompanyrangestart in range(1, lngcompanyrangemax + 1, lngchunksize):
                            lngcompanyrangeend = min(lngcompanyrangestart + lngchunksize - 1, lngcompanyrangemax)
                            print(f"Processing companies from ID {lngcompanyrangestart} to {lngcompanyrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentcompanyid",str(lngcompanyrangestart),"Current company ID in the TMDb database preprocess",0)
                            
                            strsqlcompanies = f"""
    INSERT INTO T_WC_T2S_COMPANY (
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
    WHERE ID_COMPANY >= {lngcompanyrangestart} AND ID_COMPANY <= {lngcompanyrangeend}
    AND ((MOVIE_COUNT IS NOT NULL AND MOVIE_COUNT > 0) OR (SERIE_COUNT IS NOT NULL AND SERIE_COUNT > 0))
    ON DUPLICATE KEY UPDATE
        COMPANY_NAME = VALUES(COMPANY_NAME),
        DESCRIPTION = VALUES(DESCRIPTION),
        LOGO_PATH = VALUES(LOGO_PATH),
        TIM_CREDITS_DOWNLOADED = VALUES(TIM_CREDITS_DOWNLOADED),
        ORIGIN_COUNTRY = VALUES(ORIGIN_COUNTRY),
        HEADQUARTERS = VALUES(HEADQUARTERS),
        DAT_CREAT = VALUES(DAT_CREAT),
        TIM_UPDATED = VALUES(TIM_UPDATED),
        DELETED = VALUES(DELETED),
        ID_PARENT = VALUES(ID_PARENT),
        HOMEPAGE_URL = VALUES(HOMEPAGE_URL),
        MOVIE_COUNT = VALUES(MOVIE_COUNT),
        SERIE_COUNT = VALUES(SERIE_COUNT) """
                            cursor2.execute(strsqlcompanies)
                            cp.connectioncp.commit()
                            
                            strsqlcompaniesdelete = f"""
    DELETE FROM T_WC_T2S_COMPANY 
    WHERE ID_COMPANY >= {lngcompanyrangestart} AND ID_COMPANY <= {lngcompanyrangeend}
    AND ID_COMPANY NOT IN (
        SELECT ID_COMPANY FROM T_WC_TMDB_COMPANY 
        WHERE ID_COMPANY >= {lngcompanyrangestart} AND ID_COMPANY <= {lngcompanyrangeend}
        AND ((MOVIE_COUNT IS NOT NULL AND MOVIE_COUNT > 0) OR (SERIE_COUNT IS NOT NULL AND SERIE_COUNT > 0))
    ) """
                            cursor2.execute(strsqlcompaniesdelete)
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
                        cursor.execute("SELECT MAX(ID_NETWORK) as max_id FROM T_WC_TMDB_NETWORK")
                        result = cursor.fetchone()
                        lngnetworkrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_NETWORK in database: {lngnetworkrangemax}")
                        
                        # Process database in chunks of 1000 records
                        lngchunksize = 1000
                        lngtotalprocessed = 0
                        
                        for lngnetworkrangestart in range(1, lngnetworkrangemax + 1, lngchunksize):
                            lngnetworkrangeend = min(lngnetworkrangestart + lngchunksize - 1, lngnetworkrangemax)
                            print(f"Processing networks from ID {lngnetworkrangestart} to {lngnetworkrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentnetworkid",str(lngnetworkrangestart),"Current network ID in the TMDb database preprocess",0)
                            
                            strsqlnetworks = f"""
    INSERT INTO T_WC_T2S_NETWORK (
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
    WHERE ID_NETWORK >= {lngnetworkrangestart} AND ID_NETWORK <= {lngnetworkrangeend}
    AND (SERIE_COUNT IS NOT NULL AND SERIE_COUNT > 0)
    ON DUPLICATE KEY UPDATE
        NETWORK_NAME = VALUES(NETWORK_NAME),
        LOGO_PATH = VALUES(LOGO_PATH),
        TIM_CREDITS_DOWNLOADED = VALUES(TIM_CREDITS_DOWNLOADED),
        ORIGIN_COUNTRY = VALUES(ORIGIN_COUNTRY),
        HEADQUARTERS = VALUES(HEADQUARTERS),
        DAT_CREAT = VALUES(DAT_CREAT),
        TIM_UPDATED = VALUES(TIM_UPDATED),
        HOMEPAGE_URL = VALUES(HOMEPAGE_URL),
        SERIE_COUNT = VALUES(SERIE_COUNT),
        DELETED = VALUES(DELETED) """
                            cursor2.execute(strsqlnetworks)
                            cp.connectioncp.commit()
                            
                            strsqlnetworksdelete = f"""
    DELETE FROM T_WC_T2S_NETWORK 
    WHERE ID_NETWORK >= {lngnetworkrangestart} AND ID_NETWORK <= {lngnetworkrangeend}
    AND ID_NETWORK NOT IN (
        SELECT ID_NETWORK FROM T_WC_TMDB_NETWORK 
        WHERE ID_NETWORK >= {lngnetworkrangestart} AND ID_NETWORK <= {lngnetworkrangeend}
        AND (SERIE_COUNT IS NOT NULL AND SERIE_COUNT > 0)
    ) """
                            cursor2.execute(strsqlnetworksdelete)
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
                            print(f"Processing person-movie relations from ID {lngpersonmovierangestart} to {lngpersonmovierangeend}")
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
                            print(f"Processing person-serie relations from ID {lngpersonserierangestart} to {lngpersonserierangeend}")
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
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_MOVIE_GENRE")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentmoviegenreid",str(lngrangestart),"Current movie-genre ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_MOVIE_GENRE (
    ID_ROW, ID_MOVIE, ID_GENRE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_MOVIE, ID_GENRE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_MOVIE_GENRE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
ON DUPLICATE KEY UPDATE
    ID_MOVIE = VALUES(ID_MOVIE),
    ID_GENRE = VALUES(ID_GENRE),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_MOVIE_GENRE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_MOVIE_GENRE
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 12:
                    #----------------------------------------------------
                    print("T2S_SERIE_GENRE processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_SERIE_GENRE")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentseriegenreid",str(lngrangestart),"Current serie-genre ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_SERIE_GENRE (
    ID_ROW, ID_SERIE, ID_GENRE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_SERIE, ID_GENRE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_SERIE_GENRE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
ON DUPLICATE KEY UPDATE
    ID_SERIE = VALUES(ID_SERIE),
    ID_GENRE = VALUES(ID_GENRE),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_SERIE_GENRE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_SERIE_GENRE
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 13:
                    #----------------------------------------------------
                    print("T2S_MOVIE_COMPANY processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_MOVIE_COMPANY")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentmoviecompanyid",str(lngrangestart),"Current movie-company ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_MOVIE_COMPANY (
    ID_ROW, ID_MOVIE, ID_COMPANY,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_MOVIE, ID_COMPANY,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_MOVIE_COMPANY
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
  AND ID_COMPANY IN (SELECT ID_COMPANY FROM T_WC_T2S_COMPANY)
ON DUPLICATE KEY UPDATE
    ID_MOVIE = VALUES(ID_MOVIE),
    ID_COMPANY = VALUES(ID_COMPANY),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_MOVIE_COMPANY
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_MOVIE_COMPANY
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
      AND ID_COMPANY IN (SELECT ID_COMPANY FROM T_WC_T2S_COMPANY)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 14:
                    #----------------------------------------------------
                    print("T2S_SERIE_COMPANY processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_SERIE_COMPANY")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentseriecompanyid",str(lngrangestart),"Current serie-company ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_SERIE_COMPANY (
    ID_ROW, ID_SERIE, ID_COMPANY,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_SERIE, ID_COMPANY,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_SERIE_COMPANY
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
  AND ID_COMPANY IN (SELECT ID_COMPANY FROM T_WC_T2S_COMPANY)
ON DUPLICATE KEY UPDATE
    ID_SERIE = VALUES(ID_SERIE),
    ID_COMPANY = VALUES(ID_COMPANY),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_SERIE_COMPANY
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_SERIE_COMPANY
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
      AND ID_COMPANY IN (SELECT ID_COMPANY FROM T_WC_T2S_COMPANY)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 15:
                    #----------------------------------------------------
                    print("T2S_SERIE_NETWORK processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_SERIE_NETWORK")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentserienetworkid",str(lngrangestart),"Current serie-network ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_SERIE_NETWORK (
    ID_ROW, ID_SERIE, ID_NETWORK,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_SERIE, ID_NETWORK,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_SERIE_NETWORK
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
  AND ID_NETWORK IN (SELECT ID_NETWORK FROM T_WC_T2S_NETWORK)
ON DUPLICATE KEY UPDATE
    ID_SERIE = VALUES(ID_SERIE),
    ID_NETWORK = VALUES(ID_NETWORK),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_SERIE_NETWORK
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_SERIE_NETWORK
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
      AND ID_NETWORK IN (SELECT ID_NETWORK FROM T_WC_T2S_NETWORK)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 16:
                    #----------------------------------------------------
                    print("T2S_MOVIE_PRODUCTION_COUNTRY processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentmoviecountryid",str(lngrangestart),"Current movie production country ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_MOVIE_PRODUCTION_COUNTRY (
    ID_ROW, ID_MOVIE, COUNTRY_CODE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_MOVIE, COUNTRY_CODE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
ON DUPLICATE KEY UPDATE
    ID_MOVIE = VALUES(ID_MOVIE),
    COUNTRY_CODE = VALUES(COUNTRY_CODE),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_MOVIE_PRODUCTION_COUNTRY
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 17:
                    #----------------------------------------------------
                    print("T2S_SERIE_PRODUCTION_COUNTRY processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_SERIE_PRODUCTION_COUNTRY")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentseriedcountryid",str(lngrangestart),"Current serie production country ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_SERIE_PRODUCTION_COUNTRY (
    ID_ROW, ID_SERIE, COUNTRY_CODE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_SERIE, COUNTRY_CODE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_SERIE_PRODUCTION_COUNTRY
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
ON DUPLICATE KEY UPDATE
    ID_SERIE = VALUES(ID_SERIE),
    COUNTRY_CODE = VALUES(COUNTRY_CODE),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_SERIE_PRODUCTION_COUNTRY
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_SERIE_PRODUCTION_COUNTRY
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 18:
                    #----------------------------------------------------
                    print("T2S_MOVIE_SPOKEN_LANGUAGE processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_MOVIE_SPOKEN_LANGUAGE")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentmoviespokenlangid",str(lngrangestart),"Current movie spoken language ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_MOVIE_SPOKEN_LANGUAGE (
    ID_ROW, ID_MOVIE, SPOKEN_LANGUAGE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_MOVIE, SPOKEN_LANGUAGE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_MOVIE_SPOKEN_LANGUAGE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
ON DUPLICATE KEY UPDATE
    ID_MOVIE = VALUES(ID_MOVIE),
    SPOKEN_LANGUAGE = VALUES(SPOKEN_LANGUAGE),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_MOVIE_SPOKEN_LANGUAGE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_MOVIE_SPOKEN_LANGUAGE
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 19:
                    #----------------------------------------------------
                    print("T2S_SERIE_SPOKEN_LANGUAGE processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_SERIE_SPOKEN_LANGUAGE")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentseriespokenlangid",str(lngrangestart),"Current serie spoken language ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_SERIE_SPOKEN_LANGUAGE (
    ID_ROW, ID_SERIE, SPOKEN_LANGUAGE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
)
SELECT
    ID_ROW, ID_SERIE, SPOKEN_LANGUAGE,
    DELETED, DISPLAY_ORDER,
    ID_CREATOR, DAT_CREAT, ID_OWNER, TIM_UPDATED, ID_USER_UPDATED
FROM T_WC_TMDB_SERIE_SPOKEN_LANGUAGE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
ON DUPLICATE KEY UPDATE
    ID_SERIE = VALUES(ID_SERIE),
    SPOKEN_LANGUAGE = VALUES(SPOKEN_LANGUAGE),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_SERIE_SPOKEN_LANGUAGE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_SERIE_SPOKEN_LANGUAGE
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 20:
                    #----------------------------------------------------
                    print("T2S_COMPANY_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_COMPANY_IMAGE")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentcompanyimageid",str(lngrangestart),"Current company image ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_COMPANY_IMAGE (
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
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_COMPANY IN (SELECT ID_COMPANY FROM T_WC_T2S_COMPANY)
ON DUPLICATE KEY UPDATE
    ID_COMPANY = VALUES(ID_COMPANY),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED),
    TYPE_IMAGE = VALUES(TYPE_IMAGE),
    LANG = VALUES(LANG),
    IMAGE_PATH = VALUES(IMAGE_PATH),
    ASPECT_RATIO = VALUES(ASPECT_RATIO),
    WIDTH = VALUES(WIDTH),
    HEIGHT = VALUES(HEIGHT),
    VOTE_AVERAGE = VALUES(VOTE_AVERAGE),
    VOTE_COUNT = VALUES(VOTE_COUNT) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_COMPANY_IMAGE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_COMPANY_IMAGE
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_COMPANY IN (SELECT ID_COMPANY FROM T_WC_T2S_COMPANY)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 21:
                    #----------------------------------------------------
                    print("T2S_MOVIE_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_MOVIE_IMAGE")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovieimageid",str(lngrangestart),"Current movie image ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_MOVIE_IMAGE (
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
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
ON DUPLICATE KEY UPDATE
    ID_MOVIE = VALUES(ID_MOVIE),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED),
    TYPE_IMAGE = VALUES(TYPE_IMAGE),
    LANG = VALUES(LANG),
    IMAGE_PATH = VALUES(IMAGE_PATH),
    ASPECT_RATIO = VALUES(ASPECT_RATIO),
    WIDTH = VALUES(WIDTH),
    HEIGHT = VALUES(HEIGHT),
    VOTE_AVERAGE = VALUES(VOTE_AVERAGE),
    VOTE_COUNT = VALUES(VOTE_COUNT) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_MOVIE_IMAGE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_MOVIE_IMAGE
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 22:
                    #----------------------------------------------------
                    print("T2S_NETWORK_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_NETWORK_IMAGE")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentnetworkimageid",str(lngrangestart),"Current network image ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_NETWORK_IMAGE (
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
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_NETWORK IN (SELECT ID_NETWORK FROM T_WC_T2S_NETWORK)
ON DUPLICATE KEY UPDATE
    ID_NETWORK = VALUES(ID_NETWORK),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED),
    TYPE_IMAGE = VALUES(TYPE_IMAGE),
    LANG = VALUES(LANG),
    IMAGE_PATH = VALUES(IMAGE_PATH),
    ASPECT_RATIO = VALUES(ASPECT_RATIO),
    WIDTH = VALUES(WIDTH),
    HEIGHT = VALUES(HEIGHT),
    VOTE_AVERAGE = VALUES(VOTE_AVERAGE),
    VOTE_COUNT = VALUES(VOTE_COUNT) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_NETWORK_IMAGE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_NETWORK_IMAGE
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_NETWORK IN (SELECT ID_NETWORK FROM T_WC_T2S_NETWORK)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 23:
                    #----------------------------------------------------
                    print("T2S_PERSON_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_PERSON_IMAGE")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentpersonimageid",str(lngrangestart),"Current person image ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_PERSON_IMAGE (
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
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_PERSON IN (SELECT ID_PERSON FROM T_WC_T2S_PERSON)
ON DUPLICATE KEY UPDATE
    ID_PERSON = VALUES(ID_PERSON),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED),
    TYPE_IMAGE = VALUES(TYPE_IMAGE),
    LANG = VALUES(LANG),
    IMAGE_PATH = VALUES(IMAGE_PATH),
    ASPECT_RATIO = VALUES(ASPECT_RATIO),
    WIDTH = VALUES(WIDTH),
    HEIGHT = VALUES(HEIGHT),
    VOTE_AVERAGE = VALUES(VOTE_AVERAGE),
    VOTE_COUNT = VALUES(VOTE_COUNT) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_PERSON_IMAGE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_PERSON_IMAGE
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_PERSON IN (SELECT ID_PERSON FROM T_WC_T2S_PERSON)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 24:
                    #----------------------------------------------------
                    print("T2S_SERIE_IMAGE processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_SERIE_IMAGE")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentserieimageid",str(lngrangestart),"Current serie image ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_SERIE_IMAGE (
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
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
ON DUPLICATE KEY UPDATE
    ID_SERIE = VALUES(ID_SERIE),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED),
    TYPE_IMAGE = VALUES(TYPE_IMAGE),
    LANG = VALUES(LANG),
    IMAGE_PATH = VALUES(IMAGE_PATH),
    ASPECT_RATIO = VALUES(ASPECT_RATIO),
    WIDTH = VALUES(WIDTH),
    HEIGHT = VALUES(HEIGHT),
    VOTE_AVERAGE = VALUES(VOTE_AVERAGE),
    VOTE_COUNT = VALUES(VOTE_COUNT) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_SERIE_IMAGE
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_SERIE_IMAGE
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 25:
                    #----------------------------------------------------
                    print("T2S_MOVIE_VIDEO processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_MOVIE_VIDEO")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovievideoid",str(lngrangestart),"Current movie video ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_MOVIE_VIDEO (
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
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
ON DUPLICATE KEY UPDATE
    ID_MOVIE = VALUES(ID_MOVIE),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED),
    LANG = VALUES(LANG),
    COUNTRY_CODE = VALUES(COUNTRY_CODE),
    VIDEO_KEY = VALUES(VIDEO_KEY),
    VIDEO_NAME = VALUES(VIDEO_NAME),
    VIDEO_SITE = VALUES(VIDEO_SITE),
    VIDEO_TYPE = VALUES(VIDEO_TYPE),
    QUALITY = VALUES(QUALITY),
    QUALITY_TEXT = VALUES(QUALITY_TEXT),
    DAT_PUBLISHED = VALUES(DAT_PUBLISHED),
    ID_CREDIT = VALUES(ID_CREDIT),
    OFFICIAL = VALUES(OFFICIAL) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_MOVIE_VIDEO
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_MOVIE_VIDEO
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_MOVIE IN (SELECT ID_MOVIE FROM T_WC_T2S_MOVIE)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 26:
                    #----------------------------------------------------
                    print("T2S_SERIE_VIDEO processing")
                    if 1:
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_TMDB_SERIE_VIDEO")
                        result = cursor.fetchone()
                        lngrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngrangemax}")
                        lngchunksize = 1000
                        for lngrangestart in range(1, lngrangemax + 1, lngchunksize):
                            lngrangeend = min(lngrangestart + lngchunksize - 1, lngrangemax)
                            print(f"Processing rows from ID {lngrangestart} to {lngrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentserievideoid",str(lngrangestart),"Current serie video ID in the TMDb database preprocess",0)
                            strsql = f"""
INSERT INTO T_WC_T2S_SERIE_VIDEO (
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
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
ON DUPLICATE KEY UPDATE
    ID_SERIE = VALUES(ID_SERIE),
    DELETED = VALUES(DELETED),
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER),
    ID_CREATOR = VALUES(ID_CREATOR),
    DAT_CREAT = VALUES(DAT_CREAT),
    ID_OWNER = VALUES(ID_OWNER),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    ID_USER_UPDATED = VALUES(ID_USER_UPDATED),
    LANG = VALUES(LANG),
    COUNTRY_CODE = VALUES(COUNTRY_CODE),
    VIDEO_KEY = VALUES(VIDEO_KEY),
    VIDEO_NAME = VALUES(VIDEO_NAME),
    VIDEO_SITE = VALUES(VIDEO_SITE),
    VIDEO_TYPE = VALUES(VIDEO_TYPE),
    QUALITY = VALUES(QUALITY),
    QUALITY_TEXT = VALUES(QUALITY_TEXT),
    DAT_PUBLISHED = VALUES(DAT_PUBLISHED),
    ID_CREDIT = VALUES(ID_CREDIT),
    OFFICIAL = VALUES(OFFICIAL) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()
                            strsql = f"""
DELETE FROM T_WC_T2S_SERIE_VIDEO
WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
  AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_TMDB_SERIE_VIDEO
    WHERE ID_ROW >= {lngrangestart} AND ID_ROW <= {lngrangeend}
      AND ID_SERIE IN (SELECT ID_SERIE FROM T_WC_T2S_SERIE)
  ) """
                            cursor2.execute(strsql)
                            cp.connectioncp.commit()

                elif intindex == 60:
                    #----------------------------------------------------
                    print("TMDB_KEYWORD processing")


                elif intindex == 40:
                    #----------------------------------------------------
                    print("T2S_ITEM processing")
                    if 1:
                        cp.f_setservervariable("strtmdbmoviepreprocesscurrentsubprocess","Copying from WIKIDATA_ITEM to T2S_ITEM","Current sub process in the TMDb database movie preprocess",0)
                        # Get the maximum ID_ROW value from the database
                        cursor.execute("SELECT MAX(ID_ROW) as max_id FROM T_WC_WIKIDATA_ITEM_V1")
                        result = cursor.fetchone()
                        lngitemrangemax = result['max_id'] if result['max_id'] is not None else 0
                        print(f"Maximum ID_ROW in database: {lngitemrangemax}")
                        
                        # Process database in chunks of 1000 records
                        lngchunksize = 1000
                        lngtotalprocessed = 0
                        
                        for lngitemrangestart in range(1, lngitemrangemax + 1, lngchunksize):
                            lngitemrangeend = min(lngitemrangestart + lngchunksize - 1, lngitemrangemax)
                            print(f"Processing items from ID {lngitemrangestart} to {lngitemrangeend}")
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentitemid",str(lngitemrangestart),"Current row ID in the TMDb database preprocess",0)
                            
                            strsqlitems = f"""
INSERT INTO T_WC_T2S_ITEM (
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
AND ID_ROW >= {lngitemrangestart} AND ID_ROW <= {lngitemrangeend}
ON DUPLICATE KEY UPDATE
    ID_WIKIDATA = VALUES(ID_WIKIDATA),
    ITEM_LABEL = VALUES(ITEM_LABEL),
    ALIASES = VALUES(ALIASES),
    DESCRIPTION = VALUES(DESCRIPTION),
    WIKIPEDIA_IMAGE_PATH = VALUES(WIKIPEDIA_IMAGE_PATH),
    INSTANCE_OF = VALUES(INSTANCE_OF),
    DAT_CREAT = VALUES(DAT_CREAT),
    TIM_UPDATED = VALUES(TIM_UPDATED),
    DELETED = VALUES(DELETED) """
                            cursor2.execute(strsqlitems)
                            cp.connectioncp.commit()
                            
                            strsqlitemsdelete = f"""
DELETE FROM T_WC_T2S_ITEM 
WHERE ID_ROW >= {lngitemrangestart} AND ID_ROW <= {lngitemrangeend}
AND ID_ROW NOT IN (
    SELECT ID_ROW FROM T_WC_WIKIDATA_ITEM_V1 
    WHERE LANG = 'en'
        AND ID_ROW >= {lngitemrangestart} AND ID_ROW <= {lngitemrangeend}
) """
                            cursor2.execute(strsqlitemsdelete)
                            cp.connectioncp.commit()

                            strsqlitems = f"""
UPDATE T_WC_T2S_ITEM t2s
INNER JOIN T_WC_WIKIDATA_ITEM_V1 t 
    ON t2s.ID_WIKIDATA = t.ID_WIKIDATA
SET t2s.ITEM_LABEL_FR = t.LABEL
WHERE t2s.ID_ROW >= {lngitemrangestart} 
    AND t2s.ID_ROW <= {lngitemrangeend}
    AND t.LANG = 'fr' """
                            cursor2.execute(strsqlitems)
                            cp.connectioncp.commit()

                    print(f"T2S_ITEM processing completed. ")

                elif intindex == 48:
                    #----------------------------------------------------
                    print("T2S_CHARACTER processing")
                    if 1:
                        cp.f_setservervariable(
                            "strtmdbmoviepreprocesscurrentsubprocess",
                            "Copying from T2S_PERSON_MOVIE/SERIE to T2S_CHARACTER",
                            "Current sub process in the TMDb database movie preprocess",
                            0,
                        )

                        strsql = """INSERT INTO T_WC_T2S_CHARACTER (CAST_CHARACTER, CAST_CHARACTER_KEY)
SELECT src.CAST_CHARACTER,
       src.CAST_CHARACTER_KEY
FROM (
    SELECT
        MIN(t.CAST_CHARACTER) AS CAST_CHARACTER,
        t.CAST_CHARACTER_KEY
    FROM (
        SELECT
            CAST_CHARACTER,
            replace(trim(regexp_replace(lcase(regexp_replace(CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','') AS CAST_CHARACTER_KEY
        FROM T_WC_T2S_PERSON_MOVIE
        WHERE CREDIT_TYPE = 'cast'
          AND (DELETED = 0 OR DELETED IS NULL)
          AND CAST_CHARACTER IS NOT NULL
          AND CAST_CHARACTER <> ''
        UNION ALL
        SELECT
            CAST_CHARACTER,
            replace(trim(regexp_replace(lcase(regexp_replace(CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','') AS CAST_CHARACTER_KEY
        FROM T_WC_T2S_PERSON_SERIE
        WHERE CREDIT_TYPE = 'cast'
          AND (DELETED = 0 OR DELETED IS NULL)
          AND CAST_CHARACTER IS NOT NULL
          AND CAST_CHARACTER <> ''
    ) t
    GROUP BY t.CAST_CHARACTER_KEY
) src
LEFT JOIN T_WC_T2S_CHARACTER c
  ON c.CAST_CHARACTER_KEY = src.CAST_CHARACTER_KEY
WHERE c.ID_CHARACTER IS NULL
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """INSERT INTO T_WC_T2S_MOVIE_CHARACTER (ID_MOVIE, ID_CHARACTER, DISPLAY_ORDER)
SELECT
    pm.ID_MOVIE,
    c.ID_CHARACTER,
    MIN(COALESCE(pm.DISPLAY_ORDER, 0)) AS DISPLAY_ORDER
FROM T_WC_T2S_PERSON_MOVIE pm
INNER JOIN T_WC_T2S_CHARACTER c
    ON c.CAST_CHARACTER_KEY = replace(trim(regexp_replace(lcase(regexp_replace(pm.CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','')
WHERE pm.CREDIT_TYPE = 'cast'
  AND (pm.DELETED = 0 OR pm.DELETED IS NULL)
  AND pm.CAST_CHARACTER IS NOT NULL
  AND pm.CAST_CHARACTER <> ''
GROUP BY pm.ID_MOVIE, c.ID_CHARACTER
ON DUPLICATE KEY UPDATE
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER)
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """DELETE mc
FROM T_WC_T2S_MOVIE_CHARACTER mc
LEFT JOIN (
    SELECT
        pm.ID_MOVIE,
        c.ID_CHARACTER
    FROM T_WC_T2S_PERSON_MOVIE pm
    INNER JOIN T_WC_T2S_CHARACTER c
        ON c.CAST_CHARACTER_KEY = replace(trim(regexp_replace(lcase(regexp_replace(pm.CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','')
    WHERE pm.CREDIT_TYPE = 'cast'
      AND (pm.DELETED = 0 OR pm.DELETED IS NULL)
      AND pm.CAST_CHARACTER IS NOT NULL
      AND pm.CAST_CHARACTER <> ''
    GROUP BY pm.ID_MOVIE, c.ID_CHARACTER
) src
  ON src.ID_MOVIE = mc.ID_MOVIE
 AND src.ID_CHARACTER = mc.ID_CHARACTER
WHERE src.ID_MOVIE IS NULL
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """INSERT INTO T_WC_T2S_SERIE_CHARACTER (ID_SERIE, ID_CHARACTER, DISPLAY_ORDER)
SELECT
    ps.ID_SERIE,
    c.ID_CHARACTER,
    MIN(COALESCE(ps.DISPLAY_ORDER, 0)) AS DISPLAY_ORDER
FROM T_WC_T2S_PERSON_SERIE ps
INNER JOIN T_WC_T2S_CHARACTER c
    ON c.CAST_CHARACTER_KEY = replace(trim(regexp_replace(lcase(regexp_replace(ps.CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','')
WHERE ps.CREDIT_TYPE = 'cast'
  AND (ps.DELETED = 0 OR ps.DELETED IS NULL)
  AND ps.CAST_CHARACTER IS NOT NULL
  AND ps.CAST_CHARACTER <> ''
GROUP BY ps.ID_SERIE, c.ID_CHARACTER
ON DUPLICATE KEY UPDATE
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER)
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """DELETE sc
FROM T_WC_T2S_SERIE_CHARACTER sc
LEFT JOIN (
    SELECT
        ps.ID_SERIE,
        c.ID_CHARACTER
    FROM T_WC_T2S_PERSON_SERIE ps
    INNER JOIN T_WC_T2S_CHARACTER c
        ON c.CAST_CHARACTER_KEY = replace(trim(regexp_replace(lcase(regexp_replace(ps.CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','')
    WHERE ps.CREDIT_TYPE = 'cast'
      AND (ps.DELETED = 0 OR ps.DELETED IS NULL)
      AND ps.CAST_CHARACTER IS NOT NULL
      AND ps.CAST_CHARACTER <> ''
    GROUP BY ps.ID_SERIE, c.ID_CHARACTER
) src
  ON src.ID_SERIE = sc.ID_SERIE
 AND src.ID_CHARACTER = sc.ID_CHARACTER
WHERE src.ID_SERIE IS NULL
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """INSERT INTO T_WC_T2S_PERSON_CHARACTER (ID_PERSON, ID_CHARACTER, DISPLAY_ORDER)
SELECT
    src.ID_PERSON,
    c.ID_CHARACTER,
    MIN(COALESCE(src.DISPLAY_ORDER, 0)) AS DISPLAY_ORDER
FROM (
    SELECT ID_PERSON, CAST_CHARACTER, DISPLAY_ORDER
    FROM T_WC_T2S_PERSON_MOVIE
    WHERE CREDIT_TYPE = 'cast'
      AND (DELETED = 0 OR DELETED IS NULL)
      AND CAST_CHARACTER IS NOT NULL
      AND CAST_CHARACTER <> ''
    UNION ALL
    SELECT ID_PERSON, CAST_CHARACTER, DISPLAY_ORDER
    FROM T_WC_T2S_PERSON_SERIE
    WHERE CREDIT_TYPE = 'cast'
      AND (DELETED = 0 OR DELETED IS NULL)
      AND CAST_CHARACTER IS NOT NULL
      AND CAST_CHARACTER <> ''
) src
INNER JOIN T_WC_T2S_CHARACTER c
    ON c.CAST_CHARACTER_KEY = replace(trim(regexp_replace(lcase(regexp_replace(src.CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','')
GROUP BY src.ID_PERSON, c.ID_CHARACTER
ON DUPLICATE KEY UPDATE
    DISPLAY_ORDER = VALUES(DISPLAY_ORDER)
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """DELETE pc
FROM T_WC_T2S_PERSON_CHARACTER pc
LEFT JOIN (
    SELECT
        src.ID_PERSON,
        c.ID_CHARACTER
    FROM (
        SELECT ID_PERSON, CAST_CHARACTER
        FROM T_WC_T2S_PERSON_MOVIE
        WHERE CREDIT_TYPE = 'cast'
          AND (DELETED = 0 OR DELETED IS NULL)
          AND CAST_CHARACTER IS NOT NULL
          AND CAST_CHARACTER <> ''
        UNION ALL
        SELECT ID_PERSON, CAST_CHARACTER
        FROM T_WC_T2S_PERSON_SERIE
        WHERE CREDIT_TYPE = 'cast'
          AND (DELETED = 0 OR DELETED IS NULL)
          AND CAST_CHARACTER IS NOT NULL
          AND CAST_CHARACTER <> ''
    ) src
    INNER JOIN T_WC_T2S_CHARACTER c
        ON c.CAST_CHARACTER_KEY = replace(trim(regexp_replace(lcase(regexp_replace(src.CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','')
    GROUP BY src.ID_PERSON, c.ID_CHARACTER
) src
  ON src.ID_PERSON = pc.ID_PERSON
 AND src.ID_CHARACTER = pc.ID_CHARACTER
WHERE src.ID_PERSON IS NULL
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()
                    if 0:
                        strsql = """UPDATE T_WC_T2S_CHARACTER ch
LEFT JOIN (
    SELECT ID_CHARACTER, COUNT(DISTINCT ID_MOVIE) AS MOVIE_COUNT
    FROM T_WC_T2S_MOVIE_CHARACTER
    GROUP BY ID_CHARACTER
) m ON m.ID_CHARACTER = ch.ID_CHARACTER
LEFT JOIN (
    SELECT ID_CHARACTER, COUNT(DISTINCT ID_SERIE) AS SERIE_COUNT
    FROM T_WC_T2S_SERIE_CHARACTER
    GROUP BY ID_CHARACTER
) s ON s.ID_CHARACTER = ch.ID_CHARACTER
LEFT JOIN (
    SELECT ID_CHARACTER, COUNT(DISTINCT ID_PERSON) AS PERSON_COUNT
    FROM T_WC_T2S_PERSON_CHARACTER
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

                        strsql = """UPDATE T_WC_T2S_CHARACTER ch
JOIN (
    SELECT
        x.ID_CHARACTER,
        AVG(x.IMDB_RATING) AS AVG_IMDB_RATING,
        AVG(x.IMDB_RATING_ADJUSTED) AS AVG_IMDB_RATING_ADJUSTED
    FROM (
        SELECT mc.ID_CHARACTER, m.IMDB_RATING, m.IMDB_RATING_ADJUSTED
        FROM T_WC_T2S_MOVIE_CHARACTER mc
        INNER JOIN T_WC_T2S_MOVIE m
            ON m.ID_MOVIE = mc.ID_MOVIE
        UNION ALL
        SELECT sc.ID_CHARACTER, s.IMDB_RATING, s.IMDB_RATING_ADJUSTED
        FROM T_WC_T2S_SERIE_CHARACTER sc
        INNER JOIN T_WC_T2S_SERIE s
            ON s.ID_SERIE = sc.ID_SERIE
    ) x
    GROUP BY x.ID_CHARACTER
) r ON r.ID_CHARACTER = ch.ID_CHARACTER
SET
    ch.IMDB_RATING = r.AVG_IMDB_RATING,
    ch.IMDB_RATING_ADJUSTED = r.AVG_IMDB_RATING_ADJUSTED
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = """UPDATE T_WC_T2S_CHARACTER ch
JOIN (
    SELECT
        pc.ID_CHARACTER,
        AVG(p.POPULARITY) AS AVG_POPULARITY
    FROM T_WC_T2S_PERSON_CHARACTER pc
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
                        strsql = """DELETE ch
FROM T_WC_T2S_CHARACTER ch
LEFT JOIN (
    SELECT DISTINCT replace(trim(regexp_replace(lcase(regexp_replace(CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','') AS CAST_CHARACTER_KEY
    FROM T_WC_T2S_PERSON_MOVIE
    WHERE CREDIT_TYPE = 'cast'
      AND (DELETED = 0 OR DELETED IS NULL)
      AND CAST_CHARACTER IS NOT NULL
      AND CAST_CHARACTER <> ''
    UNION
    SELECT DISTINCT replace(trim(regexp_replace(lcase(regexp_replace(CAST_CHARACTER,'[^[:alnum:] ]',' ')),' +',' ')),' ','') AS CAST_CHARACTER_KEY
    FROM T_WC_T2S_PERSON_SERIE
    WHERE CREDIT_TYPE = 'cast'
      AND (DELETED = 0 OR DELETED IS NULL)
      AND CAST_CHARACTER IS NOT NULL
      AND CAST_CHARACTER <> ''
) src
  ON src.CAST_CHARACTER_KEY = ch.CAST_CHARACTER_KEY
WHERE src.CAST_CHARACTER_KEY IS NULL
"""
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                        strsql = "DELETE FROM T_WC_T2S_MOVIE_CHARACTER WHERE ID_CHARACTER NOT IN (SELECT ID_CHARACTER FROM T_WC_T2S_CHARACTER)"
                        print(strsql)
                        cursor2.execute(strsql)

                        strsql = "DELETE FROM T_WC_T2S_SERIE_CHARACTER WHERE ID_CHARACTER NOT IN (SELECT ID_CHARACTER FROM T_WC_T2S_CHARACTER)"
                        print(strsql)
                        cursor2.execute(strsql)

                        strsql = "DELETE FROM T_WC_T2S_PERSON_CHARACTER WHERE ID_CHARACTER NOT IN (SELECT ID_CHARACTER FROM T_WC_T2S_CHARACTER)"
                        print(strsql)
                        cursor2.execute(strsql)
                        cp.connectioncp.commit()

                elif intindex == 30:
                    #----------------------------------------------------
                    intmovielangmeta = False
                    intmovielangmeta = True
                    # Check if today is Wednesday
                    if datetime.now().weekday() == 2:
                        print("Today is Wednesday!")
                        intmovielangmeta = True
                    if intmovielangmeta:
                        print("TMDB_MOVIE processing to TMDB_MOVIE_LANG_META")
                        strmovieidold = cp.f_getservervariable("strtmdbmoviepreprocesscurrentvalue",0)
                        strcurrentprocess = ""
                        intindex = 1
                        arrlang = {1: 'fr', 2: 'en'}
                        arrlang = {1: 'fr'}
                        #arrlang = {2: 'en'}
                        for intlang, strlang in arrlang.items():
                            strlangalt = ""
                            if strlang == "fr":
                                strlangalt = "en"
                            strcurrentprocess = f"Preprocessing {strlang} movies from TMDB_MOVIE to TMDB_MOVIE_LANG_META"
                            strsql = ""
                            strsql += "SELECT "
                            strsql += "T_WC_TMDB_MOVIE.ID_MOVIE, T_WC_TMDB_MOVIE.DAT_RELEASE, T_WC_TMDB_MOVIE.GENRES, T_WC_TMDB_MOVIE.ID_IMDB, T_WC_TMDB_MOVIE.ID_WIKIDATA, "
                            strsql += "T_WC_TMDB_MOVIE.ID_COLLECTION, T_WC_TMDB_MOVIE.ORIGINAL_LANGUAGE, T_WC_TMDB_MOVIE.ORIGINAL_TITLE, T_WC_TMDB_MOVIE.RUNTIME, "
                            strsql += "T_WC_TMDB_MOVIE.COLOR_TECHNOLOGY, T_WC_TMDB_MOVIE.FILM_TECHNOLOGY, T_WC_TMDB_MOVIE.ASPECT_RATIO, T_WC_TMDB_MOVIE.FILM_FORMAT, "
                            strsql += "T_WC_TMDB_MOVIE.SOUND_SYSTEM, T_WC_TMDB_MOVIE.SOUND_TECHNOLOGY, T_WC_TMDB_MOVIE.NUM_AUDIO_TRACKS "
                            #strsql += ", T_WC_TMDB_MOVIE.TITLE, T_WC_TMDB_MOVIE.OVERVIEW "
                            strsql += "FROM T_WC_TMDB_MOVIE "
                            # Exporting only movies with an IMDb ID
                            strsql += "WHERE T_WC_TMDB_MOVIE.ID_IMDB IS NOT NULL AND T_WC_TMDB_MOVIE.ID_IMDB <> '' "
                            # And movies with a Wikidata ID
                            strsql += "AND T_WC_TMDB_MOVIE.ID_WIKIDATA IS NOT NULL AND T_WC_TMDB_MOVIE.ID_WIKIDATA <> '' "
                            if strmovieidold != "":
                                strsql += "AND T_WC_TMDB_MOVIE.ID_MOVIE >= " + strmovieidold + " "
                            #strsql += "AND T_WC_TMDB_MOVIE.ID_MOVIE IN (392207) "
                            #strsql += "AND T_WC_TMDB_MOVIE.ID_MOVIE <= 100 "
                            #strsql += "ORDER BY T_WC_TMDB_MOVIE.POPULARITY DESC "
                            strsql += "ORDER BY T_WC_TMDB_MOVIE.ID_MOVIE ASC "
                            #strsql += "LIMIT 10 "
                            #strsql += "LIMIT 1000 "
                            
                            #intresettables = True
                            intresettables = False
                            #intresetusedforsimilarity = True
                            intresetusedforsimilarity = False
                            
                            if strsql != "":
                                print(strcurrentprocess)
                                cp.f_setservervariable("strtmdbmoviepreprocesscurrentprocess",strcurrentprocess,"Current process in the TMDb database preprocess",0)
                                if intresettables:
                                    # First we delete all the records in the target tables
                                    # T_WC_TMDB_MOVIE_LANG_META
                                    strsqlpurge = "DELETE FROM T_WC_TMDB_MOVIE_LANG_META WHERE LANG = '" + strlang + "' "
                                    #print(strsqlpurge)
                                    cursor2.execute(strsqlpurge)
                                    cp.connectioncp.commit()
                                    # T_WC_TMDB_MOVIE_LANG_PREPROCESSED
                                    strsqlpurge = "DELETE FROM T_WC_TMDB_MOVIE_LANG_PREPROCESSED WHERE LANG = '" + strlang + "' "
                                    #print(strsqlpurge)
                                    cursor2.execute(strsqlpurge)
                                    cp.connectioncp.commit()
                                if intresetusedforsimilarity:
                                    #Now we reset all used keywords for tags
                                    strsqlupdate = "UPDATE T_WC_TMDB_KEYWORD SET USED_FOR_SIMILARITY = NULL WHERE USED_FOR_SIMILARITY IS NOT NULL "
                                    #print(strsqlupdate)
                                    cursor2.execute(strsqlupdate)
                                    cp.connectioncp.commit()
                                    #Now we reset all used persons for tags
                                    strsqlupdate = "UPDATE T_WC_TMDB_PERSON SET USED_FOR_SIMILARITY = NULL WHERE USED_FOR_SIMILARITY IS NOT NULL "
                                    #print(strsqlupdate)
                                    cursor2.execute(strsqlupdate)
                                    cp.connectioncp.commit()
                                
                                # Now we process the SELECT query
                                print(strsql)
                                #cp.f_setservervariable("strtmdbmoviepreprocesscurrentsql",strsql,"Current SQL query in the TMDb database preprocess",0)
                                cursor.execute(strsql)
                                lngrowcount = cursor.rowcount
                                print(f"{lngrowcount} lines")
                                #time.sleep(5)
                                lnglinesprocessed = 0
                                # Fetching all rows from the last executed statement
                                results = cursor.fetchall()
                                # Iterating through the results and printing
                                for row in results:
                                    # print("------------------------------------------")
                                    lnglinesprocessed += 1
                                    lngmovieid = row['ID_MOVIE']
                                    print(f"{lnglinesprocessed}: ID_MOVIE={lngmovieid}")
                                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentmovieid",str(lngmovieid),"Current movie ID in the TMDb database preprocess",0)
                                    if intindex == 1:
                                        datrelease = row['DAT_RELEASE']
                                        strmoviegenres = row['GENRES']
                                        strmovieidimdb = row['ID_IMDB']
                                        strmovieidwikidata = row['ID_WIKIDATA']
                                        lngmoviecollectionid = row['ID_COLLECTION']
                                        strmovieoriginallanguage = row['ORIGINAL_LANGUAGE']
                                        strmovieoriginaltitle = row['ORIGINAL_TITLE']
                                        lngmovieruntime = row['RUNTIME']
                                        strmoviecolortech = row['COLOR_TECHNOLOGY']
                                        strmoviefilmtech = row['FILM_TECHNOLOGY']
                                        strmovieaspectratio = row['ASPECT_RATIO']
                                        strmoviefilmformat = row['FILM_FORMAT']
                                        strmoviesoundsystem = row['SOUND_SYSTEM']
                                        strmoviesoundtech = row['SOUND_TECHNOLOGY']
                                        lngmovienumaudiotracks = row['NUM_AUDIO_TRACKS']
                                        #strmovieformatline = row['WIKIPEDIA_FORMAT_LINE']

                                        strmovietitle = ""
                                        strmovieoverview = ""
                                        strtags = ""
                                        strmovieoverviewlemma = ""
                                        strmoviekeywordslemma = ""
                                        
                                        intiscolor = 0
                                        intisblackandwhite = 0
                                        intissilent = 0
                                        intis3d = 0
                                        strcolortechnology = ""
                                        strfilmtechnology = ""
                                        straspectratio = ""
                                        strfilmformat = ""
                                        strsoundsystem = ""
                                        intnumaudiotracks = 0
                                        intisvalidformat = 0
                                        
                                        # Retrieving title and overview in the current language if any
                                        if strlang == "en":
                                            strsqlmovielang = "SELECT TITLE, OVERVIEW FROM T_WC_TMDB_MOVIE WHERE ID_MOVIE = " + str(lngmovieid) + " "
                                        else:
                                            strsqlmovielang = "SELECT TITLE, OVERVIEW FROM T_WC_TMDB_MOVIE_LANG WHERE ID_MOVIE = " + str(lngmovieid) + " AND LANG = '" + strlang + "' "
                                        #print(strsqlmovielang)
                                        cursor2.execute(strsqlmovielang)
                                        results2 = cursor2.fetchall()
                                        # Iterating through the results 
                                        for row2 in results2:
                                            strmovietitle = row2['TITLE']
                                            strmovieoverview = row2['OVERVIEW']
                                            break
                                        
                                        strmovietitlealt = ""
                                        strmovieoverviewalt = ""
                                        # Retrieving title and overview in the current language if any
                                        if strlangalt == 'en':
                                            strsqlmovielang = "SELECT TITLE, OVERVIEW FROM T_WC_TMDB_MOVIE WHERE ID_MOVIE = " + str(lngmovieid) + " "
                                            #print(strsqlmovielang)
                                            cursor2.execute(strsqlmovielang)
                                            results2 = cursor2.fetchall()
                                            # Iterating through the results 
                                            for row2 in results2:
                                                if row2['TITLE']:
                                                    strmovietitlealt = row2['TITLE']
                                                if row2['OVERVIEW']:
                                                    strmovieoverviewalt = row2['OVERVIEW']
                                                break
                                        
                                        if strmovieoverview != "" and strmovietitle != "":
                                            if strlang == "fr":
                                                # Fixing movie overview when it contains a \' element (espaped as \\\')
                                                strmovieoverview=strmovieoverview.replace("\\\'", "'")
                                                # Fixing movie overview when it contains a \" element (espaped as \\\")
                                                strmovieoverview=strmovieoverview.replace('\\\"', '"')
                                                #print(strmovieoverview)
                                                # Movie title and movie overview are provided so we can go further
                                                # Process movie overview with Spacy to get lemmas and NER
                                                doc = nlp(strmovieoverview)
                                                # Return tokens and their POS tags only for NOUN, PROPN, VERB, or ADJ
                                                #doc_pos = [(token.lemma_, token.pos_, token.idx) for token in doc if token.pos_ in ["NOUN", "PROPN", "VERB", "ADJ"]]
                                                #doc_ner = [(ent.text, ent.label_, ent.start_char) for ent in doc.ents]
                                                # Process tokens for Part Of Speech (POS)
                                                for token in doc:
                                                    if token.pos_ in ["NOUN", "PROPN", "VERB", "ADJ", "X", "NUM"]:
                                                        lnglemmeid = 0
                                                        strtokenlemma = token.lemma_.strip()
                                                        if strtokenlemma != "\\":
                                                            strtokenpos = token.pos_
                                                            lngstartchar = token.idx
                                                            if strmovieoverviewlemma != "":
                                                                strmovieoverviewlemma += " "
                                                            strmovieoverviewlemma += strtokenlemma
                                                            strsqllemme = "SELECT ID_LEMME FROM T_WC_TMDB_SPACY_LEMME WHERE LANG = '" + strlang + "' AND NAME = '" + strtokenlemma.replace("'", "\\'") + "' AND LABEL = '" + strtokenpos + "' ORDER BY ID_LEMME "
                                                            #print(strsqllemme)
                                                            cursor2.execute(strsqllemme)
                                                            lngrowcount = cursor2.rowcount
                                                            if lngrowcount > 0:
                                                                results2 = cursor2.fetchall()
                                                                # Iterating through the results 
                                                                for row2 in results2:
                                                                    lnglemmeid = row2['ID_LEMME']
                                                                    #print(strsqllemme,"-> FOUND",lnglemmeid)
                                                                    break
                                                            if lnglemmeid == 0:
                                                                # Lemme not found so INSERT 
                                                                #print(strsqllemme,"-> NOT FOUND")
                                                                arrcouples = {}
                                                                arrcouples["LANG"] = strlang
                                                                arrcouples["NAME"] = strtokenlemma
                                                                arrcouples["LABEL"] = strtokenpos
                                                                # INSERT/UPDATE this record
                                                                strsqltablename = "T_WC_TMDB_SPACY_LEMME"
                                                                strsqlupdatecondition = "1 = 0 "
                                                                lnglemmeid = cp.f_sqlupdatearray(strsqltablename,arrcouples,strsqlupdatecondition,1)
                                                            if lnglemmeid != 0:
                                                                # We now have a lemma id so we link this lemma to the current movie
                                                                arrcouples = {}
                                                                arrcouples["ID_MOVIE"] = lngmovieid
                                                                arrcouples["ID_LEMME"] = lnglemmeid
                                                                arrcouples["START_CHAR"] = lngstartchar
                                                                # INSERT/UPDATE this record
                                                                strsqltablename = "T_WC_TMDB_MOVIE_LEMME"
                                                                strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND ID_LEMME = {lnglemmeid} AND START_CHAR = {lngstartchar} "
                                                                lngresult = cp.f_sqlupdatearray(strsqltablename,arrcouples,strsqlupdatecondition,1)
                                                # Process NER
                                                for ent in doc.ents:
                                                    lnglemmeid = 0
                                                    strtokenlemma = ent.text.strip()
                                                    if strtokenlemma != "\\":
                                                        strtokenpos = ent.label_
                                                        lngstartchar = ent.start_char
                                                        strsqllemme = "SELECT ID_LEMME FROM T_WC_TMDB_SPACY_LEMME WHERE LANG = '" + strlang + "' AND NAME = '" + strtokenlemma.replace("'", "\\'") + "' AND LABEL = '" + strtokenpos + "' ORDER BY ID_LEMME "
                                                        cursor2.execute(strsqllemme)
                                                        lngrowcount = cursor2.rowcount
                                                        if lngrowcount > 0:
                                                            results2 = cursor2.fetchall()
                                                            # Iterating through the results 
                                                            for row2 in results2:
                                                                lnglemmeid = row2['ID_LEMME']
                                                                #print(strsqllemme,"-> FOUND",lnglemmeid)
                                                                break
                                                        if lnglemmeid == 0:
                                                            # Lemme not found so INSERT 
                                                            #print(strsqllemme,"-> NOT FOUND")
                                                            arrcouples = {}
                                                            arrcouples["LANG"] = strlang
                                                            arrcouples["NAME"] = strtokenlemma
                                                            arrcouples["LABEL"] = strtokenpos
                                                            # INSERT/UPDATE this record
                                                            strsqltablename = "T_WC_TMDB_SPACY_LEMME"
                                                            strsqlupdatecondition = "1 = 0 "
                                                            lnglemmeid = cp.f_sqlupdatearray(strsqltablename,arrcouples,strsqlupdatecondition,1)
                                                        if lnglemmeid != 0:
                                                            # We now have a lemma id so we link this lemma to the current movie
                                                            arrcouples = {}
                                                            arrcouples["ID_MOVIE"] = lngmovieid
                                                            arrcouples["ID_LEMME"] = lnglemmeid
                                                            arrcouples["START_CHAR"] = lngstartchar
                                                            # INSERT/UPDATE this record
                                                            strsqltablename = "T_WC_TMDB_MOVIE_LEMME"
                                                            strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND ID_LEMME = {lnglemmeid} AND START_CHAR = {lngstartchar} "
                                                            lngresult = cp.f_sqlupdatearray(strsqltablename,arrcouples,strsqlupdatecondition,1)
                                            
                                        stryearrelease = ""
                                        if datrelease:
                                            stryearrelease = datrelease.strftime("%Y")
                                        
                                        # Retrieving collection name if any
                                        strcollectionname = ""
                                        if lngmoviecollectionid:
                                            # In English
                                            # Adding this collection id to the tag list for the current movie
                                            strtags += " " + "c" + str(lngmoviecollectionid)
                                            strsqlcollection = "SELECT NAME FROM T_WC_TMDB_COLLECTION WHERE ID_COLLECTION = " + str(lngmoviecollectionid) + " "
                                            #print(strsqlcollection)
                                            cursor2.execute(strsqlcollection)
                                            results2 = cursor2.fetchall()
                                            # Iterating through the results 
                                            for row2 in results2:
                                                if row2['NAME'] != "":
                                                    strcollectionname = row2['NAME']
                                                    #print("-> strcollectionname",strcollectionname)
                                                break
                                            if strlang != "en":
                                                # In the current language
                                                strsqlcollection = "SELECT NAME FROM T_WC_TMDB_COLLECTION_LANG WHERE ID_COLLECTION = " + str(lngmoviecollectionid) + " AND LANG = '" + strlang + "' "
                                                #print(strsqlcollection)
                                                cursor2.execute(strsqlcollection)
                                                results2 = cursor2.fetchall()
                                                # Iterating through the results 
                                                for row2 in results2:
                                                    if row2['NAME'] != "":
                                                        strcollectionname = row2['NAME']
                                                        #print("-> strcollectionname",strcollectionname)
                                                    break
                                        
                                        # Retrieving original language
                                        stroriginallanguagename = ""
                                        if strmovieoriginallanguage != "":
                                            strsqllang = "SELECT DESCRIPTION FROM T_WC_TMDB_LANG_LANG WHERE LANG = '" + strmovieoriginallanguage + "' AND LANG_DISPLAY = '" + strlang + "' "
                                            #print(strsqllang)
                                            cursor2.execute(strsqllang)
                                            results2 = cursor2.fetchall()
                                            # Iterating through the results 
                                            for row2 in results2:
                                                if row2['DESCRIPTION']:
                                                    stroriginallanguagename = row2['DESCRIPTION']
                                                break
                                        
                                        # Retrieving genres
                                        #print(strmoviegenres)
                                        intdocumentary = False
                                        if strmoviegenres != "":
                                            if "|Documentary|" in strmoviegenres:
                                                # This is a documentary
                                                intdocumentary = True
                                        strmoviegenres = cp.f_genrestranslatefr(strmoviegenres)
                                        if strmoviegenres != "":
                                            if strmoviegenres[0] == "|":
                                                strmoviegenres = strmoviegenres[1:]
                                            if strmoviegenres != "":
                                                if strmoviegenres[-1] == "|":
                                                    strmoviegenres = strmoviegenres[:-1]
                                            strmoviegenresdb = strmoviegenres
                                            strmoviegenresdb = strmoviegenresdb.replace("-", "")
                                            strmoviegenresdb = strmoviegenresdb.replace(" ", "")
                                            strmoviegenresdb = strmoviegenresdb.replace("|"," ")
                                            strtags += " " + strmoviegenresdb
                                            strmoviegenres = strmoviegenres.replace("|",", ")
                                        
                                        # Retrieving color technology
                                        #print('strmoviecolortech', strmoviecolortech)
                                        if strmoviecolortech:
                                            if strmoviecolortech != "":
                                                if strmoviecolortech[0] == "|":
                                                    strmoviecolortech = strmoviecolortech[1:]
                                                if strmoviecolortech != "":
                                                    if strmoviecolortech[-1] == "|":
                                                        strmoviecolortech = strmoviecolortech[:-1]
                                                strmoviecolortech = strmoviecolortech.replace("|",", ")
                                            
                                        # Retrieving film technology
                                        #print('strmoviefilmtech', strmoviefilmtech)
                                        if strmoviefilmtech:
                                            if strmoviefilmtech != "":
                                                if strmoviefilmtech[0] == "|":
                                                    strmoviefilmtech = strmoviefilmtech[1:]
                                                if strmoviefilmtech != "":
                                                    if strmoviefilmtech[-1] == "|":
                                                        strmoviefilmtech = strmoviefilmtech[:-1]
                                                strmoviefilmtech = strmoviefilmtech.replace("|",", ")
                                            
                                        # Retrieving sound system
                                        #print('strmoviesoundsystem', strmoviesoundsystem)
                                        if strmoviesoundsystem:
                                            if strmoviesoundsystem != "":
                                                if strmoviesoundsystem[0] == "|":
                                                    strmoviesoundsystem = strmoviesoundsystem[1:]
                                                if strmoviesoundsystem != "":
                                                    if strmoviesoundsystem[-1] == "|":
                                                        strmoviesoundsystem = strmoviesoundsystem[:-1]
                                                strmoviesoundsystem = strmoviesoundsystem.replace("|",", ")
                                            
                                        # Retrieving sound technology
                                        #print('strmoviesoundtech', strmoviesoundtech)
                                        if strmoviesoundtech:
                                            if strmoviesoundtech != "":
                                                if strmoviesoundtech[0] == "|":
                                                    strmoviesoundtech = strmoviesoundtech[1:]
                                                if strmoviesoundtech != "":
                                                    if strmoviesoundtech[-1] == "|":
                                                        strmoviesoundtech = strmoviesoundtech[:-1]
                                                strmoviesoundtech = strmoviesoundtech.replace("|",", ")
                                            
                                        # Retrieving keywords
                                        if strlang != "en":
                                            strkeywordstable = cp.strsqlns + "TMDB_KEYWORD_LANG"
                                        else:
                                            strkeywordstable = cp.strsqlns + "TMDB_KEYWORD"
                                        strsqlkeywords = "SELECT " + strkeywordstable + ".ID_KEYWORD, " + strkeywordstable + ".NAME "
                                        strsqlkeywords +="FROM T_WC_TMDB_MOVIE_KEYWORD "
                                        strsqlkeywords +="INNER JOIN " + strkeywordstable + " ON T_WC_TMDB_MOVIE_KEYWORD.ID_KEYWORD = " + strkeywordstable + ".ID_KEYWORD "
                                        strsqlkeywords +="WHERE T_WC_TMDB_MOVIE_KEYWORD.ID_MOVIE = " + str(lngmovieid) + " AND T_WC_TMDB_MOVIE_KEYWORD.DELETED = 0 "
                                        if strlang != "en":
                                            strsqlkeywords += " AND " + strkeywordstable + ".LANG = '" + strlang + "' "
                                        strsqlkeywords += "ORDER BY T_WC_TMDB_MOVIE_KEYWORD.DISPLAY_ORDER "
                                        strmoviekeywords = ""
                                        strsep = ", "
                                        #print(strsqlkeywords)
                                        cursor2.execute(strsqlkeywords)
                                        results2 = cursor2.fetchall()
                                        # Iterating through the results 
                                        for row2 in results2:
                                            lngmoviekeywordid = row2['ID_KEYWORD']
                                            # Adding this keyword id to the tag list for the current movie
                                            strtags += " " + "k" + str(lngmoviekeywordid)
                                            strmoviekeyword = row2['NAME']
                                            if strmoviekeywordslemma != "":
                                                strmoviekeywordslemma += " "
                                            strmoviekeywordslemma += f_getlemma(strmoviekeyword)
                                            if len(strmoviekeywords + strsep + strmoviekeyword) > lngmaxlengthkeywords:
                                                # String is too long, so we stop appending keywords
                                                break
                                            else:
                                                # Adding this keyword
                                                if strmoviekeywords != "":
                                                    strmoviekeywords += strsep
                                                strmoviekeywords += strmoviekeyword
                                                # Marking this keyword as used for tags
                                                strsqlupdate = "UPDATE T_WC_TMDB_KEYWORD SET USED_FOR_SIMILARITY = 1 WHERE ID_KEYWORD = " + str(lngmoviekeywordid)
                                                # print(strsqlupdate)
                                                cursor3.execute(strsqlupdate)
                                                # Commit the changes to the database
                                                cp.connectioncp.commit()
                                        
                                        # Retrieving companies
                                        strmoviecompanieslemma = ""
                                        strsqlcompanies = "SELECT T_WC_TMDB_MOVIE_COMPANY.ID_COMPANY, T_WC_TMDB_COMPANY.NAME "
                                        strsqlcompanies +="FROM T_WC_TMDB_MOVIE_COMPANY "
                                        strsqlcompanies +="INNER JOIN T_WC_TMDB_COMPANY ON T_WC_TMDB_MOVIE_COMPANY.ID_COMPANY = T_WC_TMDB_COMPANY.ID_COMPANY "
                                        strsqlcompanies +="WHERE T_WC_TMDB_MOVIE_COMPANY.ID_MOVIE = " + str(lngmovieid) + " AND T_WC_TMDB_MOVIE_COMPANY.DELETED = 0 "
                                        strsqlcompanies += "ORDER BY T_WC_TMDB_MOVIE_COMPANY.DISPLAY_ORDER "
                                        strmoviecompanies = ""
                                        strsep = ", "
                                        #print(strsqlcompanies)
                                        cursor2.execute(strsqlcompanies)
                                        results2 = cursor2.fetchall()
                                        # Iterating through the results 
                                        for row2 in results2:
                                            lngmoviecompanyid = row2['ID_COMPANY']
                                            # Adding this company ID_COMPANY to the tag list for the current movie
                                            strtags += " " + "o" + str(lngmoviecompanyid)
                                            strmoviecompany = row2['NAME']
                                            if strmoviecompanieslemma != "":
                                                strmoviecompanieslemma += " "
                                            strmoviecompanieslemma += f_getlemma(strmoviecompany)
                                            if len(strmoviecompanies + strsep + strmoviecompany) > lngmaxlengthcompanies:
                                                # String is too long, so we stop appending companies
                                                break
                                            else:
                                                # Adding this company
                                                if strmoviecompanies != "":
                                                    strmoviecompanies += strsep
                                                strmoviecompanies += strmoviecompany
                                                # Marking this company as used for tags
                                                strsqlupdate = "UPDATE T_WC_TMDB_COMPANY SET USED_FOR_SIMILARITY = 1 WHERE ID_COMPANY = " + str(lngmoviecompanyid)
                                                # print(strsqlupdate)
                                                cursor3.execute(strsqlupdate)
                                                # Commit the changes to the database
                                                cp.connectioncp.commit()
                                        
                                        # Retrieving the IMDb rating if the IMDb ID is known and if the IMDb rating is set
                                        dblimdbrating = 0
                                        if strmovieidimdb != "":
                                            strsqlimdbrating = "SELECT averageRating FROM T_WC_IMDB_MOVIE_RATING_IMPORT WHERE tconst = '" + strmovieidimdb + "' "
                                        #print(strsqlimdbrating)
                                        cursor2.execute(strsqlimdbrating)
                                        results2 = cursor2.fetchall()
                                        # Iterating through the results 
                                        for row2 in results2:
                                            dblimdbrating = row2['averageRating']
                                            break
                                        if intdocumentary:
                                            # Adjust IMdB rating for documentary
                                            dblimdbratingadjusted = round(dblimdbrating - 1.5, 1)
                                            intisdocumentary = 1
                                            intismovie = 0
                                        else:
                                            # Keep IMdB rating for movie
                                            dblimdbratingadjusted = dblimdbrating
                                            intisdocumentary = 0
                                            intismovie = 1
                                        
                                        if dblimdbratingadjusted >= 6 or True:
                                            # IMDb rating is 6 or above, so we can go further
                                            # Retrieving the lists for the current movie
                                            strsqlmovielists="SELECT " + cp.strsqlns + "TMDB_LIST.ID_LIST, " + cp.strsqlns + "TMDB_LIST.NAME, " + cp.strsqlns + "TMDB_LIST.SHORT_NAME "
                                            #if strlang != "en":
                                            #    strsqlmovielists += ", " + cp.strsqlns + "TMDB_LIST_LANG.SHORT_NAME AS SHORT_NAME_LANG "
                                            strsqlmovielists += "FROM " + cp.strsqlns + "TMDB_LIST "
                                            strsqlmovielists += "INNER JOIN " + cp.strsqlns + "TMDB_MOVIE_LIST ON " + cp.strsqlns + "TMDB_MOVIE_LIST.ID_LIST = " + cp.strsqlns + "TMDB_LIST.ID_LIST "
                                            #if strlang != "en":
                                            #    strsqlmovielists += "LEFT JOIN " + cp.strsqlns + "TMDB_LIST_LANG ON " + cp.strsqlns + "TMDB_LIST.ID_LIST = " + cp.strsqlns + "TMDB_LIST_LANG.ID_LIST "
                                            strsqlmovielists += "WHERE " + cp.strsqlns + "TMDB_MOVIE_LIST.ID_MOVIE = " + str(lngmovieid) + " AND " + cp.strsqlns + "TMDB_LIST.DELETED = 0 "
                                            strsqlmovielists += "AND " + cp.strsqlns + "TMDB_LIST.USE_FOR_TAGGING >= 1 "
                                            #if strlang != "en":
                                            #    strsqlmovielists += "AND " + cp.strsqlns + "TMDB_LIST_LANG.LANG = '" + strlang + "' "
                                            strmovielists = ""
                                            strmovielistslemma = ""
                                            #print(strsqlmovielists)
                                            cursor2.execute(strsqlmovielists)
                                            results2 = cursor2.fetchall()
                                            # Iterating through the results 
                                            for row2 in results2:
                                                lngmovielistid = row2['ID_LIST']
                                                # Adding this list id to the tag list for the current movie
                                                strtags += " " + "l" + str(lngmovielistid)
                                                strmovielistname = row2['NAME']
                                                #print('strmovielistname', strmovielistname, type(strmovielistname))
                                                strmovielistshortname = row2['SHORT_NAME']
                                                #print('strmovielistshortname', strmovielistshortname, type(strmovielistshortname))
                                                #strmovielistshortnamelang = row2['SHORT_NAME_LANG']
                                                if type(strmovielistshortname) is str:
                                                    strmovielistname = strmovielistshortname
                                                    #print('strmovielistname', strmovielistname, type(strmovielistname))
                                                #if strlang != "en" and strmovielistshortnamelang != "":
                                                #    strmovielistname = strmovielistshortnamelang
                                                if strlang != "en":
                                                    strsqllistlang = "SELECT SHORT_NAME FROM " + cp.strsqlns + "TMDB_LIST_LANG WHERE DELETED = 0 AND ID_LIST = " + str(lngmovielistid) + " AND LANG = '" + strlang + "' "
                                                    #print(strsqllistlang)
                                                    cursor3.execute(strsqllistlang)
                                                    results3 = cursor3.fetchall()
                                                    for row3 in results3:
                                                        if row3['SHORT_NAME'] != "":
                                                            if type(row3['SHORT_NAME']) is str:
                                                                strmovielistname = row3['SHORT_NAME']
                                                                #print('strmovielistname', strmovielistname, type(strmovielistname))
                                                if strmovielistname:
                                                    if strmovielists != "":
                                                        strmovielists += ", "
                                                    strmovielists += strmovielistname
                                                    if strmovielistslemma != "":
                                                        strmovielistslemma += " "
                                                    strmovielistslemma += f_getlemma(strmovielistname)
                                                    #print('strmovielists',strmovielists)
                                            
                                            # If the current movie has a Wikidata id, retrieving wikidata data including Criterion Collection data
                                            lngmovieidcriterion = 0
                                            lngmovieidcriterionspine = 0
                                            strmoviealiases = ""
                                            if strmovieidwikidata != "":
                                                strsqlwikidata = "SELECT * FROM " + cp.strsqlns + "WIKIDATA_MOVIE_V1 WHERE ID_WIKIDATA = '" + strmovieidwikidata +"' "
                                                cursor3.execute(strsqlwikidata)
                                                results3 = cursor3.fetchall()
                                                for row3 in results3:
                                                    if row3['ID_CRITERION']:
                                                        lngmovieidcriterion = row3['ID_CRITERION']
                                                    if row3['ID_CRITERION_SPINE']:
                                                        lngmovieidcriterionspine = row3['ID_CRITERION_SPINE']
                                                    if row3['ALIASES']:
                                                        strmoviealiases = row3['ALIASES']
                                            
                                            strwikidatainstanceoftext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P31",", ")
                                            strwikidataformofcreativeworktext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P7937",", ")
                                            strwikidatagenrestext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P136",", ")
                                            strwikidatadepictstext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P180",", ")
                                            strwikidatacolortext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P462",", ")
                                            strwikidataaspectratiotext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P2061",", ")
                                            strwikidataoriginalfilmformattext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P3803",", ")
                                            strwikidatafabricationmethodtext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P2079",", ")
                                            strwikidatadistributedbytext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P750",", ")
                                            strwikidataproductioncompanytext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P272",", ")
                                            strwikidatamainsubjecttext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P921",", ")
                                            strwikidatanarrativelocationtext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P840",", ")
                                            strwikidatafilminglocationtext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P915",", ")
                                            strwikidatacharacterstext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P674",", ")
                                            strwikidatacasttext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P161",", ")
                                            #strwikidatanominatedfortext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P1411",", ")
                                            strwikidataawardreceivedtext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P166",", ")
                                            strwikidatabasedontext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P144",", ")
                                            strwikidatainspiredbytext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P941",", ")
                                            strwikidataderivativeworktext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P4969",", ")
                                            strwikidatamovementtext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P135",", ")
                                            strwikidatasetinenvironmenttext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P8411",", ")
                                            strwikidatatakesplaceinfictionaluniversetext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P1434",", ")
                                            strwikidatacountryoforigintext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P495",", ")
                                            strwikidatapartoftext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P361",", ")
                                            strwikidatapartoftheseriestext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P179",", ")
                                            strwikidatadescribedbysourcetext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P1343",", ")
                                            strwikidatacollectiontext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P195",", ")
                                            strwikidatamediafranchisetext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P8345",", ")
                                            strwikidatacncfilmratingfrancetext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P2758",", ")
                                            strwikidatainternetarchiveidtext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P724",", ")
                                            strwikidatasetinperiodtext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P2408",", ")
                                            strwikidatasetduringrecurringeventtext = f_wikidataitemproperties(strlang,strmovieidwikidata,"P9215",", ")
                                            
                                            #print(f"Cast (P161): {strwikidatacasttext}")
                                            
                                            
                                            
                                            
                                            
                                            # Cast processing
                                            strsqlmoviecast = ""
                                            strsqlmoviecast += "SELECT " + cp.strsqlns + "TMDB_PERSON.ID_PERSON, " + cp.strsqlns + "TMDB_PERSON.NAME, " + cp.strsqlns + "TMDB_PERSON.ID_WIKIDATA, " + cp.strsqlns + "WIKIDATA_PERSON_V1.ALIASES, " + cp.strsqlns + "TMDB_PERSON_MOVIE.CAST_CHARACTER "
                                            strsqlmoviecast += "FROM " + cp.strsqlns + "TMDB_PERSON_MOVIE "
                                            strsqlmoviecast += "INNER JOIN " + cp.strsqlns + "TMDB_PERSON ON " + cp.strsqlns + "TMDB_PERSON_MOVIE.ID_PERSON = " + cp.strsqlns + "TMDB_PERSON.ID_PERSON "
                                            strsqlmoviecast += "INNER JOIN " + cp.strsqlns + "WIKIDATA_PERSON_V1 ON " + cp.strsqlns + "TMDB_PERSON.ID_WIKIDATA = " + cp.strsqlns + "WIKIDATA_PERSON_V1.ID_WIKIDATA "
                                            strsqlmoviecast += "WHERE " + cp.strsqlns + "TMDB_PERSON_MOVIE.ID_MOVIE = " + str(lngmovieid) + " "
                                            strsqlmoviecast += "AND " + cp.strsqlns + "TMDB_PERSON_MOVIE.CREDIT_TYPE = 'cast' "
                                            # Adding a condition to keep only persons with a Wikidata id, so probably a Wikipedia page
                                            #strsqlmoviecast += "AND T_WC_TMDB_PERSON.ID_WIKIDATA <> '' "
                                            strsqlmoviecast += "AND " + cp.strsqlns + "TMDB_PERSON.ID_WIKIDATA LIKE 'Q%' "
                                            strsqlmoviecast += "ORDER BY " + cp.strsqlns + "TMDB_PERSON_MOVIE.DISPLAY_ORDER "
                                            strmoviecastswithaliases = ""
                                            strmoviecastswithoutaliases = ""
                                            lngcast = 0
                                            strsep = ", "
                                            #print(strsqlmoviecast)
                                            cursor2.execute(strsqlmoviecast)
                                            results2 = cursor2.fetchall()
                                            # Iterating through the results 
                                            for row2 in results2:
                                                lngmoviecastid = row2['ID_PERSON']
                                                strmoviecastname = row2['NAME']
                                                strmoviecastaliases = row2['ALIASES']
                                                if len(strmoviecastswithaliases + strsep + strmoviecastname) > lngmaxlengthcastwithaliases:
                                                    # String is too long, so we stop appending credits
                                                    break
                                                else:
                                                    # Adding this person id to the tag list for the current movie
                                                    strtags += " " + "p" + str(lngmoviecastid)
                                                    f_tmdbpersonsetusedfortags(lngmoviecastid)
                                                    # Adding this cast credit
                                                    if strmoviecastswithaliases != "":
                                                        strmoviecastswithaliases += strsep
                                                    strmoviecastswithaliases += strmoviecastname
                                                    if strmoviecastswithoutaliases != "":
                                                        strmoviecastswithoutaliases += strsep
                                                    strmoviecastswithoutaliases += strmoviecastname
                                                    if strmoviecastaliases:
                                                        if strmoviecastaliases != "":
                                                            strmoviecastaliases = strmoviecastaliases.replace("|"," ").strip()
                                                            if strmoviecastaliases != "":
                                                                strmoviecastswithaliases += " " + strmoviecastaliases
                                                    lngcast += 1
                                                    if lngcast >= lngmaxcast:
                                                        break
                                            
                                            # Crew processing
                                            strsqlmoviecrew = ""
                                            strsqlmoviecrew += "SELECT " + cp.strsqlns + "TMDB_PERSON.ID_PERSON, " + cp.strsqlns + "TMDB_PERSON.NAME, " + cp.strsqlns + "TMDB_PERSON.ID_WIKIDATA, " + cp.strsqlns + "WIKIDATA_PERSON_V1.ALIASES, " + cp.strsqlns + "TMDB_PERSON_MOVIE.CREW_DEPARTMENT, " + cp.strsqlns + "TMDB_PERSON_MOVIE.CREW_JOB "
                                            strsqlmoviecrew += "FROM " + cp.strsqlns + "TMDB_PERSON_MOVIE "
                                            strsqlmoviecrew += "INNER JOIN " + cp.strsqlns + "TMDB_PERSON ON " + cp.strsqlns + "TMDB_PERSON_MOVIE.ID_PERSON = " + cp.strsqlns + "TMDB_PERSON.ID_PERSON "
                                            strsqlmoviecrew += "INNER JOIN " + cp.strsqlns + "WIKIDATA_PERSON_V1 ON " + cp.strsqlns + "TMDB_PERSON.ID_WIKIDATA = " + cp.strsqlns + "WIKIDATA_PERSON_V1.ID_WIKIDATA "
                                            strsqlmoviecrew += "WHERE " + cp.strsqlns + "TMDB_PERSON_MOVIE.ID_MOVIE = " + str(lngmovieid) + " "
                                            strsqlmoviecrew += "AND " + cp.strsqlns + "TMDB_PERSON_MOVIE.CREDIT_TYPE = 'crew' "
                                            strsqlmoviecrew += "AND " + cp.strsqlns + "TMDB_PERSON_MOVIE.CREW_DEPARTMENT IN ('Art', 'Camera', 'Directing', 'Editing', 'Sound', 'Writing', 'Production', 'Costume & Make-Up', 'Visual Effects') "
                                            #strsqlmoviecrew += "AND CREW_JOB IN ('Director', 'Camera', 'Directing', 'Editing', 'Sound', 'Writing') "
                                            # Adding a condition to keep only persons with a Wikidata id, so probably a Wikipedia page
                                            #strsqlmoviecrew += "AND T_WC_TMDB_PERSON.ID_WIKIDATA <> '' "
                                            strsqlmoviecrew += "AND " + cp.strsqlns + "TMDB_PERSON.ID_WIKIDATA LIKE 'Q%' "
                                            strsqlmoviecrew += "ORDER BY " + cp.strsqlns + "TMDB_PERSON_MOVIE.DISPLAY_ORDER "
                                            strmoviecrewswithaliases = ""
                                            strmoviecrewswithoutaliases = ""
                                            strmoviedirectorswithaliases = ""
                                            strmoviedirectorswithoutaliases = ""
                                            strmoviewriterswithaliases = ""
                                            strmoviewriterswithoutaliases = ""
                                            strmovieproducerswithaliases = ""
                                            strmovieproducerswithoutaliases = ""
                                            strmovieeditorswithoutaliases = ""
                                            strmovieartwithoutaliases = ""
                                            strmoviecamerawithoutaliases = ""
                                            strmovielightningwithoutaliases = ""
                                            strmoviesoundwithoutaliases = ""
                                            strmoviecostumemakeupwithoutaliases = ""
                                            strmovievisualeffectswithoutaliases = ""
                                            lngcrew = 0
                                            lngdirectors = 0
                                            lngwriters = 0
                                            lngproducers = 0
                                            lngeditors = 0
                                            lngart = 0
                                            lngcamera = 0
                                            lnglightning = 0
                                            lngsound = 0
                                            lngcostumemakeup = 0
                                            lngvisualeffects = 0
                                            strsep = ", "
                                            #print(strsqlmoviecrew)
                                            cursor2.execute(strsqlmoviecrew)
                                            results2 = cursor2.fetchall()
                                            # Iterating through the results 
                                            for row2 in results2:
                                                lngmoviecrewid = row2['ID_PERSON']
                                                strmoviecrewname = row2['NAME']
                                                strmoviecrewaliases = row2['ALIASES']
                                                strmoviecrewdepartment = row2['CREW_DEPARTMENT']
                                                strmoviecrewjob = row2['CREW_JOB']
                                                if strmoviecrewaliases:
                                                    if strmoviecrewaliases != "":
                                                        strmoviecrewaliases = strmoviecrewaliases.replace("|"," ").strip()
                                                if strmoviecrewdepartment == "Directing":
                                                    if strmoviecrewjob == "Director":
                                                        # Handling director credit
                                                        strtmp = strsep + strmoviedirectorswithaliases + strsep
                                                        lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                        if lngpos == -1:
                                                            # Credit not found in directors so we can add it
                                                            strtmp = strsep + strmoviewriterswithaliases + strsep
                                                            lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                            if lngpos == -1 or intallowpersonmultiplecredit:
                                                                # Credit not found in writers so we can add it
                                                                strtmp = strsep + strmoviecrewswithaliases + strsep
                                                                lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                                if lngpos == -1 or intallowpersonmultiplecredit:
                                                                    # Credit not found in movie crew so we can add it
                                                                    if len(strmoviedirectorswithaliases + strsep + strmoviecrewname) <= lngmaxlengthdirectors:
                                                                        # String is NOT too long, so we continue appending directors credits
                                                                        if lngdirectors < lngmaxdirectors:
                                                                            # Adding this person id to the tag list for the current movie
                                                                            strtags += " " + "p" + str(lngmoviecrewid)
                                                                            f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                            # Adding this director credit
                                                                            lngdirectors += 1
                                                                            if strmoviedirectorswithaliases != "":
                                                                                strmoviedirectorswithaliases += strsep
                                                                            strmoviedirectorswithaliases += strmoviecrewname
                                                                            if strmoviedirectorswithoutaliases != "":
                                                                                strmoviedirectorswithoutaliases += strsep
                                                                            strmoviedirectorswithoutaliases += strmoviecrewname
                                                                            if strmoviecrewaliases:
                                                                                if strmoviecrewaliases != "":
                                                                                    strmoviedirectorswithaliases += " " + strmoviecrewaliases
                                                elif strmoviecrewdepartment == "Writing":
                                                    # Handling writing credit
                                                    strtmp = strsep + strmoviewriterswithaliases + strsep
                                                    lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                    if lngpos == -1:
                                                        # Credit not found in writers so we can add it
                                                        strtmp = strsep + strmoviedirectorswithaliases + strsep
                                                        lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                        if lngpos == -1 or intallowpersonmultiplecredit:
                                                            # Credit not found in directors so we can add it
                                                            strtmp = strsep + strmoviecrewswithaliases + strsep
                                                            lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                            if lngpos == -1 or intallowpersonmultiplecredit:
                                                                # Credit not found in movie crew so we can add it
                                                                if len(strmoviewriterswithaliases + strsep + strmoviecrewname) <= lngmaxlengthwriters:
                                                                    # String is NOT too long, so we continue appending writing credits
                                                                    if lngwriters < lngmaxwriters:
                                                                        # Adding this person id to the tag list for the current movie
                                                                        strtags += " " + "p" + str(lngmoviecrewid)
                                                                        f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                        # Adding this writing credit
                                                                        lngwriters += 1
                                                                        if strmoviewriterswithaliases != "":
                                                                            strmoviewriterswithaliases += strsep
                                                                        strmoviewriterswithaliases += strmoviecrewname
                                                                        if strmoviewriterswithoutaliases != "":
                                                                            strmoviewriterswithoutaliases += strsep
                                                                        strmoviewriterswithoutaliases += strmoviecrewname
                                                                        if strmoviecrewaliases:
                                                                            if strmoviecrewaliases != "":
                                                                                strmoviewriterswithaliases += " " + strmoviecrewaliases
                                                elif strmoviecrewdepartment == "Production":
                                                    # Handling production credit
                                                    #print(strmoviecrewname)
                                                    strtmp = strsep + strmovieproducerswithaliases + strsep
                                                    lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                    if lngpos == -1:
                                                        # Credit not found in producers so we can add it
                                                        #print("Credit not found in producers so we can add it")
                                                        strtmp = strsep + strmoviedirectorswithaliases + strsep
                                                        lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                        if lngpos == -1 or intallowpersonmultiplecredit:
                                                            # Credit not found in directors so we can add it
                                                            #print("Credit not found in directors so we can add it")
                                                            strtmp = strsep + strmoviecrewswithaliases + strsep
                                                            lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                            if lngpos == -1 or intallowpersonmultiplecredit:
                                                                #print("Credit not found in movie crew so we can add it")
                                                                # Credit not found in movie crew so we can add it
                                                                if len(strmovieproducerswithaliases + strsep + strmoviecrewname) <= lngmaxlengthproducers:
                                                                    # String is NOT too long, so we continue appending production credits
                                                                    if lngproducers < lngmaxproducers:
                                                                        # Adding this person id to the tag list for the current movie
                                                                        #print("Adding this person id to the tag list for the current movie")
                                                                        strtags += " " + "p" + str(lngmoviecrewid)
                                                                        f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                        # Adding this writing credit
                                                                        lngproducers += 1
                                                                        if strmovieproducerswithaliases != "":
                                                                            strmovieproducerswithaliases += strsep
                                                                        strmovieproducerswithaliases += strmoviecrewname
                                                                        #print("strmovieproducerswithaliases =",strmovieproducerswithaliases)
                                                                        if strmovieproducerswithoutaliases != "":
                                                                            strmovieproducerswithoutaliases += strsep
                                                                        strmovieproducerswithoutaliases += strmoviecrewname
                                                                        #print("strmovieproducerswithoutaliases =",strmovieproducerswithoutaliases)
                                                                        if strmoviecrewaliases:
                                                                            if strmoviecrewaliases != "":
                                                                                strmovieproducerswithaliases += " " + strmoviecrewaliases
                                                                                #print("strmovieproducerswithaliases =",strmovieproducerswithaliases)
                                                elif strmoviecrewdepartment == "Editing":
                                                    # Handling editing credit
                                                    #print(strmoviecrewname)
                                                    strtmp = strsep + strmovieeditorswithoutaliases + strsep
                                                    lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                    if lngpos == -1:
                                                        # Credit not found in editors so we can add it
                                                        #print("Credit not found in editors so we can add it")
                                                        if len(strmovieeditorswithoutaliases + strsep + strmoviecrewname) <= lngmaxlengtheditors:
                                                            # String is NOT too long, so we continue appending editing credits
                                                            if lngeditors < lngmaxeditors:
                                                                # Adding this person id to the tag list for the current movie
                                                                #print("Adding this person id to the tag list for the current movie")
                                                                strtags += " " + "p" + str(lngmoviecrewid)
                                                                f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                # Adding this editor credit
                                                                lngeditors += 1
                                                                if strmovieeditorswithoutaliases != "":
                                                                    strmovieeditorswithoutaliases += strsep
                                                                strmovieeditorswithoutaliases += strmoviecrewname
                                                                #print("strmovieeditorswithoutaliases =",strmovieeditorswithoutaliases)
                                                elif strmoviecrewdepartment == "Art":
                                                    # Handling art credit
                                                    #print(strmoviecrewname)
                                                    strtmp = strsep + strmovieartwithoutaliases + strsep
                                                    lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                    if lngpos == -1:
                                                        # Credit not found in art so we can add it
                                                        #print("Credit not found in art so we can add it")
                                                        if len(strmovieartwithoutaliases + strsep + strmoviecrewname) <= lngmaxlengthart:
                                                            # String is NOT too long, so we continue appending art credits
                                                            if lngart < lngmaxart:
                                                                # Adding this person id to the tag list for the current movie
                                                                #print("Adding this person id to the tag list for the current movie")
                                                                strtags += " " + "p" + str(lngmoviecrewid)
                                                                f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                # Adding this art credit
                                                                lngart += 1
                                                                if strmovieartwithoutaliases != "":
                                                                    strmovieartwithoutaliases += strsep
                                                                strmovieartwithoutaliases += strmoviecrewname
                                                                #print("strmovieartwithoutaliases =",strmovieartwithoutaliases)
                                                elif strmoviecrewdepartment == "Camera":
                                                    # Handling camera credit
                                                    #print(strmoviecrewname)
                                                    strtmp = strsep + strmoviecamerawithoutaliases + strsep
                                                    lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                    if lngpos == -1:
                                                        # Credit not found in camera so we can add it
                                                        #print("Credit not found in camera so we can add it")
                                                        if len(strmoviecamerawithoutaliases + strsep + strmoviecrewname) <= lngmaxlengthcamera:
                                                            # String is NOT too long, so we continue appending camera credits
                                                            if lngcamera < lngmaxcamera:
                                                                # Adding this person id to the tag list for the current movie
                                                                #print("Adding this person id to the tag list for the current movie")
                                                                strtags += " " + "p" + str(lngmoviecrewid)
                                                                f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                # Adding this camera credit
                                                                lngcamera += 1
                                                                if strmoviecamerawithoutaliases != "":
                                                                    strmoviecamerawithoutaliases += strsep
                                                                strmoviecamerawithoutaliases += strmoviecrewname
                                                                #print("strmoviecamerawithoutaliases =",strmoviecamerawithoutaliases)
                                                elif strmoviecrewdepartment == "Lightning":
                                                    # Handling lightning credit
                                                    #print(strmoviecrewname)
                                                    strtmp = strsep + strmovielightningwithoutaliases + strsep
                                                    lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                    if lngpos == -1:
                                                        # Credit not found in lightning so we can add it
                                                        #print("Credit not found in lightning so we can add it")
                                                        if len(strmovielightningwithoutaliases + strsep + strmoviecrewname) <= lngmaxlengthlightning:
                                                            # String is NOT too long, so we continue appending lightning credits
                                                            if lnglightning < lngmaxlightning:
                                                                # Adding this person id to the tag list for the current movie
                                                                #print("Adding this person id to the tag list for the current movie")
                                                                strtags += " " + "p" + str(lngmoviecrewid)
                                                                f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                # Adding this lightning credit
                                                                lnglightning += 1
                                                                if strmovielightningwithoutaliases != "":
                                                                    strmovielightningwithoutaliases += strsep
                                                                strmovielightningwithoutaliases += strmoviecrewname
                                                                #print("strmovielightningwithoutaliases =",strmovielightningwithoutaliases)
                                                elif strmoviecrewdepartment == "Sound":
                                                    # Handling sound credit
                                                    #print(strmoviecrewname)
                                                    strtmp = strsep + strmoviesoundwithoutaliases + strsep
                                                    lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                    if lngpos == -1:
                                                        # Credit not found in sound so we can add it
                                                        #print("Credit not found in sound so we can add it")
                                                        if len(strmoviesoundwithoutaliases + strsep + strmoviecrewname) <= lngmaxlengthsound:
                                                            # String is NOT too long, so we continue appending sound credits
                                                            if lngsound < lngmaxsound:
                                                                # Adding this person id to the tag list for the current movie
                                                                #print("Adding this person id to the tag list for the current movie")
                                                                strtags += " " + "p" + str(lngmoviecrewid)
                                                                f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                # Adding this sound credit
                                                                lngsound += 1
                                                                if strmoviesoundwithoutaliases != "":
                                                                    strmoviesoundwithoutaliases += strsep
                                                                strmoviesoundwithoutaliases += strmoviecrewname
                                                                #print("strmoviesoundwithoutaliases =",strmoviesoundwithoutaliases)
                                                elif strmoviecrewdepartment == "Costume & Make-Up":
                                                    # Handling costumemakeup credit
                                                    #print(strmoviecrewname)
                                                    strtmp = strsep + strmoviecostumemakeupwithoutaliases + strsep
                                                    lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                    if lngpos == -1:
                                                        # Credit not found in costumemakeup so we can add it
                                                        #print("Credit not found in costumemakeup so we can add it")
                                                        if len(strmoviecostumemakeupwithoutaliases + strsep + strmoviecrewname) <= lngmaxlengthcostumemakeup:
                                                            # String is NOT too long, so we continue appending costumemakeup credits
                                                            if lngcostumemakeup < lngmaxcostumemakeup:
                                                                # Adding this person id to the tag list for the current movie
                                                                #print("Adding this person id to the tag list for the current movie")
                                                                strtags += " " + "p" + str(lngmoviecrewid)
                                                                f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                # Adding this costumemakeup credit
                                                                lngcostumemakeup += 1
                                                                if strmoviecostumemakeupwithoutaliases != "":
                                                                    strmoviecostumemakeupwithoutaliases += strsep
                                                                strmoviecostumemakeupwithoutaliases += strmoviecrewname
                                                                #print("strmoviecostumemakeupwithoutaliases =",strmoviecostumemakeupwithoutaliases)
                                                elif strmoviecrewdepartment == "Visual Effects":
                                                    # Handling visualeffects credit
                                                    #print(strmoviecrewname)
                                                    strtmp = strsep + strmovievisualeffectswithoutaliases + strsep
                                                    lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                    if lngpos == -1:
                                                        # Credit not found in visualeffects so we can add it
                                                        #print("Credit not found in visualeffects so we can add it")
                                                        if len(strmovievisualeffectswithoutaliases + strsep + strmoviecrewname) <= lngmaxlengthvisualeffects:
                                                            # String is NOT too long, so we continue appending visualeffects credits
                                                            if lngvisualeffects < lngmaxvisualeffects:
                                                                # Adding this person id to the tag list for the current movie
                                                                #print("Adding this person id to the tag list for the current movie")
                                                                strtags += " " + "p" + str(lngmoviecrewid)
                                                                f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                # Adding this visualeffects credit
                                                                lngvisualeffects += 1
                                                                if strmovievisualeffectswithoutaliases != "":
                                                                    strmovievisualeffectswithoutaliases += strsep
                                                                strmovievisualeffectswithoutaliases += strmoviecrewname
                                                                #print("strmovievisualeffectswithoutaliases =",strmovievisualeffectswithoutaliases)
                                                else:
                                                    # Handling other crew credits
                                                    strtmp = strsep + strmoviecrewswithaliases + strsep
                                                    lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                    if lngpos == -1:
                                                        # Credit not found in crew so we can add it
                                                        strtmp = strsep + strmoviewriterswithaliases + strsep
                                                        lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                        if lngpos == -1 or intallowpersonmultiplecredit:
                                                            # Credit not found in writers so we can add it
                                                            strtmp = strsep + strmoviedirectorswithaliases + strsep
                                                            lngpos = strtmp.find(strsep + strmoviecrewname + strsep)
                                                            if lngpos == -1 or intallowpersonmultiplecredit:
                                                                # Credit not found in directors so we can add it
                                                                if len(strmoviecrewswithaliases + strsep + strmoviecrewname) <= lngmaxlengthcrewswithaliases:
                                                                    # String is NOT too long, so we continue appending crew credits
                                                                    if lngcrew < lngmaxcrews:
                                                                        # Adding this person id to the tag list for the current movie
                                                                        strtags += " " + "p" + str(lngmoviecrewid)
                                                                        f_tmdbpersonsetusedfortags(lngmoviecrewid)
                                                                        # Adding this crew credit
                                                                        lngcrew += 1
                                                                        if strmoviecrewswithaliases != "":
                                                                            strmoviecrewswithaliases += strsep
                                                                        strmoviecrewswithaliases += strmoviecrewname
                                                                        if strmoviecrewswithoutaliases != "":
                                                                            strmoviecrewswithoutaliases += strsep
                                                                        strmoviecrewswithoutaliases += strmoviecrewname
                                                                        if strmoviecrewaliases:
                                                                            if strmoviecrewaliases != "":
                                                                                strmoviecrewswithaliases += " " + strmoviecrewaliases
                                            
                                            # Finishing tags processing
                                            if strtags[0] == " ":
                                                strtags = strtags[1:]
                                            #print(lngmovieid,stryearrelease,strmovieidimdb,dblimdbrating,strmoviegenres,strmoviekeywords)
                                            
                                            strtextdocfr = ""
                                            strtextsbertfr = ""
                                            if intdocumentary:
                                                if strlang == "fr":
                                                    strmovietype = "documentaire"
                                                elif strlang == "en":
                                                    strmovietype = "documentary"
                                            else:
                                                if strlang == "fr":
                                                    strmovietype = "film"
                                                elif strlang == "en":
                                                    strmovietype = "movie"
                                            
                                            if strmovietitle:
                                                if strmovietitle != "":
                                                    strtextdocfr += " " + strmovietitle
                                                    if strlang == "fr":
                                                        strtextsbertfr += " \"" + strmovietitle + "\" est un " + strmovietype + " de " + stryearrelease
                                                    elif strlang == "en":
                                                        strtextsbertfr += " \"" + strmovietitle + "\" is a " + stryearrelease + " " + strmovietype
                                                    strmovietitlelemma = f_getlemma(strmovietitle)
                                                    if strmovietitlelemma != "" and strmovietitlelemma != strmovietitle:
                                                        strtextdocfr += " " + strmovietitlelemma
                                            if strmovieoriginaltitle:
                                                if strmovietitle:
                                                    if strmovieoriginaltitle != "" and strmovieoriginaltitle != strmovietitle:
                                                        strtextdocfr += " " + strmovieoriginaltitle
                                                        if strlang == "fr":
                                                            strtextsbertfr += " dont la langue et le titre original en " + stroriginallanguagename + " est \"" + strmovieoriginaltitle + "\""
                                                        elif strlang == "en":
                                                            strtextsbertfr += " whose language and original title in " + stroriginallanguagename + " is \"" + strmovieoriginaltitle + "\""
                                                    else:
                                                        if strlang == "fr":
                                                            strtextsbertfr += " dont la langue originale est " + stroriginallanguagename
                                                        elif strlang == "en":
                                                            strtextsbertfr += " whose original language is " + stroriginallanguagename
                                            strtextsbertfr += "."
                                            
                                            if strmovietitlealt:
                                                if strmovietitle:
                                                    if strmovieoriginaltitle:
                                                        if strmovietitlealt != "" and strmovietitlealt != strmovietitle and strmovietitlealt != strmovieoriginaltitle:
                                                            strtextdocfr += " " + strmovietitlealt
                                                            if strlang == "fr":
                                                                strtextsbertfr += " Son titre en anglais est \"" + strmovietitlealt + "\"."
                                            
                                            if strmoviealiases:
                                                if strmoviealiases != "":
                                                    strmoviealiases = strmoviealiases.replace("|" + strmovietitle + "|","|")
                                                    strmoviealiases = strmoviealiases.replace("|" + strmovieoriginaltitle + "|","|")
                                                    strmoviealiases = strmoviealiases.replace("|" + strmovietitlealt + "|","|")
                                                    #strmoviealiases = strmoviealiases.replace("|"," ").strip()
                                                    if strmoviealiases != "":
                                                        strtextdocfr += " " + strmoviealiases.replace("|"," ").strip()
                                                        strmoviealiasestmp = strmoviealiases.replace("|",", ")
                                                        strmoviealiasestmp = strmoviealiasestmp[2:]
                                                        strmoviealiasestmp = strmoviealiasestmp[:-2]
                                                        if strmoviealiasestmp != "," and strmoviealiasestmp != "":
                                                            if strlang == "fr":
                                                                strtextsbertfr += " Les autres titres sont: " + strmoviealiasestmp + "."
                                                            elif strlang == "en":
                                                                strtextsbertfr += " The other titles are: " + strmoviealiasestmp + "."
                                            
                                            if stryearrelease:
                                                if stryearrelease != "":
                                                    strtextdocfr += " " + stryearrelease
                                                    #strtextsbertfr += " Le film est sorti en " + stryearrelease + "."
                                            """
                                            if strmovieidimdb:
                                                if strmovieidimdb != "":
                                                    strtextsbertfr += " L'identifiant IMDb est "
                                                    strtextsbertfr += strmovieidimdb + "."
                                            if dblimdbrating:
                                                if dblimdbrating > 0:
                                                    strtextsbertfr += " La note IMDb est "
                                                    strtextsbertfr += str(dblimdbrating) + "."
                                            """
                                            if stroriginallanguagename:
                                                if stroriginallanguagename != "":
                                                    strtextdocfr += " " + stroriginallanguagename
                                                    #strtextsbertfr += " La langue originale du film est " + stroriginallanguagename + "."
                                            
                                            if not intdocumentary:
                                                # This is not a documentary but a movie
                                                strtextdocfr += " Film fiction"
                                                if strlang == "fr":
                                                    strtextsbertfr += " C'est une fiction."
                                                elif strlang == "en":
                                                    strtextsbertfr += " This is a fiction."
                                            if strmoviegenres:
                                                if strmoviegenres != "":
                                                    strtextdocfr += " " + strmoviegenres.replace(",", "")
                                                    if strlang == "fr":
                                                        strtextsbertfr += " Les genres du " + strmovietype + " sont : " + strmoviegenres
                                                    elif strlang == "en":
                                                        strtextsbertfr += " " + strmovietype + " genres are: " + strmoviegenres
                                                    if strwikidatagenrestext != "":
                                                        strtextsbertfr += ", " + strwikidatagenrestext
                                                    strtextsbertfr += "."
                                            
                                            if lngmovieruntime:
                                                if lngmovieruntime > 0:
                                                    if lngmovieruntime > 58:
                                                        if strlang == "fr":
                                                            strtextdocfr += " Long métrage"
                                                            strtextsbertfr += " C'est un long métrage de " + str(lngmovieruntime) + " minutes."
                                                        elif strlang == "en":
                                                            strtextdocfr += " Feature film"
                                                            strtextsbertfr += " This is a " + str(lngmovieruntime) + " minutes feature film."
                                                    else:
                                                        if strlang == "fr":
                                                            strtextdocfr += " Court métrage"
                                                            strtextsbertfr += " C'est un court métrage de " + str(lngmovieruntime) + " minutes."
                                                        elif strlang == "en":
                                                            strtextdocfr += " short film"
                                                            strtextsbertfr += " This is a " + str(lngmovieruntime) + " minutes short film."
                                            
                                            if strmoviekeywords:
                                                if strmoviekeywords != "":
                                                    strtextdocfr += " " + strmoviekeywordslemma
                                                    if strlang == "fr":
                                                        strtextsbertfr += " Les mots-clés du " + strmovietype + " sont : " + strmoviekeywords + "."
                                                    elif strlang == "en":
                                                        strtextsbertfr += " " + strmovietype + " keywords are: " + strmoviekeywords + "."
                                            
                                            if strcollectionname:
                                                if strcollectionname != "":
                                                    strtextdocfr += " " + strcollectionname
                                                    if strlang == "fr":
                                                        strtextsbertfr += " Ce " + strmovietype + " fait partie de la collection \"" + strcollectionname + "\""
                                                    elif strlang == "en":
                                                        strtextsbertfr += " This " + strmovietype + " is a part of the \"" + strcollectionname + "\" collection"
                                            if strmovielists:
                                                if strmovielists != "":
                                                    strtextdocfr += " " + strmovielistslemma
                                                    if strlang == "fr":
                                                        strtextsbertfr += " et il fait partie des listes suivantes : " + strmovielists + ""
                                                    elif strlang == "en":
                                                        strtextsbertfr += " and also part of the following lists: " + strmovielists + ""
                                            strtextsbertfr += "."
                                            
                                            if lngmovieidcriterion > 0:
                                                strcriteriontext = "Criterion Collection"
                                                if strlang == "fr":
                                                    strtextsbertfr + " Il fait partie de la Collection Criterion"
                                                elif strlang == "en":
                                                    strtextsbertfr + " It is in the Criterion Collection"
                                                if lngmovieidcriterionspine > 0:
                                                    strcriteriontext += " spine " + str(lngmovieidcriterionspine)
                                                    strtextsbertfr + " spine " + str(lngmovieidcriterionspine)
                                                strtextsbertfr += "."
                                                strtextdocfr += " " + strcriteriontext
                                            
                                            if strmoviecompanies:
                                                if strmoviecompanies != "":
                                                    strtextdocfr += " " + strmoviecompanies
                                                    #strtextdocfr += " " + strmoviecompanieslemma
                                                    if strlang == "fr":
                                                        strtextsbertfr += " Les sociétés de production du " + strmovietype + " sont : " + strmoviecompanies + "."
                                                    elif strlang == "en":
                                                        strtextsbertfr += " Production companies of this " + strmovietype + " are: " + strmoviecompanies + "."
                                            
                                            if strmoviecolortech:   
                                                if strmoviecolortech != "":
                                                    strtextdocfr += " " + strmoviecolortech.replace(",", "")
                                                    if strlang == "fr":
                                                        strtextsbertfr += " Les technologies de couleur du " + strmovietype + " sont : " + strmoviecolortech + "."
                                                    elif strlang == "en":
                                                        strtextsbertfr += " Color technologies of this " + strmovietype + " are: " + strmoviecolortech + "."
                                            if strmoviefilmtech:
                                                if strmoviefilmtech != "":
                                                    strtextdocfr += " " + strmoviefilmtech.replace(",", "")
                                                    if strlang == "fr":
                                                        strtextsbertfr += " Les technologies cinématographiques du " + strmovietype + " sont : " + strmoviefilmtech + "."
                                                    elif strlang == "en":
                                                        strtextsbertfr += " Cinematographic technologies of this " + strmovietype + " are: " + strmoviefilmtech + "."
                                            if strmoviesoundsystem:
                                                if strmoviesoundsystem != "":
                                                    strtextdocfr += " " + strmoviesoundsystem.replace(",", "")
                                                    if strlang == "fr":
                                                        strtextsbertfr += " Les systèmes sonores du " + strmovietype + " sont : " + strmoviesoundsystem + "."
                                                    elif strlang == "en":
                                                        strtextsbertfr += " Sound systems of this " + strmovietype + " are: " + strmoviesoundsystem + "."
                                            if strmoviesoundtech:
                                                if strmoviesoundtech != "":
                                                    strtextdocfr += " " + strmoviesoundtech.replace(",", "")
                                                    if strlang == "fr":
                                                        strtextsbertfr += " Les technologies sonores du " + strmovietype + " sont : " + strmoviesoundtech + "."
                                                    elif strlang == "en":
                                                        strtextsbertfr += " Sound technologies of this " + strmovietype + " are: " + strmoviesoundtech + "."
                                            
                                            if intincludepersonaliases:
                                                if strmoviedirectorswithaliases:
                                                    if strmoviedirectorswithaliases != "":
                                                        strtextdocfr += " " + strmoviedirectorswithaliases.replace(",", "")
                                            else:
                                                if strmoviedirectorswithoutaliases:
                                                    if strmoviedirectorswithoutaliases != "":
                                                        strtextdocfr += " " + strmoviedirectorswithoutaliases.replace(",", "")
                                            if strmoviedirectorswithoutaliases:
                                                if strmoviedirectorswithoutaliases != "":
                                                    if strlang == "fr":
                                                        strtextsbertfr += " Le " + strmovietype + " a été réalisé par " + strmoviedirectorswithoutaliases + ""
                                                    elif strlang == "en":
                                                        strtextsbertfr += " This " + strmovietype + " was directed by " + strmoviedirectorswithoutaliases + ""
                                            
                                            if intincludepersonaliases:
                                                if strmoviewriterswithaliases:
                                                    if strmoviewriterswithaliases != "":
                                                        strtextdocfr += " " + strmoviewriterswithaliases.replace(",", "")
                                            else:
                                                if strmoviewriterswithoutaliases:
                                                    if strmoviewriterswithoutaliases != "":
                                                        strtextdocfr += " " + strmoviewriterswithoutaliases.replace(",", "")
                                            if strmoviewriterswithoutaliases:
                                                if strmoviewriterswithoutaliases != "":
                                                    if strlang == "fr":
                                                        strtextsbertfr += " et il a été écrit par " + strmoviewriterswithoutaliases + ""
                                                    elif strlang == "en":
                                                        strtextsbertfr += " and written by " + strmoviewriterswithoutaliases + ""
                                            
                                            if intincludepersonaliases:
                                                if strmovieproducerswithaliases:
                                                    if strmovieproducerswithaliases != "":
                                                        strtextdocfr += " " + strmovieproducerswithaliases.replace(",", "")
                                            else:
                                                if strmovieproducerswithoutaliases:
                                                    if strmovieproducerswithoutaliases != "":
                                                        strtextdocfr += " " + strmovieproducerswithoutaliases.replace(",", "")
                                            if strmovieproducerswithoutaliases:
                                                if strmovieproducerswithoutaliases != "":
                                                    if strlang == "fr":
                                                        strtextsbertfr += " et il a été produit par " + strmovieproducerswithoutaliases + ""
                                                    elif strlang == "en":
                                                        strtextsbertfr += " and produced by " + strmovieproducerswithoutaliases + ""
                                            strtextsbertfr += "."
                                            
                                            if intincludepersonaliases:
                                                if strmoviecastswithaliases:
                                                    if strmoviecastswithaliases != "":
                                                        strtextdocfr += " " + strmoviecastswithaliases.replace(",", "")
                                            else:
                                                if strmoviecastswithoutaliases:
                                                    if strmoviecastswithoutaliases != "":
                                                        strtextdocfr += " " + strmoviecastswithoutaliases.replace(",", "")
                                            if strmoviecastswithoutaliases:
                                                if strmoviecastswithoutaliases != "":
                                                    if strlang == "fr":
                                                        strtextsbertfr += " La distribution principale est la suivante : " + strmoviecastswithoutaliases + "."
                                                    elif strlang == "en":
                                                        strtextsbertfr += " Main cast is with " + strmoviecastswithoutaliases + "."
                                            """
                                            if intincludepersonaliases:
                                                if strmoviecrewswithaliases:
                                                    if strmoviecrewswithaliases != "":
                                                        strtextdocfr += " " + strmoviecrewswithaliases.replace(",", "")
                                            else:
                                                if strmoviecrewswithoutaliases:
                                                    if strmoviecrewswithoutaliases != "":
                                                        strtextdocfr += " " + strmoviecrewswithoutaliases.replace(",", "")
                                            if strmoviecrewswithoutaliases:
                                                if strmoviecrewswithoutaliases != "":
                                                    if strlang == "fr":
                                                        strtextsbertfr += " L'équipe technique est la suivante : " + strmoviecrewswithoutaliases + "."
                                                    elif strlang == "en":
                                                        strtextsbertfr += " Crew is: " + strmoviecrewswithoutaliases + "."
                                            """
                                            if strmovieoverview:
                                                if strmovieoverview != "":
                                                    strtextdocfr += " " + strmovieoverviewlemma
                                                    if strlang == "fr":
                                                        strtextsbertfr += " Voici le résumé du " + strmovietype + " : " + strmovieoverview
                                                    elif strlang == "en":
                                                        strtextsbertfr += " Here is the " + strmovietype + " overview: " + strmovieoverview
                                            
                                            if strwikidatainstanceoftext != "":
                                                strtextdocfr += " " + strwikidatainstanceoftext
                                            if strwikidataformofcreativeworktext != "":
                                                strtextdocfr += " " + strwikidataformofcreativeworktext
                                            if strwikidatagenrestext != "":
                                                strtextdocfr += " " + strwikidatagenrestext
                                            if strwikidatadepictstext != "":
                                                strtextdocfr += " " + strwikidatadepictstext
                                            """
                                            if strwikidatacolortext != "":
                                                strtextdocfr += " " + strwikidatacolortext
                                                if strlang == "fr":
                                                    strtextsbertfr += " Le " + strmovietype + " est en " + strwikidatacolortext + "."
                                                elif strlang == "en":
                                                    strtextsbertfr += " The " + strmovietype + " is in " + strwikidatacolortext + "."
                                            if strwikidataaspectratiotext != "":
                                                strtextdocfr += " " + strwikidataaspectratiotext
                                                if strlang == "fr":
                                                    strtextsbertfr += " Le " + strmovietype + " est en format " + strwikidataaspectratiotext + "."
                                                elif strlang == "en":
                                                    strtextsbertfr += " The " + strmovietype + " is in " + strwikidataaspectratiotext + " format."
                                            if strwikidataoriginalfilmformattext != "":
                                                strtextdocfr += " " + strwikidataoriginalfilmformattext
                                            if strwikidatafabricationmethodtext != "":
                                                strtextdocfr += " " + strwikidatafabricationmethodtext
                                            if strwikidatadistributedbytext != "":
                                                strtextdocfr += " " + strwikidatadistributedbytext
                                            if strwikidataproductioncompanytext != "":
                                                strtextdocfr += " " + strwikidataproductioncompanytext
                                            """
                                            if strwikidatamainsubjecttext != "":
                                                strtextdocfr += " " + strwikidatamainsubjecttext
                                                if strlang == "fr":
                                                    strtextsbertfr += " Le sujet principal est " + strwikidatamainsubjecttext + "."
                                                elif strlang == "en":
                                                    strtextsbertfr += " The main subject is " + strwikidatamainsubjecttext + "."
                                            if strwikidatanarrativelocationtext != "":
                                                strtextdocfr += " " + strwikidatanarrativelocationtext
                                                if strlang == "fr":
                                                    strtextsbertfr += " Il se passe à " + strwikidatanarrativelocationtext + "."
                                                elif strlang == "en":
                                                    strtextsbertfr += " Narrative location is in " + strwikidatanarrativelocationtext + "."
                                            if strwikidatafilminglocationtext != "":
                                                strtextdocfr += " " + strwikidatafilminglocationtext
                                                if strlang == "fr":
                                                    strtextsbertfr += " Il a été tourné à " + strwikidatafilminglocationtext + "."
                                                elif strlang == "en":
                                                    strtextsbertfr += " Filming location is in " + strwikidatafilminglocationtext + "."
                                            if strwikidatacharacterstext != "":
                                                strtextdocfr += " " + strwikidatacharacterstext
                                                if strlang == "fr":
                                                    strtextsbertfr += " Les personnages du " + strmovietype + " sont " + strwikidatacharacterstext + "."
                                                elif strlang == "en":
                                                    strtextsbertfr += " Characters of the " + strmovietype + " are " + strwikidatacharacterstext + "."
                                            if strwikidatacasttext != "":
                                                strtextdocfr += " " + strwikidatacasttext
                                            #if strwikidatanominatedfortext != "":
                                            #    strtextdocfr += " " + strwikidatanominatedfortext
                                            if strwikidataawardreceivedtext != "":
                                                strtextdocfr += " " + strwikidataawardreceivedtext
                                                if strlang == "fr":
                                                    strtextsbertfr += " Il a reçu les prix suivants : " + strwikidataawardreceivedtext + "."
                                                elif strlang == "en":
                                                    strtextsbertfr += " It received the following awards: " + strwikidataawardreceivedtext + "."
                                            if strwikidatabasedontext != "":
                                                strtextdocfr += " " + strwikidatabasedontext
                                            if strwikidatainspiredbytext != "":
                                                strtextdocfr += " " + strwikidatainspiredbytext
                                            if strwikidataderivativeworktext != "":
                                                strtextdocfr += " " + strwikidataderivativeworktext
                                            if strwikidatamovementtext != "":
                                                strtextdocfr += " " + strwikidatamovementtext
                                                if strlang == "fr":
                                                    strtextsbertfr += " Il fait partie du mouvement " + strwikidatamovementtext + "."
                                                elif strlang == "en":
                                                    strtextsbertfr += " It is part of the " + strwikidatamovementtext + " movement."
                                            if strwikidatasetinenvironmenttext != "":
                                                strtextdocfr += " " + strwikidatasetinenvironmenttext
                                            if strwikidatatakesplaceinfictionaluniversetext != "":
                                                strtextdocfr += " " + strwikidatatakesplaceinfictionaluniversetext
                                                if strlang == "fr":
                                                    strtextsbertfr += " Il a lieu dans l'univers " + strwikidatatakesplaceinfictionaluniversetext + "."
                                                elif strlang == "en":
                                                    strtextsbertfr += " It happens in the " + strwikidatatakesplaceinfictionaluniversetext + " universe."
                                            if strwikidatacountryoforigintext != "":
                                                strtextdocfr += " " + strwikidatacountryoforigintext
                                            if strwikidatapartoftext != "":
                                                strtextdocfr += " " + strwikidatapartoftext
                                            if strwikidatapartoftheseriestext != "":
                                                strtextdocfr += " " + strwikidatapartoftheseriestext
                                            if strwikidatadescribedbysourcetext != "":
                                                strtextdocfr += " " + strwikidatadescribedbysourcetext
                                            if strwikidatacollectiontext != "":
                                                strtextdocfr += " " + strwikidatacollectiontext
                                            if strwikidatamediafranchisetext != "":
                                                strtextdocfr += " " + strwikidatamediafranchisetext
                                            if strwikidatacncfilmratingfrancetext != "":
                                                strtextdocfr += " " + strwikidatacncfilmratingfrancetext
                                            if strwikidatainternetarchiveidtext != "":
                                                strtextdocfr += " " + strwikidatainternetarchiveidtext
                                            if strwikidatasetinperiodtext != "":
                                                strtextdocfr += " " + strwikidatasetinperiodtext
                                            if strwikidatasetduringrecurringeventtext != "":
                                                strtextdocfr += " " + strwikidatasetduringrecurringeventtext
                                                if strlang == "fr":
                                                    strtextsbertfr += " Il a lieu pendant " + strwikidatasetduringrecurringeventtext + "."
                                                elif strlang == "en":
                                                    strtextsbertfr += " It is set during " + strwikidatasetduringrecurringeventtext + " recurring event."
                                            
                                            if strtextdocfr != "":
                                                strtextdocfr = strtextdocfr[1:]
                                                # Remove duplicate keywords while preserving order
                                                keywords = strtextdocfr.split()
                                                seen = set()
                                                unique_keywords = []
                                                for keyword in keywords:
                                                    if keyword not in seen:
                                                        seen.add(keyword)
                                                        unique_keywords.append(keyword)
                                                strtextdocfr = " ".join(unique_keywords)
                                            
                                            if strtextsbertfr != "":
                                                strtextsbertfr = strtextsbertfr[1:]

                                            # Storing pre processed data
                                            arrmoviecouples = {}
                                            arrmoviecouples["ID_MOVIE"] = lngmovieid
                                            arrmoviecouples["LANG"] = strlang
                                            arrmoviecouples["YEAR_RELEASE"] = stryearrelease
                                            arrmoviecouples["GENRES"] = strmoviegenres
                                            arrmoviecouples["KEYWORDS"] = strmoviekeywords
                                            arrmoviecouples["IMDB_RATING"] = dblimdbrating
                                            arrmoviecouples["IMDB_RATING_ADJUSTED"] = dblimdbratingadjusted
                                            arrmoviecouples["COLLECTION"] = strcollectionname
                                            arrmoviecouples["LISTS"] = strmovielists
                                            arrmoviecouples["CAST_TOP"] = strmoviecastswithoutaliases
                                            arrmoviecouples["CREW_TOP"] = strmoviecrewswithoutaliases
                                            arrmoviecouples["DIRECTORS"] = strmoviedirectorswithoutaliases
                                            arrmoviecouples["WRITERS"] = strmoviewriterswithoutaliases
                                            arrmoviecouples["PRODUCERS"] = strmovieproducerswithoutaliases
                                            arrmoviecouples["EDITORS"] = strmovieeditorswithoutaliases
                                            arrmoviecouples["ART"] = strmovieartwithoutaliases
                                            arrmoviecouples["CAMERA"] = strmoviecamerawithoutaliases
                                            arrmoviecouples["LIGHTNING"] = strmovielightningwithoutaliases
                                            arrmoviecouples["SOUND"] = strmoviesoundwithoutaliases
                                            arrmoviecouples["COSTUME_MAKEUP"] = strmoviecostumemakeupwithoutaliases
                                            arrmoviecouples["VISUAL_EFFECTS"] = strmovievisualeffectswithoutaliases
                                            arrmoviecouples["ORIGINAL_LANGUAGE"] = stroriginallanguagename
                                            arrmoviecouples["TAGS"] = strtags
                                            arrmoviecouples["OVERVIEW_LEMMA"] = strmovieoverviewlemma
                                            arrmoviecouples["KEYWORDS_LEMMA"] = strmoviekeywordslemma
                                            arrmoviecouples["LISTS_LEMMA"] = strmovielistslemma
                                            arrmoviecouples["TEXT_DOC"] = strtextdocfr
                                            arrmoviecouples["TEXT_SBERT"] = strtextsbertfr
                                            # INSERT/UPDATE this record
                                            strsqltablename = "T_WC_TMDB_MOVIE_LANG_META"
                                            strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND LANG = '{strlang}' "
                                            lngresult = cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
                                            
                                    
                                    strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
                                    cp.f_setservervariable("strtmdbmoviepreprocessdatetime",strnow,"Date and time of the last preprocessed record in the TMDb database",0)
                                    cp.f_setservervariable("strtmdbmoviepreprocesscurrentvalue",str(lngmovieid),"Current value while preprocessing data",0)
                            cp.f_setservervariable("strtmdbmoviepreprocesscurrentvalue","","Current value while preprocessing data",0)
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

