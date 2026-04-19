import time
import pymysql.cursors
import citizenphil as cp
import pandas as pd
import psutil
import re

MYSQL_RETRYABLE_ERROR_CODES = {1205, 1213}
nlp = None


def set_nlp(model):
    global nlp
    nlp = model


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
