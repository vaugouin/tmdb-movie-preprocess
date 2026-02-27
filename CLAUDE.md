# CLAUDE.md - TMDb Movie Preprocess Project

## Project Overview

This project is a Python-based data preprocessing system for TMDb (The Movie Database) movie information. It fetches movie data from the TMDb API, processes it, and stores it in a MySQL database for further analysis and use.

## Architecture

### Main Components

1. **tmdb-movie-preprocess.py** - Main preprocessing script that:
   - Fetches movie data from TMDb API
   - Processes cast, crew, keywords, companies, and other movie metadata
   - Uses spaCy (fr_core_news_lg) for NLP processing
   - Applies configurable limits for data storage
   - Stores processed data in MySQL database

2. **citizenphil.py** - Custom library module providing:
   - Database connection and operations
   - SQL utility functions for inserting/updating records
   - TMDb API interaction helpers
   - Authentication and configuration management

3. **citizenphilsecrets.py** - Configuration file (gitignored) containing:
   - Database credentials (host, port, user, password, database name)
   - TMDb API credentials (API key and bearer token)
   - User timezone settings
   - SQL namespace configuration

## Configuration

### Environment Setup

The project requires a `citizenphilsecrets.py` file with the following configuration:
```python
strdbhost = "your-database-host"
lngdbport = 3306
strdbuser = "your-username"
strdbpassword = "your-password"
strdbname = "your-database"
strsqlns = "your-sql-namespace"
strtmdbapidomainurl = "https://api.themoviedb.org/3"
strtmdbapikey = "your-api-key"
strtmdbapitoken = "your-bearer-token"
strusertimezone = "Europe/Paris"
```

Use `citizenphilsecrets.example.py` as a template.

### Preprocessing Limits

The main script includes configurable limits for data processing:
- `lngmaxcast = 20` - Maximum number of cast members to process
- `lngmaxdirectors = 10` - Maximum number of directors
- `lngmaxwriters = 10` - Maximum writers
- `lngmaxproducers = 10` - Maximum producers
- Various other crew role limits (editors, art, camera, sound, etc.)
- `lngmaxlengthkeywords = 3000` - Maximum length for keywords string
- `lngmaxlengthcompanies = 3000` - Maximum length for companies string
- `intallowpersonmultiplecredit = True` - Allow people to have multiple credits
- `intincludepersonaliases = False` - Include person name aliases

These settings can be adjusted in tmdb-movie-preprocess.py based on your needs.

## Dependencies

Key Python packages (see requirements.txt):
- **numpy, pandas** - Data processing
- **requests** - HTTP API calls
- **pymysql** - MySQL database connectivity
- **beautifulsoup4, lxml, html5lib** - HTML/XML parsing
- **spacy** - NLP processing (requires `fr_core_news_lg` model)
- **thefuzz** - Fuzzy string matching
- **python-dotenv** - Environment variable management
- **schedule, pytz** - Scheduling and timezone handling
- **psutil** - System resource monitoring

### spaCy Model Installation

Before running, install the French language model:
```bash
python -m spacy download fr_core_news_lg
```

## Docker Support

The project includes a Dockerfile for containerized deployment. This allows running the preprocessing system in an isolated environment with all dependencies pre-installed.

## Database Schema

The project uses a MySQL database with tables managed through the citizenphil module. The schema includes:
- Movie metadata tables
- Cast and crew tables with role information
- Keywords and companies tables
- Relationship tables connecting movies to people and companies

SQL operations use the `f_sqlupdatearray()` function for inserting/updating records with automatic timestamp management.

## API Integration

The project integrates with TMDb API v3:
- Uses bearer token authentication
- Default language: "en-US"
- API domain configurable via secrets file
- Includes rate limiting and error handling

## Development Guidelines

### Code Conventions

1. **Variable Naming**: Hungarian notation is used extensively
   - `str` prefix for strings (e.g., `strdbhost`)
   - `lng` prefix for long integers (e.g., `lngdbport`)
   - `int` prefix for integers/booleans (e.g., `intallowpersonmultiplecredit`)
   - `arr` prefix for arrays (e.g., `arrpersoncouples`)

2. **Module Structure**: Core functionality is abstracted into the `citizenphil` module to promote reusability

3. **Configuration**: All sensitive credentials are kept in gitignored secrets file

### Security Notes

- Never commit `citizenphilsecrets.py` to version control
- API tokens and database credentials must be kept secure
- The `.gitignore` file excludes secrets and data directories

## Data Flow

1. Script connects to MySQL database using credentials from secrets
2. Fetches movie data from TMDb API with bearer token authentication
3. Processes and normalizes data (cast, crew, keywords, etc.)
4. Applies configured limits to prevent data overflow
5. Uses spaCy for NLP processing of text fields
6. Stores processed data in MySQL database
7. Updates timestamps for tracking data freshness

## File Structure

```
.
├── tmdb-movie-preprocess.py    # Main preprocessing script
├── citizenphil.py              # Core library module
├── citizenphilsecrets.py       # Configuration (gitignored)
├── citizenphilsecrets.example.py  # Configuration template
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Container configuration
├── .gitignore                  # Git ignore rules
└── LICENSE                     # MIT License
```

## Common Tasks

### Running the Preprocessor

```bash
python tmdb-movie-preprocess.py
```

### Installing Dependencies

```bash
pip install -r requirements.txt
python -m spacy download fr_core_news_lg
```

### Docker Deployment

```bash
docker build -t tmdb-movie-preprocess .
docker run -v ./citizenphilsecrets.py:/app/citizenphilsecrets.py tmdb-movie-preprocess
```

## License

MIT License - Copyright (c) 2025 Philippe Vaugouin

## Notes for AI Assistants

- The codebase uses Hungarian notation extensively - maintain this convention
- Database operations should use the citizenphil module functions
- All API calls should use the configured bearer token from secrets
- Consider preprocessing limits when modifying data processing logic
- spaCy French language model (fr_core_news_lg) is required for NLP features
- The project processes large amounts of data - be mindful of memory usage
- Timestamps are managed in Paris timezone (Europe/Paris)
