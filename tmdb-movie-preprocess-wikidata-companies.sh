#!/bin/bash

# Decoupled scheduler for Process 63 (Link Wikidata items to companies) -- PILOT.
# Like Process 60 (keyword/topic linker), this is network-bound and rate-limited,
# so it runs on its OWN schedule, separate from the main DB ETL. It reuses the
# same image and env file; TMDB_PREPROCESS_SCOPE=wikidata-companies (passed via
# -e, overriding the env file) restricts the run to Process 63 only.
#
# PREREQUISITE: apply doc/sql/migration-company-wikidata.sql once on the live DB before
# the first run (it adds ID_WIKIDATA / WIKIDATA_LABEL / CONFIDENCE /
# TIM_WIKIPEDIA_SEARCH to T_WC_TMDB_COMPANY).
#
# Schedule this on its own cron cadence (e.g. once a day) independently of the
# main tmdb-movie-preprocess.sh run.

# Check if the decoupled container is already running
if [ $(docker ps -q -f name=tmdb-movie-preprocess-wikidata-companies) ]; then
    echo "tmdb-movie-preprocess-wikidata-companies Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    docker build -t tmdb-movie-preprocess-python-app .
    # Secrets are passed at runtime via --env-file; the scope override is passed
    # with -e so the same env file serves both the main and the decoupled job.
    docker run -d --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=wikidata-companies \
        --name tmdb-movie-preprocess-wikidata-companies \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-wikidata-companies Docker container started."
fi
