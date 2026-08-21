#!/bin/bash

# On-demand copy of the Wikipedia lead image into the T2S serving layer (Process 71 ONLY).
# Reads T_WC_WIKIPEDIA_PAGE_LANG.MAIN_IMAGE_URL, written per (ID_WIKIDATA, LANG) by
# wikipedia-crawler, and fills WIKIPEDIA_MAIN_IMAGE_URL / _FR on the 16 T2S entity tables.
#
# Run it right after a wikipedia-crawler pass: the images it just resolved reach the
# serving layer without replaying the whole ETL (the main scope is ~45 processes).
# Reuses the same image + env file as the main job; TMDB_PREPROCESS_SCOPE is passed via
# -e so the shared env file serves every job.
#
# Prerequisite, once: doc/sql/migration-wikipedia-main-image.sql applied on the live DB. Without
# it every statement is skipped by the per-statement try/except, which prints 32 SKIPPED
# lines and updates nothing -- a silence that reads like success, so check the count.
#
# Runs in the FOREGROUND (no -d): the job is short and you want to see the per-table,
# per-language row counts as they print.

if [ $(docker ps -q -f name=tmdb-movie-preprocess-wikipedia-main-image) ]; then
    echo "tmdb-movie-preprocess-wikipedia-main-image Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    # Rebuild so the image carries the latest code (Process 71 was added 2026-08-17).
    docker build -t tmdb-movie-preprocess-python-app .
    docker run --rm --network="host"         --env-file /home/debian/docker/tmdb-movie-preprocess/.env         -e TMDB_PREPROCESS_SCOPE=wikipedia-main-image         --name tmdb-movie-preprocess-wikipedia-main-image         tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-wikipedia-main-image run finished."
fi
