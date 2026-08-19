#!/bin/bash

# Decoupled scheduler for ALL Wikidata linkers, run SEQUENTIALLY in one container.
# TMDB_PREPROCESS_SCOPE=wikidata-all selects the combined scope, which runs every
# Wikidata-linking process in order (Process 60 topics, then 63 companies, then any
# future network/genre/character linkers) inside a single Python run. One process =
# one Wikimedia request stream, so the linkers never contend for the rate limit.
#
# This replaces firing the per-entity launchers
# (tmdb-movie-preprocess-wikidata-topics.sh / -companies.sh) in parallel; those are
# kept for targeted / debug runs of a single linker.
#
# PREREQUISITE: each linker's schema migration must be applied first (e.g.
# doc/sql/migration-company-wikidata.sql for Process 63).
#
# Schedule this on its own cron cadence (e.g. once a day), independently of the
# main tmdb-movie-preprocess.sh run.

# Check if the decoupled container is already running
if [ $(docker ps -q -f name=tmdb-movie-preprocess-wikidata) ]; then
    echo "tmdb-movie-preprocess-wikidata Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    docker build -t tmdb-movie-preprocess-python-app .
    # Secrets are passed at runtime via --env-file; the scope override is passed
    # with -e so the same env file serves both the main and the decoupled job.
    docker run -d --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=wikidata-all \
        --name tmdb-movie-preprocess-wikidata \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-wikidata Docker container started."
fi
