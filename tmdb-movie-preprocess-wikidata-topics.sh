#!/bin/bash

# Decoupled scheduler for Process 60 (Link Wikidata items to topics).
# This network-bound, rate-limited keyword linker (~3h45m) is run on its OWN
# schedule, separate from the main DB ETL, so it no longer blocks it. It reuses
# the same image and env file; TMDB_PREPROCESS_SCOPE=wikidata-topics (passed via
# -e, overriding the env file) restricts the run to Process 60 only.
#
# Schedule this on its own cron cadence (e.g. once a day) independently of the
# main tmdb-movie-preprocess.sh run.

# Check if the decoupled container is already running
if [ $(docker ps -q -f name=tmdb-movie-preprocess-wikidata-topics) ]; then
    echo "tmdb-movie-preprocess-wikidata-topics Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    docker build -t tmdb-movie-preprocess-python-app .
    # Secrets are passed at runtime via --env-file; the scope override is passed
    # with -e so the same env file serves both the main and the decoupled job.
    docker run -d --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=wikidata-topics \
        --name tmdb-movie-preprocess-wikidata-topics \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-wikidata-topics Docker container started."
fi
