#!/bin/bash

# On-demand refresh of the living-eval assertions (Process 70 ONLY).
# Rebuilds ASSERTIONS_QUERY_RESULT from ASSERTION_REFRESH_SQL for every eval that
# has one -- handy right after seeding new IS_SHOWCASE / sample rows, without running
# the whole main pipeline. Reuses the same image + env file as the main job;
# TMDB_PREPROCESS_SCOPE=assertion-refresh (passed via -e, overriding the env file)
# restricts the run to Process 70.
#
# Runs in the FOREGROUND (no -d) so the refresh count / skip count prints live; the
# job is short. Run it manually whenever you add or edit living-eval samples.

if [ $(docker ps -q -f name=tmdb-movie-preprocess-assertion-refresh) ]; then
    echo "tmdb-movie-preprocess-assertion-refresh Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    # Rebuild so the image carries the latest code (assertion-refresh scope, etc.).
    docker build -t tmdb-movie-preprocess-python-app .
    # Secrets via --env-file; scope override via -e so the same env file serves every job.
    # Foreground + --rm: you see the result immediately and the container self-removes.
    docker run --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=assertion-refresh \
        --name tmdb-movie-preprocess-assertion-refresh \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-assertion-refresh run finished."
fi
