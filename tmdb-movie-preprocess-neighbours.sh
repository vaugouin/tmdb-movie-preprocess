#!/bin/bash

# On-demand build of the grounded TMDb neighbour tables (Processes 36-39 ONLY):
# T2S_MOVIE_SIMILAR, T2S_MOVIE_RECOMMENDATION, T2S_SERIE_SIMILAR, T2S_SERIE_RECOMMENDATION.
# Rebuilds each T_WC_T2S_*_SIMILAR / _RECOMMENDATION twin from the raw
# T_WC_TMDB_*_SIMILAR / _RECOMMENDATION (TMDB-CRAWLER-022/-023), keeping only neighbours
# whose source AND target exist in the read-model. Use this as a unit test right after
# creating the four T2S tables, without running the whole main pipeline. Reuses the same
# image + env file as the main job; TMDB_PREPROCESS_SCOPE=neighbours (passed via -e,
# overriding the env file) restricts the run to Processes 36-39.
#
# Runs in the FOREGROUND (no -d) so the per-process source-row counts print live; the job
# is short (four table rebuilds). Backlog: TMDB-MOVIE-PREPROCESS-027 / -028.

if [ $(docker ps -q -f name=tmdb-movie-preprocess-neighbours) ]; then
    echo "tmdb-movie-preprocess-neighbours Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    # Rebuild so the image carries the latest code (neighbours scope, Processes 36-39).
    docker build -t tmdb-movie-preprocess-python-app .
    # Secrets via --env-file; scope override via -e so the same env file serves every job.
    # Foreground + --rm: you see the result immediately and the container self-removes.
    docker run --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=neighbours \
        --name tmdb-movie-preprocess-neighbours \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-neighbours run finished."
fi
