#!/bin/bash

# Build of the genre and person-alias read-model tables (Processes 51 and 50 ONLY), on demand.
# TMDB-MOVIE-PREPROCESS-053 and -054.
#
# Process 51 rebuilds T_WC_T2S_PERSON_ALSO_KNOWN_AS, the aliases of the persons in
# T_WC_T2S_PERSON only. Process 50 rebuilds T_WC_T2S_GENRE and T_WC_T2S_GENRE_LANG under
# T2S column names (ID_GENRE, GENRE_NAME). The three tables create themselves when missing,
# so no migration has to be applied first.
#
# In the main scope, 51 runs right after 6 and 50 right before 11: this wrapper is for the
# first build and for reruns, without waiting for the nightly run.
#
# Acceptance after the run: doc/sql/check-053-054-genre-alias.sql through the runner.

if [ $(docker ps -q -f name=tmdb-movie-preprocess-genre-alias) ]; then
    echo "tmdb-movie-preprocess-genre-alias Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    docker build -t tmdb-movie-preprocess-python-app .
    docker run --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=genre-alias \
        --name tmdb-movie-preprocess-genre-alias \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-genre-alias run finished."
fi
