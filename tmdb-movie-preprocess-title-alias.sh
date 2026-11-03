#!/bin/bash

# Build of the movie and series title alias tables (Process 52 ONLY), on demand.
# TMDB-MOVIE-PREPROCESS-069.
#
# Process 52 rebuilds T_WC_T2S_MOVIE_TITLE_ALIAS and T_WC_T2S_SERIE_TITLE_ALIAS from the
# Wikidata P1476 titles, labels and aliases, minus the forms equal to a TMDb title. The two
# tables create themselves when missing, so no migration has to be applied first.
#
# In the main scope, 52 runs right after 4 and 5: this wrapper is for the first build and
# for reruns, without waiting for the nightly run.
#
# Preview before, without writing: doc/sql/check-069-title-alias-preview.sql through the runner.

if [ $(docker ps -q -f name=tmdb-movie-preprocess-title-alias) ]; then
    echo "tmdb-movie-preprocess-title-alias Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    docker build -t tmdb-movie-preprocess-python-app .
    docker run --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=title-alias \
        --name tmdb-movie-preprocess-title-alias \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-title-alias run finished."
fi
