#!/bin/bash

# Build of the award tables (Process 44 ONLY), on demand.
# TMDB-MOVIE-PREPROCESS-071.
#
# Process 44 rebuilds T_WC_T2S_AWARD and the three win tables T_WC_T2S_MOVIE_AWARD,
# T_WC_T2S_SERIE_AWARD and T_WC_T2S_PERSON_AWARD: one row per win, with the year, the
# ceremony and, for a person, the work rewarded; then the family of each category
# (T_WC_T2S_AWARD.ID_AWARD_PARENT).
#
# Prerequisite once: doc/sql/migration-071-award-wins.sql through the runner. Without it
# the process skips itself and says so (server variable
# strtmdbmoviepreprocessawardwinskipreason); nothing is written.
#
# In the main scope, 44 runs right after 43: this wrapper is for the first build of the
# win rows and for reruns, without waiting for the nightly run. It also skips itself
# while wikidata-crawler is loading.
#
# Acceptance after the build: doc/sql/test-071-award-wins.sql through the runner.

if [ $(docker ps -q -f name=tmdb-movie-preprocess-awards) ]; then
    echo "tmdb-movie-preprocess-awards Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    docker build -t tmdb-movie-preprocess-python-app .
    docker run --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=awards \
        --name tmdb-movie-preprocess-awards \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-awards run finished."
fi
