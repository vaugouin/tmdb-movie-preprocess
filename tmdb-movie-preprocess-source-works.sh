#!/bin/bash

# Rebuild of the "based on" read-model (Process 73 ONLY), on demand.
# TMDB-MOVIE-PREPROCESS-055.
#
# Rebuilds in full T_WC_T2S_SOURCE_WORK, T_WC_T2S_MOVIE_SOURCE_WORK and
# T_WC_T2S_SERIE_SOURCE_WORK from the Wikidata V2 statements P144 (based on), plus the
# class table T_WC_T2S_SOURCE_WORK_CLASS that carries the P279 cones SOURCE_WORK_TYPE
# and SOURCE_WORK_FORM are derived from.
#
# PREREQUISITE, once: doc/sql/migration-t2s-source-work.sql applied on the live base.
# Without the three tables the process fails on a CREATE TABLE ... LIKE, which is loud
# and therefore harmless.
#
# EXPECTED DURATION: a few minutes. About 17,500 sources and 25,000 links measured on
# 2026-10-07. If the pass goes beyond a quarter of an hour, look at the cones: the
# "written work" cone alone holds about 54,000 classes.
#
# DO NOT RUN DURING A wikidata-crawler LOAD unless the log says READ COMMITTED: the
# INSERT ... SELECT on T_WC_WIKIDATA_STATEMENT would otherwise lock the source rows
# (lesson of process 72, 2026-09-12).
#
# THIS SCOPE DOES NOT FILL THE IMAGE COLUMNS. WIKIPEDIA_MAIN_IMAGE_URL and its French
# twin are written by process 71. After the first pass, chain:
#     ./tmdb-movie-preprocess-wikipedia-main-image.sh
#
# AFTER: run doc/sql/test-055-post-run.sql (counts, no orphan link, the witnesses
# The Shining, Arrival, Scarface 1983, The Last of Us, and the three book covers).
#
# Runs in the FOREGROUND (no -d): the pass is short and the counts print as it goes.

if [ $(docker ps -q -f name=tmdb-movie-preprocess-source-works) ]; then
    echo "tmdb-movie-preprocess-source-works Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    # Rebuild so the image carries the latest code (Process 73 added 2026-10-09).
    docker build -t tmdb-movie-preprocess-python-app .
    docker run --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=source-works \
        --name tmdb-movie-preprocess-source-works \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-source-works run finished."
fi
