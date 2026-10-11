#!/bin/bash

# Rebuild of a group's works (Process 74 ONLY), on demand.
# TMDB-MOVIE-PREPROCESS-059.
#
# Fills T_WC_T2S_GROUP_MOVIE and T_WC_T2S_GROUP_SERIE from the members' credits
# (T_WC_T2S_PERSON_MOVIE, and for series T_WC_T2S_PERSON_SERIE, _SEASON and _EPISODE
# rolled up) and from the group's own Wikidata item credited on the work (V2 statements
# P161, P175, P58, P86, P170, P162, P57, P725). Only P463 and custom groups.
# Rule: group credited, or at least 2 members AND at least 40 % of the group.
#
# In the main scope it runs after 43 (groups) and 9, 10, 29, 31 (person credits). This
# wrapper is for the first pass and for reruns after a change of rule.
#
# PREREQUISITE, once: doc/sql/migration-t2s-group-work.sql applied on the live base.
# Without the two tables the process fails inside its own try; the error is in the
# server variable strtmdbmoviepreprocessgroupworkerror.
#
# DO NOT RUN DURING A wikidata-crawler LOAD unless the log says READ COMMITTED: the
# INSERT ... SELECT on T_WC_WIKIDATA_STATEMENT would otherwise lock the source rows
# (lesson of process 72, 2026-09-12).
#
# AFTER: run doc/sql/test-059-group-work.sql (rows per GROUP_SOURCE, Monty Python and
# The Beatles witnesses, Pete Best, and the dry run per family).
#
# Runs in the FOREGROUND (no -d): the counts print as it goes.

if [ $(docker ps -q -f name=tmdb-movie-preprocess-group-works) ]; then
    echo "tmdb-movie-preprocess-group-works Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    # Rebuild so the image carries the latest code (Process 74 added 2026-10-11).
    docker build -t tmdb-movie-preprocess-python-app .
    docker run --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=group-works \
        --name tmdb-movie-preprocess-group-works \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-group-works run finished."
fi
