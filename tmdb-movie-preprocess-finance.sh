#!/bin/bash

# Rebuild of the budgets and box office (Process 75 ONLY), on demand.
# TMDB-MOVIE-PREPROCESS-066.
#
# Fills T_WC_T2S_WORK_FINANCE (created by the process itself on its first run) from the
# Wikidata V2 statements P2130 (cost) and P2142 (box office) of the T2S movies and
# series: amount, currency, place (P3005, Q13780930 = worldwide), date (P585, P580 /
# P582, period P1264), rank on a chart (P1352), method (P459). Deprecated statements are
# left out.
#
# In the main scope it runs after 72 (locations). This wrapper is for the first pass and
# for reruns. Skipped while wikidata-crawler loads (server variable
# strtmdbmoviepreprocessworkfinanceskipped says why).
#
# AFTER: run doc/sql/test-066-work-finance.sql.
#
# Runs in the FOREGROUND (no -d): the counts print as it goes.

if [ $(docker ps -q -f name=tmdb-movie-preprocess-finance) ]; then
    echo "tmdb-movie-preprocess-finance Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    docker build -t tmdb-movie-preprocess-python-app .
    docker run --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=finance \
        --name tmdb-movie-preprocess-finance \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-finance run finished."
fi
