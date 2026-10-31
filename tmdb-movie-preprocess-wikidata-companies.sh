#!/bin/bash

# Decoupled scheduler for Process 63 (Link Wikidata items to companies) -- PILOT.
# Like Process 60 (keyword/topic linker), this is network-bound and rate-limited,
# so it runs on its OWN schedule, separate from the main DB ETL. It reuses the
# same image and env file; TMDB_PREPROCESS_SCOPE=wikidata-companies (passed via
# -e, overriding the env file) restricts the run to Process 63 only.
#
# PREREQUISITE: apply doc/sql/migration-company-wikidata.sql once on the live DB before
# the first run (it adds ID_WIKIDATA / WIKIDATA_LABEL / CONFIDENCE /
# TIM_WIKIPEDIA_SEARCH to T_WC_TMDB_COMPANY).
#
# Schedule this on its own cron cadence (e.g. once a day) independently of the
# main tmdb-movie-preprocess.sh run.

# --- Run log on the host (VPS-wide convention, first in sqlite-plex-to-tmdb/sqlite.sh) ---
# `docker run -d --rm` sent the container's output to Docker's log driver, and --rm
# destroyed it on exit: after a crash there was nothing left to read. f_runlogged runs
# the container in the FOREGROUND of a backgrounded block whose output goes to
#   /home/debian/docker/shared_data/logs/<process>/<YYYYMMDD-HHMMSS>.log   (kept 30 days)
#   /home/debian/docker/shared_data/logs/<process>/latest.log              (the last run)
# The last line is "=== <process> exited with status N at <date> ===": the ops dashboard
# of data-monitoring reads it (N > 0 = FAILED). `docker logs -f <name>` still works
# while the container runs; `tail -f .../latest.log` works during and after.
# Usage: f_runlogged <process> <docker run arguments, without -d>
f_runlogged() {
    local process="$1"; shift
    local dir="/home/debian/docker/shared_data/logs/$process"
    local log name="" prev="" arg
    mkdir -p "$dir"
    log="$dir/$(date '+%Y%m%d-%H%M%S').log"
    : > "$log"
    ln -sfn "$log" "$dir/latest.log"
    find "$dir" -maxdepth 1 -type f -name '*.log' -mtime +30 -delete 2>/dev/null
    for arg in "$@"; do [ "$prev" = "--name" ] && name="$arg"; prev="$arg"; done
    {
        trap '' HUP
        echo "=== $process started $(date '+%Y-%m-%d %H:%M:%S') ==="
        docker run -e PYTHONUNBUFFERED=1 "$@"
        status=$?
        echo "=== $process exited with status $status at $(date '+%Y-%m-%d %H:%M:%S') ==="
    } >> "$log" 2>&1 < /dev/null &
    disown 2>/dev/null
    # Return once the container exists, so a `docker ps` guard or a `docker logs -f`
    # right after this call sees it (60 s at most).
    if [ -n "$name" ]; then
        for _ in $(seq 1 60); do
            [ -n "$(docker ps -a -q -f name="^${name}$")" ] && break
            sleep 1
        done
    fi
    echo "$process: container started in the background, run log $log"
}

# Check if the decoupled container is already running
if [ -n "$(docker ps -q -f name=^tmdb-movie-preprocess-wikidata-companies$)" ]; then
    echo "tmdb-movie-preprocess-wikidata-companies Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    docker build -t tmdb-movie-preprocess-python-app .
    # Secrets are passed at runtime via --env-file; the scope override is passed
    # with -e so the same env file serves both the main and the decoupled job.
    f_runlogged tmdb-movie-preprocess-wikidata-companies --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=wikidata-companies \
        --name tmdb-movie-preprocess-wikidata-companies \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-wikidata-companies Docker container started."
fi
