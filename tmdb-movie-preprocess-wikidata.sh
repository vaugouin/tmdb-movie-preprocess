#!/bin/bash

# Decoupled scheduler for ALL Wikidata linkers, run SEQUENTIALLY in one container.
# TMDB_PREPROCESS_SCOPE=wikidata-all selects the combined scope, which runs every
# Wikidata-linking process in order (Process 60 topics, then 63 companies, then any
# future network/genre/character linkers) inside a single Python run. One process =
# one Wikimedia request stream, so the linkers never contend for the rate limit.
#
# This replaces firing the per-entity launchers
# (tmdb-movie-preprocess-wikidata-topics.sh / -companies.sh) in parallel; those are
# kept for targeted / debug runs of a single linker.
#
# PREREQUISITE: each linker's schema migration must be applied first (e.g.
# doc/sql/migration-company-wikidata.sql for Process 63).
#
# Schedule this on its own cron cadence (e.g. once a day), independently of the
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

# Check if the decoupled container is already running (filtre ANCRE, voir la note du
# script principal : la correspondance par sous-chaine confondait les deux conteneurs)
if [ -n "$(docker ps -q -f name=^tmdb-movie-preprocess-wikidata$)" ]; then
    echo "tmdb-movie-preprocess-wikidata Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    docker build -t tmdb-movie-preprocess-python-app .
    # Secrets are passed at runtime via --env-file; the scope override is passed
    # with -e so the same env file serves both the main and the decoupled job.
    f_runlogged tmdb-movie-preprocess-wikidata --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=wikidata-all \
        --name tmdb-movie-preprocess-wikidata \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-wikidata Docker container started."
fi
