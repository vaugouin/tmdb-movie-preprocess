#!/bin/bash

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

# Switched off by off.sh, back on with on.sh: the flag file CRON_OFF next to this script makes
# every cron tick exit here. The script keeps its name, so the git clone stays clean.
if [ -f "$(dirname "$(readlink -f "$0")")/CRON_OFF" ]; then
    echo "CRON_OFF present: tmdb-movie-preprocess switched off (bash on.sh to switch it back on)."
    exit 0
fi

# Check if the tmdb-movie-preprocess Docker container is running.
#
# LE FILTRE EST ANCRE, et ce n'est pas cosmetique. `-f name=tmdb-movie-preprocess`
# correspond par SOUS-CHAINE, donc il attrapait aussi tmdb-movie-preprocess-wikidata :
# si le conteneur wikidata tournait encore d'un passage precedent, ce script annoncait
# "already running" et sautait le traitement principal EN SILENCE. `^...$` ne designe
# plus que le conteneur voulu. Meme correction dans le script wikidata.
if [ -n "$(docker ps -q -f name=^tmdb-movie-preprocess$)" ]; then
    echo "tmdb-movie-preprocess Docker container is already running."
else
    # Start the tmdb-movie-preprocess container if it is not running
    cd /home/debian/docker/tmdb-movie-preprocess
    docker build -t tmdb-movie-preprocess-python-app .
    # Secrets are passed at runtime via --env-file. The env file lives outside
    # the app source tree so it cannot end up in image layers or build cache.
    # docker run -it --rm --network="host" --env-file /home/debian/docker/tmdb-movie-preprocess/.env --name tmdb-movie-preprocess tmdb-movie-preprocess-python-app
    # Optional first argument: the process to resume from (TMDB_PREPROCESS_FROM), e.g.
    # ./tmdb-movie-preprocess.sh 43. The cron passes none, so the whole scope runs.
    f_runlogged tmdb-movie-preprocess --rm --network="host" --env-file /home/debian/docker/tmdb-movie-preprocess/.env -e TMDB_PREPROCESS_FROM="${1:-}" --name tmdb-movie-preprocess tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess Docker container started."
fi

# ---- Chainage wikidata COUPE (2026-10-16) ---------------------------------------
# Ce script n'enchaine plus sur tmdb-movie-preprocess-wikidata.sh : le job Wikidata
# est desactive. Le bloc d'origine est garde en commentaire ci-dessous pour le
# remettre tel quel ; le lanceur wikidata reste appelable a la main.
#
## ---- SEQUENTIEL, pas parallele (2026-09-01) ------------------------------------
## Ce script demarrait le conteneur principal en detache puis enchainait aussitot sur
## le conteneur wikidata : les deux travaillaient la MEME base en meme temps. C'est la
## contention qui a fait echouer un UPDATE de -045 sur « Lock wait timeout exceeded ».
## On attend donc que le principal ait fini avant de lancer le second.
##
## `docker wait` bloque jusqu'a l'arret du conteneur et rend son code de sortie. Il
## peut cependant rendre la main en erreur quand `--rm` a deja supprime le conteneur,
## d'ou la boucle de garde qui suit : elle ne coute rien quand `docker wait` a fait son
## travail, et rattrape la course sinon. Le `|| true` evite qu'un `set -e` futur ne
## fasse tomber le script sur ce cas parfaitement normal.
# echo "Waiting for tmdb-movie-preprocess to finish before starting the Wikidata linkers..."
# docker wait tmdb-movie-preprocess >/dev/null 2>&1 || true
# while [ -n "$(docker ps -q -f name=^tmdb-movie-preprocess$)" ]; do
#     sleep 10
# done
# echo "tmdb-movie-preprocess finished."
#
## Decoupled Wikidata linkers: ALL of them, sequentially, in ONE container
## (scope wikidata-all = Process 60 topics -> 63 companies -> future linkers).
## One process = one Wikimedia request stream, so they never contend for the rate
## limit. For a targeted single-linker run, call -wikidata-topics.sh or
## -wikidata-companies.sh directly instead.
# cd /home/debian/docker/tmdb-movie-preprocess
# ./tmdb-movie-preprocess-wikidata.sh
