#!/bin/bash

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
    docker run -d --rm --network="host" --env-file /home/debian/docker/tmdb-movie-preprocess/.env --name tmdb-movie-preprocess tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess Docker container started."
fi

# ---- SEQUENTIEL, pas parallele (2026-09-01) ------------------------------------
# Ce script demarrait le conteneur principal en detache puis enchainait aussitot sur
# le conteneur wikidata : les deux travaillaient la MEME base en meme temps. C'est la
# contention qui a fait echouer un UPDATE de -045 sur « Lock wait timeout exceeded ».
# On attend donc que le principal ait fini avant de lancer le second.
#
# `docker wait` bloque jusqu'a l'arret du conteneur et rend son code de sortie. Il
# peut cependant rendre la main en erreur quand `--rm` a deja supprime le conteneur,
# d'ou la boucle de garde qui suit : elle ne coute rien quand `docker wait` a fait son
# travail, et rattrape la course sinon. Le `|| true` evite qu'un `set -e` futur ne
# fasse tomber le script sur ce cas parfaitement normal.
echo "Waiting for tmdb-movie-preprocess to finish before starting the Wikidata linkers..."
docker wait tmdb-movie-preprocess >/dev/null 2>&1 || true
while [ -n "$(docker ps -q -f name=^tmdb-movie-preprocess$)" ]; do
    sleep 10
done
echo "tmdb-movie-preprocess finished."

# Decoupled Wikidata linkers: ALL of them, sequentially, in ONE container
# (scope wikidata-all = Process 60 topics -> 63 companies -> future linkers).
# One process = one Wikimedia request stream, so they never contend for the rate
# limit. For a targeted single-linker run, call -wikidata-topics.sh or
# -wikidata-companies.sh directly instead.
cd /home/debian/docker/tmdb-movie-preprocess
./tmdb-movie-preprocess-wikidata.sh
