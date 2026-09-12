#!/bin/bash

# Rebuild of the location read-model (Process 72 ONLY), on demand.
# TMDB-MOVIE-PREPROCESS-014, cible API 1.1.19.
#
# Reconstruit en entier T_WC_T2S_LOCATION, T_WC_T2S_MOVIE_LOCATION et
# T_WC_T2S_SERIE_LOCATION depuis les statements Wikidata V2 (P840 lieu de l'action,
# P915 lieu de tournage), plus la table de classes T_WC_T2S_LOCATION_CLASS qui porte
# les cones P279 dont LOCATION_TYPE est deduit.
#
# PREREQUIS, une fois : doc/sql/migration-t2s-location.sql applique sur la base vivante.
# Les trois tables ont ete creees le 2026-09-11 a 10:49. Sans elles le processus echoue
# sur un CREATE TABLE ... LIKE, ce qui est bruyant et donc sans danger.
#
# DUREE ATTENDUE : quelques minutes. 11 922 lieux et environ 95 000 associations mesures
# le 2026-09-11, c'est petit. Si le passage depasse le quart d'heure, regarder du cote
# des cones : la fermeture P279 se parcourt sur 5 227 784 aretes de sous-classe.
#
# ⚠ CE SCOPE NE REMPLIT PAS LES COLONNES D'IMAGE. WIKIPEDIA_MAIN_IMAGE_URL et sa jumelle
# francaise sont ecrites par le processus 71, qui n'est pas dans ce scope. Apres le
# premier passage, enchainer :
#     ./tmdb-movie-preprocess-wikipedia-main-image.sh
# sans quoi les lieux restent sans image, ce qui se lit comme une absence d'image alors
# que c'est une absence de passage.
#
# APRES : lancer doc/sql/test-014-post-run.sql, qui pose ses attentes AVANT l'execution
# et juge la conception sur huit temoins de priorite. Berlin doit sortir en 'city' et non
# en 'region', Singapour en 'country', Bikini Bottom en 'fiction'.
#
# Runs in the FOREGROUND (no -d): le passage est court et les comptes s'impriment au fil.

if [ $(docker ps -q -f name=tmdb-movie-preprocess-locations) ]; then
    echo "tmdb-movie-preprocess-locations Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    # Rebuild so the image carries the latest code (Process 72 added 2026-09-11).
    docker build -t tmdb-movie-preprocess-python-app .
    docker run --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=locations \
        --name tmdb-movie-preprocess-locations \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-locations run finished."
fi
