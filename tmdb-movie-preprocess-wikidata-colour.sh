#!/bin/bash

# Colour flags from Wikidata P462 (Process 64 ONLY), on demand.
# TMDB-MOVIE-PREPROCESS-049, option A (2026-09-13).
#
# Remplit IS_COLOR / IS_BLACK_AND_WHITE sur T_WC_TMDB_MOVIE depuis la propriete Wikidata
# P462 pour les films SANS ligne Format francaise, les signe COLOR_SOURCE = 'wikidata', et
# recrit leurs lignes de jonction color_movie / black_and_white_movie. La ligne Format garde
# la main : le processus 1 signe 'format_line' et ce processus ne touche jamais un film qui
# porte une ligne.
#
# PREREQUIS, une fois : doc/sql/migration-049-color-source.sql applique sur la base vivante
# (deux colonnes sur T_WC_TMDB_MOVIE et T_WC_T2S_MOVIE, signature de l'existant). Sans elles
# le processus echoue sur une colonne inconnue, bruyamment, et n'emporte que lui-meme.
#
# DUREE ATTENDUE : quelques minutes. Une table temporaire d'environ 197 000 items, une
# UPDATE jointe par index sur environ 140 000 films, deux INSERT ... SELECT. Le premier
# passage touche de l'ordre de 90 000 films (avec P462, sans ligne) ; les suivants n'ecrivent
# que ce qui change, souvent rien.
#
# APRES LE PREMIER PASSAGE : le processus 4 (scope main) recopie les films touches vers
# T_WC_T2S_MOVIE, puisque TIM_UPDATED a avance. Puis doc/sql/test-049-colour-source.sql,
# qui pose ses attentes avant l'execution (Pleasantville 2657 aux deux flags, Colt .45
# 55472 signe format_line, zero film 'wikidata' porteur d'une ligne).
#
# ENSUITE : rien a planifier, le processus 64 tourne dans le scope main entre 1 et 2.
#
# Runs in the FOREGROUND (no -d): le passage est court et les comptes s'impriment au fil.

if [ $(docker ps -q -f name=^tmdb-movie-preprocess-wikidata-colour$) ]; then
    echo "tmdb-movie-preprocess-wikidata-colour Docker container is already running."
else
    cd /home/debian/docker/tmdb-movie-preprocess
    # Rebuild so the image carries the latest code (Process 64 added 2026-09-13).
    docker build -t tmdb-movie-preprocess-python-app .
    docker run --rm --network="host" \
        --env-file /home/debian/docker/tmdb-movie-preprocess/.env \
        -e TMDB_PREPROCESS_SCOPE=wikidata-colour \
        --name tmdb-movie-preprocess-wikidata-colour \
        tmdb-movie-preprocess-python-app
    echo "tmdb-movie-preprocess-wikidata-colour run finished."
fi
