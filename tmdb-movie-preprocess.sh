#!/bin/bash

# Check if the tmdb-movie-preprocess Docker container is running
if [ $(docker ps -q -f name=tmdb-movie-preprocess) ]; then
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
