#!/bin/sh
# usage: run.sh <python script> [args]   (runs inside the recorder container)
F=${FILM_DIR:-$(cd "$(dirname "$0")/.." && pwd)}
# --cpu-shares: the Docker VM (8 vCPU/4 GB) is shared with Airflow + Helical; when they saturate it
# (the Airflow scheduler healthcheck was seen at 600-800% CPU) Chromium's CDP stalls for 45 s+.
# A high weight gives this container most of the CPU only while it is contended (no effect otherwise).
exec docker run --rm --shm-size=2g --cpu-shares=16384 -e FILM_PROFILE=${FILM_PROFILE:-/film/profile} -e FILM_CHROME_FLAGS --add-host=host.docker.internal:host-gateway -v "$F":/film -w /film/recorder bq2-recorder python3 -u "$@"
