#!/bin/sh
set -e

# Directory for multiprocess metrics
export PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc

# Prepare the directory
mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
rm -f "$PROMETHEUS_MULTIPROC_DIR"/*

# Now start whatever command was given (uvicorn)
exec "$@"