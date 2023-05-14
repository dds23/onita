#!/bin/bash

case $1 in

"" | "staging" | "stag")
    echo "staging"
    (
    set -a && . deployment/environments/staging.ini && set +a
    uvicorn app.server:api --port 8080 --reload
    )
    # this allows listening to sigterm
    ;;

"production")
    echo "production"
    (
    set -a && . deployment/environments/production.ini && set +a
    uvicorn app.server:api --port 8080 --reload
    )
    ;;

*)
  echo "unknown: $1"
  ;;
esac
