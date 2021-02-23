#!/usr/bin/env bash

set -e
set -o pipefail

# The test script is not supposed to make any changes to the files
# e.g. add/update missing dependencies. Such divergences should be
# detected and trigger a failure that needs explicit developer's action.
export GOFLAGS=-mod=readonly

if [ -z "$GOARCH" ]; then
  GOARCH=$(go env GOARCH);
fi

# determine whether target supports race detection
if [ -z "${RACE}" ] ; then
  if [ "$GOARCH" == "amd64" ]; then
    RACE="--race"
  else
    RACE="--race=false"
  fi
else
  RACE="--race=${RACE:-true}"
fi