#!/bin/sh
#
# Copies the read-only ConfigMap-mounted config tree into a writable
# /dhshome/etc so fdbsvr / fdb-write can write monitor files and logs
# alongside the configuration. Run by both the seed initContainer and the
# main fdbsvr container before the workload starts.

set -eu

SOURCE="${MARS_FDB_MOCK_CONFIG_SOURCE:-/dhshome/etc-source}"
TARGET="${MARS_FDB_MOCK_CONFIG_TARGET:-/dhshome/etc}"

mkdir -p "${TARGET}"

if [ ! -d "${SOURCE}" ]; then
    echo "prep-dhshome: source ${SOURCE} not present; assuming /dhshome/etc is already populated" >&2
    exit 0
fi

# Mirror the directory layout, deref symlinks so the writable copy is real
# files rather than ConfigMap-backed symlinks.
cp -RL "${SOURCE}/." "${TARGET}/"
