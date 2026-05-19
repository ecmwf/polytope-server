#!/bin/sh
#
# Pre-populates the PVC seed cache from a GRIB baked into the image,
# then exec's seed.py. The baked GRIB exists at
#   /opt/mars-fdb-mock/seed-cache/opendata.cached.grib
# and is copied to /data/fdb/seed-cache/ only if no cached GRIB is already
# present. With MARS_FDB_MOCK_PREFER_CACHE=1 set by the chart, seed.py then
# bypasses the public opendata download entirely \u2014 which matters in
# cluster environments without outbound HTTPS to data.ecmwf.int.

set -eu

# Materialise the ConfigMap into a writable /dhshome/etc so fdb-write/fdb-list
# can write logs/monitor alongside the config.
/opt/mars-fdb-mock/scripts/prep-dhshome.sh

DATA_DIR="${MARS_FDB_MOCK_SEED_DIR_DATA:-/data/fdb}"
PVC_CACHE="${DATA_DIR}/seed-cache"
FDB_ROOT_DIR="${DATA_DIR}/root"
BAKED_CACHE="/opt/mars-fdb-mock/seed-cache"

mkdir -p "${PVC_CACHE}" "${FDB_ROOT_DIR}"

if [ -d "${BAKED_CACHE}" ] && ! ls "${PVC_CACHE}"/*.grib >/dev/null 2>&1; then
    echo "seed-with-fallback: priming ${PVC_CACHE} from ${BAKED_CACHE}"
    cp "${BAKED_CACHE}"/*.grib "${PVC_CACHE}/" 2>/dev/null || true
fi

exec python3 /opt/mars-fdb-mock/scripts/seed.py "$@"
