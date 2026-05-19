#!/bin/sh
set -eu

export DHSHOME="${DHSHOME:-/dhshome}"
export FDB_HOME="${FDB_HOME:-/data/fdb}"
# eckit resolves `~` to `executablePath().dirName().dirName()` which is
# `/opt/mars-fdb-mock` for our install. Config files therefore live there.
export FDB5_CONFIG_FILE="${FDB5_CONFIG_FILE:-/opt/mars-fdb-mock/etc/fdb/config.yaml}"
export PATH="/opt/mars-fdb-mock/bin:${PATH}"
export LD_LIBRARY_PATH="/opt/mars-fdb-mock/lib:/opt/mars-fdb-mock/runtime-libs${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

mkdir -p \
    "${DHSHOME}" \
    "${DHSHOME}/etc" \
    "${DHSHOME}/tmp" \
    "${DHSHOME}/log" \
    "${FDB_HOME}" \
    "${FDB_HOME}/root"

# Copy ConfigMap-mounted config into writable /dhshome/etc so fdbsvr can
# write its monitor/log files alongside the config.
if [ -d "${MARS_FDB_MOCK_CONFIG_SOURCE:-/dhshome/etc-source}" ]; then
    /opt/mars-fdb-mock/scripts/prep-dhshome.sh
fi

if [ "$#" -eq 0 ]; then
    set -- mars
fi

# Stay inside DHSHOME so eckit's `~` / config resolution finds etc/ + admin/.
cd "${DHSHOME}"

case "$1" in
    mars|/opt/mars-fdb-mock/bin/mars)
        if [ ! -s "/opt/mars-fdb-mock/etc/config/mars" ]; then
            echo "mars-fdb-mock: /opt/mars-fdb-mock/etc/config/mars is missing" >&2
            exit 65
        fi
        if [ ! -s "${FDB5_CONFIG_FILE}" ]; then
            echo "mars-fdb-mock: ${FDB5_CONFIG_FILE} is missing" >&2
            exit 65
        fi
        exec "$@"
        ;;
    fdbsvr|/opt/mars-fdb-mock/bin/fdbsvr)
        exec "$@"
        ;;
    seed)
        echo "mars-fdb-mock: seed logic is implemented by the seed task; this image currently only provides the runtime layout" >&2
        exit 64
        ;;
    *)
        exec "$@"
        ;;
esac
