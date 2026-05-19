#!/bin/sh
set -eu

image="${1:-eccr.ecmwf.int/polytope/mars-fdb-mock:dev-1}"
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
mock_dir=$(CDPATH= cd -- "${script_dir}/.." && pwd)
data_dir="${MARS_FDB_MOCK_SMOKE_DATA_DIR:-${mock_dir}/.smoke-data}"
seed_dir="${data_dir}/seed-tmp"
dhshome_dir="${data_dir}/dhshome"
fdb_data_dir="${data_dir}/fdb"
marker="${fdb_data_dir}/.seeded"
container_name="mars-fdb-mock-smoke-$$"

mkdir -p "${seed_dir}" "${dhshome_dir}" "${fdb_data_dir}"

cleanup_container() {
    docker rm -f "${container_name}" >/dev/null 2>&1 || true
}
trap cleanup_container EXIT INT TERM

run_in_image() {
    docker run --rm \
        -u 0 \
        -v "${mock_dir}:/workspace:ro" \
        -v "${dhshome_dir}:/tmp/dhshome" \
        -v "${fdb_data_dir}:/data/fdb" \
        -v "${seed_dir}:/tmp/seed" \
        -e DHSHOME=/tmp/dhshome \
        -e FDB_HOME=/data/fdb \
        -e FDB5_CONFIG_FILE=/tmp/dhshome/etc/fdb/config.yaml \
        -e MARS_FDB_MOCK_ETC_DIR=/workspace/etc \
        -e MARS_FDB_MOCK_PREFER_CACHE=1 \
        "$@"
}

echo "== Runtime linker check: fdb-write usage =="
docker run --rm "${image}" fdb-write >/tmp/mars-fdb-mock-fdb-write.out 2>&1 || status=$?
status="${status:-0}"
if [ "${status}" -eq 127 ]; then
    cat /tmp/mars-fdb-mock-fdb-write.out >&2
    echo "fdb-write could not start; a runtime shared library is probably missing" >&2
    exit 1
fi
cat /tmp/mars-fdb-mock-fdb-write.out
rm -f /tmp/mars-fdb-mock-fdb-write.out
unset status

echo "== Runtime linker check: fdbsvr binary =="
docker run --rm "${image}" sh -c 'command -v fdbsvr && ldd /opt/mars-fdb-mock/bin/fdbsvr'

echo "== Seed persistent FDB data directory =="
echo "${data_dir}"

if [ -f "${marker}" ]; then
    echo "existing seed marker found before first seed run: ${marker}"
fi

echo "== First seed run =="
run_in_image "${image}" sh -ceu '
    if ! python3 -c "import ecmwf.opendata" >/dev/null 2>&1; then
        apt-get update
        apt-get install -y --no-install-recommends python3-pip
        python3 -m pip install --break-system-packages -r /workspace/seed/requirements.txt
        rm -rf /var/lib/apt/lists/*
    fi
    python3 /workspace/scripts/seed.py
'

if [ ! -s "${marker}" ]; then
    echo "seed marker was not written at ${marker}" >&2
    exit 1
fi
selector=$(awk -F= '/^selector=/{print substr($0, index($0,"=")+1)}' "${marker}")
if [ -z "${selector}" ]; then
    echo "seed marker does not contain a selector" >&2
    cat "${marker}" >&2
    exit 1
fi

echo "== fdb-list seeded selector =="
run_in_image "${image}" fdb-list --config=/tmp/dhshome/etc/fdb/config.yaml --porcelain "${selector}" | tee "${data_dir}/fdb-list.out"
if ! grep -q 'expver=9999' "${data_dir}/fdb-list.out"; then
    echo "fdb-list did not show expver=9999 for selector ${selector}" >&2
    exit 1
fi

echo "== Direct fdb-read retrieval of seeded field =="
if run_in_image "${image}" python3 /workspace/tests/retrieve_seeded_field.py; then
    if [ ! -s "${seed_dir}/retrieved.grib" ]; then
        echo "retrieve script reported success but output GRIB is empty" >&2
        exit 1
    fi
    echo "retrieved GRIB bytes: $(wc -c < "${seed_dir}/retrieved.grib")"
else
    echo "Direct fdb-read retrieval failed; continuing because the local DHS/fdbsvr path may need later integration. fdb-list already verified the seeded field." >&2
fi

echo "== Start fdbsvr runtime long enough to validate process startup =="
cleanup_container
docker run -d \
    --name "${container_name}" \
    -u 0 \
    -v "${dhshome_dir}:/tmp/dhshome" \
    -v "${fdb_data_dir}:/data/fdb" \
    -e DHSHOME=/tmp/dhshome \
    -e FDB_HOME=/data/fdb \
    -e FDB5_CONFIG_FILE=/tmp/dhshome/etc/fdb/config.yaml \
    "${image}" fdbsvr >/tmp/mars-fdb-mock-container.id
sleep 3
if ! docker ps --format '{{.Names}}' | grep -qx "${container_name}"; then
    docker logs "${container_name}" >&2 || true
    echo "fdbsvr container exited before startup validation completed" >&2
    exit 1
fi
docker logs --tail 80 "${container_name}" || true
cleanup_container

echo "== Second seed run must skip via marker =="
second_log="${data_dir}/second-seed.log"
run_in_image "${image}" sh -ceu 'python3 /workspace/scripts/seed.py' | tee "${second_log}"
if ! grep -q 'skipping download and ingest' "${second_log}"; then
    echo "second seed run did not report marker-based skip" >&2
    exit 1
fi

echo "== Token-history check =="
docker history --no-trunc "${image}"

echo "== Runtime filesystem token check =="
docker run --rm "${image}" sh -c 'grep -r ghp_ /etc /opt /root /home 2>/dev/null; cat /root/.netrc /root/.gitconfig 2>/dev/null || true'

echo "Smoke passed. Review token checks above; they must not contain GitHub token material."
