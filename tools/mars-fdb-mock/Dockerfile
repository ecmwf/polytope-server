# syntax=docker/dockerfile:1

FROM docker.io/library/debian:bookworm AS builder

ARG GIT_AUTH_TOKEN=""

ARG INSTALL_PREFIX=/opt/mars-fdb-mock
ARG MARS_SERVER_BUNDLE_SHA=2786a97f386ddd7db3b403d8ac4451eb8368e03e
ARG ECBUILD_SHA=158ae84807f2436248ab397f13c5ad43ec15c45d
ARG LIBAEC_SHA=7204505af7d6635734fc12a38d6bd0a6253c9c6d
ARG ECKIT_SHA=9daf0377f2fd0ddc2ee1bdd47c7064ea037a28c6
ARG ECCODES_SHA=cf28d1a6eb4ab14da16055a48268cef9096bf7dc
ARG ODC_SHA=424ce3dcd9d569d42f40e6acd28cdc6308352d6a
ARG METKIT_SHA=b280949ef1e60490a45bfc66b9c96bf3af2fd260
ARG FDB5_SHA=54bfbb4d645f5a335bb2b732400ad976964100df
ARG DHSKIT_SHA=13c161d98ba9d66c8a9b026f2dd173d402ac4378
ARG MARS_SERVER_SHA=bcb1e412fe5abee45d9c7042275c346d5932cb35

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bison \
        build-essential \
        ca-certificates \
        cmake \
        curl \
        flex \
        gfortran \
        git \
        libaec-dev \
        libncurses-dev \
        libssl-dev \
        ninja-build \
        patchelf \
        pkg-config \
        python3 \
    && curl -fsSL https://github.com/Kitware/CMake/releases/download/v3.31.6/cmake-3.31.6-linux-x86_64.sh -o /tmp/cmake.sh \
    && sh /tmp/cmake.sh --skip-license --prefix=/usr/local \
    && rm -f /tmp/cmake.sh \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

RUN set -eu; \
    if [ -n "${GIT_AUTH_TOKEN}" ]; then \
        printf 'machine github.com\nlogin x-access-token\npassword %s\n' "${GIT_AUTH_TOKEN}" > /root/.netrc; \
        chmod 0600 /root/.netrc; \
        git config --global url."https://github.com/".insteadOf "ssh://git@github.com/"; \
        git config --global url."https://github.com/".insteadOf "git@github.com:"; \
    fi; \
    git clone https://github.com/ecmwf/ecbuild.git /build/ecbuild; \
    git -C /build/ecbuild checkout --detach "${ECBUILD_SHA}"; \
    git clone https://github.com/ecmwf/mars-server-bundle.git /build/mars-server-bundle; \
    git -C /build/mars-server-bundle checkout --detach "${MARS_SERVER_BUNDLE_SHA}"; \
    cd /build/mars-server-bundle; \
    sed -i \
        -e "s|ecbuild_bundle( PROJECT libaec .*|ecbuild_bundle( PROJECT libaec GIT \"https://gitlab.dkrz.de/k202009/libaec.git\" TAG ${LIBAEC_SHA} UPDATE)|" \
        -e "s|ecbuild_bundle( PROJECT eckit .*|ecbuild_bundle( PROJECT eckit GIT \"https://github.com/ecmwf/eckit\" TAG ${ECKIT_SHA} UPDATE)|" \
        -e "s|ecbuild_bundle( PROJECT eccodes .*|ecbuild_bundle( PROJECT eccodes GIT \"https://github.com/ecmwf/eccodes\" TAG ${ECCODES_SHA} UPDATE)|" \
        -e "s|ecbuild_bundle( PROJECT odc .*|ecbuild_bundle( PROJECT odc GIT \"https://github.com/ecmwf/odc\" TAG ${ODC_SHA} UPDATE)|" \
        -e "s|ecbuild_bundle( PROJECT metkit .*|ecbuild_bundle( PROJECT metkit GIT \"https://github.com/ecmwf/metkit\" TAG ${METKIT_SHA} UPDATE)|" \
        -e "s|ecbuild_bundle( PROJECT fdb5 .*|ecbuild_bundle( PROJECT fdb5 GIT \"https://github.com/ecmwf/fdb\" TAG ${FDB5_SHA} UPDATE)|" \
        -e "s|ecbuild_bundle( PROJECT dhskit .*|ecbuild_bundle( PROJECT dhskit GIT \"git@github.com:ecmwf/dhskit\" TAG ${DHSKIT_SHA} UPDATE)|" \
        -e "s|ecbuild_bundle( PROJECT mars-server .*|ecbuild_bundle( PROJECT mars-server GIT \"git@github.com:ecmwf/mars-server\" TAG ${MARS_SERVER_SHA} UPDATE)|" \
        CMakeLists.txt; \
    cmake -S . -B /build/mars-server-bundle/build \
        -G Ninja \
        -DLOCALCONFIG=/build/mars-server-bundle/ecmwf-cds.cmake \
        -DENABLE_AEC=OFF \
        -DENABLE_DUMMY_TAPES=ON \
        -DENABLE_FDB5_SERVER=ON \
        -DENABLE_MPI=OFF \
        -DENABLE_FORTRAN=OFF \
        -DENABLE_PYTHON=OFF \
        -DENABLE_TESTS=OFF \
        -DENABLE_MARS2GRIB=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${INSTALL_PREFIX}"; \
    cmake --build /build/mars-server-bundle/build --parallel "$(nproc)"; \
    cmake --install /build/mars-server-bundle/build --prefix "${INSTALL_PREFIX}"; \
    # mars.t is forked by `mars` to handle each request, but is linked without
    # libFdbserver.so (no direct symbol reference). The static `FDBRetriever`
    # / `FDBLister` / `FDBArchiver` instances therefore never register as
    # MarsHandlers, and every retrieve fails with "No handler for request
    # retrieve". Force libFdbserver into DT_NEEDED via patchelf so the static
    # initializer fires when mars.t starts.
    patchelf --add-needed libFdbserver.so "${INSTALL_PREFIX}/bin/mars.t"; \
    rm -f /root/.netrc /root/.gitconfig; \
    git config --global --unset-all url."https://github.com/".insteadOf || true; \
    test ! -e /root/.netrc; \
    test ! -e /root/.gitconfig

RUN set -eu; \
    mkdir -p /runtime-root/opt/mars-fdb-mock/runtime-libs; \
    for binary in \
        "${INSTALL_PREFIX}/bin/fdbsvr" \
        "${INSTALL_PREFIX}/bin/fdb-write" \
        "${INSTALL_PREFIX}/bin/fdb-list" \
        "${INSTALL_PREFIX}/bin/fdb-read" \
        "${INSTALL_PREFIX}/bin/marsadm" \
        "${INSTALL_PREFIX}/bin/grib_get" \
        "${INSTALL_PREFIX}/bin/grib_set"; do \
        test -x "${binary}"; \
        ldd "${binary}" >&2; \
        ldd "${binary}" | awk '/=> \/.* \(/ { print $3 } /^[[:space:]]*\/.* \(/ { print $1 }'; \
    done | sort -u > /tmp/runtime-libs.txt; \
    while read -r lib; do \
        case "${lib}" in \
            "${INSTALL_PREFIX}"/*|/lib64/ld-linux-*|*/ld-linux-*) continue ;; \
        esac; \
        cp -L "${lib}" "/runtime-root/opt/mars-fdb-mock/runtime-libs/$(basename "${lib}")"; \
    done < /tmp/runtime-libs.txt

FROM docker.io/library/debian:bookworm-slim AS runtime

LABEL org.opencontainers.image.title="mars-fdb-mock" \
      org.opencontainers.image.description="Minimal fdbsvr runtime for the Polytope MARS FDB mock"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        python3 \
        python3-pip \
        tini \
    && pip3 install --no-cache-dir --break-system-packages ecmwf-opendata==0.3.23 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 1000 mars \
    && useradd --system --uid 1000 --gid mars --home-dir /dhshome --shell /usr/sbin/nologin mars \
    && mkdir -p \
        /data/fdb \
        /dhshome \
        /dhshome/etc \
        /dhshome/tmp \
        /dhshome/log \
        /opt/mars-fdb-mock/etc \
    && chown -R mars:mars /data /dhshome /opt/mars-fdb-mock

COPY --from=builder /opt/mars-fdb-mock /opt/mars-fdb-mock
COPY --from=builder /runtime-root/opt/mars-fdb-mock/runtime-libs /opt/mars-fdb-mock/runtime-libs
COPY tools/mars-fdb-mock/scripts /opt/mars-fdb-mock/scripts
COPY tools/mars-fdb-mock/seed /opt/mars-fdb-mock/seed
COPY tools/mars-fdb-mock/seed-cache /opt/mars-fdb-mock/seed-cache
COPY tools/mars-fdb-mock/scripts/entrypoint.sh /usr/local/bin/mars-fdb-mock-entrypoint

RUN chmod 0755 /usr/local/bin/mars-fdb-mock-entrypoint /opt/mars-fdb-mock/scripts/*.sh /opt/mars-fdb-mock/scripts/*.py \
    && chown -R mars:mars /opt/mars-fdb-mock /data /dhshome

ENV PATH="/opt/mars-fdb-mock/bin:${PATH}" \
    LD_LIBRARY_PATH="/opt/mars-fdb-mock/lib:/opt/mars-fdb-mock/runtime-libs" \
    DHSHOME="/dhshome" \
    FDB_HOME="/data/fdb" \
    FDB5_CONFIG_FILE="/opt/mars-fdb-mock/etc/fdb/config.yaml" \
    MARS_FDB_MOCK_PORT="9000"

USER mars:mars
WORKDIR /dhshome
EXPOSE 9000

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/mars-fdb-mock-entrypoint"]
CMD ["fdbsvr"]
