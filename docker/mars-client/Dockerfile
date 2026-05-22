# syntax=docker/dockerfile:1.4
##
## Copyright 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##

FROM debian:bookworm AS mars-source-build-base
ARG ecbuild_version

RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    bison \
    build-essential \
    ca-certificates \
    cmake \
    curl \
    flex \
    gfortran \
    git \
    libblas-dev \
    libcurl4-openssl-dev \
    libeigen3-dev \
    libfftw3-dev \
    libhdf5-dev \
    liblapack-dev \
    liblz4-dev \
    libnetcdf-dev \
    libpng-dev \
    libsqlite3-dev \
    libssl-dev \
    libxml2-dev \
    ninja-build \
    patchelf \
    pkg-config \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir --break-system-packages --upgrade pip cmake

RUN set -eux \
    && git clone https://github.com/ecmwf/ecbuild.git /tmp/ecbuild \
    && git -C /tmp/ecbuild checkout "${ecbuild_version}" \
    && cmake -S /tmp/ecbuild -B /tmp/ecbuild-build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /tmp/ecbuild-build --parallel \
    && cmake --install /tmp/ecbuild-build \
    && rm -rf /tmp/ecbuild /tmp/ecbuild-build \
    && test -f /usr/local/share/ecbuild/cmake/ecbuild-config.cmake

RUN set -eux \
    && git clone https://gitlab.dkrz.de/dkrz-sw/libaec.git /tmp/libaec \
    && git -C /tmp/libaec checkout v1.1.4 \
    && cmake -S /tmp/libaec -B /tmp/libaec-build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /tmp/libaec-build --parallel \
    && cmake --install /tmp/libaec-build \
    && rm -rf /tmp/libaec /tmp/libaec-build \
    && test -f /usr/local/lib/cmake/libaec/libaec-config.cmake

FROM mars-source-build-base AS mars-client-c-bundle-build
ARG mars_client_c_bundle_ref

RUN --mount=type=secret,id=github_token set -eu \
    && token=$(cat /run/secrets/github_token) \
    && git config --global --add url."https://x-access-token:${token}@github.com/".insteadOf "https://github.com/" \
    && git config --global --add url."https://x-access-token:${token}@github.com/".insteadOf "ssh://git@github.com/" \
    && git config --global --add url."https://x-access-token:${token}@github.com/".insteadOf "git@github.com:" \
    && export INPUT_GITHUB_TOKEN="${token}" \
    && git clone https://github.com/ecmwf/mars-client-bundle.git /tmp/mars-client-c-bundle \
    && git -C /tmp/mars-client-c-bundle checkout "${mars_client_c_bundle_ref}" \
    && cmake -S /tmp/mars-client-c-bundle -B /tmp/mars-client-c-build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/opt/ecmwf/mars-client \
    -DAEC_DIR=/usr/local \
    -DSTACK_DEP_LIBAEC=ON \
    -DSTACK_DEP_PROJ=ON \
    -DECMWF_NEXUS_NIGHTLY=1 \
    -DMIR_ENABLE_TESTS=OFF \
    -DATLAS_ENABLE_TESTS=OFF \
    -DATLAS-ORCA_ENABLE_TESTS=OFF \
    && cmake --build /tmp/mars-client-c-build --parallel \
    && cmake --install /tmp/mars-client-c-build \
    && bundle_prefix=/opt/ecmwf/mars-client \
    && copy_runtime_dep() { \
        prefix="$1"; \
        dep="$2"; \
        base=$(basename "$dep"); \
        case "$dep" in \
            "$prefix"/*|/lib/x86_64-linux-gnu/libc.so.*|/lib/x86_64-linux-gnu/libm.so.*|/lib/x86_64-linux-gnu/libpthread.so.*|/lib/x86_64-linux-gnu/libdl.so.*|/lib/x86_64-linux-gnu/librt.so.*|/lib64/ld-linux-*.so.*) return 0 ;; \
        esac; \
        real=$(readlink -f "$dep"); \
        real_base=$(basename "$real"); \
        cp -a "$real" "$prefix/lib/$real_base"; \
        if [ "$base" != "$real_base" ]; then ln -sf "$real_base" "$prefix/lib/$base"; fi; \
    } \
    && bundle_runtime_deps() { \
        prefix="$1"; \
        mkdir -p "$prefix/lib"; \
        for path in "$prefix"/bin/* "$prefix"/lib/*.so "$prefix"/lib/*.so.*; do \
            [ -e "$path" ] || continue; \
            [ -d "$path" ] && continue; \
            if ! patchelf --print-rpath "$path" >/dev/null 2>&1; then continue; fi; \
            for needed in $(patchelf --print-needed "$path"); do \
                case "$needed" in \
                    /*) copy_runtime_dep "$prefix" "$needed" && patchelf --replace-needed "$needed" "$(basename "$needed")" "$path" ;; \
                esac; \
            done; \
            ldd "$path" | while IFS= read -r line; do \
                set -- $line; \
                if [ "${2-}" = "=>" ] && [ "${3-}" != "not" ] && [ "${3#/}" != "$3" ]; then copy_runtime_dep "$prefix" "$3"; fi; \
            done; \
        done; \
        for path in "$prefix"/lib/*.so "$prefix"/lib/*.so.*; do \
            [ -e "$path" ] || continue; \
            [ -d "$path" ] && continue; \
            if ! patchelf --print-rpath "$path" >/dev/null 2>&1; then continue; fi; \
            patchelf --set-rpath '$ORIGIN' "$path"; \
        done; \
        for path in "$prefix"/bin/*; do \
            [ -e "$path" ] || continue; \
            [ -d "$path" ] && continue; \
            if ! patchelf --print-rpath "$path" >/dev/null 2>&1; then continue; fi; \
            patchelf --set-rpath '$ORIGIN/../lib' "$path"; \
        done; \
    } \
    && bundle_runtime_deps "$bundle_prefix" \
    && if [ -x /opt/ecmwf/mars-client/bin/mars.bin ] && [ ! -e /opt/ecmwf/mars-client/bin/mars ]; then ln -s mars.bin /opt/ecmwf/mars-client/bin/mars; fi \
    && rm -rf /tmp/mars-client-c-bundle /tmp/mars-client-c-build \
    && rm -f /root/.gitconfig \
    && if grep -R -F -- "${token}" /opt/ecmwf/mars-client >/dev/null 2>&1; then exit 1; fi \
    && unset token INPUT_GITHUB_TOKEN \
    && test -x /opt/ecmwf/mars-client/bin/mars

FROM mars-source-build-base AS mars-client-cpp-bundle-build
ARG mars_client_cpp_bundle_ref

RUN --mount=type=secret,id=github_token set -eu \
    && token=$(cat /run/secrets/github_token) \
    && git config --global --add url."https://x-access-token:${token}@github.com/".insteadOf "https://github.com/" \
    && git config --global --add url."https://x-access-token:${token}@github.com/".insteadOf "ssh://git@github.com/" \
    && git config --global --add url."https://x-access-token:${token}@github.com/".insteadOf "git@github.com:" \
    && export INPUT_GITHUB_TOKEN="${token}" \
    && git clone https://github.com/ecmwf/mars-client-bundle.git /tmp/mars-client-cpp-bundle \
    && git -C /tmp/mars-client-cpp-bundle checkout "${mars_client_cpp_bundle_ref}" \
    && cmake -S /tmp/mars-client-cpp-bundle -B /tmp/mars-client-cpp-build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=/opt/ecmwf/mars-client-cpp \
    -DAEC_DIR=/usr/local \
    -DSTACK_DEP_LIBAEC=ON \
    -DSTACK_DEP_PROJ=ON \
    -DECMWF_NEXUS_NIGHTLY=1 \
    -DMIR_ENABLE_TESTS=OFF \
    -DATLAS_ENABLE_TESTS=OFF \
    -DATLAS-ORCA_ENABLE_TESTS=OFF \
    && cmake --build /tmp/mars-client-cpp-build --parallel \
    && cmake --install /tmp/mars-client-cpp-build \
    && bundle_prefix=/opt/ecmwf/mars-client-cpp \
    && copy_runtime_dep() { \
        prefix="$1"; \
        dep="$2"; \
        base=$(basename "$dep"); \
        case "$dep" in \
            "$prefix"/*|/lib/x86_64-linux-gnu/libc.so.*|/lib/x86_64-linux-gnu/libm.so.*|/lib/x86_64-linux-gnu/libpthread.so.*|/lib/x86_64-linux-gnu/libdl.so.*|/lib/x86_64-linux-gnu/librt.so.*|/lib64/ld-linux-*.so.*) return 0 ;; \
        esac; \
        real=$(readlink -f "$dep"); \
        real_base=$(basename "$real"); \
        cp -a "$real" "$prefix/lib/$real_base"; \
        if [ "$base" != "$real_base" ]; then ln -sf "$real_base" "$prefix/lib/$base"; fi; \
    } \
    && bundle_runtime_deps() { \
        prefix="$1"; \
        mkdir -p "$prefix/lib"; \
        for path in "$prefix"/bin/* "$prefix"/lib/*.so "$prefix"/lib/*.so.*; do \
            [ -e "$path" ] || continue; \
            [ -d "$path" ] && continue; \
            if ! patchelf --print-rpath "$path" >/dev/null 2>&1; then continue; fi; \
            for needed in $(patchelf --print-needed "$path"); do \
                case "$needed" in \
                    /*) copy_runtime_dep "$prefix" "$needed" && patchelf --replace-needed "$needed" "$(basename "$needed")" "$path" ;; \
                esac; \
            done; \
            ldd "$path" | while IFS= read -r line; do \
                set -- $line; \
                if [ "${2-}" = "=>" ] && [ "${3-}" != "not" ] && [ "${3#/}" != "$3" ]; then copy_runtime_dep "$prefix" "$3"; fi; \
            done; \
        done; \
        for path in "$prefix"/lib/*.so "$prefix"/lib/*.so.*; do \
            [ -e "$path" ] || continue; \
            [ -d "$path" ] && continue; \
            if ! patchelf --print-rpath "$path" >/dev/null 2>&1; then continue; fi; \
            patchelf --set-rpath '$ORIGIN' "$path"; \
        done; \
        for path in "$prefix"/bin/*; do \
            [ -e "$path" ] || continue; \
            [ -d "$path" ] && continue; \
            if ! patchelf --print-rpath "$path" >/dev/null 2>&1; then continue; fi; \
            patchelf --set-rpath '$ORIGIN/../lib' "$path"; \
        done; \
    } \
    && bundle_runtime_deps "$bundle_prefix" \
    && rm -rf /tmp/mars-client-cpp-bundle /tmp/mars-client-cpp-build \
    && rm -f /root/.gitconfig \
    && if grep -R -F -- "${token}" /opt/ecmwf/mars-client-cpp >/dev/null 2>&1; then exit 1; fi \
    && unset token INPUT_GITHUB_TOKEN \
    && test -d /opt/ecmwf/mars-client-cpp \
    && [ "$(ls -A /opt/ecmwf/mars-client-cpp)" ]

FROM debian:bookworm-slim AS mars-cloud-wrapper
ARG rpm_repo

RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && curl -o /tmp/stable-public.gpg.key "${rpm_repo}/private-raw-repos-config/debian/bookworm/stable/public.gpg.key" \
    && gpg --dearmor -o /usr/share/keyrings/mars-client-cloud.gpg /tmp/stable-public.gpg.key \
    && echo "deb [signed-by=/usr/share/keyrings/mars-client-cloud.gpg] ${rpm_repo}/private-debian-bookworm-stable/ bookworm main" > /etc/apt/sources.list.d/mars-client-cloud.list \
    && apt-get update \
    && apt-get install -y mars-client-cloud=0.2.1 \
    && rm -rf /var/lib/apt/lists/* /tmp/stable-public.gpg.key

FROM debian:bookworm-slim AS mars-base-c

COPY --from=mars-client-c-bundle-build /opt/ecmwf/mars-client /opt/ecmwf/mars-client
COPY --from=mars-cloud-wrapper /usr/local/bin/mars /usr/local/bin/mars

RUN set -eux \
    && test -x /usr/local/bin/mars \
    && test -d /opt/ecmwf/mars-client \
    && test -x /opt/ecmwf/mars-client/bin/mars

FROM debian:bookworm-slim AS mars-base-cpp

COPY --from=mars-client-cpp-bundle-build /opt/ecmwf/mars-client-cpp /opt/ecmwf/mars-client-cpp

RUN set -eux \
    && test -d /opt/ecmwf/mars-client-cpp \
    && [ "$(ls -A /opt/ecmwf/mars-client-cpp)" ]
