# Compat test image: the dev image (Tarantool 3.9 + luatest) plus a reference
# memcached 1.6.45 built from source (the version
# tarantool-contrib/memcached follows and its VERSION reply names). The source tree, with its t/ tests and the
# test helpers (memcached-debug, timedrun), stays in /opt/memcached-src.
#
#   docker build -t mcc-compat:3.9 test/compat
#   MCC_IMAGE=mcc-compat:3.9 docker/run.sh 'luatest -v test/compat'
ARG BASE=mcc-dev:3.9
FROM ${BASE}
ARG MEMCACHED_VERSION=1.6.45
ARG MEMCACHED_SHA1=45038980ea7045a548b9b5b5125ef7116312a768
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      libevent-dev pkg-config perl autoconf automake \
      && rm -rf /var/lib/apt/lists/*
# The release tarball, checked against the sha1 from memcached.org; if the
# download fails, the release tag from GitHub.
RUN set -e; mkdir -p /opt && cd /opt; \
    if curl -fsSL "https://memcached.org/files/memcached-${MEMCACHED_VERSION}.tar.gz" \
            -o memcached.tar.gz; then \
        echo "${MEMCACHED_SHA1}  memcached.tar.gz" | sha1sum -c -; \
        tar -xzf memcached.tar.gz; rm memcached.tar.gz; \
        mv "memcached-${MEMCACHED_VERSION}" memcached-src; \
    else \
        git clone --depth 1 --branch "${MEMCACHED_VERSION}" \
            https://github.com/memcached/memcached memcached-src; \
        cd memcached-src && ./autogen.sh; \
    fi; \
    cd /opt/memcached-src && ./configure --prefix=/usr/local && \
    make -j"$(nproc)" && make install && \
    chmod -R a+rX /opt/memcached-src && memcached -V
