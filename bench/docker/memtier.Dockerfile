# memtier_benchmark built from source. The base is the C module's dev image
# (Tarantool 3.9 + autotools, libevent, pcre); zlib is added here.
# Build context: a memtier_benchmark source tree, for example a copy of
# tarantool/memcached's third_party/memtier (1.3.0-17-g820605c).
ARG BASE=mc-dev:3.9
FROM ${BASE}
USER root
RUN apt-get update && apt-get install -y --no-install-recommends zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . /usr/src/memtier
RUN cd /usr/src/memtier && autoreconf -ivf >/dev/null && \
    ./configure --disable-tls >/dev/null && make -j4 >/dev/null && \
    make install && memtier_benchmark --version
