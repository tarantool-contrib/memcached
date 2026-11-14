# Development image: Tarantool 3.9, a C compiler and tt v3, which builds the
# package from app.manifest.toml (the C data path included) and installs its
# dev dependencies - luatest, vshard, checks and luacheck, pinned there and in
# app.manifest.lock - into the project's .rocks/.
#
#   docker build -t mcc-dev:3.9 docker
#
# tt v3 is built from tarantool/tt at the commit in docker/tt-commit
# (docker/tt-build.sh). TT_IMAGE names an image with /usr/local/bin/tt to
# take it from instead, as CI does with the binary it caches.
ARG BASE=tarantool/tarantool:3.9.0
ARG GO_IMAGE=golang:1.27.4
ARG TT_IMAGE=tt-build

FROM ${GO_IMAGE} AS tt-build
COPY tt-build.sh tt-commit /src/
RUN /src/tt-build.sh /usr/local/bin/tt

FROM ${TT_IMAGE} AS tt

FROM ${BASE}
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl unzip build-essential cmake \
      && rm -rf /var/lib/apt/lists/*
# The image's tt 2 goes: tt is tt v3.
RUN rm -f /usr/bin/tt
COPY --from=tt /usr/local/bin/tt /usr/local/bin/tt
RUN tt version
