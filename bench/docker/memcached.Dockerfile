# Real memcached from the distribution packages.
ARG BASE=ubuntu:24.04
FROM ${BASE}
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends memcached \
    && rm -rf /var/lib/apt/lists/* && memcached -V
