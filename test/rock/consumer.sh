#!/bin/sh
# Install the memcached rock from a rock server into a new project, as an
# application does, and check it there with test/rock/check.lua.
#
#   test/rock/consumer.sh SERVER [DIR]
#
# SERVER is a directory `tt package pack --out` wrote. With a binary rock of
# the host there, tt installs it; with the source rock only, tt builds it,
# which takes a C compiler and Tarantool's headers. DIR is the project, a
# new temporary directory by default. MCC_ROCK=binary or MCC_ROCK=src fails
# the check unless tt installed the binary rock, or built the source rock.
# Needs tt v3 and tarantool.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
server=$(cd "$1" && pwd)
dir=${2:-$(mktemp -d)}
mkdir -p "$dir"
cat > "$dir/app.manifest.toml" <<EOF
manifest_version = "0.1"

[package]
name = "memcached-consumer"

[platform]
tarantool = ">=3.9.0"
tt = ">=3.0.0"

[dependencies]
memcached = { version = "*", registry = "$server" }
EOF

# Every module of the checkout.
modules=$(cd "$root" && find memcached roles -name '*.lua' | sort |
    sed 's|\.lua$||; s|/init$||; s|/|.|g')

cd "$dir"
tt package build
# tt builds a source rock in _build/src/ (doc/concepts/packages.md of tt).
case ${MCC_ROCK:-} in
binary)
    if [ -e _build/src ]; then
        echo "tt built the source rock, not the binary one" >&2
        exit 1
    fi
    ;;
src)
    if ! ls _build/src/memcached-*.built >/dev/null 2>&1; then
        echo "tt did not build the source rock" >&2
        exit 1
    fi
    ;;
esac
# shellcheck disable=SC2086 # one argument per module
tarantool "$here/check.lua" $modules
