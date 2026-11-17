#!/bin/sh
# Run test/ipc/ipc_san.c, the harness of memcached/ipc.c, under the
# sanitizers, with both wakeups: the eventfd of Linux and the pipe of macOS
# (-DIPC_WAKEUP_PIPE). Needs gcc and Tarantool's module.h.
#
#   test/ipc/san.sh [asan] [tsan]          # both by default
#
# asan is AddressSanitizer with UndefinedBehaviorSanitizer. tsan needs ASLR
# off (setarch -R), so in a container one without the default seccomp
# profile (CONTRIBUTING.md, "The C data path"). MCC_INCLUDE is the
# directory of module.h (/usr/include/tarantool).
set -eu

src=$(cd "$(dirname "$0")" && pwd)/ipc_san.c
inc=${MCC_INCLUDE:-/usr/include/tarantool}
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

[ $# -gt 0 ] || set -- asan tsan
for san in "$@"; do
    case $san in
    asan) flags='-fsanitize=address,undefined -fno-sanitize-recover=all' ;;
    tsan) flags='-fsanitize=thread' ;;
    *)
        echo "usage: $0 [asan] [tsan]" >&2
        exit 2
        ;;
    esac
    for wakeup in eventfd pipe; do
        def=
        [ "$wakeup" = eventfd ] || def=-DIPC_WAKEUP_PIPE
        echo "== $san, $wakeup"
        # shellcheck disable=SC2086 # $flags and $def are lists of words
        gcc -O1 -g -Wall -Wextra -Werror $def $flags -I"$inc" -pthread \
            "$src" -o "$out/ipc_san" -Wl,--unresolved-symbols=ignore-all
        if [ "$san" = tsan ]; then
            setarch "$(uname -m)" -R "$out/ipc_san"
        else
            "$out/ipc_san"
        fi
    done
done
