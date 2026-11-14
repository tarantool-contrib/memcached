#!/bin/sh
# Build tt v3, the next major version of tarantool/tt (its v3 branch, not
# released yet), at the commit pinned in docker/tt-commit. The dev image
# (docker/Dockerfile) and CI (.github/actions/tt) build it so.
#
#   docker/tt-build.sh OUT                 # fetch and build: OUT is the binary
#   docker/tt-build.sh fetch SRC           # a checkout of the commit in SRC
#   docker/tt-build.sh build SRC OUT       # build the checkout in SRC
#
# Needs git and Go: the version tt's go.mod names, which GOTOOLCHAIN=auto
# (Go's default) downloads when the local one is older. The version is
# stamped as tt's magefile.go does it (`mage build`, internal/buildflags):
# git describe of the checkout.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
repo=${TT_REPO:-https://github.com/tarantool/tt}

fetch() {
    commit=$(cat "$here/tt-commit")
    # Commits only (tree:0): enough for git describe; the checkout fetches
    # the tree of the commit.
    git clone -q --filter=tree:0 --no-checkout "$repo" "$1"
    git -C "$1" -c advice.detachedHead=false checkout -q "$commit"
}

build() {
    src=$(cd "$1" && pwd)
    out=$2
    case $out in
    /*) ;;
    *) out=$(pwd)/$out ;;
    esac
    tag=$(git -C "$src" describe --tags)
    since=$(git -C "$src" rev-list --count \
        "$(git -C "$src" describe --tags --abbrev=0)..")
    sha=$(git -C "$src" rev-parse --short HEAD)
    v=github.com/tarantool/tt/v3/cli/version
    mkdir -p "$(dirname "$out")"
    (cd "$src" && CGO_ENABLED=0 go build -o "$out" -tags netgo,osusergo \
        -ldflags "-s -w -X $v.gitTag=$tag -X $v.gitCommit=$sha -X $v.gitCommitSinceTag=$since -X $v.versionLabel=" \
        -asmflags "all=-trimpath=$src" -gcflags "all=-trimpath=$src" ./cli)
    "$out" version
}

case ${1:-} in
fetch)
    fetch "$2"
    ;;
build)
    build "$2" "$3"
    ;;
'')
    echo "usage: $0 OUT | fetch SRC | build SRC OUT" >&2
    exit 2
    ;;
*)
    src=$(mktemp -d)
    trap 'rm -rf "$src"' EXIT
    fetch "$src/tt"
    build "$src/tt" "$1"
    ;;
esac
