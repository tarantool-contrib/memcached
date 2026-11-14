#!/bin/sh
# Run a command in the dev container with the repo mounted at /src.
# usage: docker/run.sh <command...>
#
# .rocks/bin of the repository, where tt puts the commands of the dev
# dependencies (luatest, luacheck), comes first on PATH.
#
# tt derives the version of the package with git, so in a git worktree,
# whose .git is a file naming a directory outside it, the git directory of
# the repository is mounted as well, read-only, at the same path.
img=${MCC_IMAGE:-mcc-dev:3.9}
src=$(cd "$(dirname "$0")/.." && pwd)
git=
if [ -f "$src/.git" ]; then
    common=$(git -C "$src" rev-parse --path-format=absolute --git-common-dir \
        2>/dev/null) && git="-v $common:$common:ro"
fi
# shellcheck disable=SC2086 # $git is empty or two words
exec docker run --rm --entrypoint "" --user "$(id -u):$(id -g)" -e HOME=/tmp \
    $git -v "$src":/src -w /src "$img" \
    sh -c "unset TT_APP_NAME TT_INSTANCE_NAME; PATH=/src/.rocks/bin:\$PATH; $*"
