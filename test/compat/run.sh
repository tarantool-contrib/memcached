#!/bin/sh
# Run the differential compat suite (layers 1-3) from the repository root,
# building the compat image first if it is missing.
#
#   test/compat/run.sh                       # build, luacheck, tt test test/compat
#   test/compat/run.sh tt test test/compat/fuzz_test.lua -- -v
#   MCC_COMPAT_FUZZ_ITERS=20000 test/compat/run.sh tt test test/compat/fuzz_test.lua -- -v -c
set -e
root=$(cd "$(dirname "$0")/../.." && pwd)
img=${MCC_IMAGE:-mcc-compat:3.9}
if ! docker image inspect "$img" >/dev/null 2>&1; then
    docker build -t "$img" "$root/test/compat"
fi
cmd=${*:-tt package build \&\& .rocks/bin/luacheck test/compat \&\& tt test test/compat -- -v -c}
envs=''
for v in $(env | sed -n 's/^\(MCC_COMPAT_[A-Z0-9_]*\)=.*/\1/p'); do
    envs="$envs $v=$(eval "printf %s \"\$$v\"")"
done
cd "$root"
MCC_IMAGE=$img exec docker/run.sh "env$envs sh -c '$cmd'"
