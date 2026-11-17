# Contributing to tarantool-contrib/memcached

## DESIGN.md first

[`DESIGN.md`](DESIGN.md) is the contract between the modules: the data
format, the engine and front-end contracts, the backends, threads, read
views, hot reload, the role, the metrics and the CAS rules. It describes the
code as it is.

- A change to an interface or a semantic updates `DESIGN.md` in the same
  change, and the design change comes first: write down what changes, then
  change the code to match it.
- Keep it current, not historical: replace the statements that no longer
  hold instead of appending "as built" notes, and don't document what isn't
  in the code.
- User-visible changes also go to [`README.md`](README.md) (options,
  protocol support, limitations) and to [`CHANGELOG.md`](CHANGELOG.md)
  under `[Unreleased]`.

## The package

The package is described by [`app.manifest.toml`](app.manifest.toml), which
[tt v3](https://github.com/tarantool/tt) reads, with its lock,
`app.manifest.lock` (commit it); there is no rockspec and no Makefile.

- Components: `memcached/` (every `*.lua`, the modules `memcached` and
  `memcached.*`) with the C data path, `memcached.datapath`, compiled from
  `memcached/datapath.c` by the `lua-c` backend; `ipc`, the thread
  transport `memcached.ipc` from `memcached/ipc.c`, the same way; and
  `roles/` (the module `roles.memcached`). A new Lua file under `memcached/` or `roles/` is in the
  rock with nothing to declare.
- `[dev_dependencies]`: luatest, vshard, checks and luacheck at the
  versions of CI, and the EmmyLua programs (`tt emmylua`). vshard is a
  runtime dependency of the vshard backend only, which the application
  installs: it is not in `[dependencies]`.
- The version is that of the `v*` git tags: a release is the tag `vX.Y.Z`;
  between tags tt gives `X.Y.Z-dev.N+g<sha>`, with `dirty` for uncommitted
  changes. tt writes it into `memcached/version.lua` of the build, which
  `server.VERSION` reads (`scm` in a source tree tt didn't build).

tt v3 is not released yet: [`docker/tt-build.sh`](docker/tt-build.sh) builds
it from tarantool/tt at the commit in [`docker/tt-commit`](docker/tt-commit),
with git and the Go its `go.mod` names (`GOTOOLCHAIN=auto` fetches it):

```sh
docker/tt-build.sh ~/.local/bin/tt
```

The dev image and CI build it so; moving the pin is a change of
`docker/tt-commit`.

`tt package build` builds the package into the checkout's `.rocks/`: the dev
dependencies by the lock, the C modules into
`.rocks/lib/tarantool/memcached/datapath.so` and `ipc.so`,
`memcached/version.lua` and
copies of the Lua files. The tests and Tarantool started in the checkout run
the sources and take the rest from `.rocks/` (`test/helpers/env.lua`).
`tt test` builds first. `_build/` holds tt's build tree. An old in-tree
`memcached/datapath.so` or `memcached/ipc.so` would shadow the one in
`.rocks/`, so the tests refuse to start while there is one.

## The dev image

Everything runs in a Docker image with Tarantool 3.9.0, a C compiler and tt
v3, built from [`docker/Dockerfile`](docker/Dockerfile):

```sh
docker build -t mcc-dev:3.9 docker
docker/run.sh '<command>'
```

`docker/run.sh` runs the command in the image with the repository mounted at
`/src` (the working directory), as your user, with `HOME=/tmp`, and
`.rocks/bin` (luatest, luacheck) first on `PATH`. In a git worktree it
mounts the repository's git directory too, read-only, since tt derives the
version with git. `MCC_IMAGE` selects another image, for example
`MCC_IMAGE=mcc-compat:3.9`.

Don't wrap test runs in a short `timeout`: its SIGTERM cancels the fibers,
which then fail with a misleading "fiber is cancelled".

## Test suites

| suite | command | CI workflow |
|---|---|---|
| lint | `docker/run.sh 'tt package build && luacheck memcached roles test'` | `test` |
| unit + integration | `docker/run.sh 'tt test test/unit test/integration -- -v'` | `test` |
| the same on the Lua engine only | `docker/run.sh 'MEMCACHED_DATAPATH=lua tt test test/unit test/integration -- -v'` | `test` |
| compat vs memcached | `test/compat/run.sh` | `compat` |
| the rocks | `docker/run.sh 'tt package pack --format rock,rock-src --out _build/dist && test/rock/consumer.sh _build/dist'` | `pack` |
| benchmarks | `bench/run.sh`, see [`bench/README.md`](bench/README.md) | none |

`tt test` builds the package, then runs luatest on the paths it is given
(here without `test/compat`, which needs the compat image), and passes the
arguments after `--` to luatest.

### The C data path

`memcached/datapath.c`, with `memcached/cloop.c` (the C connection loop,
on `datapath.h`), is built by `tt package build` and `tt test` (and so
by `tt package pack`), against Tarantool's `module.h`. A running Tarantool
keeps the `.so` it loaded until it restarts. CI builds it with
`CFLAGS='-O2 -g -Wall -Wextra -Werror'`, which tt passes to the compiler;
do the same before pushing a change of the C file.

- Without the `.so` (a source tree tt didn't build) everything runs on the
  Lua engine, and `test/integration/datapath_test.lua` is skipped.
- `MEMCACHED_DATAPATH=lua` runs any suite on the Lua engine with the `.so`
  built; both runs must pass. `datapath_test.lua` compares the two paths
  byte for byte. `MEMCACHED_DATAPATH=batch` keeps the C batch and runs the
  Lua connection loop; `cloop_test.lua` compares the two loops byte for
  byte.
- A change of the get or set semantics in `engine.lua` (`ops.get`,
  `ops.store`, `is_dead`, `next_cas`, the tuple layout) must be mirrored in
  `datapath.c`, or the requests concerned taken off the C path (`fast` in
  `proto/text.lua`, `fast()` in `engine.lua`). A change of the C functions'
  arguments or results bumps `DATAPATH_ABI` there and
  `engine.DATAPATH_ABI`.
- The C code uses only `module.h` (the box C API) and the Lua C API, no
  FFI declarations: a hot reload must be able to require it again.
- `memcached/ipc.c`, the channels between the application threads and TX
  (DESIGN.md "Application threads"), also uses libc, pthreads and eventfd
  (a pipe on macOS, or with `-DIPC_WAKEUP_PIPE`).
  Its process-wide state must survive a second `luaopen` (a reload). A
  change of its messages or functions bumps `IPC_ABI` there,
  `threads.IPC_ABI` and the one in `backend/thread.lua`. Without the `.so`
  the threads use `threads.call`; `threads_test.lua` covers both. The
  rings, slots and lifetime also run outside Tarantool under the
  sanitizers, with `test/ipc/ipc_san.c`, for both wakeups
  (`test/ipc/san.sh`; TSAN needs ASLR off, so a container without the
  default seccomp profile):

  ```sh
  docker/run.sh test/ipc/san.sh asan
  docker run --rm --security-opt seccomp=unconfined --user "$(id -u):$(id -g)" -v "$PWD":/src -w /src mcc-dev:3.9 test/ipc/san.sh tsan
  ```
- The C code builds on Linux and macOS: no Linux-only calls or flags
  without a fallback (`eventfd`, `MSG_NOSIGNAL`), and the tests name a
  module `<name>.so` or `<name>.dylib` by `jit.os` (`test/helpers/env.lua`
  `soext`).

### Unit and integration tests

- `test/unit/`: the parsers and encoders, without box.
- `test/integration/`: real sockets against Tarantool instances started by
  luatest, including clusters (`luatest.cluster`) for the role and vshard,
  child servers with application threads, and hot reload on a patched copy
  of the code. The helpers are in `test/helpers/`.
- One file or one test:
  `docker/run.sh 'tt test test/integration/text_test.lua -- -v'`,
  `docker/run.sh 'tt test test/integration/vshard_test.lua -- -v -p test_bucket_move'`.
  `luatest` from `.rocks/bin` runs them without a build:
  `docker/run.sh 'luatest -v test/integration/text_test.lua'`.
- Opt-in benchmarks inside the suites:
  `docker/run.sh 'MCC_BENCH=1 tt test test/integration/threads_test.lua -- -c -p test_bench'`,
  and the same for `vshard_threads_test.lua`.
- The whole suite takes a few minutes. Run it twice after changes to
  shared files; the cluster tests are timing-sensitive.

### The compat suite

`test/compat/` sends the same requests to a real memcached and to
tarantool-contrib/memcached and compares the replies; see
[`test/compat/README.md`](test/compat/README.md). Its image adds memcached
1.6.45, the reference, to the dev image:

```sh
docker build -t mcc-compat:3.9 test/compat      # after mcc-dev:3.9
MCC_IMAGE=mcc-compat:3.9 docker/run.sh 'tt package build && luacheck test/compat && tt test test/compat -- -v -c'
test/compat/run.sh                              # the same; builds the image if missing
```

After a protocol change, also run a long fuzz:

```sh
MCC_IMAGE=mcc-compat:3.9 docker/run.sh 'MCC_COMPAT_FUZZ_ITERS=20000 MCC_COMPAT_FUZZ_SEED=1,2,3,4 tt test test/compat/fuzz_test.lua -- -c'
```

A new divergence from memcached is either a bug to fix, or an entry in
[`test/compat/allowlist.lua`](test/compat/allowlist.lua) with its class
(a known bug or intentional), the reason and the memcached source
reference. The `compat`
workflow can run a longer fuzz by hand (`workflow_dispatch` with
`fuzz_iters` and `fuzz_seeds`).

### The rocks

`tt package pack --format rock,rock-src --out DIR` packs the binary rock of
the host (`memcached-<version>-1.linux-x86_64.rock`: the Lua files,
`roles/memcached.lua`, `memcached/version.lua` and the compiled data path),
the source rock (`memcached-<version>-1.src.rock`: the files git tracks, the
manifest and a `VERSION` file) and the rockspec tt generates from the
manifest, and indexes `DIR` as a rock server. Its version is the tag's on a
`vX.Y.Z` tag of a clean tree.

[`test/rock/consumer.sh`](test/rock/consumer.sh) `DIR` installs the rock
from `DIR` into a new project, as an application does, and checks it there
with [`test/rock/check.lua`](test/rock/check.lua): every module of the
checkout loads from that project's `.rocks`, the data path has the engine's
ABI, and an instance answers a set and a get. The `pack` workflow runs it
on the binary rock, and on a server with the source rock alone, which tt
builds (`MCC_ROCK=binary` and `MCC_ROCK=src` check which it was).

### Lint

CI runs luacheck (`.luacheckrc`) and checks the formatting with luafmt
(`.luafmt.toml`, the formatter of EmmyLua):

```sh
docker/run.sh 'luacheck memcached roles test bench'
docker/run.sh 'tt emmylua fmt'            # the diff
docker/run.sh 'tt emmylua fmt --write'
```

Without a path tt formats the Lua files git sees: the tracked ones and the
untracked ones that `.gitignore` and `.git/info/exclude` don't ignore, never
`_build/` and `.rocks/`. luafmt doesn't always reach its result in one pass:
if the check still shows a diff after `--write`, run it again.
It can also produce code LuaJIT rejects, a line break between a function
and its parenthesized arguments ("ambiguous syntax"), in a long method
chain on a call such as `require('x').get(y):info().z`: put the head of
such a chain in a local. Two string literals it would join on one line
(`'a ' .. 'b'`) are better written as one.

The EmmyLua analyzer is not enforced (`docker/run.sh 'tt emmylua check
memcached roles'`): its warnings are inference false positives, missing
annotations (the `state` tables, `ffi.C`, luatest's API) and missing stubs
of Tarantool's built-in modules. The project has no configuration of its
own: tt writes it (LuaJIT, the globals of Tarantool, the stubs of the
Tarantool it runs on, the modules of `.rocks/`) and leaves `_build/` out.

### Benchmarks

`bench/` compares tarantool-contrib/memcached with tarantool/memcached and memcached
using memtier_benchmark, and profiles it. Other people may be editing the
working tree: benchmark a copy (`MCC_SRC`). See
[`bench/README.md`](bench/README.md) for the images and the settings.

## Code rules

- Lua. C is added only where a benchmark shows a real gain (so far the
  data path, `memcached/datapath.c`), and the Lua code keeps the same
  semantics, so that the C part stays optional.
- luacheck must be clean (`.luacheckrc`).
- A module that may run in an application thread (`server.lua`, `proto/*`,
  `backend/thread.lua`, `backend/vshard.lua`, the thread side of
  `threads.lua`, `readview.lua`) must not use `box.space`, `box.schema` or
  other TX-only `box` APIs.
- Hot reload: mutable state lives in `memcached/state.lua` (add new
  fields to its `DEFAULTS`), and long-lived fibers, callbacks and objects
  look the code up in `package.loaded` instead of keeping module tables.
- CAS values and `incr`/`decr` numbers are `uint64_t` cdata, never Lua
  numbers.
- A protocol fix gets a unit test and an integration test, and the compat
  suite stays green.
- Don't commit generated files: `.rocks/`, `_build/`, `.tarantool/`,
  snapshots, xlogs and logs are in `.gitignore`.

## Continuous integration

`.github/workflows/`, with tt v3 from
[`.github/actions/tt`](.github/actions/tt/action.yml): built with
`docker/tt-build.sh` and `actions/setup-go`, and cached by the commit of
`docker/tt-commit`:
- `test.yml`: Tarantool 3.9 via `tarantool/setup-tarantool@v4`,
  `tt package build --locked`, luacheck, and `tt test` on `test/unit` and
  `test/integration`, with the C data path (built with warnings as errors),
  once more on the Lua engine only, and the thread suites once more with
  the pipe wakeups (`-DIPC_WAKEUP_PIPE`); `test/ipc/san.sh` under ASan,
  UBSan and TSan; and on macOS (`macos-15`, arm64) `tt package build` and
  `tt test` on `test/unit` and `test/integration`, with the latest
  Tarantool 3.9 release built from source and cached;
- `compat.yml`: builds the dev image (with the cached tt) and the compat
  image and runs the compat suite with a short fuzz against memcached
  1.6.45;
- `pack.yml`: `tt package pack --format rock,rock-src`, then
  `test/rock/consumer.sh` on the binary rock and on the source rock alone;
  the rocks are an artifact of the run.
