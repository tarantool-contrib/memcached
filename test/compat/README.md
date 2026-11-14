# Differential compat suite: memcached vs tarantool-contrib/memcached

The same request bytes go to a reference memcached (a child process) and to
tarantool-contrib/memcached ("contrib", in the luatest process). The
normalised replies must be equal, or an allowlist entry must explain the
difference.

- **Layer 1**, `scenarios_test.lua`: hand-written scenarios in
  `scenarios/{smoke,classic,meta,errors,pipeline,ttl}.lua`.
- **Layer 2**, `fuzz_test.lua`: a seeded differential fuzzer. It shrinks
  every unexplained mismatch with ddmin.
- **Layer 3**, `memcached_t_test.lua`: memcached's own `t/*.t` tests, run
  against contrib through a `memcached-debug` shim (`shim/`). The expected
  failures are listed in `memcached_t_expected.lua`.

## Running

The image `mcc-compat:3.9` is the dev image plus memcached 1.6.45, the
reference (built from source, with `t/` in `/opt/memcached-src`).
Run everything from the repository root:

```sh
docker build -t mcc-compat:3.9 test/compat          # once
MCC_IMAGE=mcc-compat:3.9 docker/run.sh 'tt package build && luacheck test/compat && tt test test/compat -- -v -c'
test/compat/run.sh                                  # the same; builds the image if missing
```

`tt test` builds the package first (the C data path, into `.rocks/`), then
runs luatest with the arguments after `--`.

The variables below go inside the container command, for example
`docker/run.sh 'MCC_COMPAT_FUZZ_ITERS=20000 tt test test/compat/fuzz_test.lua -- -v'`.
`run.sh` passes them through:

| variable | meaning |
|---|---|
| `MCC_COMPAT_MEMCACHED` | reference binary, default `memcached` (1.6.45) |
| `MCC_COMPAT_TRACE` | log every exchange |
| `MCC_COMPAT_TIMEOUT` | reply timeout in seconds, default 3 |
| `MCC_COMPAT_FUZZ_ITERS` | fuzz steps per seed, default 300 (CI); use e.g. 20000 for a long run |
| `MCC_COMPAT_FUZZ_SEED` | seeds, comma separated, default `1,2,3` |
| `MCC_COMPAT_FUZZ_EPISODE` | steps per episode (fresh state), default 40 |
| `MCC_COMPAT_FUZZ_SHRINK` | replay budget for shrinking, default 400 |
| `MCC_COMPAT_T_FILES`, `MCC_COMPAT_T_REFERENCE`, `MCC_COMPAT_T_VERBOSE`, `MCC_COMPAT_T_TIMEOUT` | layer 3, see `memcached_t_test.lua` |

The tests are skipped when the reference memcached, or for layer 3 the
memcached source tree, perl or prove, is missing.

## How a step is compared

- **Sync.** Each step is sent, and then `mg <prefix>sync O<n>`. The reply is
  read up to `EN O<n>`. The sync is not `mn`, because steps contain `mn` and
  memcached flushes its `MN` at once. With `nosync = true`, the reply is read
  until the connection has been idle for 0.5 s. Each scenario gets a fresh
  connection, a `flush_all` and a new key prefix. Both servers run a scenario
  at the same time, so the sleeps overlap.
- **CAS.** CAS values become `<Cn>`, numbered by first appearance on each
  server, so two CAS values compare by consistency, not by value. `0` and
  client-supplied `E<n>` values stay literal. Templates `{casN}` and
  `{lastcas}` expand to the server's own values. A missing one expands to
  2^64-1, which no server hands out and which is above every CAS.
- **TTL and last access.** Meta `t`/`l` and `me` `exp=`/`la=` become `~n`,
  compared within ±1. `-1` stays exact.
- **Masked fields.** `me` `cls=` and `size=` become `*`. The VERSION line
  becomes `VERSION *`.
- **Stats.** Stats blocks become `<STATS>`. The summary prints, per
  subcommand, the key names only one side has (fd numbers are generic), so
  stats never fail a test.
- **Templates.** `{k}` is the prefix, `{key:N}` a key of N bytes,
  `{abs+N}` an absolute time, `{b64:...}`, `{rep:C:N}`.

## Allowlist (`allowlist.lua`)

Each entry has an `id`, a `class` and a `reason`, plus a `source` reference
into memcached (and a `repro` for class a). The optional fields:

- `ref`: the reference versions the entry applies to, e.g. `'>1.6.45'`
  (none uses it: 1.6.45 is the only reference).
- `match(c)`: whether the entry explains a whole step.
- `fix(c) -> ref, got`: rewrites the replies to undo exactly this divergence.
  Fixes compose, so one step can hit several entries.
- `auto`: the entry may explain untagged steps; this is what the fuzzer relies
  on.

A scenario step, or a whole scenario, names entries with
`allow = '<id>' | {...}`. A tag that does not diverge is listed in the summary
as stale (`flaky = true` mutes this for races in memcached).

The classes:

- **a**: a tarantool-contrib/memcached bug. It is known and kept green until fixed. The
  summary lists it first.
- **b**: intentional.
- **c**: specific to a memcached version. There are none: contrib follows
  memcached 1.6.45 for both protocols and its `version` reply names 1.6.45.
  The bugs of 1.6.45 that contrib doesn't copy are class b.

The full classified list, with reproducers, is in `allowlist.lua`; the
expected layer 3 failures are in `memcached_t_expected.lua`.

## Fuzzer

A xorshift32 PRNG drives a grammar of all classic and meta commands over
eight keys, so commands collide:

- four text keys;
- two numeric keys;
- two binary keys, used only base64-encoded with `b`.

A step is 1-4 commands sent in one write. About 4% of the steps are
malformed commands with consistent answers, each in a step of its own.

The generator keeps out only the inputs whose divergence is listed in
`allowlist.lua` and can't be undone by a fix:
- everywhere: short opaques; in `mg`, no `t` together with `T` or with a
  negative `N`, no `N` together with `T`, no `R` after `T`;
- per key kind (`KINDS` in `fuzz_test.lua`): numbers get no
  `h`/`me`/`R`/`N` in `mg`, no `md I` and no `ma T`; base64-only keys
  get no `k` in `mg`/`ma` and no `me`, and the text one no `ma`;
- an episode uses either `E` or stale sets (`I`), never both: `E` literals
  sit between the two servers' CAS ranges, so "older than" differs.

The aligned divergences are generated too and must match: uppercase
commands (`ERROR`), classic client flags above 2^32-1 (truncated), `md x`
without `T`, and `append`/`prepend`, `ms` `MA`/`MP` and `mg s` on numbers
(the space padding of `incr`/`decr`).

The former class (a) cases are generated and must match: `C0`, `N-1`,
`h`/`me` after a vivify or an `md x`, `md x` without `E`, and `ma C` on empty
or text values.

On a mismatch, the failing prefix is replayed on a fresh state. It is first
reduced by steps with ddmin, then split into single commands and reduced
again. The failure prints the seed and the minimal reproducer as sent
(templates), plus a command line that reproduces it.
