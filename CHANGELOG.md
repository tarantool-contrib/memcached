# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The first version: a memcached server for Tarantool 3.9+ in Lua with a C
data path, distributed as the rock `memcached`.

### Added

- The C data path, `memcached.datapath` (`memcached/datapath.c`, compiled
  by the rock): the most common commands on memory storage run in C, with
  the semantics, the replies and the counters of the Lua engine, which
  serves everything else and everything the C code can't finish. The C
  code parses the commands straight from a connection's input and writes
  their replies (the C batch): the classic `get`, `gets`, `gat`, `gats`,
  `set`, `add`, `replace`, `append`, `prepend`, `cas`, `delete`, `touch`,
  `incr`, `decr`, and the meta `mg`, `ms`, `md`, `ma` and `mn` with their
  common flags (not the win token, vivify, recache and explicit CAS
  cases, nor `me`). In the TX thread with the `local` backend, the Lua
  front-end takes the commands the C batch doesn't serve and hands the
  input back to it at the next one it serves, within one batch and one
  write. With front-ends in application threads, a thread sends the input
  bytes of a batch that starts with `get`, `gets` or `set` to TX and gets
  the replies back as bytes, instead of request and response tables:
  through a shared-memory channel per thread (`memcached.ipc`,
  `memcached/ipc.c`, compiled by the rock: lock-free rings of slots with
  eventfd wakeups, served in TX by a pool of fibers that lets the writes
  overlap), or in one `threads.call` without it. The channel cuts the round
  trip of a batch from about 100 µs to 25 µs on one connection and from
  20 µs to 2-3 µs each with 50 concurrent ones; in the in-suite benchmark
  the threads go 1.4-2.1x faster than with `threads.call`, and with many
  connections at pipeline 1 faster than TX alone (1.4-2.1x).
  `memcached.datapath([enabled])` and `MEMCACHED_DATAPATH=lua` switch it
  off; without the compiled module the Lua engine serves every request.
- The C connection loop (`memcached/cloop.c`, in the same module): in the
  TX thread with the `local` backend, a connection reads, runs the C batch
  and writes in C for as long as the C batch serves its input, and goes
  to the Lua loop only for the commands it doesn't serve, EOF, the idle
  timeout, a close, a configuration change or a hot reload. Its counters
  are added to the server's on demand, so `stats` from any connection,
  `inst:info()` and the metrics stay exact. At pipeline 1 it takes about a
  quarter off the TX CPU time per request (in-container benchmarks: 15.1-
  15.6 -> 11.7-12.6 µs for gets, 16.0-16.1 -> 12.2-13.7 µs for a 1:10
  set:get mix). `memcached.datapath_loop([enabled])` and
  `MEMCACHED_DATAPATH=batch` switch it off, the bytes on the wire are the
  same.
- The C batch writes the commands of a pipeline in one transaction (a
  group) instead of one autocommit statement per write, committed before
  the replies go out: one WAL write and one wakeup per group instead of per
  write. With `wal_mode = 'write'` a pipeline of 16 sets on one connection
  takes 7-8 instead of 34-38 µs per set in an in-container benchmark,
  4.8-5.5 instead of 7.5-9.6 µs with 16 such connections; without the WAL
  the C batch of 16 sets takes 10-20% less CPU per set. A write that fails in a group (out of
  memory included) leaves the commands before it committed and goes to the
  Lua engine as before; a commit that fails sends every command of the
  group to the Lua front-end, which answers each as if C had never tried
  it. The replies are the same; other fibers no longer run between the
  writes of one batch.

- `expire.mode` (instance, role and vshard storage space option):
  `'scan'`, the default for memory storage, keeps no index on the
  expiration time in the data space, so a set and the first get after it
  update one index less (about 1 µs of a 3.5 µs `replace`; a 100 B set
  through the C path 5.6 -> 4.7 µs in the in-container benchmark), and the
  sweeper scans the space with a cursor, adaptively, as Redis' active
  expiration does (bounded to 25 ms per 0.1 s, a full pass within 10
  minutes when idle), for expired and flushed items alike. `'index'`, the
  default and the only mode for disk storage, keeps the index and the
  sweeper that walks it. A writable instance creates or drops the index of
  an existing space when the mode changes; read-only instances and hot
  reloads leave it.

- The memcached text protocol of memcached 1.6.45 (the `version` reply is
  `VERSION 1.6.45-mcc-<version>`): the classic commands (`get`,
  `gets`, `gat`, `gats`, `set`, `add`, `replace`, `append`, `prepend`,
  `cas`, `delete`, `incr`, `decr`, `touch`, `flush_all`, `stats`,
  `version`, `verbosity`, `quit`) and the meta commands (`mg`, `ms`, `md`,
  `ma`, `mn`, `me`) with their flags, including win tokens, stale items,
  autovivification, explicit CAS (`E`) and base64 keys. Command names
  are case-sensitive, keys may hold control characters, client flags
  above 2^32-1 are truncated to 32 bits, and a too long line closes the
  connection silently, and `incr`/`decr` pad a shorter number with
  spaces to the old length and keep the fetched flag, the stale marker and
  the win token when memcached updates the number in place, and drop all
  three when it writes a new item. `max_item_size` limits the
  item size as memcached counts it (the value, the key and the item
  header). Pipelining, with the
  replies of a read written at once.
- Win tokens stored in the items (the `token` field): persisted,
  replicated and moved with vshard buckets, so they survive a restart, a
  failover and a bucket move; a win costs one WAL write. Read-only
  instances keep the tokens they hand out in memory, per replica.
- `lru_crawler metadump` and `mgdump`, from a read view of the local space
  or bucket by bucket on vshard.
- A data space per instance (format v3, memtx or vinyl; a format v2 space
  of an earlier development version is upgraded in place) with expiration by
  a sweeper, `flush_all` persisted and replicated, CAS values that
  never repeat for a key across restarts and bucket moves, and last access
  and fetched tracking for the meta `l`/`h` flags.
- The Lua API: `memcached.create/check/get/destroy/exec/reload` and
  the instance methods `cfg`, `start`, `stop`, `destroy`, `info`, with
  runtime reconfiguration (rebinding keeps the connections) and a start on
  read-only instances that waits for the space to be replicated.
- The Tarantool 3 role `roles.memcached`: instances from
  `roles_cfg`, with validation, reconciliation on config reload, and a
  readiness health check per instance (Tarantool 3.9 named subchecks).
- The vshard backend: front-ends on the routers, sharded spaces on the
  storages served by the same role config, per-bucket async routing that
  follows bucket moves, read modes `ro` and `bro`, `flush_all` and `stats`
  over all the storages.
- Front-ends in Tarantool application threads (the `threads` option), for
  the local backend (the threads do the socket work and send the input
  bytes of the classic gets and sets to the C data path in TX through a
  shared-memory channel, or one cross-thread call for those at the head of
  a batch; one `threads.call` for the rest)
  and for vshard (a vshard router in every thread, kept in sync with the
  router in TX).
- Stale reads from read views (`read_mode = 'snapshot'`), refreshed every
  `snapshot_refresh` seconds and shared with the application threads.
- Hot reload of the code (`memcached.reload()`) in TX and in the
  application threads; listeners, connections, counters, read views and win
  tokens survive it.
- Metrics for the built-in `metrics` module (`tnt_memcached_*`), per
  instance and per application thread, and for the spaces of vshard
  storages.
- The instance option `track_access` (default `true`): with `false`, gets
  never write the item for the last access and fetched flags, so reads make
  no WAL writes (a win token still does). The role accepts it; with vshard
  it is set per space on the storages (`storage.cfg({spaces = {[<space>] =
  {track_access = false}}})`), and the role passes it there.
- The instance option `idle_timeout` (seconds, default 0 = off), as
  memcached's `-o idle_timeout`: a TCP connection that waits for a command
  longer than that is closed and counted in `idle_kicks` (`stats`, and the
  metric `idle_kicks_total`); `stats settings` shows it. It works in TX and
  in application threads, changes at runtime with `inst:cfg()`, and the
  role accepts it. The wait for a command gets a timeout: no syscall or
  timer more per request.
- `threads = false` (or `box.NULL`) in `create()` and `cfg()`: the
  front-ends run in TX, so `inst:cfg()` moves them back from the threads
  (closing their connections, as any move does). The role uses it when
  `threads` is removed from the config, instead of recreating the
  instance.
- `stats conns` as in memcached: the listener and every connection with
  `<fd>:addr`, `listen_addr`, `state` (memcached's state names) and
  `secs_since_last_cmd`; with application threads, the connections of
  every thread. `stats sizes` answers `sizes_status disabled`, as
  memcached does by default.
- `mcc.storage_stats` on a vshard storage leaves out the tuples of the
  buckets it doesn't own (receiving, sent, garbage), with an index count per
  such bucket, so a bucket in transfer counts once; `bytes` is approximate
  then.
- The role applies a config in which listen URIs move between instances
  (a swap, a cycle, a chain) in one reload: it first releases the listeners
  that another instance takes over, keeping their connections
  (`srv:release()`), then each `cfg` binds its new URI. A failed bind is
  reported as before and the instance keeps (or gets back) its old URI.
- The instance option `commit_wait` (default `complete`), the wait mode of
  Tarantool's `box.commit({wait = ...})` for every write of the instance,
  the C data path and the writes of gets included: `submit` and `none`
  answer a write before its WAL write, so that the writes of a pipelined
  batch go to the WAL together (group commit); `none` fails a write with
  `SERVER_ERROR The WAL queue is full` instead of waiting for room in the
  queue. Such a write is rolled back if the WAL write fails after the
  reply. A connection still reads its own writes, with memtx MVCC and on
  vinyl too. The role accepts it; with vshard it is set per space on the
  storages, as `track_access`. `flush_all` and the sweepers keep
  `complete`.
- Eviction when memtx memory is near full, on by default as in memcached:
  the instance option `evict = {enabled, high, low, samples, batch}` (and the
  same per space in `storage.cfg()`, passed by the role for vshard). A
  sampled approximated LRU (the oldest last access, or store time, among
  random items; dead items first), run by one fiber per process between the
  watermarks and on demand by a write that memtx can't allocate, which then
  runs once more. Without it, or when that fails, a store answers memcached's
  `SERVER_ERROR out of memory storing object`. `stats` has `evictions`,
  `reclaimed`, `evicted_unfetched`, `evicted_active`, `direct_reclaims` and
  `store_no_memory`, `stats settings` `evictions on|off`; the metrics
  `tnt_memcached_evictions_total` and `tnt_memcached_storage_evictions_total`
  count evictions.
- `memcached.tune_gc()`, an explicit knob for the Lua GC pause and
  step multiplier of the calling thread; the module never tunes the
  collector by itself.
- A data path tuned for LuaJIT: no closure per batch, a parser fast path for
  `get <key>`, one-concat encoding of retrievals, raw input buffer pointers,
  one syscall fewer per request, plain memtx gets without a transaction, and
  the flush time and the read-only state cached (the flush time invalidated
  by named triggers on `_mcc_meta` and `_truncate`).
- A differential test suite against memcached 1.6.45 (scenarios, a fuzzer
  and memcached's own `t/*.t` tests), and benchmarks against
  tarantool/memcached and memcached.
- The package is built, tested and packed with tt v3 (built from
  tarantool/tt at a pinned commit, `docker/tt-commit`) from
  `app.manifest.toml` and its lock: `tt test` runs the suites, and
  `tt package pack --format rock,rock-src` packs the binary rock (the C
  data path built), the source rock and the rockspec, which tt generates.
  The repository has no rockspec and no Makefile.
- The version is that of the `v*` git tags; `memcached.VERSION` and the
  `version` reply read it from `memcached/version.lua`, which tt writes
  (`scm` in a source tree tt didn't build).
- CI workflows: lint and tests on Tarantool 3.9 with the C data path on and
  off, the compat suite, and the rocks installed into new projects, the
  source rock alone included (`pack`).
- The vshard backend supports vshard `>= 1.0.0, < 2.0.0` and logs a
  warning once when the vshard (or vshard-ee) it loads is outside that
  range or has no version.

### Fixed

- `append`/`prepend` whose joined item is over `max_item_size` answer
  `NOT_STORED` (meta `NS`) and keep the old item, as memcached does; they
  used to store it.

