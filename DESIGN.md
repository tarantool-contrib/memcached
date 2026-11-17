# tarantool-contrib/memcached: design

A memcached server for Tarantool 3 written in Lua, with the hot data path
(the classic get, gets and set) in C. It succeeds
[tarantool/memcached](https://github.com/tarantool/memcached), whose data
path is in C. This document is the contract between the modules and
describes the code as it is. Change it first when an interface or a
semantic changes; see `CONTRIBUTING.md`.

## Goals

- **Protocol.** The memcached text protocol: the classic commands and the
  meta commands (`mg` `ms` `md` `ma` `mn` `me`), as in memcached 1.6. The
  binary protocol is not supported, since upstream deprecated it.
- **Tarantool 3 role.** `roles.memcached`, configured declaratively.
- **vshard.** A memcached front-end on vshard routers. The data lives in a
  sharded space on vshard storages.
- **Multithreading.** Front-ends (network, parsing, routing) can run in
  Tarantool application threads. Data operations on a local space always
  run in the TX thread.
- **Hot reload.** The code can be reloaded without a restart, and listeners
  and connections survive it.
- **Read views.** Dumps (`lru_crawler metadump` / `mgdump`) and an optional
  stale-read mode (`read_mode = 'snapshot'`) run over read views, and the
  iteration runs in the thread of the connection.
- **Packaging.** Lua, distributed only as a rock, which tt v3 builds and
  packs from `app.manifest.toml` (see "Build"). C is added only where a
  benchmark shows a real gain: so far the get/set data path ("The C data
  path"), which is optional, since the Lua engine has the same semantics.

## Requirements

- Tarantool 3.9 or newer: application threads, the `threads` module, and
  read views usable in them.
- vshard, only for the vshard backend: `>= 1.0.0, < 2.0.0`
  (`backend/vshard.lua` `VSHARD_RANGE`). The tests run on vshard 1.0.0. The
  routers use `callrw`, `callro`, `callbro` and `map_callrw` plus the
  internals listed in "vshard" below.
- tt v3 to build the package and pack its rocks; a C compiler and
  Tarantool's headers (`module.h`, the `tarantool-dev` package) to build
  it from source.
- luatest, luacheck, vshard and checks for the tests: the dev dependencies
  of `app.manifest.toml`.

## Layout

```
memcached/
  init.lua            public API: create/check/get/destroy/exec/reload,
                      instance methods, options
  state.lua           state that survives hot reload (see "Hot reload")
  schema.lua          space format v3, meta space, creation, RO-safe ensure
  engine.lua          command semantics; TX thread only; batch exec
  datapath.c          the C data path: the batch of the common commands (memtx)
  datapath.h          its C API (mcc_dp_batch_run()), for C code in the module
  cloop.c             the C connection loop of TX (part of the datapath module)
  ipc.c               the shared-memory channels of the raw batch between
                      the application threads and TX
  expire.lua          expiration sweeper (exptime index or scan); TX only
  evict.lua           eviction when memtx memory is near full; TX only
  server.lua          transport: listen (SO_REUSEPORT), connection loop,
                      pipelining, dispatch to a backend; works in any thread
  proto/text.lua      classic text commands: parse + encode
  proto/meta.lua      meta commands: parse + encode
  backend/local.lua   front-end in TX: calls engine.exec directly
  backend/thread.lua  front-end in an app thread: a channel (ipc.c) or
                      threads.call('tx', ...)
  backend/vshard.lua  front-end on a vshard router: bucket_id, call*
  storage.lua         vshard storage side: sharded spaces, exported functions
  threads.lua         front-ends in application threads, TX exports
  readview.lua        snapshot reads and dumps over read views
  reload.lua          mcc.reload()
  metrics.lua         metrics integration (TX)
roles/memcached.lua   Tarantool 3 role
app.manifest.toml     the package for tt v3: components, dev dependencies
test/unit/            parser/encoder tests (no box)
test/integration/     protocol, engine, lifecycle, role, vshard, threads,
                      read view, reload and metrics tests
test/compat/          differential tests against real memcached
test/rock/            the rock installed into a new project, checked there
bench/                memtier-based benchmarks
```

A module that may run in an application thread must not touch `box.space`,
`box.schema` or other TX-only `box` APIs. These are `server.lua`,
`proto/*`, `backend/thread.lua`, `backend/vshard.lua`, `threads.lua` (its
thread side) and `readview.lua`, which uses only `box.read_view` there.

## Data format v3

The space is named `<space_name>`, by default `__mcc_<instance name>`. It has
these fields:

| # | name        | type      | meaning |
|---|-------------|-----------|---------|
| 1 | key         | string    | at most 250 bytes; see "Keys" |
| 2 | value       | string    | raw bytes |
| 3 | flags       | unsigned  | client flags (32 bit) |
| 4 | exptime     | number    | absolute unix time in seconds, 0 = never |
| 5 | cas         | unsigned  | see "CAS" |
| 6 | created     | number    | time of the last write of the value (store, arithmetic, `md x`), for `flush_all` |
| 7 | last_access | number    | unix time, for meta `l`/`h` and dumps |
| 8 | fetched     | boolean   | fetched since the last store (meta `h`) |
| 9 | stale       | boolean   | marked stale by `md I`, or by `ms I` with an older CAS (meta) |
| 10| bucket_id   | unsigned  | vshard bucket; 0 when vshard is not used |
| 11| token       | boolean, nullable | the meta win token is out for this version of the item; missing = false (see "Win tokens") |

Indexes:
- `primary` on `key`: HASH for memtx, TREE for vinyl;
- `exptime` on `exptime`: TREE, not unique; only with `expire.mode =
  'index'` (the default for vinyl), see "Expiration modes";
- `bucket_id` on `bucket_id`: TREE, not unique, only in vshard storage mode
  (`schema.ensure(name, storage, {vshard = true})`).

`storage = 'memory'` is memtx and `'disk'` is vinyl. An existing space is
used if it has no format or its format starts with these field names,
`token` included or not; otherwise creating the instance fails with "Space
'<name>' has an incompatible format: field <n> must be '<name>'".

Format v2 is v3 without `token`. `schema.ensure()` on a writable instance
upgrades a v2 space in place: it appends the nullable `token` field to the
format (`space:format()`). Tarantool checks the existing tuples once (all
pass: they end before the new field); the tuples are not rewritten. A
read-only instance uses a v2 space as it is and gets the new format by
replication. The engine never depends on the format: it reads field 11 of a
10-field tuple as false, and writing it appends it (`=` on the field after
the last), so every tuple length and every format work, also for tuples
that come with a bucket move from a storage not upgraded yet. A space is
upgraded by the next `ensure()` on a writable instance: the instance's
create, start or backend change, or the vshard storage's apply (which also
runs when the storage becomes writable). A local instance that becomes
writable keeps a v2 space as it is until then.

The meta space `_mcc_meta` (memtx, TREE primary key on a string) holds
key/value pairs. One of them is `{'flush:<space_name>', <flush time>}`. The
space is replicated, so `flush_all` survives a restart and a failover.

`schema.ensure()` returns the space, creating it (and `_mcc_meta`) on a
writable instance, or returns nil on a read-only instance where the space
has not arrived by replication yet.

`schema.flush_time(space_name)` is cached per space in `state.flush_cache`
(TX), so a batch doesn't read `_mcc_meta`. The cache is emptied:
- by the named trigger `memcached.flush_cache` on
  `box.space._mcc_meta.on_replace`, on any change of `_mcc_meta` (the
  engine's flush, another fiber, an application writing the row,
  replication, recovery): at once, and again from `box.on_commit` /
  `box.on_rollback` of that transaction, so a value read while the change is
  in flight can't outlive it;
- by the same trigger on `box.space._truncate.on_replace`, since `truncate`
  fires no `on_replace`;
- when `box.space._mcc_meta` is another object (dropped and created again).

The triggers (Tarantool 3 `trigger` module) exist before the space does,
survive drop and create, and a reload replaces them instead of adding more.
Without the `trigger` module there is no cache. So a flush is seen by the
first batch that starts after it is applied.

### Keys

- Classic commands: at most 250 bytes and no spaces. As in memcached, the
  line is split on spaces only, so control characters are fine (but not
  the line's `\n`, and a `\r` right before it ends the line).
- Meta commands: the key token follows the same rules. With the `b` flag
  it is base64 and is decoded, so a stored key may contain any byte; the
  classic commands can't address keys with spaces.

### Times

memcached semantics of `exptime` in requests:
- 0 means never;
- a negative value means already expired;
- up to 30 days (2592000 seconds) it is relative to now;
- above that it is an absolute unix time.

`engine` gets the raw value and converts it (`engine.abs_time`, -1 for "already
expired"). The clock is `fiber.time()`, read once per request.

`engine.stamp()` makes the `created` values and the flush times strictly
increasing (steps of 1 µs), so that an item stored right after an immediate
`flush_all` survives it within one event loop iteration.

### CAS

A CAS value never repeats for a key, across restarts and bucket moves. On
every store, `new_cas = max(old_cas + 1, counter())`. `counter()` is a
per-process counter (`state.cas_counter`). At its first use it is
initialized to the current time in microseconds and is then incremented by
one per call. CAS values are 64-bit: use `uint64_t` cdata (`0ULL`,
`tonumber64`). Never use Lua numbers for CAS or for `incr`/`decr` values,
since doubles lose precision above 2^53.

Exceptions, as in memcached:
- `md x` (`delete` with `clear_value`, without `invalidate`) keeps the old
  CAS for the empty tombstone (`process_mdelete_cmd` stores it with
  `ITEM_get_cas(it)`), so the old value and the tombstone share a CAS. Meta
  `E` still sets it, and `md x I` gets a new one.
- Meta `E` sets any CAS the client gives, on stores, vivifies, arithmetic and
  `md I`/`md x`.

A request CAS of 0 means "no CAS" (`do_store_item`: `CAS_NONE`;
`do_add_delta`: `*cas != 0`): `append`/`prepend` and `arith` skip the check,
and the `cas` command (`set` with a CAS) fails with `EXISTS`, `invalidate`
or not. `delete` and meta `mg C` compare it like any other value. `ms` with
`ME` (add) ignores a CAS, and `ms MR` with a CAS is the `cas` command.

### Expiration

An item is dead if either of these holds:
- `exptime ~= 0 and exptime <= now`;
- a flush time F exists, `F <= now` and `created <= F`.

Dead items are never returned. They are deleted lazily on access, unless the
instance is read-only or the access is a peek (`me`); the evictor deletes
the dead items it samples (counted as `reclaimed`, see "Eviction"). Besides
that, `expire.lua` runs a sweeper fiber in TX per instance, which only gives
the memory of the dead items back early: nothing depends on it, as in
memcached, which expires lazily and reclaims with its LRU crawler. How it
finds them depends on the indexes the space has, looked up on every round:
- **with the `exptime` index** (`expire.mode = 'index'`, and vinyl) it
  walks the index upwards from `exptime > 0` while `exptime <= now` and
  deletes in transactions of `expire.batch` keys (100 by default), yielding
  between them; after a flush it scans the whole space once (paginated
  through the `exptime` index) and deletes the items with `created <= F`;
  it sleeps until the next expiration, between 0.01 and 1 s;
- **without it** (`expire.mode = 'scan'`, memtx) it scans the primary HASH
  index with a cursor and deletes the dead items, expired or flushed, see
  "Expiration modes";
- a vinyl space without the index (one that has not arrived by
  replication yet) is left alone;
- it is woken up by a flush;
- it runs only while the instance is writable (it checks `box.info.ro` every
  second otherwise);
- its counters `expired` and `flushed` (and `checked`, the items the scan
  looked at) are in `inst:info()` and the metrics; a new sweeper that
  replaces one of the same space (`cfg()` of `expire`, a reload) keeps them;
- its own cancellation (Tarantool cancels every fiber at exit) is a normal
  stop, not an error.

#### Expiration modes

`expire.mode` (instance option, vshard storage space option, role option)
chooses whether the data space has the `exptime` index:

- **`'index'`**: the TREE index on `exptime`. Every write of a tuple
  updates it, also with exptime 0: a `replace` makes a new tuple, and so
  does the `box_update` of `last_access`/`fetched` on the first get after
  a set. That is about 1 µs of a 3.5 µs `replace` (`bench/micro.lua`,
  wal_mode none: 3.5-3.7 µs with it, 2.5-2.8 µs without; a 100 B set
  through the C connection loop 5.5-5.8 -> 4.5-4.8 µs), and the index
  takes memory per item and per read view. The sweeper deletes every
  expired item within about a second of its exptime.
- **`'scan'`**: no `exptime` index. The sweeper scans the primary HASH
  index with a cursor, a memtx iterator it keeps between its
  transactions, as Redis (since 6) scans its expires dict:
  - a transaction checks the next `expire.batch` items (fewer in a smaller
    space, none twice) through the box C API, with no Lua references,
    deletes the dead ones and releases them at once, so their memory goes
    when it commits (see "Prompt release" in "Eviction"). It doesn't yield
    before the commit, so what it found dead is still dead;
  - a cycle goes on with the next transaction, after a yield, while more
    than `SCAN_DEAD_RATIO` (10%) of the last one's items were dead, for at
    most `SCAN_BUDGET` (25 ms); the next cycle starts `SCAN_PERIOD` (0.1 s)
    later. So it takes at most about a fifth of TX while there is much to
    delete, and catches up with a mass expiration or a flush in a few
    cycles;
  - a cycle that finds nothing doubles the wait, up to 1 s; a new flush
    time resets it, and a delayed flush wakes it up when it comes;
  - a cycle checks at least `max(batch, len * wait / SCAN_PASS)` items
    (`wait`: the one before the cycle), so a pass over the whole space
    takes at most `SCAN_PASS` (600 s), and every dead item goes within that
    time even when the dead ones are too few to speed the scan up. A check
    costs about 0.57 µs (in-container probe, 1 M live items of 100 B), so
    10 M items cost about 1% of a core; a deletion, with its checks, about
    5 µs (wal_mode none, half of 1 M items expired);
  - the HASH iterator is a position in the hash table: writes between the
    transactions may make it skip or repeat a few items in a pass (the next
    pass gets them), a truncate, a drop or an index change ends the pass;
  - flushes need nothing more: the scan checks `created <= F` as it checks
    the exptime, with the same rates.

  Random samples (`box_index_random()`, as the evictor takes them) don't do
  for this: memtx returns the first tuple after a random slot of the hash
  table, which never shrinks, so after many deletes the tuples that follow
  another one in the table are seldom picked, and a few dead items stayed
  for many seconds in a test where 10% of the space was live.

  Only memtx can scan this way, so `'scan'` with `storage = 'disk'` is
  refused ("Bad value for option 'expire': mode 'scan' needs storage =
  'memory': vinyl keeps the exptime index"), and vinyl always has the
  index.

**The default is `'scan'` for memory and `'index'` for disk**: plain sets
stop paying for the index, the expiration semantics are the same, and
memcached itself gives no promptness guarantee for the memory of expired
items. `'index'` remains for workloads that need every expired item gone
within about a second regardless of the space size.

**Existing spaces.** The mode is applied to the space by `schema.ensure()`
on a writable instance, as the v2 -> v3 upgrade is: it creates the
`exptime` index for `'index'` when it is missing and drops it for `'scan'`
(memtx; logged). That happens at the instance's create and start, when
`cfg()` changes the mode of a running instance (a failure is logged, the
option is kept), and at every apply of a vshard storage, which also runs
when the storage becomes writable. A memtx index is built with yields; on
a big space that takes a while. A read-only instance never changes the
indexes: it uses what it has and gets the DDL of the master by
replication, and the sweeper looks the index up on every round, so a
promoted replica sweeps by what is there until its next `ensure()`. A
reload changes no index (the new code applies the mode at the next
`ensure()`), and the sweepers restart with a new cursor. Without a mode
(`schema.ensure(name, storage)`, `schema.create()`), an existing space is
left as it is and a new one gets the default of the storage. Instances
that share a space must agree on the mode, or each would change it for
its own: the role checks it; through the Lua API it is up to the caller.

**Counters.** `reclaimed` (the dead items the evictor found) keeps its
meaning in both modes, the sweeper's deletions are `expired` and `flushed`
in `inst:info()` and the metrics, as in `'index'` mode. memcached's
`expired_unfetched` and the `crawler_*` stats are not reported in either
mode, so `stats` is the same.

### Last access and fetched

With the instance option `track_access = true` (the default), a fetch (a get
that is not a peek and has no `u`) writes `last_access = now` and `fetched =
true` on the first fetch after a store, and then at most once per
`engine.LA_UPDATE_INTERVAL` (60 s), like memcached's `ITEM_UPDATE_INTERVAL`.
So the first get after every store is also a write. A store and `md x` set
`last_access` to now and reset `fetched`; so does an `incr`/`decr` that
writes a new item, while one in place keeps `fetched` and bumps
`last_access` as a fetch does, at most once per interval (see the
arithmetic in "Engine contract").

With `track_access = false` a get never writes the item for access tracking:
- `fetched` stays `false` (`mg h` is always `h0`, `me` says `fetch=no`), and
  `last_access` stays the time of the last store;
- `touch`/`gat` and meta `T` still set the TTL, and only the TTL;
- a vivify (`mg N`) still stores its stub, with `fetched = false`, and an
  `mg` that wins a token still writes it (see "Win tokens");
- otherwise the only write a get can still make is the lazy deletion of a
  dead item. So with no dead items and no wins, gets make no WAL writes and
  no replication traffic.

With the vshard backend the gets run on the storages, so the option is set
there, per space: `storage.cfg({spaces = {[<space>] = {track_access =
false}}})`, which `storage.exec()` passes into the engine `ctx`. The role
takes `track_access` from the instance config and passes it to the storage
side; the front-end on the router keeps the default. Instances that share a
vshard space must agree on it. Through the Lua API, `track_access = false`
on an instance with `backend = 'vshard'` is refused ("Option 'track_access'
= false is not supported by the 'vshard' backend: set it on the storages,
..."), because it would have no effect there.

### Eviction

`evict.lua` evicts items when memtx memory is near full, as memcached does
by default. It deletes items of the memcached memtx spaces whose instance
(or storage space) has `evict.enabled`, and nothing else.

- **Option.** `evict = {enabled = true, high = 0.95, low = 0.9, samples =
  8, batch = 100}`, merged key by key like `expire`. `high` and `low` are
  fractions of the memtx quota (`memtx_memory`), `0 < low < high <= 1`;
  `samples` is 1..64. With `storage = 'disk'` it is ignored: vinyl's limit
  is the disk, and nothing is evicted.
- **The memory signal** comes from `box.slab.info()`: `used = quota_used -
  arena_size + arena_used`, the bytes that memtx allocations take (tuples,
  index extents and large tuples). `quota_used` alone is useless: the slab
  arena never gives a slab back to the quota, so it stays at the high-water
  mark after deletes. `used` drops as soon as a tuple is freed. From
  `box.stat.memtx()`, `live = used - data.read_view - data.garbage -
  index.read_view` leaves out what read views hold (a checkpoint, a dump,
  `read_mode = 'snapshot'`): deleting more doesn't free that memory, it
  comes back when they close.
- **Approximated LRU.** memtx keeps no LRU order, so a round samples
  `samples` random tuples of the primary HASH index (`box_index_random()`)
  and deletes every dead one (expired or flushed; counted as `reclaimed`),
  then the one with the oldest `last_access` with `track_access`, or the
  oldest `created` without it (counted in `evictions`, and in
  `evicted_unfetched` when it was never fetched, or `evicted_active` when it
  was fetched within the last 60 s; both only with `track_access`).
- **The evictor** is one fiber per process, `mcc/evict`: the memory is
  shared by every space. Every 0.1 s it compares `used` with the `high` of
  each space. A space whose `high` is reached is in an episode until `live`
  is at or below its `low`. The evictor evicts the bytes between `live` and
  the largest gap below it in transactions of `batch` rounds, yielding
  between them; each transaction evicts from one space of the episode,
  picked with a probability proportional to its size (`space:bsize()`). So
  with the same options everywhere the evictions are spread in proportion to
  the size of the spaces. An empty space is left out, so the evictor never
  touches other spaces' memory. It runs only while the instance is writable;
  the deletes are replicated.
- **On demand.** A write that fails with a memtx OutOfMemory error
  (`Failed to allocate <n> bytes in slab allocator for memtx_tuple`; an
  index extent fails `in mempool for new slab`) calls `evict.direct()` from
  `run()` (see "Engine contract"), when its own space is evicted from. It
  evicts up to 8 items in one transaction from the evictable spaces, picked
  by size as above, preferring in each of up to 4 rounds a sample whose
  size is within `slab_alloc_factor` of the failed allocation: the slab
  allocator has a pool per size class, and a freed slot of another class
  doesn't help. The write then runs once more. When that fails too, the
  writes of that size class don't evict on demand for 1 s (the eviction
  didn't help: read views, or a size class without items). `direct_reclaims`
  counts the writes that evicted, as memcached's.
- **Prompt release.** Lua holds a reference to every tuple it reads, and
  memtx frees a deleted tuple only when the Lua GC collects that reference.
  The samples are read through the box C API (`box_index_random`,
  `box_tuple_field`), with no Lua reference, and the tuple that
  `space:delete()` returns is released at once (its finalizer is dropped and
  run by hand), so the memory drops when the eviction commits. A store of
  the Lua engine releases the same way the old tuple it read and the new one
  `replace()` returns: otherwise an evicted or overwritten value would stay
  allocated until a GC cycle, and the GC runs seldom when there is little
  garbage.
- **Owners.** `evict.start(owner, space_name, opts)` / `evict.stop(owner)`:
  an instance (its name; `inst:cfg()` follows `evict` and `track_access`)
  or a vshard storage space (`vshard:<space_name>`). Several owners of one
  space: the first by name gives the options. `evict.info()` returns the
  memory (`quota`, `used`, `held`, `fill`, `live`), whether the fiber runs
  and the spaces with their options and counters. The state is
  `state.evict` (owners, per-space counters, the fiber, the back-off).
- **Limits.** Sampling is not an exact LRU: an item is evicted when it is the
  oldest of its sample, so recently used items survive more often, not
  always. The last access is written at most once a minute. Slab
  calcification: a write of a size class that has no free slot and no items
  to evict (the memory is in slots of other classes, no whole slab is free)
  still fails, as in memcached without slab rebalancing; when writes fail
  below `high`, lower it. Tuples referenced from Lua elsewhere (the Lua
  engine's own reads and stores) are freed only by the Lua GC, so the
  evictor may evict a little more than needed, and on the Lua path a write
  of a new size class may fail where the C path's frees would have emptied a
  slab. A round costs about 2 µs per sample; each batch is one WAL write.
  The counters are per space, are not reset by `stats reset`, and start from
  zero on restart.

### Win tokens

The win token of meta `W`/`Z` (memcached's `ITEM_TOKEN_SENT`) is the `token`
field of the item: true when the token was handed out for the item's
current version.
- **A win** (see the `get` details below) sets it with an update in the
  transaction of the get, together with its last-access and TTL updates
  (one statement). So a win costs one WAL write; wins are rare (`mg` with
  `N`/`R`, or a stale item). A vivified stub (`mg N`) is stored with it.
- **A new version** writes it false: every store (`new_tuple()`), an
  arithmetic that writes a new item, `md I` and `md x` (in their update of
  the CAS). The exceptions keep the token as memcached keeps
  `ITEM_TOKEN_SENT`: the stale store (`ms I` with an older CAS, memcached's
  `CAS_STALE`) and an arithmetic in place, which also moves a token a
  read-only instance kept in memory into the field. A touch and a
  last-access update keep it. A deletion (lazy, by the sweeper, `md`)
  takes it away with the tuple.
- **Rollback.** A request whose transaction fails is rolled back with its
  token, so the engine has nothing to restore.
- **It moves with the item**: it is persisted, replicated, survives a
  restart and a failover, and is copied with the tuple on a vshard bucket
  move.
- **A win needs its write.** A get whose write fails is read again without
  its optional writes (see "Reads with failed side writes"), and that read
  takes no win: it answers without `W` and without `Z`. With memtx MVCC an
  autocommit read doesn't see a prepared change, so a get that may take a
  token (`win_token`) runs in `box.atomic` there: of two gets of a stale
  item during the WAL write of the first one's win, the second is aborted
  by the conflict and read again without a win. With `commit_wait =
  'submit'` or `'none'` every get is a read-committed transaction there
  (see "Commit wait"): the second get sees the first one's prepared win and
  answers `Z`, as without MVCC.
- **Read-only instances** can't write the field (a replica serving
  `read_mode` `ro`/`bro` reads, a master that went read-only). There a win
  is kept in memory: `state.tokens[space_name][key] = {cas, created}`, the
  version it was handed out for. A token is out when the field is true or
  such an entry matches the item's CAS and `created`; every new version
  changes one of them (`md x` keeps the CAS but stamps `created`), so no
  write has to drop an entry, and one that doesn't match, or whose item is
  gone, is dropped when seen. These tokens are per instance (per replica on
  read-only replicas), lost on restart, and still count after the instance
  becomes writable. The tokens of the master reach a replica in the items,
  so a replica answers `Z` for them. On a writable instance the field is
  authoritative: its wins always go to the field. An entry that is a bare
  CAS (kept through a hot reload from the code before the field) counts for
  that CAS.

## Engine contract (TX thread only)

```lua
engine.exec(ctx, reqs) -> resps, held?      -- held: see "The C data path"
engine.stats(ctx) -> {curr_items, bytes}     -- space:len(), space:bsize()
engine.batch(ctx, input, output, max_commands, max_item_size, held?)
    -> consumed, commands, gets, hits, stored, held, <counters> | nil
                                             -- see "The C data path"
```

- `ctx` is `{space_name = <string>, bucket_id = <number|nil>, track_access =
  <boolean|nil>, commit_wait = <'complete'|'submit'|'none'|nil>}`. The
  engine writes `bucket_id` (0 if nil) into the tuples it creates.
  `track_access = false` makes every get behave as with `bump_la = false`
  (see "Last access and fetched"). `commit_wait` is the commit wait mode of
  the requests' transactions, `'complete'` if nil (see "Commit wait").
- `reqs` and `resps` are arrays of plain tables that msgpack can encode, since
  they cross threads and the network. One exception: a request marked
  `fast = 'get'|'gets'` (see "The C data path") may get a string, its hit
  already encoded as a text `VALUE` block. Responses are read-only: those
  with nothing but a status (`MISS`, `NOT_FOUND`, `NOT_STORED`, `EXISTS`,
  `DELETED`) are shared tables. `exec()` keeps no reference to `reqs`
  after it returns.
- Each request runs in its own `box.atomic`, except a plain get (no touch,
  no vivify) on memtx: it makes at most one write (the lazy deletion, or the
  update of the last access and a win token) with no yield between its read
  and that write, so the write is one autocommit statement. vinyl gets keep
  `box.atomic`: a vinyl read yields, and the transaction turns a store in
  between into a conflict instead of a lost item. So do the gets that may
  take a win token under memtx MVCC (see "Win tokens"). A plain set on the
  C data path is the other exception (see there). With `commit_wait =
  'submit'` or `'none'` there are no exceptions: every request is a
  transaction, committed with `box.commit({wait = <mode>})` (see "Commit
  wait").
- A failed commit (WAL error, read-only instance, conflict, a full WAL queue
  with `'none'`) gives `{status = 'SERVER_ERROR', message = <string>}` for
  that request; the others are not affected. With the default
  `commit_wait = 'complete'` every write waits for its own WAL commit, so a
  pipelined batch of writes gets no group commit (but the C batch's, see
  "The C data path", "The group").
- **Out of memory.** A request that fails with a memtx OutOfMemory error
  (not a plain get, which reads without its side writes instead) is rolled
  back, evicts (`evict.direct()`, see "Eviction") when its space is evicted
  from, and runs once more, with the same commit wait. Otherwise, or when it
  fails again, it answers as memcached does: a store `SERVER_ERROR out of
  memory storing object`, with `no_memory = true` for the
  `store_no_memory` counter, and as memcached's `process_update_cmd_start`
  a `set` (no CAS) then deletes the old value; any other request
  `SERVER_ERROR out of memory`.
- **Reads with failed side writes.** A `get` without touch or vivify whose
  optional side writes (last access/fetched, lazy deletion, a win token)
  fail to commit is run again without them, and without a win. This is what
  makes reads work on read-only replicas. On a read-only instance the side
  writes are skipped from the start, and a win is kept in memory (see "Win
  tokens").
- **Read-only state.** It is taken once per batch. A `box.status` watcher
  keeps `state.ro_cache.is_ro`, and only "writable" is taken from it;
  anything else asks `box.info.ro`. When a request fails and `box.info.ro`
  says the instance is read-only after all (a late watcher), the request and
  the rest of the batch run again as on a read-only instance, so the answers
  are those of a per-batch `box.info.ro`.
- The flush time is read once per batch (`schema.flush_time()`, cached), and
  a `flush` in the batch updates it for the requests after it.
- A missing space gives `SERVER_ERROR Space '<name>' does not exist` for
  every request; a request without a string key or with an unknown op gives
  a `SERVER_ERROR` for that request.
- Helpers: `u64`, `u64str`, `parse_u64`, `abs_time`, `next_cas`, `stamp`,
  `is_dead`, `flush_time`, `error_message`, and the constants
  `MAX_RELATIVE_TIME`, `LA_UPDATE_INTERVAL`, `NON_NUMERIC`.

The meta protocol is a superset of the classic semantics, so requests follow
it. Classic commands are mapped to these ops by `proto/text.lua`.

| op | request fields | response |
|----|----------------|----------|
| `get` | `key`, `touch_ttl?` (raw exptime, sets a new TTL), `vivify_ttl?` (meta N), `recache_ttl?` (meta R), `bump_la` (bool, default true), `no_lru_bump?`, `peek?` (bool: no side effects at all, for `me`), `new_cas?` (uint64, meta E, for a vivify), `win_token?` (bool, meta `mg`: may take a win token), `hold?` (bool: a hit holds the item for the rest of the batch, see `arith`) | `status = 'HIT'\|'MISS'`, `item?`, `won?` (bool), `stale?` (bool), `already_won?` (bool), `vivified?` (bool) |
| `store` | `key`, `value`, `flags`, `exptime` (raw), `mode = 'set'\|'add'\|'replace'\|'append'\|'prepend'`, `cas?` (uint64), `invalidate?` (meta I), `vivify_ttl?` (meta N with append/prepend), `new_cas?` (uint64, meta E), `max_size?` (append/prepend: the limit of the joined item) | `status = 'STORED'\|'NOT_STORED'\|'EXISTS'\|'NOT_FOUND'`, `cas?`, `size?` (value length after the store) |
| `delete` | `key`, `cas?`, `invalidate?` (mark stale instead of deleting), `ttl?` (with invalidate or clear_value), `clear_value?` (meta x: keep the item with an empty value), `flags?` (with clear_value), `new_cas?` | `status = 'DELETED'\|'NOT_FOUND'\|'EXISTS'`, `cas?` |
| `arith` | `key`, `incr` (bool), `delta` (uint64), `initial?` (uint64, meta J), `vivify_ttl?` (meta N), `ttl?` (meta T), `cas?`, `new_cas?` | `status = 'STORED'\|'NOT_FOUND'\|'EXISTS'\|'CLIENT_ERROR'`, `value?` (uint64), `cas?`, `exptime?` (absolute) |
| `flush` | `delay` (raw: 0 or negative = now, up to 30 days relative, above that absolute) | `status = 'OK'` |

Any request may carry `fast`, set by `proto/text.lua` only (see "The C
data path"): `'get'` / `'gets'` on the keys of a classic `get` / `gets`,
`'set'` on a classic `set`. It doesn't change the semantics. Any request
may also carry `release` (set by the server on the first engine request
after an `mn`): the reads before it no longer hold their items (`hold`, see
the arithmetic below); and `held` (set by the server on the first engine
request after a C batch, see "The C data path"): a set `{key = true}` of
the items that the reads of the C batch hold, as if they had run in the same
engine call. `exec()` then tracks the holds of its requests in that set and
returns it, as it is after the last request, as its second result (also an
empty set the server passes in when a C batch comes after the call).

An `item` is `{key, value, flags, exptime, cas, created, last_access,
fetched, stale}`, either as a map or as an array in space-format order (the
encoders accept both). `exptime` is absolute; the protocol layer turns it
into a TTL where needed.

A `get` has these details:
- `item.fetched` and `item.last_access` are the values *before* this access,
  as the meta `h`/`l` flags need.
- A vivified miss (`vivify_ttl`) returns `HIT` with `won = true` and an empty
  item. When the TTL has already passed (`N-1`) the stub is not stored, and
  the caller still wins. Otherwise the stub is stored with `fetched = true`
  (the vivifying get counts as the first fetch) unless `bump_la = false`, and
  it takes the win token (the stub's `token` is true).
- Only a get with `win_token` (meta `mg`) takes the win token of a stale item
  or of a recache (`R`: the item's TTL is below the given one), and reports
  `already_won` when the token is out. The classic reads (`get`, `gets`,
  `gat`, `gats`, `touch`) leave tokens alone, as memcached's
  `process_get_cmd` does.
- A touch into the past deletes the item and still answers `HIT`.
- With `peek = true` nothing changes: no last-access bump, no fetched flag, no
  TTL touch, no vivify, no lazy deletion and no win token taken.

A `store` has these details:
- `cas` with `set`/`replace` on a missing item is `NOT_FOUND`; `cas` with
  `set`/`replace` that differs is `EXISTS`, except with `invalidate` and a
  CAS older than the item's: then the value is stored, marked stale, with the
  old TTL and the win token kept.
- `append`/`prepend` keep the old flags and exptime. On a miss they answer
  `NOT_STORED`, or store the value with the `vivify_ttl` TTL. When the joined
  item doesn't fit the item size limit (`max_size`, which the parsers set to
  `max_item_size`; `engine.item_size()`), they answer `NOT_STORED` (meta `NS`)
  and the old item stays, as memcached's `do_store_item`.
- A store whose exptime has passed answers `STORED` and leaves no item.
- Any store drops the win token (except the stale store above).

A `delete` with `invalidate` marks the item stale, gives it a new CAS
(`new_cas` if given) and optionally a new TTL. A `delete` with `clear_value`
(meta `x`) replaces the item with a new empty one, as memcached does: it is
not fetched, its last access is now, it is stale only with `invalidate`,
its flags are `flags` (0 by default), and it has no win token. Its CAS
follows the exception in "CAS". A `ttl` in the past deletes the item. The
meta parser always sends a `ttl` with `x`: `md x` without `T` gives the new
item no TTL (0), as memcached's `item_alloc(..., of.exptime)` does; without
`ttl` the engine keeps the old exptime.

The arithmetic follows memcached (`do_add_delta`):
- an empty value is refused first, before the CAS check;
- the value is read as `safe_strtoull()` reads it: leading whitespace, an
  optional sign, then decimal digits followed by whitespace, a NUL or the end
  of the value, with anything after that ignored (`"12 ab"` is 12). A `-`
  wraps, and a result that is negative as an int64 is refused;
- otherwise the result is `CLIENT_ERROR`, "cannot increment or decrement
  non-numeric value";
- `incr` wraps around 2^64;
- `decr` stops at 0;
- a result no longer than the old value is padded with spaces to the old
  length, as memcached writes it in place (`res + 2 <= it->nbytes`): `decr`
  of `100` by 1 stores `"99 "`; a longer result is stored as is. memcached
  also needs no other reference to the item (`refcount == 2`): a reply not
  yet written holds one. So, as there, a result is not padded when a read
  earlier in the same batch returned the item (a classic
  `get`/`gets`/`gat`/`gats` or an `mg` hit, the requests with `hold`) and
  no store, delete or `mn` came in between (the server sets `release` on
  the first request after an `mn`, which writes the replies out). The
  reply is the number itself. The item keeps its flags and exptime (unless
  `ttl`) and gets a new CAS. Written in place, it also keeps `fetched`, the
  stale marker and the win token, and `last_access` is bumped as
  `do_item_update()` does (with `track_access`, at most once per
  `LA_UPDATE_INTERVAL`); as a new item (a longer result, or held) it is not
  fetched, not stale, its win token is not out and `last_access` is now,
  as memcached's `do_item_alloc()` gives it;
- on a miss with `vivify_ttl` the item is created with `initial` (0 by
  default) and answers `STORED`.

## The C data path

`memcached/datapath.c`, the Lua C module `memcached.datapath`, runs the most
common requests in C on a memtx space: the batch serves the classic text
storage, retrieval, delete, touch and arithmetic commands and the meta `mg`,
`ms`, `md`, `ma` and `mn` straight from a connection's input, and two
per-request functions serve the classic `get`, `gets` and `set` that reach
the engine as requests. The benchmark (`bench/`) found box access and the
reply encoding from Lua to be the one place where C pays off clearly.

- **Per request.** The text parser marks the keys of a `get` (`fast =
  'get'`) and of a `gets` (`'gets'`), and a `set` without CAS (`'set'`);
  `engine.exec()` runs them with `datapath.get()` / `datapath.set()`. The
  mark travels with the request to the engine: in TX, to TX from an
  application thread, and to a vshard storage. In TX the batch serves most
  of them before they are parsed (below).
- **The batch.** `batch()` serves the commands at the head of a
  connection's input, before the Lua front-end parses them: `server.lua`
  `conn_step()` calls `backend:batch()`, which the `local` backend has
  (`engine.batch()`) and the `thread` backend, which ships the input bytes
  to TX for it (the raw batch, see "Application threads"); not the vshard
  backend and not the `read_mode = 'snapshot'` wrapper. It is called when
  the input starts with the name of a command it serves (`server.c_kind()`).
  `engine.batch()` returns nil, and the Lua front-end does everything, when
  the C path is off or not built, the space is not memtx, or `commit_wait`
  is not `'complete'`. The C code takes the input bytes (an ibuf, read and
  not consumed, or a string: the raw batch of the application threads),
  parses the commands at their head as `proto/text.lua` and
  `proto/meta.lua` do, runs them as `engine.lua` does and appends their
  replies to an output buffer (an ibuf), as the encoders write them. It
  returns the bytes consumed, the commands served, the counters and the
  items its reads hold (see "Holds"). `mcc_dp_batch_run()` in
  `memcached/datapath.h` is the same batch as a C API, for C code built
  into the module: the C connection loop of TX runs it on the input it
  reads, and returns to `conn_step()` only where the Lua batch takes over
  (see "The C connection loop").
- **What the batch serves.** Every form below in the replies and side
  effects of the Lua path; numbers are read as the Lua parsers read them,
  and only the forms whose reading every parser agrees on are taken.
  - `get`, `gets` with any number of keys (every key checked first);
    `gat`, `gats` likewise, with a TTL that doesn't expire the items at
    once.
  - `set`, `add`, `replace`, `append`, `prepend` `<key> <flags> <exptime>
    <bytes> [noreply]` and `cas ... <cas unique> [noreply]`: the flags,
    exptime and length digits only (the flags up to 2^32-1, the exptime up
    to 2^31-1, the length up to 11 digits, `mcmc_tokto32`), the data block
    complete and followed by `\r\n`, within `max_item_size` (and for
    append and prepend the joined item too, else `NOT_STORED` as in Lua),
    every reply (`STORED`, `NOT_STORED`, `EXISTS`, `NOT_FOUND`), an item
    that expires at once included.
  - `delete <key> [noreply]`, `touch <key> <exptime> [noreply]`, `incr` /
    `decr <key> <delta> [noreply]`: every reply but the `CLIENT_ERROR` of
    a value that is empty or not a number.
  - `mg <key> <flags>*` with any flag but `N` (vivify) and `R` (recache):
    `v k f c s t h l q O u T b C`, and `E` and the others that change
    nothing without `N`; not when a win token is involved: the item is
    stale, its token is out (`t[TOKEN]`) or a token is kept in memory for
    it (`state.tokens`), where the reply has `Z`, `X` or `W`.
  - `ms <key> <datalen> <flags>*` in every mode (`MS MA MP ME MR`), with
    `T F C q O k c s b` and `I` (the stale store of an older CAS); not
    with `E` (an explicit CAS) or `N` with append/prepend.
  - `md <key> <flags>*` with `q O k b C` (and `T F E`, which change
    nothing then); not with `I` or `x`.
  - `ma <key> <flags>*` with `M D C T q v O k t c b` (and `J`, which
    changes nothing without `N`); not with `N` or `E`, nor on a value that
    is empty or not a number.
  - `mn`.
  It stops at the first command it doesn't serve: any other command
  (`me`, `flush_all`, `stats`, `version`, `verbosity`, `quit`,
  `lru_crawler`), a form or flag above left to Lua, every error (a bad
  token, a value too large, a bad data chunk, a key too long, a meta flag
  error), a line without `\n` (it may be too long), an incomplete command,
  a write when writes can't be served (read-only, memtx MVCC: the reads
  are served, their optional writes are not made read-only), a failed box
  call, the batch limit. That command and the rest are the Lua
  front-end's, which answers them as before: nothing of that command has
  changed, but for the side writes of the first keys of a multi-key get,
  which the Lua engine then finds done (the same replies and items).
- **Back to the C batch.** With the `local` backend (`batch_reentry`), the
  Lua batch that runs from a command the C batch didn't serve stops at the
  next command the C batch serves (`parse_batch()`, after one command at
  least), and the C batch goes on from there, in the same `conn_step()`:
  the C batch, the Lua batch and the C batch again take turns, up to
  `MAX_BATCH` commands in all. The replies go to the connection's output
  buffer in their order and out with one write, so a turn costs a
  function call, not a syscall. A command kind (`get`, `mg`, ...) the C
  batch didn't serve in this `conn_step()` stays with Lua for the rest of
  it, so that commands it leaves to Lua don't make it try every one; and a
  turn of the C batch after a Lua one that serves fewer than
  `REENTRY_MIN` (4) commands leaves the rest of the batch to Lua (a turn
  costs about as much as a few commands in a Lua batch). The
  Lua batch copies `PARSE_CHUNK` (4 KiB) of the input at first, all of it
  when a command goes past that. A barrier (`flush_all`, `stats`), `mn`,
  `quit`, a stream (`lru_crawler`) and a close work as in one Lua batch:
  `quit`, a stream and a close end the batch. The `thread` backend goes
  back to TX only for the head of the input (a turn would be a call to TX).
- **Batch state.** One call has one flush time, read-only flag and
  `track_access`, as an engine call. The clock is `fiber.time()` at the
  call, read again (through `fiber.time`, the function passed in) after a
  write or a commit that yielded (`fiber_csw()` moved): the Lua engine
  reads it per request, and only a yield changes it. The meta replies give
  the TTL and the last access by that clock, at the command; the Lua
  encoder reads the clock when it encodes the batch. A commit waits for
  the WAL with `'complete'`, and other fibers run then: each write reads
  `state.cas_counter` and `state.stamp_last` from the state table and
  writes them back before its write (`counter()`, `stamp()`), so every
  fiber takes values of its own; a write that fails before it yields gives
  them back, so that the Lua engine that runs the command takes the same
  ones (a failed commit: see "The group"). A miss drops a win token kept
  in memory (`state.tokens`, as `forget_win()`).
- **Holds.** The reads of the batch (the classic retrievals and `mg` hits)
  hold their items, and a store, a delete or an arithmetic of the batch
  releases them, `mn` all of them, as in a Lua batch (see the arithmetic in
  "Engine contract"); an arithmetic in place pads the number only when no
  read holds the item. `batch()` takes the items held before it (`held`,
  a set `{key = true}`) and, when it stops before the end of its input,
  returns those held after its commands. The server passes them with the
  first engine request of the Lua batch after it (the request's `held`),
  unless a barrier comes first: a barrier is an engine call of its own,
  where a Lua batch's holds don't go either. When the C batch comes next,
  `process()` gives each engine call a held set, which `engine.exec()`
  fills and returns as its second result, and the server passes the one of
  the last call (none after a barrier or an `mn`) to the C batch. A
  pipelined `lru_crawler` after other commands of the batch fails as after
  any command.
- **Counters.** `batch()` returns, after the bytes and the commands:
  `gets` (`cmd_get`: the keys of the retrievals and `mg`), `hits`
  (`get_hits`), `stored` (the stores that stored: `cmd_set` and
  `total_items`), `held`, then `unstored` (the stores that didn't:
  `cmd_set`), `touches` (`cmd_touch`: `touch`, `gat`, `gats`, `mg T`),
  `touch_hits`, `delete_hits`, `delete_misses`, `incr_hits`,
  `incr_misses`, `decr_hits`, `decr_misses`, `cas_hits`, `cas_misses`,
  `cas_badval` and `meta` (`cmd_meta`); the ones from `unstored` on only
  when one of them is not 0 (fewer results to pass on for the common get
  and set), nil meaning 0. `server.lua` adds them once per
  call as `server.account()` would count the commands (the misses are the
  differences); the C batch serves no error, so the error counters don't
  move.
- **Which batches.** `engine.exec()` uses the per-request C path when the
  module is available and switched on, and the space is memtx; vinyl keeps
  the Lua path. A `set` also keeps it on a read-only instance, for an item
  that expires at once, and with memtx MVCC (`memtx_use_mvcc_engine`):
  there the Lua path's transaction around the read and the write matters.
  For the same reason the batch serves no write there (but the reads, as
  the classic get).
- **Semantics.** Those of `ops.get`, `ops.store`, `ops.delete` and
  `ops.arith` for these requests. What needs the Lua state is passed in:
  the clock (`fiber.time()`), the CAS counter value (`counter()`; the C
  code takes the larger of it and the old live item's CAS + 1, as
  `next_cas()`), the created stamp (`stamp()`), the win tokens kept in
  memory, the flush time, the read-only flag and `track_access`. The C
  code takes no win token, and its writes write the token field as the Lua
  ones (`false` for a new item, kept by a stale store and an arithmetic in
  place). It decodes the tuples of format v2 and v3 (10 or 11 fields) and
  writes 11. Numbers are written as Tarantool's Lua serializer
  writes them (integral values as integers), so a tuple is byte for byte
  the one the Lua engine stores.
- **Responses.** A get hit of `datapath.get()` is a string, `VALUE <key>
  <flags> <bytes>[ <cas>]\r\n<data>\r\n`, which `proto/text.lua` copies
  into the reply; a miss is a shared `{status = 'MISS'}`; a set answers as
  `ops.store`. The counters (`server.account()`) take a string for a hit.
  The vshard backend passes strings through. The batch writes the replies
  itself, byte for byte those of the text and meta encoders.
- **Writes and fallback.** Each write is one box statement: the lazy
  deletion, the touch and the last-access update of a get, the replace of a
  store (over a live or a dead old item: the final state is that of
  `ops.store`'s lazy deletion and replace, in one statement), the delete,
  the update of an arithmetic. The per-request calls make them autocommit
  statements; the batch puts the writes of consecutive commands into one
  transaction (the group, below). A command whose reply depends on more
  than its own item is left to Lua before anything is written. When
  anything fails in C (a write, an unexpected tuple), nothing of the
  command has changed, and the request goes through `run()` as if C had
  never been tried. So the failures and their replies are the Lua ones: a
  get without its optional write, the read-only rerun, a `SERVER_ERROR`.
  A set whose write failed is tried once more there, so a transient
  failure may still store it. With `commit_wait = 'submit'` or `'none'`
  the C call runs in a transaction that `engine.lua` opens and commits
  around it (see "Commit wait"); a failed commit is handled the same way,
  after a `box.rollback()`.
- **The group.** The batch (`mcc_dp_batch_run()`, so both `batch()` and
  the C connection loop) writes in one box transaction for consecutive
  commands instead of one autocommit statement per write: one journal
  entry, one WAL round trip and one fiber wakeup per group, where each
  write used to wait for the WAL on its own (see "Commit wait").
  - *Bounds.* The group opens at the first write of the batch
    (`box_txn_begin()`) and is committed before the batch returns, so
    before any reply of its commands goes out (the replies are in the
    output buffer until the server writes them after the batch) and
    before Lua runs anything: at the end of the input, `MAX_BATCH`, or the
    first command the batch doesn't serve or can't finish. A command that
    is the last of the input writes autocommit statements, as before (a
    single write gains nothing from a transaction). A group that reaches
    1024 writes or 1 MiB of values is committed after that command, and
    the next write opens the next one: it bounds the memory the
    transaction holds (the redo records, the replaced tuples) and the size
    of a journal entry. An `mn` doesn't end it: it only releases the
    holds. No group when the batch serves no writes (read-only, memtx
    MVCC), when win tokens are kept in memory for the space (`forget_win()`
    changes Lua state that a rollback wouldn't give back: those batches
    write autocommit statements, as before), or when a transaction is
    already active.
  - *Inside.* memtx without MVCC: the reads of the group see its writes
    at once, and nothing in a batch yields but a commit (a yield in a
    memtx transaction aborts it, and its next statement or its commit
    fails, as below), so the group is atomic for the other fibers too.
  - *A failed write* (memtx out of memory, any box error of a statement)
    is rolled back by box as a statement, the transaction stays: the
    command gives back the CAS value and the stamp it took (no yield), is
    left to Lua as before, and the batch commits the commands before it
    and stops there. So an out of memory goes to the Lua engine after the
    commit, which frees what the group replaced, and the Lua engine evicts
    (`evict.direct()`) and runs the command again, as it did after a
    failed autocommit statement.
  - *A failed commit* (a WAL error, a cascading rollback, the rollback of
    a synchronous transaction ahead of it in the queue) rolls the whole
    group back. The batch then goes back to the state before the group's
    first command: the input, the replies, the counters, the holds; the
    CAS counter and the created stamp too, unless the commit yielded
    (other fibers may have taken the values after them). It stops there,
    so those commands and the rest are the Lua front-end's, which serves
    each in its own transaction: each command answers as it would have
    after its own failed autocommit write (which went to Lua too), from
    the state before the group: a `SERVER_ERROR` for a write that fails
    again, a read without its optional write, a write that now succeeds.
    A batch of 0 commands marks the command kind refused for the rest of
    the `conn_step()`, as any other, so a WAL that keeps failing costs one
    failed group per kind. The CAS values a failed group took after a
    yield are lost, as those of a failed autocommit write that yielded.
  - *What other fibers see.* The writes of a group are made at once and
    the group's commit waits for the WAL with all of them applied; without
    MVCC every fiber sees them meanwhile (dirty reads, as of an autocommit
    write waiting for the WAL). What changes: another fiber can no longer
    run between two writes of one batch (it used to, at each WAL wait), it
    sees the whole group or none of it, and a failed WAL write takes back
    the whole group instead of the writes from the failed one on. The
    replies are unchanged: a command's reply still goes out only after its
    write is committed (or after Lua answered it).
  - *Measured* (in the container, one TX thread, 100 B sets, `wal_mode =
    'write'`, CPU and wall time per set): `engine.batch()` of 16 sets,
    one fiber: 31-37 -> 5-6 µs wall, 17-20 -> 3.1-4.0 µs CPU; over socket
    pairs with the clients in the same thread (`bench/` style, wall time):
    one connection at pipeline 16 34-38 -> 7.2-8.2 µs, 16 connections at
    pipeline 16 7.5-9.6 -> 4.8-5.5 µs, pipeline 1 unchanged (the last
    command of an input writes autocommit statements). With `wal_mode =
    'none'` `engine.batch()` of 16 sets takes 2.1-2.4 -> 1.8-2.0 µs CPU
    per set; over the sockets the difference is within the noise.
- **Availability.** `engine.lua` requires the module once (`pcall`), and
  checks its `ABI` against `engine.DATAPATH_ABI`. Not built (a source tree
  tt didn't build): a warning, and the Lua engine serves everything. Another
  ABI (new Lua code after a reload over an old `.so`): an error in the log,
  and the same.
- **The switch.** `mcc.datapath(false)` sends every request through the Lua
  engine, `mcc.datapath(true)` back; `mcc.datapath()` tells `{available,
  enabled}`. `MEMCACHED_DATAPATH=lua` in the environment switches it off from
  the start. It is `state.datapath_off` in TX, so it survives a reload. The
  answers are the same either way. `mcc.datapath_loop()` is the switch of
  the C connection loop alone.
- **Hot reload.** A reload requires the module again; the `.so` stays loaded
  (`dlopen` keeps it), so a change of the C code needs a restart. The
  connections in the C connection loop take the new Lua code at their next
  batch (see "The C connection loop").
- **Build.** tt builds it (`tt package build`, `tt test`, `tt package
  pack`): the `lua-c` backend of the component `memcached` in
  `app.manifest.toml` compiles it against Tarantool's `module.h` into
  `.rocks/lib/tarantool/memcached/datapath.so`, which the binary rock holds
  as `lib/memcached/datapath.so`. The tests run the Lua sources of the tree
  and find it in `.rocks/`. It uses only the box C API and the Lua C API: no
  FFI declarations, which a reload couldn't repeat.
- **Tests.** `test/integration/datapath_test.lua` runs scripts with the C
  path on and off, from the same state and with a frozen clock, and compares
  the replies and the stored tuples byte for byte, the batch's included
  (every command and flag it serves and those it leaves to Lua, pipelines
  that mix them and go back and forth between the C and the Lua batch,
  commands split across reads and across the part a Lua batch copies,
  noreply and `q`, errors, dead and flushed items, read-only, `track_access
  = false`, holds and padding, failed writes, `MAX_BATCH`, the counters;
  the groups: their transactions counted per path, every kind of write and
  the reads of it in one group, CAS values and holds across a group, the
  limits, a failed write in a group, connections writing at once with the
  WAL); `group_test.lua` makes the commit of a group fail in a child
  server (a synchronous transaction ahead of it times out; the WAL error
  injections are compiled out of release builds) and fills the memory
  with groups without eviction; `vshard.test_datapath` checks the strings
  through a router.

## The C connection loop

`memcached/cloop.c`, built into the `memcached.datapath` module on the
`datapath.h` API, is the connection loop of the TX thread in C for the
connections whose commands the C batch serves. A connection fiber of a
server with the `local` backend calls it (`server.lua` `conn_step()` ->
`backend:loop()` -> `engine.loop()` -> `datapath.loop()`) instead of a step
of the Lua loop. It reads the socket into the connection's input buffer,
runs the C batch (`mcc_dp_batch_run()`) on it, writes the replies from the
output buffer, and repeats, for as long as the batch serves all the input.
Everything else stays with the Lua loop: application threads (the `thread`
backend), the vshard backend, `read_mode = 'snapshot'`, vinyl, a
`commit_wait` mode, the data path off or not built.

- **Why.** At pipeline 1 the C batch costs a few hundred ns per request;
  around it, the Lua loop (its wait, read and step bookkeeping, the call
  of `batch()` with its arguments and results, the counters, the write)
  cost about as much as the batch again, and its allocations add GC work.
  The kernel (the read, the write, epoll) is most of the time per request
  either way.
- **The loop.** As `read_more()`, `wait_input()` and `write_obuf()`: after
  the replies of a batch that took all the input, wait for readability
  (`coio_wait()`, which yields) before the next read; a read that would
  block (EAGAIN, EINTR) waits too; a write that would block waits for the
  socket to be writable, in `conn_mwrite`. So the loop yields once per
  batch as the Lua loop does, and serves up to `MAX_BATCH` commands of an
  input before the next batch, without a read in between. Reads take the
  room of the buffer, at least `READ_SIZE`.
- **Returns to Lua** with a reason, whenever Lua must act:
  - `LOOP_STOP`: the batch stopped before the end of the input, at a
    command it doesn't serve or an incomplete one. The replies of the
    commands it served are in the output buffer, not written, with the
    number of those commands and the items their reads hold (`held`).
    `conn_step()` goes on with the same batch from there, as after the
    first turn of the C batch (`turn()`, see "The C data path"): the Lua
    batch, the C batch again (`batch()`), and one write at the end. A
    batch of 0 commands marks the kind of the command at the head refused
    for the batch, as `turn()` does. The holds and `conn.last_cmd` are
    those of `conn_step()`.
  - `LOOP_CLOSE`: end of input, a socket error, or the fiber cancelled
    while it waits for a write (a close: `close_connections()` shuts the
    socket down and cancels the fiber).
  - `LOOP_WAKE`: a wait for input ended without input: its timeout (the
    idle timeout), `fiber:wakeup()` or a cancel. Lua does what
    `wait_input()` does then: close a closed connection, kick an idle one
    (`idle_kicks`), `fiber.testcancel()`, else call the loop again.
  - `LOOP_INPUTS`: the inputs of the batch it took at the call may be out
    of date. Checked before each batch, after the read: `state.dp_gen`
    (below) or `box_schema_version()` moved.
  - `LOOP_KEEP`: a batch had more than `BUFFER_KEEP` bytes of input or
    output, or a buffer has more room than that: Lua drops the buffers it
    would drop (the C API can't), after the replies are written.
- **Inputs.** Those of `engine.batch()`, taken once per call: the space,
  `max_item_size`, the flush time, `track_access`, memtx MVCC, the bucket
  id, the state table, the win tokens kept in memory; and the
  connection's idle timeout (0 for unix sockets), `conn.replied`,
  `conn.last_cmd`. Per batch: the read-only flag (`box_is_ro()`, which is
  what `box.info.ro` reads, so the loop needs no `box.status` watcher) and
  the clock (`fiber.time()`, looked up in the fiber module and called per
  batch, as the engine reads it: the tests and the compat shim replace
  it, also while a connection is in the loop). Whatever may change the others bumps
  `state.dp_gen` (`engine.inputs_changed()`), and a loop returns
  `LOOP_INPUTS` at its next batch: a change of the meta space or a
  truncate (the flush cache triggers, at once and at the commit or
  rollback), a win token kept in memory (`remember_win()`), `srv:cfg()`
  (`max_item_size`, `idle_timeout`, the backend), `inst:cfg()` when it
  makes a new backend (`track_access`, `commit_wait`, ...),
  `mcc.datapath()` and `mcc.datapath_loop()`, a hot reload; a DDL moves
  the schema version. One batch has fixed inputs, as an `engine.batch()`
  call. Without the flush cache triggers (`schema.flush_tracked()`) the
  loop doesn't run.
- **Waking.** A loop waiting for a command is in `conn_read` (or
  `conn_new_cmd`), as the Lua loop, so `srv:cfg()` wakes it when
  `idle_timeout` changes, and it returns `LOOP_WAKE` and takes the new
  value. The other changes need no wakeup: they apply from the next batch,
  which is checked after the read.
- **Stats.** The loop counts the commands of its batches (the counters of
  `batch()`, see "The C data path") and the bytes it reads and writes into
  a block of C counters per server (`srv.dp_counters`, a userdata of the
  module). `server.sync_stats(srv)` moves them into `srv.stats` (adding
  them as `c_batch()` does), and every reader of `srv.stats` calls it
  first: `stats`, `srv:info()` (and `inst:info()`), `stats reset`, the
  metrics. So a `stats` from another connection sees the counters of every
  batch done, as memcached adds up the counters of its threads on demand.
  `conn.state` and `conn.last_cmd` are written into the connection's
  table when they change (`conn_parse_cmd` before a batch, `conn_read`
  before a wait, `conn_mwrite` around a blocked write, `last_cmd` after the
  replies of a batch), so `stats conns`, `srv:cfg()` and the idle timeout
  see them as with the Lua loop. `datapath.served(counters)` gives the
  running totals of the commands, keys and stores the loops served, for
  the tests.
- **Hot reload.** The loop runs no Lua code of the module,
  and a reload bumps `state.dp_gen`: a connection in the loop returns at
  its next batch, and its fiber's next step is the new `conn_step()`. A
  connection that waits for a command stays in the old frames until its
  next input, as with the Lua loop. The `.so` stays loaded: a change of
  `cloop.c` needs a restart. `engine.loop()` looks the function up
  (`datapath.loop`), so new Lua code over an older module without it
  runs the Lua loop.
- **The switch.** `mcc.datapath_loop(false)` sends every connection back to
  the Lua loop (with the C batch), `true` back; `mcc.datapath_loop()` tells
  `{available, enabled}`. `MEMCACHED_DATAPATH=batch` switches it off from
  the start. It is `state.cloop_off`, so it survives a reload; the bytes
  on the wire are the same either way. `mcc.datapath(false)` switches the
  loop off too.
- **Tests.** `test/integration/cloop_test.lua` runs scripts on the C loop
  and on the Lua loop from the same state and compares the replies, the
  tuples and the counters of `stats` byte for byte: pipelines that mix
  served and unserved commands in every order, a pipeline split in two
  reads at every byte, values across reads and past `BUFFER_KEEP`,
  noreply and `q`, `quit`, `flush_all`, `srv:cfg()`/`inst:cfg()`, the
  read-only switch and the data path switch while a connection waits in C,
  many connections at once. It also checks the counters from another
  connection, `info()` and `stats reset`, the idle timeout and its runtime
  changes, a client that doesn't read (`conn_mwrite`) with a stop and a
  destroy, and a stop with connections in the loop; `reload_test.lua`
  checks that a connection in the loop takes the new code. The rest of the
  suite runs on the C loop too, and `MEMCACHED_DATAPATH=lua` on the Lua
  one.
- **Measured** (`bench/micro.lua`, "connection loop over socket pairs":
  16 connections at pipeline 1 with their clients in the same thread, so
  the CPU time is the client's and the server's): a get hit 27.8 -> 17.7
  µs, the 1:10 set:get mix 29.6 -> 18.1 µs, a 10-key get 31.8 -> 20.0 µs
  per request, Lua loop with the C batch -> C loop. Against a separate
  client process with application threads (TX CPU time per request, user
  + system): gets 15.1-15.6 -> 11.7-12.6 µs, 1:10 set:get 16.0-16.1 -> 12.2-13.7
  µs, the system time included: the Lua loop's allocations cost GC and
  page faults too.

## Commit wait

The instance option `commit_wait` sets when the commit of a write returns.
Its values are those of Tarantool's `box.commit({wait = <mode>})` and
`box.atomic({wait = <mode>}, ...)` (Tarantool 3.2+, `enum
txn_commit_wait_mode` in `src/box/txn.h`), hence the name:

| `commit_wait` | the commit returns |
|---|---|
| `'complete'` (default) | when the transaction is written to the WAL: every write waits for its own WAL write |
| `'submit'` | when the transaction is handed to the journal, before the WAL write. It waits (yields) only while the journal queue is full: more than `box.cfg.wal_queue_max_size` bytes (16 MiB by default) are waiting for the WAL |
| `'none'` | the same, but a full queue fails the commit at once, so it never yields |

- **What it applies to.** Every request that `engine.exec()` runs for the
  instance (`ctx.commit_wait`):
  - the requests that run in `box.atomic` with `'complete'` run between
    `box.begin()` and `box.commit({wait = <mode>})`, with a
    `box.rollback()` after an error or a failed commit;
  - the plain gets on memtx, autocommit statements with `'complete'`, run
    in such a transaction too, so that their optional writes (the lazy
    deletion, the last access) don't wait for the WAL either;
  - the C data path: `engine.lua` runs each C call between `box.begin()`
    and `box.commit({wait = <mode>})`. When the call has done nothing or the
    commit fails, it calls `box.rollback()` and the request goes through
    `run()`, as when C fails. `datapath.c` doesn't know about it: its box
    calls work in an open transaction (the redo record of a statement is
    copied into the transaction's region when the statement ends,
    `txn_commit_stmt()` -> `txn_add_redo()`, so the C code may still
    truncate the fiber region after each call). The batch is not used
    then (`engine.batch()` returns nil), the per-request calls are;
  - not `flush_all` and not the expiration sweepers, which keep
    `'complete'`. A flush is rare; one acknowledged and then lost would
    bring back every item it flushed. The sweepers' deletions are
    acknowledged to nobody, so waiting costs no client anything, and it
    keeps their pace at that of the WAL;
  - inside a transaction of the caller (`mcc.exec()` in `box.begin()`) the
    requests run as with `'complete'`.
- **Isolation.** With `'submit'` and `'none'`, on vinyl and with memtx MVCC
  (`memtx_use_mvcc_engine`), these transactions begin with `txn_isolation =
  'read-committed'`. The default isolation, best-effort, shows a
  transaction that hasn't written yet (and an autocommit read) only the
  changes whose WAL write has finished (`detect_whether_prepared_ok()` in
  `memtx_tx.c`, `vy_tx_is_prepared_ok()`). So a connection wouldn't read its
  own acknowledged writes: a get right after `STORED` would return the old
  value, and an append right after a set would fail with "Transaction has
  been aborted by conflict". memtx without MVCC shows every change at once:
  there the transactions begin without options.
- **Group commit.** A batch of writes runs without a yield: each request
  submits its transaction and the next one starts. The transactions
  submitted in one event loop iteration go to the WAL thread in one message
  (`wal_write_async()` appends to the message not yet sent), are written
  with one write, and come back to TX together. With `'complete'`, each
  write of a pipelined batch that the Lua engine runs waits for its own
  round trip to the WAL thread; the C batch (which runs with `'complete'`
  only) puts the writes of its commands into one transaction instead, which
  waits once (see "The C data path", "The group").
- **Durability.** The client gets `STORED`, `DELETED`, ... before the write
  is in the WAL.
  - If the WAL write fails (an I/O error, a full disk), Tarantool rolls the
    transaction back, and with it every transaction submitted after it (the
    cascading rollback of `wal.c`). The client was told that it succeeded,
    and no error reaches it: Tarantool logs the WAL error and counts the
    transactions in `box.stat().ROLLBACK`. The items get their previous
    state back: the old value of an item that was set, or no item; a
    deleted item is back. Writes made while the WAL keeps failing are
    acknowledged and rolled back the same way (with `'complete'` they answer
    `SERVER_ERROR Failed to write to disk`); during the rollback itself, a
    commit fails at once with `SERVER_ERROR WAL has a rollback in progress`.
  - A crash loses what was acknowledged and not yet written.
  - That is memcached's contract: it has no durability at all.
- **What readers see** between the submission and the end of the WAL write.
  The window is the same as with `'complete'`, where only the writer waits
  through it:
  - without MVCC, every fiber sees the change at once (memtx changes the
    indexes before the WAL write), and sees it go away if the write fails
    and is rolled back. A get may have returned a value that is rolled back;
  - with MVCC and on vinyl, the engine's requests see it (read-committed),
    and so does the rollback; other readers with the default isolation (an
    application's `space:get()`) see it only after the WAL write.
  - So on the same instance a connection reads its own writes: its next
    requests in the batch and in the next batches, with and without MVCC,
    on memtx and vinyl.
- **A full queue** with `'none'` fails the request with `SERVER_ERROR The
  WAL queue is full` (`ER_WAL_QUEUE_FULL`). A get then answers without its
  optional write. With synchronous replication, a full synchronous queue
  fails it with `SERVER_ERROR The synchronous transaction queue is full`.
  With `'submit'` the request waits for room in the queue instead.
- **Replication** is the same but for the timing: replicas get the rows from
  the WAL files after they are written (`relay.cc`), so they never see a
  transaction that was rolled back. A synchronous transaction (memcached
  creates no synchronous spaces) is acknowledged before its quorum.
- **Where it is set.**
  - `local`: the backend's ctx (`backend/local.lua`). So it applies with
    `threads`, whose batches run in TX with that backend, and in
    `snapshot` mode, where the writes go to the wrapped backend.
  - `vshard`: the writes run on the storages, so it is set there, per
    space: `storage.cfg({spaces = {[<space>] = {commit_wait = 'submit'}}})`,
    which `storage.exec()` passes into the ctx, as `track_access`. The role
    passes it from the instance config; instances that share a vshard space
    must agree on it. Through the Lua API, a value other than `'complete'`
    on an instance with `backend = 'vshard'` is refused ("Option
    'commit_wait' = 'submit' is not supported by the 'vshard' backend: set
    it on the storages, ..."), because it would have no effect there.
  - `inst:cfg({commit_wait = ...})` swaps the backend, so the next batch
    uses the new mode.
- **Cost.** `'complete'` reads two more fields per request: the same
  engine CPU time within noise. With `'submit'` every request pays an
  explicit transaction (about 0.7 µs more CPU per set). It pays off when
  the writes of few connections wait for the WAL one by one: 2x on one
  connection with sets at pipeline 16; with 100 connections on one core
  the WAL already groups the connections' writes and it is about even
  (`bench/`, w6 and `mcc-submit`; README "Performance").

## Front-end contract

### Server

`server.lua` listens on a URI (`host:port`, a port number, or
`unix/:<path>`). With `reuseport = true` it sets SO_REUSEPORT before bind,
so several threads can share a port. Client sockets get TCP_NODELAY. Over
`max_connections` a new client gets `ERROR Too many open connections` and
is closed. Each connection gets a fiber, which, in TX with the `local` backend and
the C data path, runs the C connection loop (see "The C connection loop")
while the C batch serves the whole input, and returns to the steps below
for the rest; otherwise:
1. reads into a buffer (at least `READ_SIZE` = 16 KiB at once). After the
   replies of a batch, when no input is left, it waits for readability
   before the next read, so no read fails with EAGAIN. With `idle_timeout`
   that wait has a timeout (see "Idle connections");
2. with a backend that has `batch()` (the `local` backend in TX, the
   `thread` backend in an application thread), lets the C batch serve the
   commands it serves at the head of the input, their replies in the
   connection's output buffer (see "The C data path"), and goes on with the
   rest below;
3. calls the protocol parser until it needs more data, up to `MAX_BATCH`
   (512) commands, those of the C batch included; with the `local` backend
   it stops at the next command the C batch serves, runs the commands
   parsed (4 to 6), and goes back to 2;
4. collects the parsed requests of one read (pipelining) into a batch. A
   multi-key retrieval is flattened into one engine request per key;
5. sends the batch to the backend;
6. encodes the responses in request order, honouring `noreply` and the meta
   quiet mode `q`;
7. writes everything, the replies of the C and the Lua batches in their
   order, with one write.

A connection keeps the arrays of a batch (the parsed requests, the
responses, the pending engine requests, the output parts) for its next
batches instead of allocating them per batch (`conn.batch`); it runs one
batch at a time. After a batch every entry it set is cleared, so an idle
connection holds no request or value; an array that grew past
`SCRATCH_MAX` (128) entries (`OUT_KEEP`, 512, for the output) is dropped
instead. The pending array goes to `backend:exec()`, and is kept only when
the backend borrows it (`borrows_reqs`, see "Backends"). `process(srv,
items, protos, n, batch?)` takes them as `batch` (`new_batch()`), and
makes new ones without it.

Protocol detection is per command: a line starting with `m` plus one of
`g s d a n e` and then a space or the line end goes to `proto/meta.lua`,
anything else to `proto/text.lua`.

**Idle connections.** With `idle_timeout` > 0 (seconds, memcached's `-o
idle_timeout`), a connection that has waited for a command for longer than
that is closed without a reply and counted in `idle_kicks`. As in memcached
(`conn_close_idle()`, the idle timeout thread):
- waiting for a command means a new connection with no command yet, one
  idle after its replies, or one with the start of a command line. A
  connection waiting for the value of a storage command (or discarding a
  too large value), executing a batch or writing replies never times out;
- only TCP connections time out, not unix socket ones;
- the idle time counts from the last batch: its start and its end (memcached
  counts from the last command line).
The connection loop already waits for readability once per batch; that wait
(`socket.iowait()`) gets the time left as its timeout, so the option adds no
syscall and no timer. `conn.state` (memcached's state names, see `stats
conns`) tells which wait a connection is in, and `srv:cfg()` wakes up the
connections waiting for a command when `idle_timeout` changes, so the new
value applies to them at once, counted from their last batch.

Some commands never reach the engine:
- `version`, `verbosity`, `quit` and `mn` are answered by the front-end
  (`me` is a peek `get` and does reach it). `version` returns `VERSION
  1.6.45-mcc-<ver>` (`server.MEMCACHED_VERSION`, `server.VERSION`): the
  protocol is the one of memcached 1.6.45. `server.VERSION` is
  `require('memcached.version').version`, the file tt generates when it
  builds the package (the `v*` git tags), and `scm` without it;
- `stats` is answered by the front-end from its counters and
  `backend:stats()`;
- `flush_all` calls `backend:flush_all(delay)`.

`flush_all` and `stats` are barriers: the engine requests before them in the
batch are executed first. `lru_crawler` dumps end a batch (see "Dumps").
`quit` ends the batch and closes the connection after the replies.

`stats` subcommands:
- `stats`: `pid`, `uptime`, `time`, `version`, `pointer_size`,
  `max_connections`, `curr_connections`, the counters (`total_connections`,
  `rejected_connections`, `cmd_get`, `cmd_set`, `cmd_flush`, `cmd_touch`,
  `cmd_meta`, `get_hits`, `get_misses`, `delete_misses`, `delete_hits`,
  `incr_*`, `decr_*`, `cas_misses`, `cas_hits`, `cas_badval`, `touch_hits`,
  `touch_misses`, `store_too_large`, `bytes_read`, `bytes_written`),
  `auth_cmds` and `auth_errors` (0), `idle_kicks` (only with
  `idle_timeout` > 0, as in memcached), `limit_maxbytes`,
  `accepting_conns`, `threads`, `bytes`, `curr_items`, `total_items`,
  and the eviction counters of the space `evicted_unfetched`,
  `evicted_active`, `evictions`, `reclaimed` and `direct_reclaims` (see
  "Eviction"); `store_no_memory` follows `store_too_large`;
- `stats settings`: `maxbytes`, `maxconns`, `tcpport`, `verbosity`,
  `evictions on|off` (whether the space is evicted from),
  `cas_enabled yes`, `item_size_max`, `num_threads`,
  `binding_protocol ascii`, `idle_timeout`, `reuseport`;
- `stats reset` resets the counters (of every thread, with threads);
- `stats sizes` answers `STAT sizes_status disabled`, as memcached without
  `-o track_sizes` (its default);
- `stats conns` lists the listener and the connections as memcached does,
  in the order of their ids: `<id>:addr` (`tcp:<ip>:<port>`,
  `tcp6:[<ip>]:<port>`, `unix:<path>`; `?:<AF 0>` for a unix socket
  client, which memcached has no address for), `<id>:listen_addr` (not for
  the listener), `<id>:state` and `<id>:secs_since_last_cmd` (since the last
  batch; for the listener, since it was bound). The id is the socket fd,
  unique in the process. The states are memcached's: `conn_listening`,
  `conn_new_cmd` (no input yet), `conn_read` (waiting for a command),
  `conn_nread` (for a value), `conn_swallow` (discarding a too large
  value), `conn_parse_cmd` (executing a batch, such as the `stats conns`
  itself) and `conn_mwrite` (writing to a client that doesn't read). With
  threads, `backend:conns()` collects the lists of every thread through TX
  (`mcc.conns`), so any connection lists the whole instance. It costs two
  syscalls per connection;
- `stats items` and `slabs` return an empty list;
- anything else answers `ERROR`.

The counters follow memcached: per key (a multi-key get counts each key),
`cas` is also a set, and `gat` is both a get and a touch. The server also
counts the error replies by class (`protocol_errors` for `ERROR`,
`client_errors`, `server_errors`): engine responses, front-end errors,
parser error lines, encode and dump failures. `stats` doesn't show them;
`srv:info()`, `inst:info()` and the metrics do.

`require('memcached.server')`:
- `new({name, listen, reuseport, max_connections, max_item_size,
  idle_timeout, backend})`;
- methods `srv:start()`, `srv:stop()` (closes the listener and every
  connection without waiting for the clients), `srv:rebind(uri)` (keeps the
  connections; if the new URI can't be bound, the server keeps the old one
  and raises; if even the old one can't be bound again, it stops listening,
  `running = false`, and keeps the connections), `srv:release()` (closes
  the listener and keeps running and the connections; the next `rebind()`
  binds the new URI, or the old one again if that fails),
  `srv:cfg({max_connections, max_item_size, idle_timeout, backend,
  reuseport, listen})`, `srv:info()`;
- module functions `restart_accept(srv)`, `close_connections(srv)`,
  `bind(listen, reuseport)`, `check_listen(listen)`, `process(...)`,
  `conn_step(conn)`, `new_batch()`, `account(...)`, `sync_stats(srv)` (the
  counters of the C loops added, see "The C connection loop"),
  `stats_reply(srv, args)`,
  `general_stats(srv)`, `settings_stats(srv)`, `reset_stats(srv)`,
  `conn_list(srv)`, `conns_stats(list)`, `stream(conn, req)` and
  `version()`.

### Parsers

```lua
parse(buf, pos, opts) -> req, next_pos          -- a complete command
                      -> nil, pos, need?        -- need more data (need = bytes from pos, if known)
                      -> false, next_pos, err_line, close?  -- protocol error
encode(req, resp, opts?) -> string              -- '' when noreply / quiet hides it
encode_parts(req, resps, out, n) -> n           -- optional: a retrieval's reply into out
```

`encode_parts` (`proto/text.lua`) is `encode` of a retrieval (a request
with `multi`) that appends the parts of the reply to the array `out` after
its first `n` entries and returns the new count; the server uses it when the
module has it, so that a value is copied once, by the concat of the batch's
output.

These rules hold for every parser:
- `opts.max_item_size` defaults to 1048576. In `encode`, `opts.now`
  overrides `fiber.time()` (meta).
- `err_line` ends with `\r\n`. It is nil when nothing is to be answered
  (an error of a `noreply` command, or a silent close).
- After a rejected storage command whose length is valid, `next_pos` may
  point past the buffer. The server turns this into a skip of the data block,
  so oversized values are never buffered.
- `close = true` means the connection must be closed after `err_line`
  (nil for both cases, a silent close as memcached does): a command line
  without `\n` longer than 2048 bytes (a line that starts with `get ` or
  `gets `, after at most 100 spaces, may be up to 1 MiB), or an unknown
  classic command whose last token starts with `HTTP/`.
- Engine fields go at the top level of `req`. Protocol-only data goes in
  `req.meta` (meta) or in `cmd`, `noreply`, `with_cas`, `touch_only`,
  `too_large` (classic); the vshard backend strips these before sending.
- The parsers enforce the key rules (see "Keys") and the item size limit
  (`max_item_size`, 1 MiB by default). The size is counted as memcached
  counts it against `-I` with its defaults (`items.c` `item_make_header`
  and `do_item_alloc`, CAS on): 48 bytes of item header + the key + 1 (its
  NUL) + 4 for client flags other than 0 + the value + 2 (its `\r\n`) + 8
  (the CAS); `proto/text.lua` `item_size()`. A store above the limit is
  "object too large", so with 1 MiB the largest value of a 10-byte key is
  1048507 bytes (1048503 with flags). The key is the decoded one for `ms
  b`. The parsers check the size of the data of `append`/`prepend`; the
  engine checks the joined item (see `store`).
- A value that is too large is swallowed. A classic `set`, and an `ms` in any
  mode, then delete the old value (`{op = 'delete', too_large = true}`) and
  answer `SERVER_ERROR object too large for cache`, as memcached does.

`proto/text.lua`:
- the commands are `get`, `gets`, `gat`, `gats`, `set`, `add`, `replace`,
  `append`, `prepend`, `cas`, `delete`, `incr`, `decr`, `touch`,
  `flush_all`, `verbosity`, `version`, `quit`, `stats` and `lru_crawler`.
  Command names are case-sensitive, as in memcached (`GET` is `ERROR`);
- `get a b` is parsed to `{op = 'retrieve', multi = {<engine get reqs>}}`;
  the server flattens `multi` into the batch, and `encode` receives the array
  of responses. A `SERVER_ERROR` for any key drops the partial result;
- the line is parsed as memcached 1.6.45's `process_request` does: a
  line shorter than 4 bytes (its `\n` included) is `CLIENT_ERROR bad
  command line format`; the command name is the line up to the first space
  (a leading space makes it empty, and then only `flush_all`, `verbosity`
  and `lru_crawler`, looked up by the first token, still work); tokens are
  split on spaces only; too few tokens is `CLIENT_ERROR bad command line
  format`, extra ones are ignored; only the first 23 tokens count, and
  `noreply` is the last of them (`get`/`gets`/`gat`/`gats` read every key;
  `gat`, `stats` and `version` have no `noreply`);
- the client flags are read with `strtoul` and cast to 32 bits, as
  memcached does: above 2^64-1 (or a `-` with a result that is negative as
  a long) is `CLIENT_ERROR bad command line format`, the rest is truncated
  (`4294967297` is 1);
- the numbers are read as memcached reads them: `mcmc_tokto32`
  (`<bytes>`, `gat`/`touch`/`flush_all` times), `mcmc_toktou32`
  (`verbosity`), `mcmc_toktou64` (`incr`/`decr` deltas, `cas` uniques:
  digits only, at most 22), and `strtol` for the exptime of the storage
  commands (a sign and leading whitespace allowed, cast to int32);
- a storage command whose `<bytes>` is missing or bad fails before its
  data block, which is then parsed as commands, and `noreply` doesn't hide
  that error; any later error (key, flags, exptime, cas unique, size)
  swallows the data block;
- `touch` is a `get` with `touch_ttl` and `touch_only`;
- `verbosity` is capped at 2 and has no effect on logging.

`proto/meta.lua`:
- also exports `detect(buf, pos)`, `b64encode`, `b64decode`, `parse_u64` and
  `u64str`;
- the flag rules, error strings and limits (20 tokens, 32-byte opaque)
  follow memcached's `proto_parser.c`; meta errors are never hidden by `q`;
- as memcached 1.6.45's tokenizer, a flag token (among the first 23
  tokens) that starts with neither a letter nor a digit is `CLIENT_ERROR
  bad command line format` before anything else, so an `ms` data block is
  not swallowed then; a line shorter than 4 bytes (`mn\n`) is that error
  too;
- `mn` is parsed to `{op = 'mn'}`. Replies are written in
  request order, so `MN` follows the replies of every command before it;
- `me <key> [b]` is parsed to a `get` with `peek = true`;
- `q` hides only the success reply of `ms`, `md` and `ma` and the `EN` of
  `mg`: `md` still returns `NF`, `ms` still returns `NS`/`EX`/`NF`;
- an `ms` with a CAS turns `set` and `replace` into the `cas` command; `add`
  ignores it; `I` works only in the `cas` command.

## Backends

- **Registry.** `mcc.backends[name] = module_name`, with `local`, `thread`
  and `vshard` preset. The allowed values of the `backend` option are in
  `mcc.BACKEND_OPTIONS` (`local`, `vshard`); `thread` is internal.
- **Module.** A backend module provides `new(cfg) -> obj` and optionally
  `validate(cfg)`, which raises if the options can't be served. `cfg` is a
  copy of the instance options plus `name`. A module that sets `REMOTE =
  true` keeps the data elsewhere: the instance then neither creates nor waits
  for a local space and runs no sweeper. Optional hooks for application
  threads: `thread_cfg`, `thread_cfg_ref`, `thread_prepare` (see
  "Application threads").
- **Object.**
  - `obj:exec(reqs) -> resps`: may yield; same shapes as `engine.exec`.
    With `obj.borrows_reqs == true` it keeps no reference to the `reqs`
    array after it returns, and the server reuses the array for its next
    batches (see "Server"); every backend here sets it (the readview
    wrapper when its inner backend does). Without it every batch gets a
    new array;
  - `obj:flush_all(delay) -> resp`;
  - `obj:stats() -> {curr_items, bytes, limit_maxbytes?, evict?,
    <eviction counters>?}`, plus `counters`,
    `curr_connections` and `threads` when the counters are summed over
    application threads;
  - `obj:close()`;
  - optional: `obj:reset_stats()` (`stats reset` in every thread),
    `obj:conns()` (`stats conns` of every thread, nil to list the server's
    own) and `obj:dump(req, write)` (dumps, see "Dumps").
  Objects look their methods up in `package.loaded` on every call, so a
  reload takes effect.
- **Swapping.** Changing `backend`, `read_mode`, `snapshot_refresh`,
  `vshard`, `track_access` or `commit_wait` through `cfg` creates a new
  backend object and swaps it into the live server; the old one is closed.

The backends:
- `backend/local.lua` calls `engine.exec({space_name, track_access,
  commit_wait}, reqs)` directly. It supports only `read_mode = 'rw'` (snapshot reads wrap it, see
  "Read views"). `limit_maxbytes` is `box.cfg.memtx_memory`.
- `backend/thread.lua` sends one `threads.call('tx', 'mcc.exec', {name,
  reqs})` per batch, after the raw batch (`mcc.batch`, see "Application
  threads"), and calls `mcc.flush_all`, `mcc.stats`, `mcc.conns` and
  `mcc.reset_stats` the same way.
- `backend/vshard.lua` routes by bucket; see "vshard".
- `readview.wrap(inner, cfg)` is the snapshot-read wrapper.

## Instance API

```lua
local mcc = require('memcached')
local inst = mcc.create(name, opts)   -- creates the space if needed and starts
inst:cfg(opts)        -- runtime options; listen rebinds and keeps connections
inst:start() / inst:stop() / inst:destroy() / inst:info()
mcc.check(name, opts) -- raises if create() would reject the options; no side effects
mcc.get(name), mcc.destroy(name)
mcc.exec(name, reqs)  -- a batch of engine requests with the instance's backend (TX)
mcc.reload()          -- hot reload of the Lua code
mcc.tune_gc(opts)     -- the Lua GC parameters of the calling thread
```

Options:

| option | default | notes |
|---|---|---|
| `listen` | required | URI: `host:port`, a port, or `unix/:<path>` |
| `space_name` | `__mcc_<name>` | can't be changed |
| `storage` | `'memory'` | `'disk'` = vinyl; can't be changed |
| `backend` | `'local'` | or `'vshard'` |
| `threads` | `nil` | name of an application thread group to run the front-ends in; `false` or `box.NULL`: in TX (`inst.opts.threads` is nil then) |
| `read_mode` | `'rw'` | `'ro'`/`'bro'` (vshard only); `'snapshot'` (local + memory only) |
| `snapshot_refresh` | 1 | seconds, at least 0.1: Tarantool throttles read view creation to 1 per 100 ms |
| `max_item_size` | 1048576 | 1 .. 1 GiB, the largest item, counted as memcached does (see "Parsers") |
| `max_connections` | 4096 | per front-end (per thread with `threads`) |
| `idle_timeout` | 0 | seconds, a number >= 0: close a connection that waits for a command longer than that (TCP only); 0 = never. See "Idle connections" |
| `expire` | `{enabled = true, batch = 100}` | the sweeper; merged key by key; `mode = 'index' \| 'scan'`, default `'scan'` for memory and `'index'` for disk, see "Expiration modes" |
| `evict` | `{enabled = true, high = 0.95, low = 0.9, samples = 8, batch = 100}` | eviction, see "Eviction"; merged key by key; ignored with `storage = 'disk'`; with `vshard`: set on the storages |
| `vshard` | `nil` | `{router, timeout}`, see "vshard" |
| `metrics` | `false` | export the instance's metrics, see "Metrics" |
| `track_access` | `true` | gets record the last access and the fetched flag; `false`: gets never write the item (with `vshard`: set on the storages), see "Last access and fetched" |
| `commit_wait` | `'complete'` | when a write's commit returns: `'complete'` after the WAL write, `'submit'` or `'none'` before it (with `vshard`: set on the storages), see "Commit wait" |

- Unknown options and wrong types raise ("Unknown option 'x'", "Option 'x'
  must be a number, got string", "Bad value for option 'x'").
- `create()` needs `box.cfg` ("Please call box.cfg{} first") and a unique
  non-empty name ("Memcached instance '<name>' already exists").
- `create()` warns in the log when `max_item_size` plus 1 KiB doesn't fit
  into `box.cfg.memtx_max_tuple_size` (or `vinyl_max_tuple_size`): bigger
  values then fail with `SERVER_ERROR`. Tarantool's default tuple limit is
  1 MiB, so set it to about 2 MiB for 1 MiB values.

Lifecycle, as in the tarantool/memcached `tarantool3-role` branch:
- statuses are `'running'`, `'stopped'`, `'waiting'`, `'failed'` and
  `'destroyed'`;
- `create()` starts the instance; if the start fails, the instance is
  destroyed and the error is raised;
- `stop()` closes the listener and the client connections and doesn't wait
  for clients; `start()` again reuses the server object and its counters;
- `destroy()` stops the instance and frees the name. The space and its data
  are kept;
- options that can't be changed raise ("Option 'storage' can't be changed
  after the instance is created");
- on a read-only instance where the space is missing, `start()` returns with
  status `'waiting'`. A fiber `mcc/wait/<name>` polls every
  `SPACE_POLL_INTERVAL` (0.1 s) and starts the instance when the space
  arrives by replication (or creates it when the instance becomes
  writable);
- a read-only instance serves reads; writes answer `SERVER_ERROR`.

`inst:cfg(opts)` merges the options into the current ones:
- `listen` rebinds first, keeping the connections; on an error the old URI is
  kept and the error is raised (the status is `'failed'` if even the old
  URI can't be bound again). After `mcc.internal.release_listener(inst)`
  (see the role) the next `cfg` binds the URI whether it changes or not;
  `mcc.internal.restore_listener(inst)` binds the current one again;
- limits and `idle_timeout` are applied to the running server (in every
  thread with `threads`);
- `backend`, `read_mode`, `snapshot_refresh`, `vshard`, `track_access` and
  `commit_wait` swap the backend (and start or stop the snapshot refresher,
  and create the local space when moving from a remote backend to
  `local`); the items keep the access values recorded so far;
- `expire` restarts or stops the sweeper (it keeps its counters); a new
  `expire.mode` creates or drops the `exptime` index of the space of a
  running instance on a writable Tarantool (a stopped one does it when it
  starts), see "Expiration modes"; `evict` (and `track_access`)
  updates or stops the evictor's owner. `low < high` is checked on the
  merged options ("Bad value for option 'evict': 'low' must be less than
  'high'"). A non-default `evict` on a remote backend is refused, as
  `track_access = false` is;
- `metrics = true` enables the export;
- `threads` moves the front-ends between TX and the threads, or between
  groups, which restarts them and closes the connections. `threads = false`
  (or `box.NULL`) moves them back to TX, and `inst.opts.threads` is nil
  afterwards. A stopped instance only takes the option and starts there.

`mcc.tune_gc({pause = <n>, stepmul = <n>}) -> {pause, stepmul}` sets
`collectgarbage('setpause'/'setstepmul')` of the calling Lua state (TX, or
the application thread it is called in: each thread has its own collector)
and returns the previous values of the parameters it set, so the caller can
restore them. `pause` is 100..1000 and `stepmul` 100..10000 (LuaJIT's
defaults are 200); it logs the change. memcached never changes the
collector by itself, since it is shared by everything in the Lua state.

`inst:info()` returns `name`, `status`, `space_name`, `listen`, `port`,
`curr_connections`, the server counters (see "Server"), the sweeper's
`expired`, `flushed` and `checked`, and for a local space the eviction counters of the
space (`evictions`, `reclaimed`, `evicted_unfetched`, `evicted_active`,
`direct_reclaims`). With threads it sums the counters over the
threads, adds `thread_group` and `threads` (per-thread info sorted by
`thread_id`), and yields.

## vshard

### Storage side: `memcached/storage.lua` (TX only)

```lua
local storage = require('memcached.storage')
storage.cfg({spaces = {
    [<space_name>] = {storage = 'memory' | 'disk',        -- default 'memory'
                      expire = {enabled = true, batch = 100,
                                mode = 'scan'},           -- or 'index'
                      evict = {enabled = true, high = 0.95, low = 0.9,
                               samples = 8, batch = 100},
                      track_access = true,                -- default true
                      commit_wait = 'complete'},          -- or 'submit', 'none'
}})
storage.info() -> {ro, spaces = {[name] = {storage, track_access,
                                           commit_wait, expire_mode, ready,
                                           sweeper, expired, flushed,
                                           checked, evict, evictor,
                                           evictions, reclaimed,
                                           evicted_unfetched, evicted_active,
                                           direct_reclaims, batches,
                                           requests}}}
storage.stop()
```

- **cfg()** replaces the whole set of served spaces. A space left out is no
  longer served, and its data is kept. Unknown options raise. It is
  idempotent; the role calls it on every apply.
- **track_access** and **commit_wait** are the instance options of the same
  names for the requests on that space: `storage.exec()` passes them into
  the engine ctx (see "Last access and fetched" and "Commit wait").
- **Spaces** are created in format v3 with the `bucket_id` index, which makes
  them sharded for vshard (a v2 space is upgraded, see "Data format v3"). A
  writable instance creates them at once.
  Otherwise a `box.watch('box.status')` watcher applies the config again when
  the instance becomes writable, as vshard's own config applier does. A
  read-only instance only checks what has arrived by replication.
- **Sweepers** are named `vshard:<space_name>` in `state.sweepers`. They run
  only while the instance is writable and follow it when its role changes.
- **expire.mode** defaults to `'scan'` for memory and `'index'` for disk,
  as for an instance. Every apply on a writable storage creates or drops
  the `exptime` index to match it (also when the storage becomes
  writable); a replica gets the DDL by replication. The bucket moves don't
  depend on it: the indexes are local to each storage.
- **Eviction** of a memory space with `evict.enabled`: the evictor's owner
  `vshard:<space_name>` exists only while the instance is writable, as the
  sweepers. Each storage evicts by its own memory.
- **State** is in `state.storage`. The globals, the watcher and the sweepers
  look the code up in `package.loaded`.
- **stop()** drops the globals, the watcher and the sweepers. The data is
  kept.

The routers call these functions:

| global | called through | returns |
|---|---|---|
| `mcc.storage_exec(space_name, bucket_id, reqs)` | `callrw`, `callro`, `callbro` | `engine.exec` responses |
| `mcc.storage_flush(space_name, delay)` | `map_callrw` | the flush response |
| `mcc.storage_stats(space_name)` | `map_callrw` | `{curr_items, bytes, limit_maxbytes, evict, <eviction counters>}` (`memtx_memory` or `vinyl_memory`), without the buckets the storage doesn't own (see "Semantics on a sharded cluster"); the router adds them up |
| `mcc.storage_dump(space_name, bucket_id, sub, after, limit)` | `callrw`, `callro`, `callbro` | `{lines, after}`, see "Dumps" |

A space the storage doesn't serve gives `SERVER_ERROR space '<name>' is
not served by this storage`; `storage_stats` and `storage_dump` raise that
error instead.

**They are plain Lua globals.** vshard 1.0.0 on Tarantool 3 runs the
function of `vshard.storage.call()`, and of the `map_callrw` map stage,
with `net.box.self:call(name, args)`. That call looks the name up in
`box.func` first and then as a dotted path of Lua globals, so
`'mcc.storage_exec'` is `_G.mcc.storage_exec`. The vshard functions are
`setuid`, so the function runs as their owner. Hence:
- no `_func` entries and no grants. A `box.func` entry would need grants,
  which Tarantool 3's credentials applier would revoke;
- the globals work on read-only replicas, where nothing can be created;
- the fields of `_G.mcc` are set one by one, so other `mcc.*` globals
  survive;
- nothing may create a `box.func` named `mcc.storage_*`: it would take
  precedence.

### Router side: `memcached/backend/vshard.lua`

The option `vshard = {router, timeout}`:
- `router`: a router object or the name of one; default
  `vshard.router.static`. Names are resolved on every batch, first among the
  routers made in this thread, then through
  `vshard.router.internal.routers[name]` (an internal vshard API), so the
  router may be created later. A router object is kept by reference: a
  vshard router is cyclic and can't be deep-copied; init.lua's `is_object()`
  treats any table whose metatable has `__index` this way.
- `timeout`: seconds per batch, default 1 (`DEFAULT_TIMEOUT`).
- `validate` needs `vshard.router` (or `vshard-ee.router`) to load: "vshard
  is not available: ...".
- **Version check.** The first time a thread gets the router module (in
  `validate` or when resolving a router), the backend reads the version
  from `vshard.consts.VERSION` (`vshard-ee.consts` for vshard-ee; the
  package's `_VERSION` as a fallback). Outside `>= 1.0.0, < 2.0.0`, or when
  there is no version, it logs one warning with the version found and the
  supported range (`state.vshard_version_checked`, so once per thread and
  not again after a reload) and goes on: nothing is known to break, since
  the internals below exist from 1.0.0 on, and an API that is missing in
  another version fails with its own error. It is not a hard refusal for
  that reason; a release known to break would be refused here.

- **exec()** groups a batch by `router:bucket_id_strcrc32(key)`. Requests on
  one key stay in one group, in their order. Each group is one call of
  `mcc.storage_exec`, so it runs on the storage under a ref of its bucket.
  All the groups go out at once with `is_async`, and the responses are put
  back in request order. A multi-key get is just more groups. Protocol-only
  request fields are not sent.
- **Resend.** An async call doesn't follow a moved bucket. Some groups never
  ran: they were not sent (no connection, no master), or the storage
  refused them with WRONG_BUCKET, BUCKET_IS_LOCKED,
  TRANSFER_IS_IN_PROGRESS, NON_MASTER or MISSING_MASTER. Those groups are
  sent again synchronously, in parallel, and vshard follows the bucket or
  waits for the master until the batch deadline. `backend.resends` counts
  them. Any other error, or the deadline, gives `SERVER_ERROR <message>` for
  every request of the group, whose writes may or may not have been applied.
- **A vshard bug is worked around.** In vshard 1.0.0 an async future
  "succeeds" when the storage refuses the call. `vshard.storage.call()`
  returns `nil, err`, the nil arrives as `box.NULL`, which is true, and
  `wait_result()` returns `{err}`. The sync path has a `box.NULL`
  workaround in `replica_call`; the future wrapper doesn't. So the backend
  treats a result that is not an array of responses as the error
  (`unwrap()`).
- **read_mode.**
  - `rw`: everything goes through `callrw`.
  - `ro`/`bro`: a group of plain gets (no touch, vivify or recache) goes
    through `callro`/`callbro`. A group with any other request goes
    through `callrw` as a whole, so it reads its own writes.
  - Tarantool 3 config gives the router no `zone`, so `callro` picks the
    master. Reads reach replicas with `bro`, or with a named router built
    with zones and weights.
  - `snapshot` is rejected: "read_mode 'snapshot' is not supported by the
    vshard backend".
- **flush_all and stats** use `map_callrw` over all the masters, with the
  batch timeout. If a master is unreachable, `flush_all` gives
  `SERVER_ERROR <replicaset>: <error>` and flushes nothing, because the ref
  stage fails before any map. `stats` raises, and the server then shows
  zeros for the items (in a thread, the counters are still shown). The ref
  stage also waits until no storage has a bucket in transfer or waiting
  for the garbage collector (vshard's `bucket_are_all_rw()`), so during a
  rebalance both wait up to the batch timeout and may fail the same way.
- **64-bit values.** msgpack gives uint64 values below 2^53 back as Lua
  numbers. The backend turns `cas`, `value` and `item.cas` back into
  `uint64_t`.
- **APIs used.** No `box.*` API; only `vshard.router`, `fiber`, `ffi` and
  `threads`.

### Semantics on a sharded cluster

- **CAS** moves with the item, since the tuple is copied as is. With
  `new_cas = max(old_cas + 1, counter())` it keeps growing on the new
  storage, whatever that storage's counter is.
- **Win tokens** (meta W/Z) are in the items, so they move with the tuple
  on a bucket move and are replicated: they survive a failover and a
  restart.
  - In `ro`/`bro`, a plain `mg` of a stale item (`md I`) goes to a replica,
    which can't write the token: it hands it out per replica, from memory
    (see "Win tokens"). It does see the tokens the master has handed out.
    Use `rw` when relying on W/Z with invalidation.
  - Gets with touch, vivify or recache always go to the master.
- **flush_all** stores a flush time on each storage, from that storage's
  clock and `engine.stamp()`. Take an item written on storage A just after
  the flush, whose bucket then moves to B, where the flush time is slightly
  later: the item is dead on B. The window is the clock skew plus the
  spread of the `map_callrw`.
- **stats** adds the storages up. `mcc.storage_stats` leaves out the tuples
  of the buckets the storage doesn't own: `receiving` ones (a transfer to
  it in progress), and `sent` and `garbage` ones (moved away, not yet
  garbage collected); a `sending` bucket is still counted by its source.
  So a bucket in transfer counts once. It looks the non-active buckets up
  in the `status` index of `_bucket` and counts each in the `bucket_id`
  index (log time in memtx, a range read in vinyl), never scans the space.
  Their bytes are taken out at the average size of the items, so `bytes`
  is approximate then. Through `map_callrw` the storage ref already waits
  for the transfers to end (see above); the exclusion keeps the function
  right without the ref.

## Application threads

### Front-ends in threads: `threads.lua`, `backend/thread.lua`

- **Option.** With `threads = '<group>'`, the front-ends run in every
  thread of that application thread group (Tarantool 3 config
  `threads.groups`). Each is a `server.lua` server with `reuseport = true`,
  so the kernel spreads the connections over the threads. Its backend is
  `threads.THREAD_BACKENDS[backend]`: `local -> thread`, `vshard -> vshard`.
  - TX doesn't listen: `inst.server` exists but is not running.
  - With `local`, `inst.backend` in TX is the backend that executes the
    batches: `local`, wrapped by `readview.wrap` in snapshot mode. A batch
    is one cross-thread call, or two with the raw batch (below), which goes
    through the thread backend object's channel (`memcached.ipc`, below)
    rather than `threads.call` when it can.
  - With `vshard`, the threads call the storages through their own routers
    (see below); TX is not in the data path.
- **Restrictions** (`threads.check`, in `create`, `check` and `cfg`):
  - the group must exist and must not be `tx`: "Application thread group
    '<g>' does not exist";
  - the backend must be in `THREAD_BACKENDS`;
  - the listen URI must be TCP with a fixed port: no unix sockets, no port
    0;
  - changing `threads` (`false`: back to TX) restarts the front-ends,
    which closes the connections.
- **TX exports.** `threads.export_tx()` exports these functions once per
  process (`state.tx_exports`). They are trampolines that look the code up
  on every call, because `threads.export` can't replace a function:
  - `mcc.exec(name, reqs) -> resps`; `memcached.exec` is the same
    function for TX callers;
  - `mcc.batch(name, data, max_commands, max_item_size) -> reply,
    consumed, commands, gets, hits, stored, held, <counters>` (the results
    of `engine.batch()` after `held`), or nothing: the raw batch
    (below) without a channel;
  - `mcc.ipc_open(name, id, abi) -> boolean`: serve the channel `id` of a
    thread backend object (below);
  - `mcc.flush_all(name, delay) -> resp`;
  - `mcc.stats(name) -> table` (the backend stats plus the counters summed
    over the threads);
  - `mcc.counters(name) -> {counters, curr_connections, threads}`;
  - `mcc.conns(name) -> array`: `server.conn_list()` of every thread, for
    `stats conns`;
  - `mcc.reset_stats(name)`;
  - `mcc.rv_open(name) -> id` and `mcc.rv_close(id)`, for the dumps.
  An instance that is not running answers `SERVER_ERROR Memcached instance
  '<name>' is not running`.
- **Control.** TX runs `threads.fe.<fn>(arg)` in the threads with
  `threads.eval`: `start`, `stop`, `destroy`, `cfg`, `info`, `reset_stats`,
  `conns`, `release`, `rv_publish`, `after_reload`.
  - A thread loads the code on first use, with TX's `package.path` and
    `package.cpath`. The search root (`package.searchroot()`) is
    process-wide, so the threads find the same files as TX.
  - If `start` fails in any thread, every thread stops and the error is
    raised.
  - If a rebind fails, every thread goes back to the old URI. If some
    thread can't (`threads.listening()`), the instance is failed.
  - `threads.release()` closes the listener in every thread and keeps the
    connections; `fe.cfg` with a URI then binds it even if it is the
    same.
  - `cfg` pushes the limits, `idle_timeout`, the backend options and the
    read mode (`threads.thread_cfg()`: a copy of the options without
    objects). A new backend object is made in a thread when the backend
    module, read mode, space, router name or timeout changes.
- **Aggregation.**
  - `inst:info()` sums the counters over the threads (`threads.collect`).
  - `stats` in any thread uses the `counters`, `curr_connections` and
    `threads` of the backend stats, so it shows the whole instance.
  - `stats settings` reports `num_threads`; `stats reset` resets every
    thread; `stats conns` lists the listeners and connections of every
    thread.
- **The raw batch.** With `local`, a thread is an I/O offload for the
  commands of the C batch: it accepts, reads and writes, and TX parses,
  executes and encodes them in C. No request or response tables cross the
  threads for them, only bytes.
  - `backend/thread.lua` `batch()` is the `batch()` of the server's
    connection loop (see "The C data path"): when the input starts with a
    complete `get `, `gets ` or `set ` command (a set with its data block,
    within `max_item_size`), it sends a prefix of the input to TX: through
    its channel (below), or as one string in one `threads.call('tx',
    'mcc.batch', ...)`. TX runs the C batch of `inst.backend` (`local`
    `batch()`, `engine.batch()`) on the bytes with an output buffer of its
    own and returns the replies' bytes and the values `engine.batch()`
    returned: the bytes consumed, the commands served, the counters and the
    held keys (and whatever values it may return after those). The thread
    appends the replies to the connection's output buffer, adds the
    counters to its own (once per call), consumes the bytes, and runs the
    rest of the input as in TX mode: parsed in the thread, one `mcc.exec`
    with the held keys passed on, replies written with those of the raw
    batch in one write.
  - The prefix is the whole input up to `RAW_MAX` (64 KiB) bytes, or the
    first command if that is longer. When it is cut, a `\0` is appended:
    the C batch then sees that input follows its last command and returns
    the keys its reads hold, also when the cut falls at the end of a
    command.
  - TX runs the C batch, which serves what it can of the prefix (the
    commands it serves after the head too), not the Lua rest of the input,
    and the thread doesn't go back to it after the Lua rest (that would be
    a call per turn). The rest needs
    the thread's connection and server: the parse state across reads
    (`need`, `skip`), the front-end commands (`stats` with the counters of
    the thread, `verbosity`, `quit`, `mn`, the barriers) and the
    `lru_crawler` streams, which write to the socket. And TX is the one
    thread that every front-end shares: the Lua parsing and encoding of the
    other commands stay in the threads. A batch that mixes commands of the
    C batch at its head with others makes two calls: the raw batch and
    `mcc.exec`.
  - No raw batch, and the Lua path as before: with the vshard backend
    (its own path), with `read_mode = 'snapshot'` (the wrapper has no
    `batch()`), and for a batch that doesn't start with a complete command
    of the C batch. When TX answers that it can't run the C batch (the
    data path is not built, switched off, `MEMCACHED_DATAPATH=lua`, not
    memtx, a `commit_wait` mode, the instance is not running) or the call
    fails (a TX of an older code), the thread backend doesn't try again for
    `RAW_RETRY` (1) second; after a set that the C batch didn't serve (it
    serves none on a read-only instance and with memtx MVCC), it doesn't
    send batches that start with a set for that long.
  - Before the channels, a dispatcher fiber per thread that sent the
    queued batches of its connections in one `threads.call` was slower in
    the in-suite benchmark: in one TX fiber the batches waited for each
    other's WAL writes, and with a fiber per batch in TX the gain of fewer
    calls was lost.
- **The channels: `memcached.ipc`** (`memcached/ipc.c`, an optional Lua C
  module built like the data path, `.rocks/lib/tarantool/memcached/ipc.so`).
  A `threads.call` costs a cross-thread message each way, a fiber of TX's
  pool and the MsgPack of the arguments and the results: about 100 µs
  sequentially, 20 µs each with 50 fibers, for one get. The raw batch
  of a thread backend object (one per instance per thread) goes through a
  channel of its own instead:
  - **Memory.** `nslots` slots (`IPC_SLOTS`, 1024): a buffer that holds the
    request (the input bytes, the `\0` of a cut input, `max_commands`,
    `max_item_size`), then the reply (the output bytes, then the values,
    tagged: nil, booleans, numbers and strings as they are, the rest
    `msgpack.encode`d in TX and decoded in the thread, as `threads.call`
    would), grown as needed by the side that owns the slot and freed above
    64 KiB after use. A slot belongs to the thread while it is free, to TX
    from the push of its request to the push of its reply. Two rings of slot
    indexes, single producer and single consumer each: `req` (thread -> TX)
    and `done` (TX -> thread), with C11 atomics (the index is published
    with a release store of the tail and taken with an acquire load, so the
    slot's contents travel with it). A ring holds at most `nslots` indexes,
    so a push never finds it full; the back-pressure is a call that finds
    no free slot. The producers and consumers are fibers of one thread on
    each side; a push or a pop doesn't yield, so each side is one producer
    and one consumer as the rings see it.
  - **Wakeups.** An `eventfd` per ring on Linux, a non-blocking pipe on
    macOS, which has no eventfd (a byte per wakeup, the consumer reads them
    all; `-DIPC_WAKEUP_PIPE` picks it on Linux for the tests). A consumer
    that finds its ring empty sets the ring's `armed` flag, checks again
    and waits on the wakeup (`coio_wait()`); a producer signals it only if
    it clears `armed` after its push (seq_cst fences on both sides). A busy
    consumer costs no system call, and a wakeup drains everything there
    is.
  - **The thread's side** (`backend/thread.lua` "The channel"). The first
    raw batch of a backend object makes the channel (`ipc.new()`), starts
    its receiver fiber (`mcc/ipc-rx/<name>`) and asks TX to serve it
    (`mcc.ipc_open` by `threads.call`); meanwhile, and when that fails, the
    raw batches go by `threads.call`. A connection takes a free slot,
    copies the input into it, pushes it and sleeps; the receiver pops the
    replies and wakes their connections, which copy the reply into their
    output buffers and free the slot. A connection also looks at the ring
    every second while it waits, so a receiver that is gone costs latency,
    not a hang. Without the module (a tree tt didn't build), with
    `IPC = false`, when TX doesn't serve the channel (no module in TX,
    another ABI), when no slot is free and for a request over `IPC_MAX` (4
    MiB), the batch goes by `threads.call`; after a refusal or a channel TX
    let go, a new one is tried after `IPC_RETRY` (5) seconds.
  - **TX's side** (`threads.lua` "The IPC"). TX attaches to the channel by
    id (`ipc.attach()`, a process-wide registry in the `.so`, which is
    loaded once per process) and serves it with a pool of fibers
    (`mcc/ipc/<name>`, up to `IPC_FIBERS`, 128). One leads: it waits on the
    eventfd and runs the requests one after another through
    `inst.backend:batch()`, as `mcc.batch` does; an instance that isn't
    running or can't run the C batch answers so, and the thread falls back
    as it does with `mcc.batch`. A batch with sets yields on the WAL, and
    the requests behind it must not wait for that write (one fiber for all
    of a thread's batches gave 145k ops/s instead of 257k earlier). So a
    second fiber stands by while the leader runs requests: woken once per
    wakeup of the leader, it runs when the leader yields; if the leader
    yielded in a request, it takes the lead with a standby of its own, and
    the old leader parks when its request is done; otherwise it parks
    again. Reads cost no fiber switch per request, and the writes overlap
    as with a fiber per call. The leader yields after `IPC_YIELD` (64)
    requests in a row.
  - **Lifetime.** A channel has a reference per side and is freed when
    both are dropped. `backend:close()` (the front-end stops, is destroyed,
    moves to TX, or gets a new backend object) closes it: no more requests,
    those in it are still served; TX takes them, waits for its fibers in
    requests, then detaches (`TX_GONE`). TX detaches only when none of its
    fibers holds a slot, so a request TX didn't take never will be: the
    thread sends it by `threads.call`. The thread keeps its reference while
    a connection waits in a call or the receiver waits on its wakeup. A
    connection cancelled while it waits leaves its slot to the receiver
    (the request still runs in TX, as with `threads.call`). If TX's pool is
    cancelled, its last fiber detaches; the thread drops the channel and
    opens a new one later.
  - **Hot reload.** The `.so` stays loaded and its registry and counters
    are not reset by `luaopen`; the channel records live in
    `state.ipc_channels` (TX) and in the backend objects (threads), and the
    fibers look the code up on every step, so a reload neither leaks nor
    doubles a channel.
  - **Not over the channels:** `mcc.exec` and the other TX functions,
    which carry request and response tables (they would need the MsgPack
    that `threads.call` already does).
- **Cost of `local` in threads.** Measured in the dev container on the 5-CPU
  host, 2026-11-15, noisy (the host was shared):
  - Round trip of one raw batch (`get k1`, 32 B hit) from a thread to TX
    and back, `backend:batch()` in a loop of 20,000 (a probe, not in the
    tree): by `threads.call` 96-104 µs sequentially and 19-21 µs each with
    50 fibers; through the channel 22-28 µs and 1.8-3.0 µs. A set
    (`wal_mode = 'write'`): 152-180 µs and 25-29 µs by `threads.call`,
    56-65 µs and 5.4-8.6 µs through the channel; with `wal_mode = 'none'`
    100-125 µs and 24-25 µs against 31-35 µs and 3.6-4.3 µs. A no-op
    `threads.call` alone is 74-85 µs and 12 µs.
  - The in-suite benchmark (`MCC_BENCH=1 luatest -c
    test/integration/threads_test.lua -p test_bench`: 4 server threads and
    a client process with 4 threads on the same 5 CPUs; TX-only, the
    threads by `threads.call` and through the channels, run one after the
    other; ops/s, two runs):

    | load | TX only | threads, `threads.call` | threads, channels |
    |---|---:|---:|---:|
    | 1 conn, depth 1, get/set 9:1 | 14.4k / 15.7k | 4.1k / 4.2k | 8.3k / 8.4k |
    | 1 conn, depth 1, gets | 16.1k / 15.9k | 4.7k / 4.2k | 9.2k / 8.8k |
    | 4x8 conns, depth 16, get/set 9:1 | 443k / 370k | 282k / 259k | 432k / 368k |
    | 4x8 conns, depth 16, gets | 905k / 739k | 385k / 407k | 672k / 625k |
    | 4x8 conns, depth 64, gets | 1.61M / 1.39M | 867k / 792k | 1.23M / 1.32M |
    | 4x64 conns, depth 1, get/set 9:1 | 47.6k / 50.2k | 47.2k / 43.9k | 99.8k / 71.0k |
    | 4x64 conns, depth 1, gets | 71.5k / 60.2k | 48.9k / 48.5k | 97.4k / 88.2k |

    The channels give 2x on one connection, 1.4-1.75x at depth 16-64 and
    1.6-2.1x with many connections over `threads.call`. Many connections
    at depth 1 now go faster in threads than in TX (1.4-2.1x): the threads
    do the socket work and TX only runs the batches. On this box, where the
    client shares the CPUs, pipelined gets are still faster in TX alone;
    one connection pays about 0.05 ms per batch for the two wakeups.

### vshard routers in threads

- **What a thread has.** An application thread loads the "minimal" Lua
  set: `net.box`, `iproto`, `threads`, `key_def`, `merger`, `checks`,
  `metrics`, tuples and read views. Its `box` has only `NULL`, `error`,
  `tuple`, `iproto`, `read_view`, `index`, `backup`, a few `box.internal`
  helpers and a stub `box.info`. There is no `box.cfg`, `box.space`,
  `box.schema`, `box.session` or `box.watch`.
- **The shim.** When loaded, `vshard.util` asserts that it can read the
  schema version (`box.info.schema_version`, or
  `box.internal.schema_version`). The stub `box.info` has neither. In an
  application thread, `backend/vshard.lua` sets
  `box.internal.schema_version = function() return 0 end` before it
  requires `vshard.router` (`thread_shim()`); only `vshard.storage` uses the
  schema version. The shim affects only that thread's Lua state. A router
  config gets `box_cfg_mode = 'manual'` in the threads, otherwise
  `router_cfg()` calls `box.cfg()`. `vshard.storage` is never loaded in a
  thread: never `require('vshard')` there.
- **Which router.** A thread router mirrors the instance's router in TX:
  - the static router (`vshard.router` unset, or `'_static_router'`)
    becomes the thread's static router (`vshard.router.cfg()`);
  - a named router (a name, or a router object in `vshard.router`)
    becomes `vshard.router.new(<the same name>)` in the thread. A router
    object reaches the threads as its name.
  All the instances with the same router share one router per thread.
- **The config as data.** It is `router.current_cfg` of the TX router:
  vshard's deep copy of the config with the defaults filled in, a new table
  after every reconfiguration. In a config-managed cluster it is what the
  sharding applier passed. There is no public API for it.
- **Hooks.** threads.lua calls optional hooks of the backend module:
  - `thread_cfg(cfg, opts)` in TX: sets `cfg.vshard = {router = <name or
    nil>, timeout}`, `cfg.router_cfg` and `cfg.fe_thread = true`;
  - `thread_prepare(cfg)` in a thread: in `fe.start` before the server
    listens, and in `fe.cfg` after the limits and the backend swap; it calls
    `thread_router(name, cfg)`;
  - `thread_cfg_ref(opts)` in TX: the value the watcher compares
    (`current_cfg`).
- **Making a router.** `thread_router(name, cfg)` in a thread makes the
  router, or reconfigures it when the config differs (deep equality) from
  the one it applied last; one configuration per router at a time. The
  record is `state.vshard_thread_routers[name or '_static_router']`.
- **Propagation.**
  - `start()`: the thread routers exist before the threads listen. An
    error fails the start.
  - Every `inst:cfg()` pushes the options, router config included, to the
    threads. With the role, which calls `cfg` after the sharding applier,
    a config reload reaches the threads during the role's apply.
  - A watcher fiber in TX per instance, `mcc/fe-watch/<name>`, checks every
    `threads.WATCH_INTERVAL` (0.5 s) whether the TX router has a new
    `current_cfg`, and if so pushes the options again. It catches config
    reloads and `vshard.router.cfg()` or `router:cfg()` by hand. Its first
    check always pushes, so a change during `start()` is not lost. It stops
    on `stop()` and `destroy()` and when the backend no longer needs it.
  - The pushes reach a thread over one connection, in order, and the
    per-router lock keeps that order, so every thread ends up with the
    latest config.
  - A router that doesn't exist in TX yet: the threads answer `SERVER_ERROR
    vshard router '<name>' does not exist` (or "vshard router is not
    configured") until the watcher pushes its config.
- **Stats.** A backend in a thread (`fe_thread`) takes `curr_items`, `bytes`
  and `limit_maxbytes` from the storages through the thread's router, and
  `counters`, `curr_connections` and `threads` from TX (`mcc.counters`). If
  the storages can't be reached, the items are zero and the counters are
  still shown. `stats reset` calls `mcc.reset_stats`.
- **Hot reload.** The thread routers live in vshard's module and in the
  state, so they survive a reload. The watcher loop calls
  `current().watch_step()`, so after a reload it runs the new code. The shim
  is a no-op the second time.
- **Cost.** TX router against thread routers on one 5-CPU container shared by
  the cluster and the client (3 router threads, rough numbers): 1.3–1.6× on
  one connection, 1.4–1.9× with 32 connections and deep pipelines, and up to
  2.5× with 256 connections (`MCC_BENCH=1 luatest -c
  test/integration/vshard_threads_test.lua -p test_bench`). Each thread
  router has its own connections to every storage and its own discovery.

## Read views

### Snapshot reads: `readview.lua`

- **Mechanism.** With `read_mode = 'snapshot'`, the instance backend is
  wrapped by `readview.wrap(inner, cfg)`, and the inner backend gets
  `read_mode = 'rw'`.
  - A refresher fiber `mcc/rv/<name>` in TX opens a read view named
    `mcc/<name>` every `snapshot_refresh` seconds.
  - It publishes the read view by id to the thread group of the instance
    (`fe.rv_publish`), then closes its old one. The threads hold their own
    references, so TX's handle stays `close_pending` until they switch.
  - Until the first read view exists, the reads go to the inner backend.
  - After an error the refresher retries in `ERROR_SLEEP` (1 s).
- **What is served from the read view:** the plain gets, that is, op `get`
  without `touch_ttl`, `vivify_ttl` or `recache_ttl`: `get`, `gets`, `mg`
  without `T`/`N`/`R`, and `me`.
  - They have no side effects: no last_access or fetched bump, no lazy
    deletion.
  - An item is dead by the current time and by the flush time stored in
    the read view.
  - An `mg` hit reports a win token that is out (`Z`) from the item in the
    read view.
  - A hit on a stale item (meta `I`) whose win token is not out goes to
    TX: the get may take it, which is a write. TX also knows the tokens it
    keeps in memory when the instance is read-only.
- **Everything else goes to the inner backend:** stores, deletes,
  arithmetic, `touch` and `gat`, `mg` with `T`, `N` or `R`, and `flush_all`.
- **Staleness.**
  - The reads lag by up to `snapshot_refresh` seconds, plus the read view
    throttle.
  - There is no read-your-writes, even on the same connection.
  - The CAS values from `gets` may be stale, so a `cas` based on them
    answers `EXISTS`.
  - A win token taken after the read view was opened is not shown (`Z`)
    by plain gets until the next refresh, nor one TX keeps in memory.
- **flush_all** through a wrapper:
  - The read view of the thread that served the flush is dropped at once,
    so the same connection sees the flush, and TX refreshes at once.
  - TX's response carries `rv_floor`, the id of TX's read view when the
    flush was done. A thread then uses no read view up to that id, so a
    publication that was in flight can't bring the flushed data back.
  - The other threads see the flush at the next publication.
- **Publication order.** Read view ids grow.
  - A thread ignores an id that is not above its current one or the floor,
    also when it arrives while a newer one is being opened.
  - A publication that arrives after the front-end stopped or left the
    snapshot mode is dropped.
- **Restrictions.**
  - Only `backend = 'local'` and `storage = 'memory'`, because read views
    are memtx-only: "read_mode 'snapshot' needs storage 'memory': read views
    are memtx-only".
  - `snapshot_refresh` is at least 0.1. Tarantool throttles read view
    creation to one per 100 ms per process, and all the instances and dumps
    share that budget.
  - Each instance has its own read view.
- **cfg.**
  - Changing `snapshot_refresh` changes the interval of the running
    refresher.
  - Switching back to `'rw'` stops the refresher, and the threads drop
    their read views.
- **API** (TX unless noted):
  - `start(inst)`, `stop(name)` (alias `drop`), `refresh(rec)`,
    `restart_all()`, `validate(cfg)`, `wrap(inner, cfg)`;
  - `current_id(name)`, `invalidate(name, floor?)`, `publish(name, id)`,
    `view(name, space_name)`, `plain_get(req)`, `lookup(view, key, now)`:
    these work in any thread.

### Dumps: `lru_crawler metadump|mgdump`

- **Syntax.** `lru_crawler metadump|mgdump <all|hash|ids>`, where ids are
  1..63, comma-separated.
  - Every item is in class 1, so a list without 1 dumps nothing (`OK`, then
    `END` or `EN`).
  - Other ids answer `BADCLASS invalid class id`.
  - Other subcommands (`crawl`, `tocrawl`, `sleep`, `enable`, `disable`, ...)
    and a wrong number of arguments answer `ERROR`.
- **Batches.** A dump command ends its batch (`STREAM_OPS` in
  `server.lua`), and the server streams its reply with `server.stream()`
  after the replies of the batch.
  - If other commands come before it in the batch, it answers `ERROR
    cannot pipeline other commands before metadump|mgdump`, as memcached
    does.
  - Commands after it in the same packet are answered after the dump.
- **Formats**, as in memcached 1.6.45:
  - metadump: `OK\r\n`, then one line per item, then `END\r\n`. A line is
    `key=<uriencoded> exp=<-1|abs> la=<abs> cas=<n> fetch=<yes|no> cls=1
    size=<#key+#value> flags=<n> \n`, with the trailing space and no
    `\r`;
  - `size` differs from memcached, which also counts the item header;
  - `la` is last_access as stored, written at most once per 60 s;
  - mgdump: `OK\r\n`, then `mg <key>\r\n` per item, then `EN\r\n`. Keys
    with spaces or control characters are written as `mg <base64> b\r\n`.
    memcached decides by how the key was set (`b`); here it depends on the
    key bytes;
  - expired and flushed items are skipped.
- **Local data.** Every dump reads a fresh read view named
  `mcc/dump/<name>`, so it is consistent as of its start.
  - In TX, the dump opens the read view itself.
  - In a thread, TX opens it (`mcc.rv_open`), the thread takes its own
    reference and releases TX's (`mcc.rv_close`). TX closes a read view
    that is not taken within `readview.DUMP_TIMEOUT` (60 s).
  - The iteration and the writes run in the thread of the connection, with
    `DUMP_BATCH` (512) lines per write and per yield. A client that
    doesn't read blocks only its own connection.
  - Storage `'disk'`: `SERVER_ERROR no read view of space '<space>': dumps
    need storage 'memory'`.
- **vshard.** A backend may implement `backend:dump(req, write)`; the vshard
  backend does, in TX and in the threads. It dumps one bucket at a time
  through the router, with the read call of `read_mode`, calling
  `mcc.storage_dump(space_name, bucket_id, sub, after, limit)` on the
  storages:
  - the storage returns the lines of the live items among the next `limit`
    tuples of the bucket in the `bucket_id` index, and `after`, an
    `index:tuple_pos()` position, nil at the end;
  - `DUMP_WORKERS` (8) buckets go at once, `DUMP_PAGE` (512) tuples per
    call. The pages go to the client through a channel as they come, so
    memory is bounded;
  - `map_callrw` is not used: it holds the storage refs only for one call,
    so a paged dump over the replicasets would race bucket moves. A routed
    call per bucket runs under that bucket's ref and follows a moved bucket,
    so each bucket is dumped once, from where it is;
  - such a dump is not a snapshot: each page is as of its call. It takes at
    least `bucket_count` calls;
  - an error before `OK` gives a `SERVER_ERROR` line and the connection
    stays open. An error after `OK` gives a `SERVER_ERROR` line and closes
    the connection, so that a client can't take a partial dump for a
    complete one.
- Any other remote backend without `dump()`: `SERVER_ERROR dumps are not
  supported by this backend`.

## Hot reload

All the mutable state lives in `state.lua`, a table that is stored in
`package.loaded['memcached.state']` and is never dropped. Every other
module keeps only constants and functions in its upvalues. There is one state
per thread (TX and every application thread that runs front-ends).

- **State fields.** `state.lua` DEFAULTS: `instances`, `sweepers`,
  `cas_counter`, `stamp_last`, `tokens`, `tx_exports`, `thread_groups`,
  `thread_servers`, `ipc_channels`, `readviews`, `dump_readviews`,
  `reload_in_progress`, `flush_cache`, `ro_cache`, `datapath_off`,
  `cloop_off` and `dp_gen`.
  `state.ensure(target)` adds the missing ones. Modules add their own on
  first use: `storage` (storage.lua), `metrics` (metrics.lua), `evict`
  (evict.lua), `role` (the role), `fe_watchers` (threads.lua) and `vshard_thread_routers`
  (backend/vshard.lua). Servers (listener, connections, counters) hang off
  `inst.server` and the thread records.
- **Method lookup.**
  - Instances, server objects and backend objects resolve their methods
    through `package.loaded` on every call.
  - Connection fibers call
    `package.loaded['memcached.server'].conn_step` on each
    iteration, and look up the parser, encoder and backend once per batch.
    One in the C connection loop returns from it at its next batch: the
    reload bumps `state.dp_gen` (see "The C connection loop").
  - Sweepers call `expire.step`, refreshers `readview.refresh`, watchers
    `threads.watch_step`, the metrics callback and poller `metrics.collect`
    and `metrics.refresh`, waiters `current().internal.do_start`, the
    fibers of the thread channels `threads.ipc_step` (their receivers in
    the threads stay in C, `ipc.c`).
- **Call.** `mcc.reload() -> {modules, instances, sweepers, threads,
  refreshers}`. A second call during a reload raises "a reload is already
  in progress".
- **Steps in TX:**
  1. run the `ensure()` of the new `state.lua` on the live state, which
     adds the fields the new code knows;
  2. drop every loaded `memcached.*` module except the state, and
     require them again. On any failure the old modules are put back and
     the call raises "memcached: reload failed, the old code is
     kept: <error>". The C modules `memcached.datapath` (the C batch and
     the C connection loop) and `memcached.ipc`
     are required again too, but their `.so` stays loaded: C changes need a
     restart (`ipc.c` keeps its channels across a `luaopen`);
  3. set the instances' metatable to the new one. This step and the ones
     after it run in the new `reload.lua` (`reload.restart(modules)`);
  4. `threads.export_tx()`, which exports only the new names;
  5. `server.restart_accept()` for the TX servers: the same listener, a
     new accept fiber;
  6. restart the sweepers, keeping their counters (a scan starts a new
     cursor; no index is created or dropped), and the evictor (its owners
     and counters are in `state.evict`);
  7. reload the threads that run front-ends (`threads.RELOAD_EVAL`): steps
     1 and 2 with TX's paths, then `fe.after_reload()` restarts their
     accept loops;
  8. restart the refreshers. They keep their read view, which is
     refreshed when it is due.
- **Finding the new code.** Modules are found as `require` finds them,
  search root first, then `package.path`.
- **What survives:** listeners (the same fds), connections, counters, read
  views, the win tokens of read-only instances, the CAS counter, thread
  routers, the thread channels with their requests in flight, the flush
  and read-only caches. The named flush-cache triggers
  are replaced by the new code. Connection fibers run the new code from
  their next batch.
- **Not reloaded:**
  - the state module;
  - the role module `roles.memcached`, which looks
    `memcached`, `.storage` and `.metrics` up in `package.loaded`
    on every call, so it uses the new code;
  - the exported TX functions and storage globals, which are trampolines;
  - dumps in progress and their timers, which finish with the old code.
- If a thread fails to reload after TX has reloaded, TX keeps the new code
  and the call raises.

## Role `roles.memcached`

`roles/memcached.lua`. `roles_cfg['roles.memcached']` maps
memcached instance names to the options of `mcc.create()`, with `expire`
and `vshard` nested. Loading it needs `experimental.config.utils.schema`.

```yaml
roles: [roles.memcached]
roles_cfg:
  roles.memcached:
    cache:
      listen: 0.0.0.0:11211        # required; a string or a number
      space_name: __mcc_cache      # default __mcc_<name>
      storage: memory              # memory | disk
      backend: local               # local | vshard (mcc.BACKEND_OPTIONS)
      threads: fe                  # a group of threads.groups
      read_mode: rw                # rw | ro | bro | snapshot
      snapshot_refresh: 1          # seconds, >= 0.1
      max_item_size: 1048576       # 1 .. 1 GiB
      max_connections: 4096        # >= 1
      idle_timeout: 0              # seconds, >= 0; 0 = never
      expire: {enabled: true, batch: 100, mode: scan}  # mode: index | scan
      evict: {enabled: true, high: 0.95, low: 0.9, samples: 8, batch: 100}
      vshard: {router: <name>, timeout: 1}
      metrics: false
      track_access: true
      commit_wait: complete        # complete | submit | none
```

For an instance with `backend = 'vshard'`, `track_access`, `commit_wait`
and `evict` go to the storage side (`storage.cfg`); the front-end on the
router gets the
defaults.

- **validate.**
  - The schema is built with `experimental.config.utils.schema`. Its errors
    look like `[roles.memcached] cache.max_item_size: Must be in
    range [1, 1073741824], got 0`. `vshard.router` is a name here.
  - Cross-checks, with the same prefix:
    - two front-ends of this Tarantool instance on the same or
      overlapping listen URIs (the same port, and the same host or a
      wildcard one). Port 0 is not checked. With `threads`, both would
      bind with SO_REUSEPORT and share the connections;
    - instances that share a space with different `storage`, with
      backends `vshard` and `local`, with different `evict` or
      `expire.mode` (the defaults filled in), or (vshard) with different
      `expire`, `track_access` or `commit_wait`;
    - `expire.mode: scan` with `storage: disk`;
    - `evict.low` not below `evict.high`, with the defaults filled in;
    - `backend: vshard` on an instance that is neither a vshard router nor
      a storage, unless `vshard.router` names a router;
    - whatever `mcc.create()` would reject, through `mcc.check(name,
      opts)`, for the instances that run a front-end here: the thread group,
      the backend and read mode restrictions, and so on.
- **apply** reconciles:
  - Every option is passed, with the defaults of `mcc.DEFAULTS` filled
    in, so an option removed from the config falls back to its default.
    `vshard` defaults to `{}`, and `threads` to `false`: removing it moves
    the front-ends back to TX through `cfg` (closing their connections).
  - Destroyed first, to free their ports: the instances removed from the
    config, those destroyed outside the role, and those to recreate. An
    instance is recreated when `space_name` or `storage` changes.
  - Listen URIs that move between instances (a swap, a cycle, a chain)
    apply in one reload. A rebind binds the new URI before it closes the
    old one, so each would find its address still taken by the other
    instance. So the role first releases the listeners of the running
    instances whose URI changes and whose current address (the same, or
    overlapping) another instance of the config wants
    (`mcc.internal.release_listener()`: the listener is closed, the
    connections are kept), with threads in every thread. Their `cfg` then
    binds the new URI; if it can't, the old one again (or the instance
    fails), and the error is reported as any other. An instance whose
    `cfg` failed before its rebind listens on its URI again
    (`mcc.internal.restore_listener()`). New clients can't connect to the
    released addresses for the moment between the release and the bind.
  - Then, in name order: `mcc.create()` for the new ones, `inst:cfg()` for
    the others, then `inst:start()` if an instance is stopped or failed.
  - An instance of the same name created outside the role is an error:
    "memcached instance '<name>' already exists and is not managed by
    roles.memcached".
  - An error doesn't stop the other instances. The errors are collected
    and raised at the end, as "memcached instance '<name>': <error>; ...",
    and the health check reports them.
- **vshard.** The role reads `require('config'):get('sharding.roles')`.
  The sharding applier configures vshard before the roles' `post_apply`,
  so the static router exists when the role applies.
  - On a router, an instance with `backend: vshard` runs a front-end of
    the static router, or of the router named by `vshard.router`.
  - On a storage, `storage.cfg({spaces = ...})` gets the spaces of the
    instances with `backend: vshard` (`space_name`, `storage`, `expire`,
    `evict`, `track_access`, `commit_wait`).
    No front-end is started for them, unless the instance is a router too
    or names its router. When no such instance is left, or the instance is
    no longer a storage, the role calls `storage.stop()`.
  - So the routers and the storages can share one `roles_cfg`.
- **stop** destroys every instance the role created and stops the storage
  side. The data spaces are kept.
- **health_check** doesn't yield. It reads only `inst.status`, the role's
  state, `box.space` and `box.info.ro`; `inst:info()` would yield with
  threads.
  - One result per memcached instance, as the subcheck
    `role.roles.memcached.<name>` (`multi_result`, Tarantool 3.9+;
    in `box.info.health.readiness.checks`):
    - `ready`;
    - `not_ready`, with the reason `stopped`, `failed to start`,
      `destroyed`, `waiting for the space '<space>' to be replicated`, or
      the error of the last apply;
    - `degraded` for a running instance whose last config failed to
      apply.
    On a storage, the space of a vshard instance is ready when it exists
    with its `bucket_id` index; otherwise `not_ready` with "waiting for the
    space '<space>' to be replicated to this storage" (read-only) or "the
    space '<space>' is not created on this storage[: <error>]".
  - With `role.internal.multi_result = false` (or a Tarantool older than
    3.9): `true`, or `false, 'memcached instances are not ready: <name>:
    <reason>; ...'`.
- **Hot reload.** The role's state is `state.role` (`instances`, `errors`,
  `storage`, `storage_spaces`, `metrics_storage`), so it survives
  `mcc.reload()`.
- **Metrics.** The instance option `metrics: true`. On a storage, a vshard
  instance with `metrics: true` also exports the metrics of its space
  (`metrics.storage(true)`).
- **Limits:**
  - Recreating an instance keeps its old space, and the data in it.
  - On a pure storage, the options of the vshard instances are checked by
    the schema and by `storage.cfg` only; `mcc.check` runs where there is a
    front-end.

## Metrics: `metrics.lua`

### Enabling

- **Option.** The instance option `metrics = true` exports the instance
  (default `false`; the role passes it through). `mcc.create()` and
  `inst:cfg()` call `metrics.enable()` when it is set. Turning it off, or
  destroying the instance, removes its series at the next collection.
- **Module API** (TX): `enable()` (idempotent; returns false without the
  `metrics` module), `disable()` (unregisters the callback and the
  collectors, stops the poller; the next create or cfg with `metrics =
  true` enables again), `collect()`, `refresh()`, `storage(on)` and
  `info()`.
- **Built-in module.** A callback is registered with
  `require('metrics').register_callback()`. Every exporter runs the
  callbacks before collecting: the Prometheus plugin,
  `metrics.http_middleware` and `roles.metrics-export`. Without the
  `metrics` module the call logs a warning once and does nothing.

### Metrics

The prefix is `tnt_memcached_`. Every metric but `storage_*` has the label
`name`, the memcached instance. Tarantool 3 adds its global labels, such
as `alias`.

| metric | kind | labels | source |
|---|---|---|---|
| `connections` | gauge | thread | current client connections |
| `connections_total` | counter | thread | accepted connections |
| `rejected_connections_total` | counter | thread | over `max_connections` |
| `idle_kicks_total` | counter | thread | connections closed by `idle_timeout` |
| `commands_total` | counter | thread, op | `op`: get, set, touch, flush, delete, incr, decr |
| `meta_commands_total` | counter | thread | meta commands |
| `hits_total`, `misses_total` | counter | thread, op | `op`: get, touch, delete, incr, decr, cas |
| `cas_badval_total` | counter | thread | `cas` with a stale CAS value |
| `stored_items_total` | counter | thread | items stored (`total_items`) |
| `read_bytes_total`, `written_bytes_total` | counter | thread | bytes from and to the clients |
| `errors_total` | counter | thread, class | `class`: protocol (`ERROR`), client (`CLIENT_ERROR`), server (`SERVER_ERROR`) |
| `running` | gauge | | 1 if the status is `running` |
| `items`, `bytes` | gauge | | `space:len()`, `space:bsize()` of the local space |
| `expired_items_total`, `flushed_items_total` | counter | | sweeper deletions |
| `evictions_total` | counter | | evictions from the local space (evict.lua) |
| `storage_items`, `storage_bytes` | gauge | space | a space of this vshard storage |
| `storage_expired_items_total`, `storage_flushed_items_total` | counter | space | its sweeper |
| `storage_evictions_total` | counter | space | its evictions |
| `storage_requests_total`, `storage_batches_total` | counter | space | from the routers |

- **Counting** follows memcached `stats` (see "Server"), so
  `commands_total` does not add up to the number of requests.
- **thread** is present only for an instance with `threads`: the
  application thread id. Such an instance has a series per thread for the
  front-end counters and none for TX.
- **items and bytes** are exported only for a local space. A vshard router
  exports none; the storages export `storage_*` per space when one of the
  role's vshard instances has `metrics: true` (`metrics.storage(true)`).

### Collection

- **The callback doesn't yield.** It reads plain Lua state only:
  - an instance in TX: `inst.server.stats` and `inst.server.nconns`;
  - an instance with threads: the per-thread `srv:info()` last read by the
    poller;
  - `space:len()` and `space:bsize()`, the sweeper records,
    `state.storage`.
  Counters are set with `reset()` + `inc()`, as Tarantool's default
  metrics do. The series of instances that are gone are removed with
  `collector:remove()`.
- **Threads.** The poller fiber `mcc/metrics` calls
  `threads.eval(group, 'info', {name})` (`fe.info`) every
  `REFRESH_INTERVAL` seconds (1 by default) for every instance with
  threads and `metrics = true`. Thread values lag by up to that interval,
  and an instance with threads has no front-end series until the first
  poll. The application threads have their own `metrics` registries, but no
  exporter reads them, so the threads don't report themselves.
- **State** is in `state.metrics` (`enabled`, `callback`, `poller`,
  `threads`, `series`, `storage`). The callback and the poller look the
  module up in `package.loaded`, so a reload takes effect.
- **Cost.** About 40 series per instance (plus about 30 per thread), set on
  every collection.

## Testing

- `test/unit/`: the parsers and encoders, with no box.
- `test/integration/`: real sockets against Tarantool started by luatest:
  - `text_test.lua`, `server_test.lua`, `engine_test.lua`,
    `expire_test.lua`, `expire_mode_test.lua` (both expiration modes,
    parametrized: expiration and flush reclaim, switching an existing
    space, read-only, vinyl, the scan cycle), `persistence_test.lua`,
    `lifecycle_test.lua`. The
    classic cases come from the tarantool/memcached text tests (getset,
    cas, incrdecr, expirations, flags, flush, noreply, multiversioning,
    bogus commands), the meta cases from memcached's own `t/` semantics,
    plus regression tests for every compat fix (`test_compat_*`).
    `engine_test.lua` also runs concurrent wins in a child server with
    memtx MVCC; `persistence_test.lua` has a master/replica pair (win
    tokens on a replica, the upgrade of a format v2 space);
  - `threads_test.lua`, `readview_test.lua` and `reload_test.lua` run in
    child servers with application threads (`test/helpers/threads.lua`,
    `test/helpers/readview.lua`). The reload tests run on a patched copy of
    the code, found through `package.setsearchroot` or `package.path`.
    `threads_test.lua` runs the same scripts against an instance in the
    threads and one in TX and compares the replies and the counters byte
    for byte (the raw batch: mixed pipelines, split reads, holds, the
    `RAW_MAX` cut, the C path switched off, a reload);
  - `vshard_test.lua` (commands, pipelining, multi-key gets, read modes, a
    manual bucket move, the stats of a transfer held by vshard's error
    injections, rebalancing under load, master down, leader switch, option
    validation) on the cluster of `test/helpers/vshard_cluster.lua`
    (rs-1 = {s1-a, s1-b}, rs-2 = {s2-a}, one router) with the test-only
    role `test/helpers/vshard_role.lua`;
  - `vshard_threads_test.lua`: routers in application threads, bucket moves,
    dumps, reload, a replicaset joining under load, a storage master down
    (`test/helpers/vshard_threads.lua`);
  - `role_test.lua`: the role on a single instance with a thread group, on a
    replicaset (rw + ro) and on the vshard cluster (`test/helpers/role.lua`);
  - `metrics_test.lua`: every metric, threads, lifecycle, reload, no yield;
  - `idle_timeout_test.lua`: which connections time out, unix sockets,
    runtime changes, application threads;
  - `commit_wait_test.lua`: the option (instance, `cfg`, role, vshard
    front-end), every mode with `wal_mode = 'write'` on memtx and vinyl with
    the C data path on and off, a pipelined batch of sets committed as a
    group (no yield, the vclock moves after the replies), `'none'` with a
    full WAL queue (`wal_queue_max_size = 1`), read-your-writes with memtx
    MVCC, and a WAL write failure after the reply (a child process whose WAL
    directory becomes read-only; skipped when running as root);
  - `track_access_test.lua`, `flush_cache_test.lua` (the cache invalidation
    by another fiber, direct writes, a rollback, truncate, drop and
    recreate, a reload, and a real master/replica pair) and
    `tune_gc_test.lua`;
  - `datapath_test.lua`: the C data path against the Lua engine (see "The
    C data path"). It is skipped when the data path is not built, as
    `group_test.lua` (child servers: a failed commit of a group, a group
    out of memory);
  - `cloop_test.lua`: the C connection loop against the Lua loop (see "The
    C connection loop"), skipped likewise;
  - `evict_test.lua`: child servers with a 64 MiB `memtx_memory` filled far
    beyond it, with the C data path on and off: the evictor, the on-demand
    path, reclaimed dead items, the approximated LRU, `evict.enabled =
    false` (the memcached `-M` replies) and runtime changes; the options in
    this process. Storage-side eviction is in `vshard_test.lua`, the role
    in `role_test.lua`.
- `tt test` builds the package first, the C data path included, and the
  suite runs with it (the C connection loop included);
  `MEMCACHED_DATAPATH=lua` runs the same suite on the Lua engine only,
  `MEMCACHED_DATAPATH=batch` with the C batch and the Lua loop. The first
  two must pass. The tests run the Lua sources of the
  tree, and take the dev dependencies, the data path and
  `memcached/version.lua` from the `.rocks/` tt built
  (`test/helpers/env.lua`: the sources first in every path).
- luatest sets a child server's search root to its own (the repository);
  `test/helpers/threads.lua` `start(..., {setsearchroot = false})` turns
  that off.
- Opt-in benchmarks in the suites: `MCC_BENCH=1`.
- `test/compat/`: the differential suite against real memcached 1.6.45
  (scenarios, a fuzzer, memcached's own `t/*.t` through a shim); see
  `test/compat/README.md`. Its allowlist `test/compat/allowlist.lua` is the
  authoritative list of divergences from memcached.
- `bench/` compares against the C module and memcached with
  memtier_benchmark; see `bench/README.md`.

## Known gaps

- **Stats:** `stats items` and `stats slabs` return empty lists. `stats
  detail on|off|dump` answers as memcached does with detail off (`OK`,
  `OK`, an empty `END`; a prefix of the word works, anything else is
  `CLIENT_ERROR usage: stats detail on|off|dump`), but `on` records no
  per-prefix stats. `stats settings` has no `client_flags_size`.
- **Eviction** is sampled, not an exact LRU, and can't rebalance slabs: see
  "Eviction" for its limits.
- **Write throughput:** with the default `commit_wait = 'complete'` every
  write request waits for its own WAL commit. `'submit'` groups them, and
  acknowledges writes that a failed WAL write may still roll back (see
  "Commit wait").
- **Value size:** Tarantool's default `memtx_max_tuple_size` is 1 MiB, so
  values within a few hundred bytes of 1 MiB (the key and the other fields
  count too) fail with `SERVER_ERROR` unless the tuple limit is raised.
- **Last access** costs a write on the first get after every store, unless
  `track_access = false`.
- **Win tokens** cost a WAL write per win (`mg` with `N`/`R`, or of a
  stale item). Read-only instances keep theirs in memory, per replica (see
  "Win tokens").
- **Snapshot reads with vshard** are not supported. Storages could serve
  `callro` from their own read views.
- **Read view throttle.** Tarantool's 100 ms throttle is per process, shared
  by every snapshot instance and every dump.
- **Unsupported `lru_crawler` subcommands.** `crawl`, `tocrawl`, `sleep`,
  `enable` and `disable` answer `ERROR`.
- **Thread routers are never dropped.** vshard has no API to destroy a
  router. When the last instance using a router in a thread is destroyed,
  the router keeps its connections and its discovery and failover fibers.
  It no longer gets new configs, and the next instance with that router
  reuses it.
- **vshard internals used:** `router.current_cfg`, what `vshard.util`
  checks at load time (the shim), and `vshard.router.internal.routers` for
  names in TX. A vshard release that needs more of `box` in the router
  fails at the instance start, with the error.
- **Delay.** A router reconfiguration outside `inst:cfg()` reaches the
  threads within `WATCH_INTERVAL`. Until then the old thread routers still
  follow a new master (`master = 'auto'`) and moved buckets by themselves.
- **Not tested in threads:** a topology change that removes a
  replicaset.
- **Metrics:** no latency histograms. `stats reset` and a recreated
  instance reset the counters; Prometheus sees a counter reset.
